package main

import (
	"encoding/base64"
	"fmt"
	"github.com/docopt/docopt-go"
	"github.com/hashicorp/consul/api"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)

//type KVPair struct {
//    Key         string
//    CreateIndex uint64
//    ModifyIndex uint64
//    LockIndex   uint64
//    Flags       uint64
//    Value       []byte
//    Session     string
//}

type ByCreateIndex api.KVPairs

func (a ByCreateIndex) Len() int      { return len(a) }
func (a ByCreateIndex) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

//Sort the KVs by createIndex
func (a ByCreateIndex) Less(i, j int) bool { return a[i].CreateIndex < a[j].CreateIndex }

func printslice(slice []string) {
	fmt.Println("slice = ", slice)
	//for i := range slice {
	//		fmt.Println(i, slice[i])
	//}
}

func NotContains(list []string, elem string) bool {
	for _, t := range list {
		if strings.Contains(elem, t) {
			return false
		}
	}
	return true
}

func backup(ipaddress string, token string, outfile string, exclusion []string) {

	config := api.DefaultConfig()
	config.Address = ipaddress
	config.Token = token

	client, _ := api.NewClient(config)
	kv := client.KV()

	pairs, _, err := kv.List("/", nil)
	if err != nil {
		panic(err)
	}

	sort.Sort(ByCreateIndex(pairs))

	outstring := ""
	if len(exclusion) > 0 {
		for _, element := range pairs {
			if NotContains(exclusion, element.Key) {
				encoded_value := base64.StdEncoding.EncodeToString(element.Value)
				outstring += fmt.Sprintf("%s:%s\n", element.Key, encoded_value)
			}
		}
	} else {
		for _, element := range pairs {
			encoded_value := base64.StdEncoding.EncodeToString(element.Value)
			outstring += fmt.Sprintf("%s:%s\n", element.Key, encoded_value)
		}
	}

	file, err := os.Create(outfile)
	if err != nil {
		panic(err)
	}

	if _, err := file.Write([]byte(outstring)[:]); err != nil {
		panic(err)
	}
}

func backupAcls(ipaddress string, token string, outfile string) {

	config := api.DefaultConfig()
	config.Address = ipaddress
	config.Token = token

	client, _ := api.NewClient(config)
	acl := client.ACL()

	tokens, _, err := acl.List(nil)
	if err != nil {
		panic(err)
	}
	// sort.Sort(ByCreateIndex(tokens))

	outstring := ""
	for _, element := range tokens {
		// outstring += fmt.Sprintf("%s:%s:%s:%s\n", element.ID, element.Name, element.Type, element.Rules)
		outstring += fmt.Sprintf("====\nID: %s\nName: %s\nType: %s\nRules:\n%s\n", element.ID, element.Name, element.Type, element.Rules)
	}

	file, err := os.Create(outfile)
	if err != nil {
		panic(err)
	}

	if _, err := file.Write([]byte(outstring)[:]); err != nil {
		panic(err)
	}
}

/* File needs to be in the following format:
   KEY1:VALUE1
   KEY2:VALUE2
*/
func restore(ipaddress string, token string, infile string) {

	config := api.DefaultConfig()
	config.Address = ipaddress
	config.Token = token

	data, err := ioutil.ReadFile(infile)
	if err != nil {
		panic(err)
	}

	client, _ := api.NewClient(config)
	kv := client.KV()

	for _, element := range strings.Split(string(data), "\n") {
		kvp := strings.Split(element, ":")

		if len(kvp) > 1 {
			decoded_value, decode_err := base64.StdEncoding.DecodeString(kvp[1])
			if decode_err != nil {
				panic(decode_err)
			}

			p := &api.KVPair{Key: kvp[0], Value: decoded_value}
			_, err := kv.Put(p, nil)
			if err != nil {
				panic(err)
			}
		}
	}
}

func main() {

	usage := `Consul KV and ACL Backup with KV Restore tool.

Usage:
  consul-backup [-i IP:PORT] [-t TOKEN] [--aclbackup] [--aclbackupfile ACLBACKUPFILE] [--exclude PATTERN]... [--restore] <filename>
  consul-backup -h | --help
  consul-backup --version

Options:
  -h --help                          Show this screen.
  --version                          Show version.
  -i, --address=IP:PORT              The HTTP endpoint of Consul [default: 127.0.0.1:8500].
  -t, --token=TOKEN                  An ACL Token with proper permissions in Consul [default: ].
  -a, --aclbackup                    Backup ACLs, does nothing in restore mode. ACL restore not available at this time.
  -b, --aclbackupfile=ACLBACKUPFILE  ACL Backup Filename [default: acl.bkp].
  -x, --exclude=[PATTERN]            Repeatable option for patterns to exclude from the backup.
  -r, --restore                      Activate restore mode`

	arguments, _ := docopt.Parse(usage, nil, true, "consul-backup 1.0", false)
	fmt.Println(arguments)

	if arguments["--restore"] == true {
		if len(arguments["--exclude"].([]string)) > 0 {
			fmt.Printf("\n--exclude, -x can be used only for backups\n\n")
			os.Exit(1)
		}
		fmt.Println("Restore mode:")
		fmt.Printf("Warning! This will overwrite existing kv. Press [enter] to continue; CTL-C to exit")
		fmt.Scanln()
		fmt.Println("Restoring KV from file: ", arguments["<filename>"].(string))
		restore(arguments["--address"].(string), arguments["--token"].(string), arguments["<filename>"].(string))
	} else {
		if len(arguments["--exclude"].([]string)) > 0 {
			fmt.Println("excluding keys with pattern(s): ", arguments["--exclude"].([]string))
		}
		fmt.Println("Backup mode:")
		fmt.Println("KV store will be backed up to file: ", arguments["<filename>"].(string))
		backup(arguments["--address"].(string), arguments["--token"].(string), arguments["<filename>"].(string), arguments["--exclude"].([]string))
		if arguments["--aclbackup"] == true {
			fmt.Println("ACL Tokens will be backed up to file: ", arguments["--aclbackupfile"].(string))
			backupAcls(arguments["--address"].(string), arguments["--token"].(string), arguments["--aclbackupfile"].(string))
		}
	}
}

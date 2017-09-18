package main

import (
	"encoding/base64"
	"fmt"
	"github.com/docopt/docopt-go"
	"github.com/hashicorp/consul/api"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
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

func Check_Socket(endpoint string) bool {
	_, err := net.Dial("tcp", endpoint)
	if err != nil {
		message := fmt.Sprintf("There is no socket listening at %s. Giving up: %s", endpoint, err)
		fmt.Println(message)
		os.Exit(1)
	}
	return true
}

func ConsulLookPath() bool {
	_, err := exec.LookPath("consul")
	if err != nil {
		return false
	}
	return true
}

func ConsulInfo(a, b string) string {
	out, err := exec.Command(a, b).Output()
	var consulinfo_output string
	if err != nil {
		message := fmt.Sprintf("There was an error querying consul. Giving up: %s", err)
		fmt.Println(message)
		os.Exit(1)
	}
	consulinfo_output = string(out)
	return consulinfo_output
}

func StartsWith(list []string, elem string) bool {
	for _, t := range list {
		if strings.HasPrefix(elem, t) {
			return true
		}
	}
	return false
}

func backup(ipaddress string, token string, outfile string, exclusion []string, inclusion []string) {

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
			if !StartsWith(exclusion, element.Key) {
				encoded_value := base64.StdEncoding.EncodeToString(element.Value)
				outstring += fmt.Sprintf("%s:%s\n", element.Key, encoded_value)
			}
		}
	} else if len(inclusion) > 0 {
		for _, element := range pairs {
			if StartsWith(inclusion, element.Key) {
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
  consul-backup [-i IP] [--http-port HTTPPORT] [-l] [-t TOKEN] [-a] [-b ACLBACKUPFILE] [-n INPREFIX]... [-x EXPREFIX]... [--restore] <filename>
  consul-backup -h | --help
  consul-backup --version

Options:
  -h --help                          Show this screen.
  --version                          Show version.
  -l, --leader-only                  Create backup only on consul leader.
  --http-port=HTTPPORT               HTTP endpoint port [default: 8500].
  -i, --address=IP                   The HTTP endpoint of Consul [default: 127.0.0.1].
  -t, --token=TOKEN                  An ACL Token with proper permissions in Consul [default: ].
  -a, --aclbackup                    Backup ACLs, does nothing in restore mode. ACL restore not available at this time.
  -b, --aclbackupfile=ACLBACKUPFILE  ACL Backup Filename [default: acl.bkp].
  -x, --exclude-prefix=[EXPREFIX]    Repeatable option for keys starting with prefix to exclude from the backup.
  -n, --include-prefix=[INPREFIX]    Repeatable option for keys starting with prefix to include in the backup.
  -r, --restore                      Activate restore mode`

	arguments, _ := docopt.Parse(usage, nil, true, "consul-backup 1.0", false)

	var (
		httpendpoint = fmt.Sprintf("%s:%s", arguments["--address"], arguments["--http-port"])
	)

	Check_Socket(httpendpoint)

	if arguments["--leader-only"] == true {
		// if consul client is not available we keep running
		if ConsulLookPath() {
			var consulinfo_output = ConsulInfo("consul", "info")
			if strings.Contains(consulinfo_output, "leader = false") {
				fmt.Println("Not a consul leader. Giving up")
				os.Exit(1)
			}
		} else {
			fmt.Println("In order to run this check you need to install consul excecutable")
			fmt.Println("Continuing operations anyway")
		}
	}

	if arguments["--restore"] == true {
		if (len(arguments["--exclude-prefix"].([]string)) > 0) || (len(arguments["--include-prefix"].([]string)) > 0) {
			fmt.Printf("\n--exclude-prefix, -x and --include-prefix, -n can be used only for backups\n\n")
			os.Exit(1)
		}
		fmt.Println("Restore mode:")
		fmt.Printf("Warning! This will overwrite existing kv. Press [enter] to continue; CTL-C to exit")
		fmt.Scanln()
		fmt.Println("Restoring KV from file: ", arguments["<filename>"].(string))
		restore(arguments["--address"].(string), arguments["--token"].(string), arguments["<filename>"].(string))
	} else {
		if (len(arguments["--exclude-prefix"].([]string)) > 0) && (len(arguments["--include-prefix"].([]string)) > 0) {
			fmt.Printf("\n--exclude-prefix and --include-prefix cannot be used together\n\n")
			os.Exit(1)
		}
		if len(arguments["--exclude-prefix"].([]string)) > 0 {
			fmt.Println("excluding keys with prefix(es): ", arguments["--exclude-prefix"].([]string))
		}
		if len(arguments["--include-prefix"].([]string)) > 0 {
			fmt.Println("including only keys with prefix(es): ", arguments["--include-prefix"].([]string))
		}
		fmt.Println("Backup mode:")
		fmt.Println("KV store will be backed up to file: ", arguments["<filename>"].(string))
		backup(httpendpoint, arguments["--token"].(string), arguments["<filename>"].(string), arguments["--exclude-prefix"].([]string), arguments["--include-prefix"].([]string))
		if arguments["--aclbackup"] == true {
			fmt.Println("ACL Tokens will be backed up to file: ", arguments["--aclbackupfile"].(string))
			backupAcls(httpendpoint, arguments["--token"].(string), arguments["--aclbackupfile"].(string))
		}
	}
}

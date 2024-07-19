package routines

import (
	"cs425-mp2/config"
	"cs425-mp2/util"
	"errors"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"time"
)

func (this *GrepService) Register() {
	rpc.Register(this)
}

type GrepService struct {
	ServerId    string
	Port        int
	Hostnames   []string
	clients     []*rpc.Client // stores clients with established connections
	LogFilePath string
}

func NewGrepService() *GrepService {
	_, err := os.ReadFile(config.LogFilePath)
	if err != nil {
		log.Fatal("Error locating log file: ", err)
	}

	this := new(GrepService)
	this.LogFilePath = config.LogFilePath
	this.Hostnames = config.ServerHostnames
	this.Port = config.RpcServerPort
	this.clients = make([]*rpc.Client, len(config.ServerHostnames))
	this.ServerId = config.LogServerId
	return this
}

// Collect logs from all log servers
func (this *GrepService) CollectLogs() string {
	hostnames := this.Hostnames
	clients := this.clients
	logs := make([]string, len(hostnames))

	for idx := range logs {
		logs[idx] = ""
	}

	calls := make([]*rpc.Call, len(hostnames))

	for index, hostname := range hostnames {
		// start connection if it is not previously established
		if clients[index] == nil {
			clients[index] = dial(hostname, this.Port)
		}

		if clients[index] != nil {
			// perform async rpc call
			call := clients[index].Go("GrepService.FetchLog", new(Args), &(logs[index]), nil)
			if call.Error != nil {
				clients[index] = dial(hostname, this.Port) // best effort re-dial
				if clients[index] != nil {
					call = clients[index].Go("GrepService.FetchLog", new(Args), &(logs[index]), nil)
				}
			}
			calls[index] = call
		}
	}

	// iterate and look for completed rpc calls
	for {
		complete := true
		for i, call := range calls {
			if call != nil {
				select {
				case _, ok := <-call.Done: // check if channel has output ready
					if !ok {
						log.Println("Channel closed for async rpc call")
					}
					calls[i] = nil
				default:
					complete = false
				}
			}
		}
		if complete {
			break
		}
	}

	ret := ""
	// calculate total line count
	for _, v := range logs {
		ret += "----------------------\n"
		ret += v
	}

	return ret
}

func dial(hostname string, port int) *rpc.Client {
	var err error
	var c *rpc.Client
	clientChan := make(chan *rpc.Client, 1)
	go func() {
		c, err = rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", hostname, port))
		clientChan <- c
	}()

	select {
	case client := <-clientChan:
		if err == nil {
			return client
		}
	case <-time.After(10 * time.Second):
		return nil
	}
	return nil
}

func (this *GrepService) FetchLog(args *Args, reply *string) error {
	var s []byte
	var err error
	s, err = os.ReadFile(this.LogFilePath)
	if err != nil {
		return err
	}
	*reply += fmt.Sprintf("%s:\n%s", this.ServerId, string(s))
	return nil
}

// execute grep command over local log file
func (this *GrepService) GrepLocal(args *Args, reply *string) error {
	grepOptions, _ := ParseUserInput(args.Input)

	*reply = ""
	fileName := this.LogFilePath

	cmdArgs := append(grepOptions, this.LogFilePath)
	cmd := exec.Command("grep", cmdArgs...)
	output, err := cmd.CombinedOutput()
	// exit code 1 means a match was not found
	if err != nil && cmd.ProcessState.ExitCode() != 1 {
		log.Println("Error while executing grep", err)
		return err
	}
	*reply += fmt.Sprintf("%s:%s", fileName, string(output))

	return nil
}

type Args struct {
	Input string
}

func (this *GrepService) Close() {
	for _, c := range this.clients {
		if c != nil {
			c.Close()
		}
	}
}

// Perform a distributed grep on all connected machines
func GrepAllMachines(ips []string, clients []*rpc.Client, input string) string {
	grepResults := make([]string, len(ips))

	for idx := range grepResults {
		grepResults[idx] = ""
	}

	calls := make([]*rpc.Call, len(ips))
	args := Args{Input: input}
	for index, ip := range ips {
		// start connection if it is not previously established
		if clients[index] == nil {
			c, err := rpc.DialHTTP("tcp", ip+":8000")
			if err == nil {
				clients[index] = c
			}
		}

		if clients[index] != nil {
			// perform async rpc call
			call := clients[index].Go("GrepService.GrepLocal", args, &(grepResults[index]), nil)
			calls[index] = call
		}
	}

	// iterate and look for completed rpc calls
	for {
		complete := true
		for i, call := range calls {
			if call != nil {
				select {
				case _, ok := <-call.Done: // check if channel has output ready
					if !ok {
						log.Println("Channel closed for async rpc call")
					}
					calls[i] = nil
				default:
					complete = false
				}
			}
		}
		if complete {
			break
		}
	}

	// aggregate server results
	var totalLineCount int64 = 0
	ret := ""
	// calculate total line count
	for _, v := range grepResults {
		ret += v
		count, countErr := util.ExtractLineCount(v)
		if countErr != nil {
			log.Fatal("Error extracting number", countErr)
		}
		totalLineCount += int64(count)
	}
	ret += fmt.Sprintf("Total:%d", totalLineCount)

	return ret
}

func ParseUserInput(input string) ([]string, error) { // parse out the options and the pattern
	containsRequiredFlag := false
	ret := make([]string, 0)
	if len(input) < 5 || input[:4] != "grep" {
		return nil, errors.New("Must be a grep command")
	}
	for i := 4; i < len(input)-1; {
		if input[i] == '-' {
			if input[i+1] == 'c' {
				containsRequiredFlag = true
			}
			// ignore some flag that might break stuff
			if input[i+1] != 'H' && input[i+1] != 'f' && input[i+1] != 'q' {
				ret = append(ret, "-"+string(input[i+1]))
			}
			i += 2
		} else if input[i] != ' ' { // a pattern
			ret = append(ret, input[i:])
			break
		} else {
			i++
		}
	}

	if !containsRequiredFlag {
		return nil, errors.New("Grep command must carry -c option.")
	}

	for j := 0; j < len(ret); j++ {
		ret[j] = strings.Trim(ret[j], " \n\r")
	}

	pattern := ret[len(ret)-1]
	if len(pattern) > 1 && pattern[0] == '"' && pattern[len(pattern)-1] == '"' {
		ret[len(ret)-1] = pattern[1 : len(pattern)-1] // strip enclosing quotes
	}

	return ret, nil
}

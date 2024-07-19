package routines

import (
	"cs425-mp2/config"
	"cs425-mp2/util"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"plugin"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Listener struct {
	rpc *net.Listener
}

type TaskRes struct {
	TaskID  int
	Status  bool
	DataMap []map[string]util.KeyValue
}

type MapleJuiceHeader struct {
	HeaderID            int
	Type                string
	Command             string
	HeaderExe           string
	NumMaplesStr        string
	Prefix              string
	SdfsSrcDirectory    string
	TaskFiles           []string
	DestinationFileName string
	DeleteInput         int
}

type HeaderMap struct {
	Node2Header   map[string][]int
	Header2Node   map[int]string
	HeaderStatus  map[int]bool
	HeaderList    map[int]*MapleJuiceHeader           //map of taskID -> task (used for reassigning tasks)
	Header2KeyMap map[int]*[]map[string]util.KeyValue // taskID -> pointer of output Key-Value pair map
	sync.RWMutex
}

var (
	memList     = LocalMembershipList
	concurrency = util.NewConcurrencyControl()
)

// leader has a VM to inputLines mapping

func HeaderMapBuilder() *HeaderMap {
	hm := HeaderMap{
		Node2Header:   make(map[string][]int),
		Header2Node:   make(map[int]string),
		HeaderStatus:  make(map[int]bool),
		HeaderList:    make(map[int]*MapleJuiceHeader, 0),
		Header2KeyMap: make(map[int]*[]map[string]util.KeyValue),
	}
	return &hm
}

func (hm *HeaderMap) printData() {
	hm.RLock()
	defer hm.RUnlock()
	colNames := make([]string, 0)
	for _, pair := range *hm.Header2KeyMap[0] {
		for _, row := range pair {
			colNames = append(colNames, row.Value[0])
		}
	}

	for headerID, data := range hm.Header2KeyMap {
		fmt.Println("Task result for Task: ", headerID)
		fmt.Println(strings.Join(colNames, ", "))
		vals := make([]string, 0)
		for _, pair := range *data {
			for _, row := range pair {
				vals = append(vals, row.Value[1])
				fmt.Println(strings.Join(vals, ", "))
			}
		}
	}
}

func (hm *HeaderMap) reset() {
	hm.Lock()
	defer hm.Unlock()
	hm.Node2Header = make(map[string][]int)
	hm.Header2Node = make(map[int]string)
	hm.HeaderStatus = make(map[int]bool)
	hm.HeaderList = make(map[int]*MapleJuiceHeader, 0)
}

func (hm *HeaderMap) addHeader() {
	hm.Lock()
	defer hm.Unlock()
	hm.Node2Header = make(map[string][]int)
	hm.Header2Node = make(map[int]string)
	hm.HeaderStatus = make(map[int]bool)
	hm.HeaderList = make(map[int]*MapleJuiceHeader, 0)
}

func (hm *HeaderMap) deleteTask(header MapleJuiceHeader) {
	hm.Lock()
	defer hm.Unlock()
	nodeIP, hasTask := hm.Header2Node[header.HeaderID]
	if hasTask {
		hm.Node2Header[nodeIP] = util.RemoveStringFromSlice(hm.Node2Header[nodeIP], header.HeaderID)
		delete(hm.Header2Node, header.HeaderID)
		delete(hm.HeaderStatus, header.HeaderID)
		delete(hm.HeaderList, header.HeaderID)
		delete(hm.Header2KeyMap, header.HeaderID)
	}
}

// delete node and all tasks in this node
func (hm *HeaderMap) deleteNode(nodeIP string) {
	hm.Lock()
	defer hm.Unlock()
	tasks, hasNode := hm.Node2Header[nodeIP]
	if hasNode {
		delete(hm.Node2Header, nodeIP)
		for _, headerID := range tasks {
			delete(hm.Header2Node, headerID)
			delete(hm.HeaderStatus, headerID)
			delete(hm.HeaderList, headerID)
			delete(hm.Header2KeyMap, headerID)
		}
	}
}
func (hm *HeaderMap) addResult(headerID int, taskRes *[]map[string]util.KeyValue) {
	hm.Lock()
	defer hm.Unlock()
	hm.Header2KeyMap[headerID] = taskRes
}

func SendTask(header MapleJuiceHeader, workerIP string) {
	//taskMonitor.addTask(task, workerIP)
	err := SendHeaderRPC(header, workerIP)
	util.HandleError(err, "run task on node "+workerIP+"Failed!")
	if err == nil {
		//mark task as complete
		//taskMonitor.TaskStatus[task.TaskID] = true

		//nodeIP, hasTask := taskMonitor.Task2Worker[task.TaskID]
		//if hasTask {
		//	taskMonitor.Worker2Task[nodeIP] = removeStringFromSlice(taskMonitor.Worker2Task[nodeIP], task.TaskID)
		//	delete(taskMonitor.Task2Worker, task.TaskID)
		//}
		//
		//fmt.Println(taskMonitor.TaskStatus)
		//fmt.Println(taskMonitor.Task2Worker)
		//
		////check if all tasks are complete
		//allTrue := true
		//for _, complete := range taskMonitor.TaskStatus {
		//	if !complete {
		//		allTrue = false
		//	}
		//}
		//if allTrue {
		//	//if all completed, start merging
		//	taskMonitor.reset()
		//	completeSignal <- "ok"
		//}
	}
}

func (Rpc *Listener) PerformTask(header MapleJuiceHeader, res *TaskRes) error {
	res.TaskID = header.HeaderID
	concurrency.StartWrite()
	headerType := header.Type
	res.Status = true
	if headerType == "maple" {
		err := MapleTask(header, res)
		if err != nil {
			util.HandleError(err, "Maple task")
			return err
		}
	} else if headerType == "juice" {
		err := JuiceTask(header, res)
		if err != nil {
			util.HandleError(err, "Juice task")
			return err
		}
	}
	concurrency.EndWrite()
	return nil
}

func StartMapleJuiceReceiver(port string) {
	fileListener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal("Failed to start MapleJuice server", err)
	}
	l := Listener{rpc: &fileListener}
	if err := rpc.RegisterName("Rpc", &l); err != nil {
		util.HandleError(err, "RPC failed to register")
		return
	}

	remoteListener, err := net.Listen("tcp", util.RPC_PORT)
	if err != nil {
		log.Fatal("Listen TCP error:", err)
	}

	for {
		conn, err := remoteListener.Accept()
		if err != nil {
			util.HandleError(err, "Error when listening to RPC connection")
		}
		go rpc.ServeConn(conn)
	}
}

func MapleTask(header MapleJuiceHeader, res *TaskRes) error {
	// Get files
	for _, file := range header.TaskFiles {
		go ProcessDfsCmd("get", []string{file, file})
	}
	time.Sleep(time.Second * 3)
	log.Println("enter mapleProcess")

	p, err := plugin.Open(config.Homedir + "/exe/" + header.HeaderExe + ".so")
	if err != nil {
		res.Status = false
	}
	util.HandleError(err, "Open MapleExe")
	symFilterMaple, err := p.Lookup("LoadAllMapleInputs")
	if err != nil {
		res.Status = false
		log.Fatal("Error looking up FilterMaple:", err)
	}
	loadAllMapleInputs, ok := symFilterMaple.(func([]string, string) ([]map[string]util.KeyValue, error))
	if !ok {
		util.HandleError(err, "Invalid function signature for FilterMaple")
	}

	// Use the function
	keyValuePairs, err := loadAllMapleInputs(header.TaskFiles, header.SdfsSrcDirectory)
	if err != nil {
		res.Status = false
	}
	res.DataMap = keyValuePairs
	util.HandleError(err, "Load key-value pairs for MapleTask")
	err = WriteJuiceInputFiles(keyValuePairs, header.Prefix, header.SdfsSrcDirectory)
	if err != nil {
		res.Status = false
	}
	//mapleJuiceManager.Header2KeyMap[header.HeaderID] = &fileToMapleOutput
	//mapleJuiceManager.HeaderStatus[header.HeaderID] = true
	return err
}

func JuiceTask(header MapleJuiceHeader, res *TaskRes) error {
	cmd := header.Command
	cmdType, cmdContent := processQuery(cmd)
	log.Println("Getting Juice Input Files...")
	for _, file := range header.TaskFiles {
		go ProcessDfsCmd("get", []string{file, file})
	}
	time.Sleep(time.Second * 3)
	log.Println("started juice", len(header.TaskFiles))

	p, err := plugin.Open(config.Homedir + "/exe/" + header.HeaderExe + ".so")
	if err != nil {
		res.Status = false
	}
	util.HandleError(err, "Open JuiceExe")
	symFilterJuice, err := p.Lookup("FilterJuice")
	if err != nil {
		res.Status = false
		log.Fatal("Error looking up FilterMaple:", err)
	}
	filterJuice, ok := symFilterJuice.(func(string, string) ([]map[string]util.KeyValue, error))
	if !ok {
		util.HandleError(err, "Invalid function signature for FilterMaple")
		res.Status = false
	}

	// Use the function
	keyValuePairs, err := filterJuice(header.SdfsSrcDirectory, header.Prefix)
	if err != nil {
		res.Status = false
	}
	util.HandleError(err, "Load key-value pairs for MapleTask")
	juiceResult := make([]map[string]util.KeyValue, 0)
	if cmdType == 1 {
		count := len(keyValuePairs)
		pair := make(map[string]util.KeyValue)
		pair[strconv.Itoa(header.HeaderID)] = util.KeyValue{"count", []string{strconv.Itoa(count)}}
		juiceResult = append(juiceResult, pair)
	} else if cmdType == 2 {
		column := cmdContent[0]
		operand := cmdContent[1]
		value := cmdContent[2]
		switch operand {
		case ",":
			regexMatch(keyValuePairs, juiceResult, cmd, res)
		case ".*":
			regexMatch(keyValuePairs, juiceResult, cmd, res)
		case "|":
			regexMatch(keyValuePairs, juiceResult, cmd, res)
		case "=":
			for _, row := range keyValuePairs {
				for _, colVal := range row {
					col := colVal.Value[0]
					val := colVal.Value[1]
					if col == column && val == value {
						juiceResult = append(juiceResult, row)
					}
				}
			}
		case "<":
			for _, row := range keyValuePairs {
				for _, colVal := range row {
					col := colVal.Value[0]
					val, err := strconv.Atoi(colVal.Value[1])
					if err != nil {
						util.HandleError(err, "Convert column value to string")
						res.Status = false
					}
					toCompare, err := strconv.Atoi(value)
					if err != nil {
						util.HandleError(err, "Convert toCompare to string")
						res.Status = false
					}
					if col == column && val < toCompare {
						juiceResult = append(juiceResult, row)
					}
				}
			}
		case ">":
			for _, row := range keyValuePairs {
				for _, colVal := range row {
					col := colVal.Value[0]
					val, err := strconv.Atoi(colVal.Value[1])
					if err != nil {
						util.HandleError(err, "Convert column value to string")
						res.Status = false
					}
					toCompare, err := strconv.Atoi(value)
					if err != nil {
						util.HandleError(err, "Convert toCompare to string")
						res.Status = false
					}
					if col == column && val > toCompare {
						juiceResult = append(juiceResult, row)
					}
				}
			}
		}
	} else {
		res.Status = false
		return fmt.Errorf("unknown cmd, juice task ends")
	}
	return err
}

func regexMatch(keyValuePairs []map[string]util.KeyValue, juiceResult []map[string]util.KeyValue, cmd string, res *TaskRes) {
	for _, row := range keyValuePairs {
		for _, value := range row {
			valueString := strings.Join(value.Value, ",")
			// TODO: grep
			matches, err := grepString(cmd, valueString)
			if err != nil {
				res.Status = false
			}
			if len(matches) != 0 {
				juiceResult = append(juiceResult, row)
			}
		}
	}
}

func grepString(pattern, input string) ([]string, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	matches := re.FindAllString(input, -1)
	return matches, nil
}

func WriteJuiceInputFiles(allMaps []map[string]util.KeyValue, juiceInputPrefix string, dir string) error {
	for _, m := range allMaps {
		//fmt.Println("Row: ", m)
		for key, values := range m {
			fileName := fmt.Sprintf("%s_%s", juiceInputPrefix, key)
			if err := writeToFile(fileName, values, dir); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeToFile(fileName string, value util.KeyValue, dir string) error {
	file, err := os.Create(config.Homedir + dir + "/" + fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("%s: %v\n", value.Key, value.Value))
	if err != nil {
		return err

	}

	return nil
}

func HandleMaple(args []string) {
	log.Println("received Maple job")
	cmd := args[0]
	mapleExe := args[1]
	numMaplesStr := args[2]
	sdfsIntermediateFilenamePrefix := args[3]
	sdfsSrcDirectory := args[4]
	numMaples, err := strconv.Atoi(numMaplesStr)
	if err != nil {
		log.Println(err)
	}
	p, err := plugin.Open("/home/haozhew3/cs425/mp4/exe/FilterMaple.so")
	util.HandleError(err, "Open plugin")
	symFilterMaple, err := p.Lookup("FilterMaple")
	if err != nil {
		log.Fatal("Error looking up FilterMaple:", err)
	}
	filterMaple, ok := symFilterMaple.(func(string, int, string, string) ([]string, error))
	if !ok {
		util.HandleError(err, "Invalid function signature for FilterMaple")
	}

	// Use the function
	mapleInputs, err := filterMaple(config.Homedir+"/local/data.csv", numMaples, sdfsSrcDirectory, sdfsIntermediateFilenamePrefix)
	if err != nil {
		util.HandleError(err, "FilterMaple")
	}

	fileMap := PartitionTasks(mapleInputs)
	// TODO
	ips := LocalMembershipList.AliveMembers()
	numSent := 0
	for _, ip := range ips {
		header := MapleJuiceHeader{
			HeaderID:  numSent,
			Type:      "maple",
			Command:   cmd,
			HeaderExe: mapleExe,
			Prefix:    sdfsIntermediateFilenamePrefix,
			TaskFiles: fileMap[ip],
		}
		go SendTask(header, ip)
		numSent++
	}
	log.Println("completed Maple task")
}

func HandleJuice(args []string) {
	cmd := args[0]
	juiceExe := args[1]
	//numJuicesStr := args[2]
	sdfsIntermediateFilenamePrefix := args[3]
	sdfsDestFilename := args[4]
	deleteInput, err := strconv.Atoi(args[5])
	if err != nil {
		log.Println(err)
	}

	p, err := plugin.Open(config.Homedir + "/exe/" + juiceExe + ".so")
	symFilterJuice, err := p.Lookup("GetAllFileListsByPrefix")
	if err != nil {
		log.Fatal("Error looking up FilterJuice:", err)
	}
	filterJuice, ok := symFilterJuice.(func(string, string) ([]string, error))
	if !ok {
		util.HandleError(err, "Invalid function signature for FilterJuice")
	}

	// Use the function
	juiceInputs, err := filterJuice(sdfsIntermediateFilenamePrefix, "local")
	if err != nil {
		util.HandleError(err, "FilterMaple")
	}
	fileMap := PartitionTasks(juiceInputs)
	ips := LocalMembershipList.AliveMembers()

	numSent := 0
	for _, ip := range ips {
		header := MapleJuiceHeader{
			HeaderID:            numSent,
			Type:                "maple",
			Command:             cmd,
			HeaderExe:           juiceExe,
			Prefix:              sdfsIntermediateFilenamePrefix,
			TaskFiles:           fileMap[ip],
			DestinationFileName: sdfsDestFilename,
			DeleteInput:         deleteInput,
		}
		go SendTask(header, ip)
		numSent++
	}
}

func PartitionTasks(files []string) map[string][]string {
	nodes := LocalMembershipList.AliveMembers()
	fileMap := make(map[string][]string)
	nodeCount := len(nodes)

	for _, node := range nodes {
		fileMap[node] = []string{}
	}

	for i, file := range files {
		node := nodes[i%nodeCount]
		fileMap[node] = append(fileMap[node], file)
	}

	return fileMap
}

func processQuery(cmd string) (int, []string) {
	if cmd == "count" {
		return 1, []string{}
	}
	regexMatch, err := splitRegex(cmd)
	if err != nil {
		return 2, regexMatch
	}
	return -1, []string{}
}

func splitRegex(input string) ([]string, error) {
	result := make([]string, 0)
	pattern := `^([^.,|*<>=]+)([.,|*<>=]+)([^.,|*<>=]+)$`
	re, err := regexp.Compile(pattern)
	if err != nil {
		return result, err
	}

	matches := re.FindStringSubmatch(input)
	if len(matches) < 4 {
		return result, fmt.Errorf("not regex")
	}
	return matches, nil
}

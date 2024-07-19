package routines

import (
	"cs425-mp2/config"
	"cs425-mp2/util"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var clientToken uint64 = 0
var tokenLock sync.Mutex

var (
	ClientProgressTracker *ProgressManager = NewProgressManager()
	mapleJuiceManager                      = HeaderMapBuilder()
)

const (
	FILE_PUT    int = 1
	FILE_GET    int = 2
	FILE_DELETE int = 3
	FILE_LIST   int = 4

	FILE_METADATA_SERVICE_QUERY_TIMEOUT_SECONDS int = 10
)

type DfsRequest struct {
	RequestType int
	FileName    string
}

// return type for DFS client metadata query
type DfsResponse struct {
	FileName string
	Master   util.FileInfo
	Servants []util.FileInfo
}

// parse and dispatch cmd line
func ProcessDfsCmd(cmd string, args []string) {
	switch cmd {
	case "put":
		PutFile(args)
	case "get":
		GetFile(args)
	case "delete":
		DeleteFile(args)
	case "ls":
		ListFile(args)
	case "multiread":
		Multiread(args)

	case "store":
		// todo: handle store
		OutputStore(args)
	default:
		log.Printf("Unsupported DFS command: (%s)", cmd)
	}
}

func GetFile(args []string) error {
	if len(args) != 2 {
		log.Printf("Invalid parameteres for DFS GET command")
		return errors.New("Invalid parameteres for DFS GET command")
	}

	remoteFileName := args[0]
	localFileName := args[1]

	if len(localFileName) == 0 || len(remoteFileName) == 0 {
		log.Printf("Invalid parameteres for DFS GET command")
		return errors.New("Invalid parameteres for DFS GET command")
	}

	fileMetadata := &DfsResponse{}
	err := queryMetadataService(FILE_GET, remoteFileName, fileMetadata)

	if err != nil {
		log.Printf("Encountered error while quering file metadata service: %s", err.Error())
		return errors.New("Encountered error while quering file metadata service")
	}

	master := fileMetadata.Master
	if master.FileStatus != util.COMPLETE {
		log.Printf("File master is not ready: file upload in progress")
		return errors.New("File master is not ready: file upload in progress")
	}

	fileMasterIP := NodeIdToIP(master.NodeId)
	port := config.RpcServerPort

	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", fileMasterIP, port))
	if err != nil {
		log.Println("Encountered error when connecting to file master: ", err)
	}

	tokenLock.Lock()
	clientToken += 1
	t := clientToken
	tokenLock.Unlock()

	getArgs := &RWArgs{
		Token:         t,
		LocalFilename: localFileName,
		SdfsFilename:  remoteFileName,
		ClientAddr:    NodeIdToIP(SelfNodeId),
	}

	var reply string
	responseErr := client.Call("FileService.ReadFile", getArgs, &reply)

	if responseErr != nil {
		fmt.Printf("File Master responsed with error: %s", responseErr.Error())
		return nil
	}

	timeout := time.After(80 * time.Second)

	for {
		time.Sleep(1 * time.Second)
		select {
		case <-timeout:
			log.Println("GET timeout")
			return nil
		default:
			if ClientProgressTracker.IsMasterCompleted(localFileName, t) {
				log.Print("Done\n\n")
				return nil
			}
		}
	}

}

func PutFile(args []string) {
	if len(args) != 2 {
		log.Printf("Invalid parameteres for DFS GET command")
		return
	}

	localFileName := args[0]
	remoteFileName := args[1]

	if len(localFileName) == 0 || len(remoteFileName) == 0 {
		log.Printf("Invalid parameteres for DFS GET command")
		return
	}

	_, err0 := os.Stat(config.Homedir + "/local/" + localFileName)
	if err0 != nil {
		log.Print("Local file does not exist")
		return
	}

	fileMetadata := &DfsResponse{}
	err := queryMetadataService(FILE_PUT, remoteFileName, fileMetadata)

	if err != nil {
		log.Printf("Encountered error while quering file metadata service: %s", err.Error())
		return
	}

	master := fileMetadata.Master

	fileMasterIP := NodeIdToIP(master.NodeId)
	port := config.RpcServerPort

	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", fileMasterIP, port))
	if err != nil {
		log.Println("Encountered error when connecting to file master: ", err)
	}

	putArgs := &RWArgs{
		LocalFilename: localFileName,
		SdfsFilename:  remoteFileName,
		ClientAddr:    NodeIdToIP(SelfNodeId),
	}
	var token uint64
	responseErr := client.Call("FileService.WriteFile", putArgs, &token)

	log.Printf("Received permission token %d", token)

	if responseErr != nil {
		fmt.Printf("File Master responsed with error: %s", responseErr.Error())
		return
	}

	// We are given a token by the scheduler, proceed with uploading files
	err1 := SendFile(config.Homedir+"/local/"+localFileName, remoteFileName, fileMasterIP+":"+strconv.Itoa(config.FileServerReceivePort), token)

	if err1 != nil {
		log.Print("Send file failed with error", err1)
	}

	reply := ""
	key := remoteFileName + "-" + strconv.Itoa(int(token))
	err2 := client.Call("FileService.CheckWriteCompleted", &key, &reply)

	if err2 != nil || reply != "ACK" {
		log.Println("Encountered error while checking write completion", err2)
		return
	}

	fmt.Printf("\n\nSDFS PUT operation completed with data stored at\n")
	fmt.Printf("File Master: %s\n", fileMetadata.Master.NodeId)

	for _, servant := range fileMetadata.Servants {
		fmt.Printf("File Servant: %s\n", servant.NodeId)
	}
	fmt.Print("\n\n\n")

}

func DeleteFile(args []string) {
	if len(args) != 1 {
		log.Printf("Invalid parameteres for DFS PUT command")
		return
	}

	remoteFileName := args[0]

	if len(remoteFileName) == 0 {
		log.Printf("Invalid parameteres for DFS DELETE command")
		return
	}

	fileMetadata := &DfsResponse{}
	err := queryMetadataService(FILE_DELETE, remoteFileName, fileMetadata)

	if err != nil {
		log.Printf("Encountered error while query file metadata service: %s", err.Error())
		return
	}

	master := fileMetadata.Master
	fileMasterIP := NodeIdToIP(master.NodeId)
	port := config.RpcServerPort

	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", fileMasterIP, port))
	if err != nil {
		log.Println("Encountered error when connecting to file master: ", err)
	}
	deletArgs := &DeleteArgs{
		Filename: remoteFileName,
	}
	var reply string
	responseErr := client.Call("FileService.DeleteFile", deletArgs, &reply)

	if responseErr != nil {
		fmt.Printf("File Master responsed with error: %s", responseErr.Error())
	} else {
		log.Print("Done\n\n")
	}
}

func ListFile(args []string) {
	if len(args) != 1 {
		log.Printf("Invalid parameteres for DFS PUT command")
		return
	}

	remoteFileName := args[0]

	if len(remoteFileName) == 0 {
		log.Printf("Invalid parameteres for DFS LIST command")
		return
	}

	fileMetadata := &DfsResponse{}
	err := queryMetadataService(FILE_LIST, remoteFileName, fileMetadata)
	if err != nil {
		log.Printf("Encountered error while query file metadata service: %s", err.Error())
	} else {
		fmt.Print(fileMetadata.toString())
	}
}

func Multiread(args []string) {
	if len(args) < 2 {
		log.Printf("Invalid parameteres for DFS multiread command")
	}

	remoteFileName := args[0]
	machineIds := make([]int, 0)

	for i := 1; i < len(args); i++ {
		id, err := strconv.Atoi(args[i])
		if err != nil || id < 1 || id > len(config.ServerHostnames) {
			log.Printf("Invalid machine Id")
		}
		machineIds = append(machineIds, id-1) // switch from 1-index to 0-index
	}

	for _, machineId := range machineIds {
		hostName := config.ServerHostnames[machineId]
		fmt.Printf("Instructing read for host %s\n", hostName)
		fmt.Printf("Hostname is %s", hostName)
		go func() {
			client := dial(hostName, config.RpcServerPort)

			if client == nil {
				log.Printf("Unable to connect to %s:%d", hostName, config.RpcServerPort)
				return
			}

			reply := ""
			call := client.Go("DfsRemoteReader.Read", &remoteFileName, &reply, nil)
			timeout := time.After(300 * time.Second)

			select {
			case <-timeout:
				log.Printf("DFS file GET times out at %s", hostName)
				return
			case _, ok := <-call.Done:
				if !ok {
					log.Println("Channel closed for async rpc call")
				} else {
					if call.Error == nil {
						log.Printf("DFS GET completed at %s", hostName)
					} else {
						log.Printf("DFS GET failed at %s. Error: %s", hostName, call.Error.Error())
					}
				}
			}
		}()
	}
}

func OutputStore(args []string) {
	sdfsFolder := config.Homedir + "/sdfs"
	files, err := ioutil.ReadDir(sdfsFolder)
	if err != nil {
		fmt.Println("Error reading sdfs folder :", err)
		return
	}
	for _, file := range files {
		fmt.Println(file.Name())
	}
}

func queryMetadataService(requestType int, fileName string, reply *DfsResponse) error {
	client := dialMetadataService()
	if client == nil {
		return errors.New("Failed to query file metadata service")
	}

	request := &DfsRequest{
		FileName:    fileName,
		RequestType: requestType,
	}

	call := client.Go("FileMetadataService.HandleDfsClientRequest", request, reply, nil)
	requestTimeout := time.After(time.Duration(FILE_METADATA_SERVICE_QUERY_TIMEOUT_SECONDS) * time.Second)

	select {
	case _, ok := <-call.Done: // check if channel has output ready
		if !ok || reply == nil {
			log.Println("Listener call corrupted")
			return errors.New("Listener call corrupted")
		}
	case <-requestTimeout:
		return errors.New("Request timeout")
	}
	return nil
}

func dialMetadataService() *rpc.Client {
	leaderId := LeaderId

	if len(leaderId) == 0 {
		log.Println("Leader election in progress, DFS service not available")
		return nil
	}

	leaderIp := NodeIdToIP(leaderId)
	client := dial(leaderIp, config.RpcServerPort)
	if client == nil {
		log.Printf("Failed to establish connection with DFS metadata service at %s:%d", leaderId, config.RpcServerPort)
	}
	return client
}

func (this *DfsResponse) toString() string {
	ret := "---------------------\nFile name: " + this.FileName + "\n"

	ret += this.Master.ToString()

	for _, servant := range this.Servants {
		ret += servant.ToString()
	}

	return ret
}

// Listener logics
func SendHeaderRPC(header MapleJuiceHeader, nodeIP string) error {
	var res TaskRes

	client, _ := connectRPC(nodeIP)

	if err := client.Call("Rpc.PerformTask", header, &res); err != nil {
		return err
	}

	mapleJuiceManager.addResult(header.HeaderID, &(res.DataMap))
	mapleJuiceManager.printData()
	return nil

}

func connectRPC(nodeIP string) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", nodeIP+util.RPC_PORT)
	if err != nil {
		util.HandleError(err, "Dialing rpc to"+nodeIP)
		return nil, err
	}
	fmt.Println("Listener connection established with: " + nodeIP)

	return client, nil
}

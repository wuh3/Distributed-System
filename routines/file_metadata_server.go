package routines

import (
	"cs425-mp2/config"
	"cs425-mp2/util"
	"errors"
	// "fmt"
	"log"
	"net/rpc"
	"sync"
	"time"
)

const (
	REPORT_COLLECTION_TIMEOUT_SECONDS = 2
	RECONCILIATION_PERIOD_MILLIS      = 2000 // period for a cycle of collect, repair and inform file metadata
)

var FileMetadataServerSigTerm chan int = make(chan int)

type FileMetadataService struct {
	metadataLock sync.RWMutex
	// fileName -> replica distribution info
	metadata util.NodeToFiles
}

func (this *FileMetadataService)ToString() string {
	this.metadataLock.RLock()
	val := this.metadata
	this.metadataLock.RUnlock()
	ret := ""

	for _, fmap := range val {
		for _, fileInfo := range fmap {
			ret += fileInfo.ToString()
		}
		
	}
	ret += "---------------"

	return ret
}

func NewFileMetadataService() *FileMetadataService {
	server := FileMetadataService{
		metadata: make(map[string]map[string]*util.FileInfo),
	}
	return &server
}

// file index & file master/servant info server, hosted by leader
func (this *FileMetadataService) Register() {
	rpc.Register(this)

	// todo: low-priority but needs graceful termination
	go func() {
		for {
			timer := time.After(time.Duration(RECONCILIATION_PERIOD_MILLIS) * time.Millisecond)
			select {
			case <-timer:
				if LeaderId == SelfNodeId {
					this.adjustCluster(collectMetadata())
				}
			}
		}
	}()

}

// --------------------------------------------
// Functions for handling DFS client requests
// --------------------------------------------

// query replica distribution about a file, for DFS client
func (this *FileMetadataService) HandleDfsClientRequest(request *DfsRequest, reply *DfsResponse) error {
	requestType := request.RequestType
	fileName := request.FileName

	// Might happen when a re-election happens right after client sending the request
	if LeaderId != SelfNodeId {
		return errors.New("Metadata service unavailable. This host is not a leader.")
	}

	if len(fileName) == 0 {
		return errors.New("File name is empty")
	}

	switch requestType {
	case FILE_GET:
		return this.handleGetRequest(fileName, reply)
	case FILE_PUT:
		return this.handlePutRequest(fileName, reply)
	case FILE_DELETE:
		return this.handleDeleteRequest(fileName, reply) // no data replied, no error means success
	case FILE_LIST:
		return this.handleListRequest(fileName, reply)
	}

	return errors.New("Unsupported request type")
}


func ingestReply(reply *DfsResponse, clusterInfo *util.ClusterInfo){
	responseVal := toResponse(clusterInfo)
	reply.FileName = responseVal.FileName
	reply.Master = responseVal.Master
	reply.Servants = responseVal.Servants
}

// clients tries to fetch distribution info of a file
func (this *FileMetadataService) handleGetRequest(fileName string, reply *DfsResponse) error {
	this.metadataLock.RLock()
	fileToClusterInfo := util.Convert2(&this.metadata)
	this.metadataLock.RUnlock()

	clusterInfo, exists := (*fileToClusterInfo)[fileName]

	if !exists {
		return errors.New("File " + fileName + "does not exist")
	}
	
	ingestReply(reply, clusterInfo)

	return nil
}

// clients tries to write a file
func (this *FileMetadataService) handlePutRequest(fileName string, reply *DfsResponse) error {
	this.metadataLock.Lock()
	defer this.metadataLock.Unlock()

	log.Println("handling request")

	fileToClusterInfo := util.Convert2(&this.metadata)

	targetCluster, exists := (*fileToClusterInfo)[fileName]

	if !exists {
		// new file, allocate a new cluster
		targetCluster = util.NewClusterInfo(fileName)

		targetCluster.RecruitFullCluster(&this.metadata, config.ReplicationFactor)

		// write back to metdata and notify invovlved nodes
		(*fileToClusterInfo)[fileName] = targetCluster
		converted := util.Convert(fileToClusterInfo)
		this.metadata = *converted
		var err error
		for _, node := range *targetCluster.Flatten() {
			err = informMetadata(node.NodeId, &this.metadata)
		}
		if err != nil {
			return err
		}
	}

	// return distribution info if found, client will contact file master if it is alive
	ingestReply(reply, targetCluster)
	// log.Printf("Sent put response: \n " + toResponse(targetCluster).toString())
	return nil
}

// clients tries to write a file
func (this *FileMetadataService) handleDeleteRequest(fileName string, reply *DfsResponse) error {
	this.metadataLock.RLock()
	fileToClusterInfo := *util.Convert2(&this.metadata)
	this.metadataLock.RUnlock()


	clusterInfo, exists := fileToClusterInfo[fileName]

	if !exists {
		return errors.New("File " + fileName + " does not exists")
	}

	ingestReply(reply, clusterInfo)
	return nil
}

// clients tries to write a file
func (this *FileMetadataService) handleListRequest(fileName string, reply *DfsResponse) error {
	this.metadataLock.RLock()
	defer this.metadataLock.RUnlock()

	fileToClusterInfo := *util.Convert2(&this.metadata)

	clusterInfo, exists := fileToClusterInfo[fileName]

	if !exists {
		// new file, allocate a new cluster
		return errors.New("File " + fileName + " does not exists")
	}

	ingestReply(reply, clusterInfo)
	return nil
}

// ----------------------------------------
// Functions for maintaining metadata
// ----------------------------------------

// for each file, examine the hosting replicas and make necessary repairs
func checkAndRepair(nodeIdToFiles *map[string]map[string]*util.FileInfo, fileNameToReplicaInfo *map[string]*util.ClusterInfo) {
	for _, clusterInfo := range *fileNameToReplicaInfo {
		if clusterInfo.Master == nil {
			// Master dead, try elect from servants
			err := clusterInfo.InstateNewMaster()
			if err != nil {
				// todo: consider removing dead file from metadata, all replicas are lost and nothing much can be done

				return
			}
		}

		if clusterInfo.ClusterSize() < config.ReplicationFactor {
			clusterInfo.RecruitServants(nodeIdToFiles, config.ReplicationFactor)
		}
	}
}

func collectMetadata() *[]util.FileServerMetadataReport {

	ips := LocalMembershipList.AliveMembers()
	ips = append(ips, NodeIdToIP(SelfNodeId))
	clients := make([]*rpc.Client, len(ips))
	reports := make([]util.FileServerMetadataReport, len(ips))

	collectionTimeout := time.After(time.Duration(REPORT_COLLECTION_TIMEOUT_SECONDS) * time.Second)

	calls := make([]*rpc.Call, len(ips))

	// todo: modularized batch rpc calls
	for index, ip := range ips {
		// start connection if it is not previously established
		if clients[index] == nil {
			clients[index] = dial(ip, config.RpcServerPort)
		}

		arg := ""

		if clients[index] != nil {
			// perform async rpc call
			call := clients[index].Go("FileService.ReportMetadata", &arg, &(reports[index]), nil)
			if call.Error != nil {
				clients[index] = dial(ip, config.RpcServerPort) // best effort re-dial
				if clients[index] != nil {
					call = clients[index].Go("FileService.ReportMetadata", &arg, &(reports[index]), nil)
				}
			}
			calls[index] = call
		}
	}

	// iterate and look for completed rpc calls
	for {
		complete := true
		for i, call := range calls {
			select {
			case <-collectionTimeout:
				complete = true
				log.Print("Collection timeout !!!")
				break
			default:
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
		}
		if complete {
			break
		}
	}

	// fmt.Println("--------------------------------")
	// for _, r := range reports{
	// 	for _, fileInfo := range r.FileEntries{
	// 		fmt.Println(fileInfo.ToString())
	// 	}
	// }
	// fmt.Println("--------------------------------\n\n\n\n")

	return &reports
}

// reallocate replicas as necessary
func (rpcServer *FileMetadataService) adjustCluster(reports *[]util.FileServerMetadataReport) {
	nodeIdToFiles, filenameToCluster := util.CompileReports(reports)

	// log.Printf("Collected report length : %d", len(*nodeIdToFiles))

	checkAndRepair(nodeIdToFiles, filenameToCluster)

	rpcServer.metadataLock.Lock()
	rpcServer.metadata = *nodeIdToFiles
	rpcServer.metadataLock.Unlock()

	nodeIdToFiles = util.Convert(filenameToCluster)
	for nodeId, _ := range *nodeIdToFiles {
		go informMetadata(nodeId, nodeIdToFiles)
	}
}

func informMetadata(nodeId string, metadata *util.NodeToFiles) error {
	timeout := time.After(30 * time.Second)
	ip := NodeIdToIP(nodeId)
	client := dial(ip, config.RpcServerPort)
	if client == nil {
		log.Printf("Cannot connect to node %s while informing metadata", nodeId)
		return errors.New("Cannot connect to node")
	}
	defer client.Close()

	retFlag := ""

	call := client.Go("FileService.UpdateMetadata", metadata, &retFlag, nil)
	if call.Error != nil {
		log.Printf("Encountered error while informing node %s", nodeId)
		return call.Error
	}

	select {
	case <-timeout:
		return errors.New("Timeout informing node" + nodeId)
	case _, ok := <-call.Done: // check if channel has output ready
		if !ok {
			log.Println("File Metadata Server: Channel closed for async rpc call")
			return errors.New("Node " + nodeId + " failed to respond to metadata update.")
		} else {
			if retFlag == "ACK" {
				// log.Printf("File Metadata Server: successfully informed node %s", nodeId)
				return nil
			} else {
				log.Printf("File Metadata Server: node %s failed to process metadata update", nodeId)
				return errors.New("Node " + nodeId + " failed to process metadata update.")
			}
		}
	}
}

func toResponse(clusterInfo *util.ClusterInfo) *DfsResponse {

	servants := make([]util.FileInfo, 0)
	for _, servant := range clusterInfo.Servants {
		if servant == nil {
			log.Printf("Warn: null servant ptr")
		} else {
			servants = append(servants, *servant)
		}
	}

	ret := &DfsResponse{
		FileName: clusterInfo.FileName,
		Servants: servants,
	}

	if clusterInfo.Master != nil {
		ret.Master = *clusterInfo.Master
	} else {
		log.Println("Warn: responded with null master")
	}

	return ret
}

package util

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

const (
	MIN_REPLICA_NUM int = 4

	// file status
	PENDING_FILE_UPLOAD int = 0 // master waiting for a client upload
	WAITING_REPLICATION int = 1 // servants waiting for replication from master
	COMPLETE            int = 2 // file is intact
	PENDING_DELETE      int = 3 // a file is waiting to be deleted
)

type FileNameToCluster = map[string]*ClusterInfo
type NodeToFiles = map[string]map[string]*FileInfo

// metadata reported by each file server and collected by the metadata server
type FileServerMetadataReport struct {
	NodeId      string
	FileEntries []FileInfo
}

type FileInfo struct {
	NodeId     string
	FileName   string
	IsMaster   bool
	FileStatus int
	Version    int
	MjDir      string
}

// replica cluster info for a file
type ClusterInfo struct {
	FileName string
	Master   *FileInfo
	Servants []*FileInfo
}

func (r *FileServerMetadataReport) GetFileInfoByPrefix(prefix string) []string {
	result := make([]string, 0)
	for _, file := range r.FileEntries {
		if strings.HasPrefix(file.FileName, prefix) {
			result = append(result, file.FileName)
		}
	}
	return result
}

func NewClusterInfo(fileName string) *ClusterInfo {
	ret := ClusterInfo{
		FileName: fileName,
		Servants: make([]*FileInfo, 0),
	}
	return &ret
}

// compile reports into map of nodeId -> fileName -> FileInfo and a map of fileName -> replicaInfo
func CompileReports(reports *[]FileServerMetadataReport) (*NodeToFiles, *FileNameToCluster) {

	nodeIdToFiles := make(map[string]map[string]*FileInfo)
	fileNameToCluster := make(map[string]*ClusterInfo)

	for _, report := range *reports {
		nodeId := report.NodeId
		_, ok := nodeIdToFiles[nodeId]

		if !ok {
			nodeIdToFiles[nodeId] = make(map[string]*FileInfo)
		}

		for _, fileInfo := range report.FileEntries {
			fileName := fileInfo.FileName

			nodeIdToFiles[nodeId][fileName] = &FileInfo{
				NodeId:     fileInfo.NodeId,
				FileName:   fileInfo.FileName,
				IsMaster:   fileInfo.IsMaster,
				FileStatus: fileInfo.FileStatus,
				Version:    fileInfo.Version,
			}

			_, ok = fileNameToCluster[fileName]
			if !ok {
				fileNameToCluster[fileName] = NewClusterInfo(fileName)
			}
			entry := fileNameToCluster[fileName]
			if fileInfo.IsMaster {
				if fileNameToCluster[fileName].Master != nil {
					log.Printf("Detected multiple masters for file %s", fileName)
				}
				entry.Master = &FileInfo{
					NodeId:     fileInfo.NodeId,
					FileName:   fileInfo.FileName,
					IsMaster:   fileInfo.IsMaster,
					FileStatus: fileInfo.FileStatus,
					Version:    fileInfo.Version,
				}
			} else {
				servants := entry.Servants
				servants = append(servants, &FileInfo{
					NodeId:     fileInfo.NodeId,
					FileName:   fileInfo.FileName,
					IsMaster:   fileInfo.IsMaster,
					FileStatus: fileInfo.FileStatus,
					Version:    fileInfo.Version,
				})
				entry.Servants = servants
			}
			fileNameToCluster[fileName] = entry
		}
	}

	// log.Print("--------------compiled report------------------")
	// for _, fmap := range nodeIdToFiles {
	// 	for _, fileInfo := range fmap {
	// 		fmt.Println(fileInfo.ToString())
	// 	}
	// }

	return &nodeIdToFiles, &fileNameToCluster
}

// try to choose servant with the largest version id as next master
// remove it from servant list
func (this *ClusterInfo) InstateNewMaster() error {
	var newMaster *FileInfo
	ti := 0
	for idx, servant := range this.Servants {
		if servant.FileStatus != COMPLETE {
			continue
		}

		if newMaster == nil || (*newMaster).Version < (*servant).Version {
			newMaster = servant
			ti = idx
		}
	}

	// found a new master, remove it from servants
	if newMaster != nil {
		newMaster.IsMaster = true
		this.Master = newMaster
		servants := this.Servants
		this.Servants = append(servants[:ti], servants[ti+1:]...)
		return nil
	}

	// this only happens when master dies and no servants have completed replication
	return errors.New("Fatal: untable to establish new master for file " + this.FileName)
}

// pull in new servants to reach replication factor
func (cluster *ClusterInfo) RecruitServants(nodeIdToFiles *NodeToFiles, replicationFactor int) {
	recruitNum := replicationFactor - cluster.ClusterSize()
	if recruitNum <= 0 {
		return
	}

	log.Printf("Current cluster node size :%d", len(*nodeIdToFiles))

	for nodeId, fmap := range *nodeIdToFiles {
		_, exists := fmap[cluster.FileName]
		if !exists {
			newFileInfo := FileInfo{
				NodeId:     nodeId,
				FileName:   cluster.FileName,
				IsMaster:   false,
				FileStatus: WAITING_REPLICATION,
				Version:    0,
			}
			cluster.Servants = append(cluster.Servants, &newFileInfo)
			if cluster.ClusterSize() >= replicationFactor {
				break
			}
		}
	}

	if cluster.ClusterSize() < replicationFactor {
		log.Printf("[Warn] unable to fulfill replication factor for file %s, current cluster size is %d", cluster.FileName, cluster.ClusterSize())
	}
}

// recruit both master and slaves, used when a file is written for the first time
func (cluster *ClusterInfo) RecruitFullCluster(nodeToFiles *NodeToFiles, replicationFactor int) {
	recruitNum := replicationFactor - cluster.ClusterSize()
	if recruitNum <= 0 {
		log.Print("Cluster is already fulfilled")
		return
	}

	avaibleNodes := FindAvailableNodes(cluster.FileName, nodeToFiles, replicationFactor)

	if len(avaibleNodes) == 0 {
		log.Printf("No available nodes left")
		return
	}

	if cluster.Master == nil {
		cluster.Master = &FileInfo{
			NodeId:     avaibleNodes[0], // we could use other strategies for picking master, but for the sake of time just pick a random one
			FileName:   cluster.FileName,
			IsMaster:   true,
			FileStatus: PENDING_FILE_UPLOAD,
			Version:    0,
		}

		avaibleNodes = avaibleNodes[1:]
		recruitNum -= 1
	}

	servants := make([]*FileInfo, 0)

	for _, nodeId := range avaibleNodes {
		fileInfo := &FileInfo{
			NodeId:     nodeId,
			FileName:   cluster.FileName,
			IsMaster:   false,
			FileStatus: WAITING_REPLICATION,
			Version:    0,
		}
		servants = append(servants, fileInfo)
		if len(servants) >= recruitNum {
			break
		}
	}

	cluster.Servants = servants
}

func (this *ClusterInfo) ClusterSize() int {
	size := 0
	if this.Master != nil {
		size += 1
	}
	return size + len(this.Servants)
}

func (this *ClusterInfo) Flatten() *[]*FileInfo {
	ret := make([]*FileInfo, 0)
	if this.Master != nil {
		ret = append(ret, this.Master)
	}

	for _, serv := range this.Servants {
		if serv != nil {
			ret = append(ret, serv)
		}
	}
	return &ret
}

func Convert(fileToCluster *FileNameToCluster) *NodeToFiles {
	ret := make(map[string]map[string]*FileInfo)

	for fileName, cluster := range *fileToCluster {
		for _, fileInfo := range *cluster.Flatten() {
			nodeId := fileInfo.NodeId
			_, exists := ret[nodeId]
			if !exists {
				ret[nodeId] = make(map[string]*FileInfo)
			}
			ret[nodeId][fileName] = fileInfo
		}
	}

	return &ret
}

func Convert2(nodeToFiles *NodeToFiles) *FileNameToCluster {

	ret := make(map[string]*ClusterInfo)

	for _, fmap := range *nodeToFiles {
		for fileName, fileInfo := range fmap {
			cluster, exists := ret[fileName]
			if !exists {
				cluster = NewClusterInfo(fileName)
			}
			if fileInfo.IsMaster {
				cluster.Master = fileInfo
			} else {
				cluster.Servants = append(cluster.Servants, fileInfo)
			}
			ret[fileName] = cluster
		}
	}
	return &ret
}

// for a file, allocate new nodes
func FindAvailableNodes(fileName string, nodeToFiles *NodeToFiles, nodeNum int) []string {

	ret := make([]string, 0)
	if nodeNum <= 0 {
		return ret
	}

	for nodeId, fmap := range *nodeToFiles {
		if len(ret) >= nodeNum {
			break
		}
		_, exists := fmap[fileName]
		if !exists {
			ret = append(ret, nodeId)
		}
	}

	if len(ret) < nodeNum {
		log.Printf("Unable to find enough new replicas for file %s. %d requested but %d found.", fileName, nodeNum, len(ret))
	}

	return ret
}

func (this *FileInfo) ToString() string {
	role := "servant"

	if this.IsMaster {
		role = "master"
	}

	fileStatus := "unknown"
	switch this.FileStatus {
	case COMPLETE:
		fileStatus = "normal"
	case PENDING_FILE_UPLOAD:
		fileStatus = "file upload in progress"
	case WAITING_REPLICATION:
		fileStatus = "replication in progress"
	}

	return fmt.Sprintf(
		"---------------------\n"+
			"File name: %s\n"+
			"Node ID: %s\n"+
			"Role: %s\n"+
			"File status: %s\n",
		this.FileName,
		this.NodeId,
		role,
		fileStatus)
}

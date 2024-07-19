package routines

import (
	"net/rpc"
	"golang.org/x/crypto/ssh"
	"cs425-mp2/config"
	"cs425-mp2/util"
	"fmt"
	"log"
	"os"
	"time"
)

type FileService struct {
	Port 					int
	SshConfig 				*ssh.ClientConfig
	Filename2FileMaster 	map[string]*FileMaster
	SdfsFolder 				string
	LocalFileFolder			string
	Report 					util.FileServerMetadataReport
}

type CopyArgs struct {
	LocalFilePath 	string
	RemoteFilePath 	string
	RemoteAddr 		string
}

type RWArgs struct {
	Token uint64
	LocalFilename 	string
	SdfsFilename 	string
	ClientAddr 		string
}

type CreateFMArgs struct {
	Filename 	string
	Servants 	[]string
}

type DeleteArgs struct {
	Filename string
}

func NewFileService(port int, homedir string, serverHostnames[]string) *FileService {
	MEMBERSHIP_SERVER_STARTED.Wait()

	this := new(FileService)
	this.Port = port
	this.SshConfig = &ssh.ClientConfig{
		User: config.SshUsername,
		Auth: []ssh.AuthMethod{
			ssh.Password(config.SshPassword),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout: 8 * time.Second,
	}
	this.Filename2FileMaster = make(map[string]*FileMaster)
	this.SdfsFolder = homedir + "/sdfs/"
	this.LocalFileFolder = homedir + "/local/"
	this.Report =  util.FileServerMetadataReport{
		NodeId: SelfNodeId,
		FileEntries: make([]util.FileInfo, 0),
	}

	// util.CreateSshClients(serverHostnames, this.SshConfig, NodeIdToIP(SelfNodeId))
	util.EmptySdfsFolder(this.SdfsFolder)

	return this
}

func (this *FileService) Register(){
	rpc.Register(this)
}


func (fm *FileService) CheckWriteCompleted(key *string, reply *string) error {
	
	timeout := time.After(300 * time.Second)
	for {
		time.Sleep(2*time.Second)
		select {
		case <-timeout:
			*reply = ""
			return nil
		default:
			if FileMasterProgressTracker.IsFullCompletedByKey(*key){	// received file, send it to servants
				*reply = "ACK"
				return nil
			}
		}
	}
}

// reroute to the corresponding file master
func (this *FileService) ReadFile(args *RWArgs, reply *string) error {
	fm, ok := this.Filename2FileMaster[args.SdfsFilename]
	// TODO: fix error checking and return the actual error
	if ok {
		fm.ReadFile(args.LocalFilename, args.ClientAddr, args.Token)
	} else {
		log.Fatal("No corresponding filemaster for " + args.SdfsFilename)
	}
	return nil
}

// reroute to the corresponding file master
func (this *FileService) WriteFile(args *RWArgs, reply *uint64) error {
	fm, ok := this.Filename2FileMaster[args.SdfsFilename]
	// TODO: fix error checking and return the actual error
	if ok {
		fm.WriteFile(args.LocalFilename, reply)
	} else {
		log.Fatal("No corresponding filemaster for " + args.SdfsFilename)
	}
	return nil
}

// reroute to the corresponding file master
func (this *FileService) ReplicateFile(args *RWArgs, reply *string) error {
	fm, ok := this.Filename2FileMaster[args.SdfsFilename]
	// TODO: fix error checking and return the actual error
	if ok {
		fm.ReplicateFile(args.ClientAddr)
	} else {
		log.Fatal("No corresponding filemaster for " + args.SdfsFilename)
	}
	return nil
}

// reroute to the corresponding file master
func (this *FileService) DeleteFile(args *DeleteArgs, reply *string) error {
	fm, ok := this.Filename2FileMaster[args.Filename]
	// TODO: fix error checking and return the actual error
	if ok {
		fm.DeleteFile()
	} else {
		log.Fatal("No corresponding filemaster for " + args.Filename)
	}
	return nil
}

func (this *FileService) CopyFileToRemote(args *CopyArgs, reply *string) error {
	return util.CopyFileToRemote(args.LocalFilePath, args.RemoteFilePath, args.RemoteAddr, this.SshConfig)
}

func (this *FileService) DeleteLocalFile(args *DeleteArgs, reply *string) error {
	return util.DeleteFile(args.Filename, this.SdfsFolder)
}

func (this *FileService) CreateFileMaster(args *CreateFMArgs, reply *string) error{
	fm := NewFileMaster(args.Filename, args.Servants, this.Port, this.SdfsFolder, this.LocalFileFolder)
	this.Filename2FileMaster[args.Filename] = fm
	return nil
}

func (this *FileService) ReportMetadata(args *string, reply *util.FileServerMetadataReport) error{
	// TODO: check all files that are pending and change status

	for i, fileInfo := range this.Report.FileEntries {
		if fileInfo.FileStatus == util.PENDING_FILE_UPLOAD || fileInfo.FileStatus == util.WAITING_REPLICATION {
			// check if file is in sdfs folder
			_, err := os.Stat(this.SdfsFolder + fileInfo.FileName)
			if err == nil {
				log.Println("file status changed to complete")
				// file is in folder
				this.Report.FileEntries[i].FileStatus = util.COMPLETE
			}
		}
	}

	reply.NodeId = SelfNodeId
	reply.FileEntries = this.Report.FileEntries

	// log.Printf("Node %s reported self metadata info", reply.NodeId)

	return nil
}

func (this *FileService) UpdateMetadata(nodeToFiles *util.NodeToFiles, reply *string) error {
	updatedFileEntries := (*nodeToFiles)[SelfNodeId]
	fileToClusters := util.Convert2(nodeToFiles)

	filename2fileInfo := make(map[string]util.FileInfo)
	for _, fileInfo := range this.Report.FileEntries {
		filename2fileInfo[fileInfo.FileName] = fileInfo
	}

	for _, updatedFileInfo := range updatedFileEntries {
		currFileInfo, ok := filename2fileInfo[updatedFileInfo.FileName]
		needToCreateFm := false
		if ok {
			// promoted to master
			if !currFileInfo.IsMaster && updatedFileInfo.IsMaster {
				// set is Master to true and create a new filemaster
				for idx, fileInfo := range this.Report.FileEntries {
					if fileInfo.FileName == currFileInfo.FileName {
						this.Report.FileEntries[idx].IsMaster = true
						break
					}
				}
				needToCreateFm = true
			}
		} else {
			// new file
			if (updatedFileInfo.IsMaster) {
				needToCreateFm = true
			} else if (updatedFileInfo.FileStatus == util.WAITING_REPLICATION) {
				cluster := (*fileToClusters)[updatedFileInfo.FileName]
				// failure repair
				// when master's status == PENDING_FILE_UPLOAD, it indicates a new file is uploaded to sdfs
				// fm will handle writing to all services, so there is no need to do anything
				if (cluster.Master == nil){
					log.Print("Warn: master is nil. Servant cannot replicate")
				}

				if (cluster.Master != nil && cluster.Master.FileStatus == util.COMPLETE) {
					masterIp := NodeIdToIP(cluster.Master.NodeId)
					client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", masterIp, config.RpcServerPort))
					if err != nil {
						log.Println("Error dailing master when trying to retrieve replica", err)
						return err
					}
					args := &RWArgs {
						LocalFilename: updatedFileInfo.FileName,
						SdfsFilename: updatedFileInfo.FileName,
						ClientAddr: NodeIdToIP(SelfNodeId),
					}
					var reply string
					client.Go("FileService.ReplicateFile", args, &reply, nil)
				}
			}
			this.Report.FileEntries = append(this.Report.FileEntries, *updatedFileInfo)
		}

		if (needToCreateFm) {
			createArgs := &CreateFMArgs{
				Filename: updatedFileInfo.FileName,
				Servants: util.GetServantIps(fileToClusters, updatedFileInfo.FileName),
			}
			var reply string
			err := this.CreateFileMaster(createArgs, &reply)
			if err != nil {
				log.Println("Error when creating file master ", err)
				return err
			}
		}
	}

	*reply = "ACK"
	return nil
}
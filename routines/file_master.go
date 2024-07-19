package routines

import (
	"cs425-mp2/config"
	"cs425-mp2/util"
	"fmt"
	"log"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

const (
	MASTER_WRITE_COMPLETE int = 1
	FULL_WRITE_COMPLETE   int = 2
)

var FileMasterProgressTracker *ProgressManager = NewProgressManager()

type FileMaster struct {
	// main queue: store the requests that are waiting in the order they are received
	Queue []*Request
	// write queue: keep track of write requests that are waiting
	WriteQueue []*Request
	// number of nodes that are currently reading the file
	CurrentRead int
	// number of nodes that are currently writing the file
	CurrentWrite int
	Filename     string
	// list of servant ip addresses
	Servants []string
	// address of self. used to prevent errors in case fm = client
	SelfAddr string
	// TODO: remove this?
	// SshConfig 	 	*ssh.ClientConfig
	FileServerPort  int
	SdfsFolder      string
	LocalFileFolder string

	taskToken     uint64
	taskTokenLock sync.Mutex
}

type ProgressManager struct {
	writeTaskCompleted map[string]int // a map of token to id for tracing the progress of write tasks
	lock               sync.RWMutex
}

func NewProgressManager() *ProgressManager {
	return &ProgressManager{
		writeTaskCompleted: make(map[string]int),
	}
}

func (this *ProgressManager) IsMasterCompleted(fileName string, token uint64) bool {
	key := fileName + "-" + strconv.Itoa(int(token))
	this.lock.RLock()
	defer this.lock.RUnlock()
	value, exists := this.writeTaskCompleted[key]
	return exists && value == MASTER_WRITE_COMPLETE
}

func (this *ProgressManager) IsFullCompleted(fileName string, token uint64) bool {
	key := fileName + "-" + strconv.Itoa(int(token))
	this.lock.RLock()
	defer this.lock.RUnlock()
	value, exists := this.writeTaskCompleted[key]
	return exists && value == FULL_WRITE_COMPLETE
}

func (this *ProgressManager) IsFullCompletedByKey(key string) bool {
	this.lock.RLock()
	defer this.lock.RUnlock()
	value, exists := this.writeTaskCompleted[key]
	return exists && value == FULL_WRITE_COMPLETE
}

func (this *ProgressManager) Complete(fileName string, token uint64, completionType int) {
	key := fileName + "-" + strconv.Itoa(int(token))
	this.lock.Lock()
	defer this.lock.Unlock()

	this.writeTaskCompleted[key] = completionType
}

func (this *FileMaster) GetNewToken() uint64 {
	this.taskTokenLock.Lock()
	this.taskToken += 1
	token := this.taskToken
	this.taskTokenLock.Unlock()
	return token
}

type Request struct {
	// type of request: read (R), write (W), delete (D)
	Type string
	// a flag indicating whether the request is still in queue
	InQueue bool
	// how many rounds a write has been waiting for consecutive read
	WaitRound int
}

func NewFileMaster(filename string, servants []string, fileServerPort int, sdfsFolder string, localFileFlder string) *FileMaster {
	//FileMasterProgressTracker = NewProgressManager()
	selfAddr := NodeIdToIP(SelfNodeId)
	return &FileMaster{
		CurrentRead:     0,
		CurrentWrite:    0,
		Filename:        filename,
		Servants:        servants,
		SelfAddr:        selfAddr,
		FileServerPort:  fileServerPort,
		SdfsFolder:      sdfsFolder,
		LocalFileFolder: localFileFlder,
	}
}

func (fm *FileMaster) CheckQueue() {
	if len(fm.Queue) > 0 {
		if !fm.Queue[0].InQueue {
			// if head of queue has InQueue = false. pop it and move on to the next one
			fm.Queue = fm.Queue[1:]
			fm.CheckQueue()
			return
		}
		if fm.Queue[0].Type == "R" {
			// satisfy read condition (current reader < 2 and no writer)
			if fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
				// no write has been waiting for more than 4 consecutive read
				if len(fm.WriteQueue) == 0 || (len(fm.WriteQueue) > 0 && fm.WriteQueue[0].WaitRound < 4) {
					fm.Queue[0].InQueue = false
					fm.Queue = fm.Queue[1:]
					return
				} else {
					// if a write has been waiting for 4 consecutive read, pop that write request
					// note now queue might have a request that have InQueue = false
					fm.WriteQueue[0].InQueue = false
					fm.WriteQueue = fm.WriteQueue[1:]
				}
			}
		} else {
			// write request or delete request
			if fm.CurrentRead == 0 && fm.CurrentWrite == 0 {
				// write condition satisifed: no reader and no writer (write-write and read-write are both conflict operations)
				fm.WriteQueue[0].InQueue = false
				fm.WriteQueue = fm.WriteQueue[1:]

				fm.Queue[0].InQueue = false
				fm.Queue = fm.Queue[1:]
			} else if fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
				// write is blocked because there is exactly 1 reader
				// then allow another read request, if write has not been waiting for more than 4 consecutive rounds
				for _, request := range fm.Queue {
					if request.Type == "R" && fm.WriteQueue[0].WaitRound < 4 {
						request.InQueue = false
					}
				}
			}
		}
	}
}

func (fm *FileMaster) ReadFile(clientFilename string, clientAddr string, token uint64) error {
	var request *Request = nil
	for {
		// if the request is not in queue and read condition (reader < 2 and no writer) satisfied
		if request == nil && fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
			// no write has been waiting for more than 4 consecutive read
			if len(fm.WriteQueue) == 0 || (len(fm.WriteQueue) > 0 && fm.WriteQueue[0].WaitRound < 4) {
				return fm.executeRead(clientFilename, clientAddr, token)
			}
		} else if request == nil {
			// initial condition to execute the read is not satifised. add to queue
			request = &Request{
				Type:    "R",
				InQueue: true,
			}
			fm.Queue = append(fm.Queue, request)
		} else if request != nil && !request.InQueue {
			// request has been poped from queue, execute read
			return fm.executeRead(clientFilename, clientAddr, token)
		}
	}
}

func (fm *FileMaster) executeRead(clientFilename string, clientAddr string, token uint64) error {
	fm.CurrentRead += 1
	// every request in the wait queue has been forced to wait another one round because of
	// the read that is currently executing
	for _, writeRequest := range fm.WriteQueue {
		writeRequest.WaitRound += 1
	}

	log.Printf("Sending file to client at %s", clientAddr)

	localFilePath := fm.SdfsFolder + fm.Filename
	SendFile(localFilePath, clientFilename, clientAddr+":"+strconv.Itoa(config.DfsClientReceivePort), token)

	// util.CopyFileToRemote(localFilePath, remoteFilePath, clientAddr, fm.SshConfig)

	fm.CurrentRead -= 1
	fm.CheckQueue()
	return nil
}

func (fm *FileMaster) ReplicateFile(clientAddr string) error {
	var request *Request = nil
	for {
		// if the request is not in queue and read condition (reader < 2 and no writer) satisfied
		if request == nil && fm.CurrentRead < 2 && fm.CurrentWrite == 0 {
			// no write has been waiting for more than 4 consecutive read
			if len(fm.WriteQueue) == 0 || (len(fm.WriteQueue) > 0 && fm.WriteQueue[0].WaitRound < 4) {
				return fm.executeReplicate(clientAddr)
			}
		} else if request == nil {
			// initial condition to execute the read is not satifised. add to queue
			request = &Request{
				Type:    "R",
				InQueue: true,
			}
			fm.Queue = append(fm.Queue, request)
		} else if request != nil && !request.InQueue {
			// request has been poped from queue, execute read
			return fm.executeReplicate(clientAddr)
		}
	}
}

func (fm *FileMaster) executeReplicate(clientAddr string) error {
	fm.CurrentRead += 1
	// every request in the wait queue has been forced to wait another one round because of
	// the read that is currently executing
	for _, writeRequest := range fm.WriteQueue {
		writeRequest.WaitRound += 1
	}

	fmt.Println("read" + fm.Filename)
	// copy to servant's sdfs folder
	localFilePath := fm.SdfsFolder + fm.Filename

	// util.CopyFileToRemote(localFilePath, remoteFilePath, clientAddr, fm.SshConfig)

	log.Println("sending replica to ", clientAddr+":"+strconv.Itoa(config.FileServerReceivePort))
	SendFile(localFilePath, fm.Filename, clientAddr+":"+strconv.Itoa(config.FileServerReceivePort), 0)

	fm.CurrentRead -= 1
	fm.CheckQueue()
	return nil
}

func (fm *FileMaster) WriteFile(clientFilename string, reply *uint64) error {
	var request *Request = nil
	for {
		// requests just come in, and the condition for write is satisfied
		if request == nil && fm.CurrentWrite == 0 && fm.CurrentRead == 0 {
			// if there is no other write pending, simply execute
			if len(fm.WriteQueue) == 0 {
				return fm.executeWrite(clientFilename, reply)
			}
		} else if request == nil {
			// otherwise add to queue
			request = &Request{
				Type:    "W",
				InQueue: true,
			}
			fm.Queue = append(fm.Queue, request)
			fm.WriteQueue = append(fm.WriteQueue, request)
		} else if request != nil && !request.InQueue {
			return fm.executeWrite(clientFilename, reply)
		}
	}
}

func (fm *FileMaster) executeWrite(clientFilename string, reply *uint64) error {
	fm.CurrentWrite += 1

	// allow client to start sending file, and assign it a token corresponding to that file write
	token := fm.GetNewToken()
	*reply = token
	
	go func(){
		timeout := time.After(60 * time.Second)
		for {
			time.Sleep(1 * time.Second) // check if client finished uploading every second
			select {
			case <-timeout:
				log.Print("Client did not finish uploading file to master in 60s")
				return
			default:
				if FileMasterProgressTracker.IsMasterCompleted(fm.Filename, token){	// received file, send it to servants
					for _, servant := range fm.Servants {
						SendFile(config.Homedir+"/sdfs/"+fm.Filename, fm.Filename, servant+":"+strconv.Itoa(config.FileServerReceivePort), 0)
					}

					log.Print("Global write completed")
					FileMasterProgressTracker.Complete(fm.Filename, token, FULL_WRITE_COMPLETE)
					return
				}
			}
		}

	}()

	fm.CurrentWrite -= 1
	fm.CheckQueue()
	return nil
}

func (fm *FileMaster) DeleteFile() error {
	var request *Request = nil
	for {
		// requests just come in, and the condition for delete is satisfied
		if request == nil && fm.CurrentWrite == 0 && fm.CurrentRead == 0 {
			// if there is no other write pending, simply execute
			if len(fm.WriteQueue) == 0 {
				return fm.executeDelete()
			}
		} else if request == nil {
			// otherwise add to queue. treat delete same as write, so add it to write queue too
			request = &Request{
				Type:    "D",
				InQueue: true,
			}
			fm.Queue = append(fm.Queue, request)
			fm.WriteQueue = append(fm.WriteQueue, request)
		} else if request != nil && !request.InQueue {
			return fm.executeDelete()
		}
	}
}

func (fm *FileMaster) executeDelete() error {
	fmt.Println("delete" + fm.Filename)
	util.DeleteFile(fm.Filename, fm.SdfsFolder)
	for _, servant := range fm.Servants {
		client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", servant, fm.FileServerPort))
		if err != nil {
			log.Fatal("Error dialing servant:", err)
		}
		deleteArgs := DeleteArgs{
			Filename: fm.Filename,
		}
		var reply string
		// TODO: change this to async
		client.Call("FileService.DeleteLocalFile", deleteArgs, &reply)
		client.Close()
	}
	return nil
}
package routines

import "net/rpc"

// rpc for facilitating DFS mutliread command from DFS client


type DfsRemoteReader struct {}



func NewDfsRemoteReader() *DfsRemoteReader {
	return &DfsRemoteReader{}
}


func (this *DfsRemoteReader) Register(){
	rpc.Register(this)
}


func (this *DfsRemoteReader) Read(fileName *string, reply *string) error {
	localFileName := "remoted_initiated_" + *fileName 
	args := []string {*fileName, localFileName}
	err := GetFile(args)

	if err == nil {
		*reply = "DONE"
	} else {
		*reply = "FAILED"
	}
	return err
}
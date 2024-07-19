package routines

import (
	"bytes"
	"encoding/binary"
	"log"
	"net"
	"strconv"
	"os"
	"io"
	"strings"
)

const (
	FILE_TRANSFER_BUFFER_SIZE int = 20*1024
)

type FilerHeader struct {
	Token uint64
	FileSize uint64 	// file size in bytes
	FileNameLength uint64 //length of name
	FileName string
}


func (this *FilerHeader) ToPayload() []byte{
	buf := bytes.NewBuffer(make([]byte, 0))

	tokenArr := make([]byte, 8)
	fileSizeArr := make([]byte, 8)
	fileNameLengthArr := make([]byte, 8)
	
	binary.LittleEndian.PutUint64(tokenArr, this.Token)
	binary.LittleEndian.PutUint64(fileSizeArr, this.FileSize)
	binary.LittleEndian.PutUint64(fileNameLengthArr, this.FileNameLength)

	buf.Write(tokenArr)
	buf.Write(fileSizeArr)
	buf.Write(fileNameLengthArr)
	buf.Write([]byte(this.FileName))
	return buf.Bytes()
}


func StartFileReceiver(receiverFileFolder string, port int, progressManager *ProgressManager){
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(port)) 
    if err != nil {
        log.Fatal("Failed to start file transfer server", err)
    }
    log.Println("File transfer server started on: " + listener.Addr().String())

	for {
		conn, err := listener.Accept()
        if err != nil {
            log.Println("File transfer server: error accepting tcp connection:", err)
            continue
        }

		go receiveFile(conn, receiverFileFolder, progressManager)
	}


}


func receiveFile(conn net.Conn, targetFolder string, progressManager *ProgressManager){
	defer conn.Close()

	buf := make([]byte, FILE_TRANSFER_BUFFER_SIZE)
	
	bytesRemained := 0

	var file *os.File
	var token uint64
	fileName := ""

	total := 0

	for {
		n, err := conn.Read(buf)
		total += n
		log.Printf("Downloading file ----------- %d kb", total/1024)
		if err == io.EOF {
			if progressManager != nil {		
				progressManager.Complete(fileName, token, MASTER_WRITE_COMPLETE)
			}
			log.Printf("Completed receving file with filename (%s) and token (%d)", fileName, token)
			return
		}
		if err != nil {
			log.Println("File transfer server: encountered error while receving file", err)
			return
		}

		if file == nil{
			f, remain, t, fn := initializeFile(targetFolder, &buf, n)
			fileName = fn
			token = t
			if f == nil {
				return
			}
			file = f
			bytesRemained = remain
			defer file.Close()
		} else {
			bytesRemained -= n
			_, err := file.Write(buf[:n])
			if err != nil {
				log.Print("File transfer server: failed to write to file.", err)
				return
			}
		}
	}
}

// parse out file header, create local file and return file pointer remaining file size and token
func initializeFile(targetFolder string, buf *[]byte, size int) (*os.File, int, uint64, string) {


	token := binary.LittleEndian.Uint64((*buf)[:8])
	fileSize := binary.LittleEndian.Uint64((*buf)[8:16])
	nameLength := binary.LittleEndian.Uint64((*buf)[16:24])
	fileName := string((*buf)[24:24+int(nameLength)])

	headerSize := 24 + int(nameLength)
	dataSize := len(*buf) - (headerSize)	// data size in this buffer

	remainingBytesToRead := int(fileSize) - dataSize

	filePath := targetFolder + "/" + fileName
	file, err := os.Create(strings.TrimSpace(filePath))
	if err != nil {
		log.Printf("File transfer server: failed to create file.", err)
		return nil, 0, token, fileName
	}

	_, fileWriteErr := file.Write((*buf)[headerSize:size])

	if fileWriteErr != nil {
		log.Printf("File transfer server: failed to complete initial write to file.", fileWriteErr)
		return nil, 0, token, fileName
	}


	return file, remainingBytesToRead, token, fileName
}



func SendFile(localFilePath string, remoteFileName, remoteAddr string, token uint64) error {
	log.Printf("Started sending file %s", localFilePath)

	var total uint64 = 0

	localFile, err := os.Open(localFilePath)
	if err != nil {
		log.Print("Error opening file", err)
		return err
	}
	defer localFile.Close()


	fileInfo, err := os.Stat(localFilePath)
    if err != nil {
		log.Print("Error getting file info", err)
        return err
    }

    fileSize := fileInfo.Size()

	log.Printf("Sending file to remote addr: %s", remoteAddr)

	conn, err := net.Dial("tcp", remoteAddr)
    if err != nil {
		return err
    }
    defer conn.Close()

	buf := make([]byte, FILE_TRANSFER_BUFFER_SIZE)

	header := FilerHeader{
		Token: token,
		FileSize: uint64(fileSize),
		FileNameLength: uint64(len([]byte(remoteFileName))),
		FileName: remoteFileName,
	}

	conn.Write(header.ToPayload())

	for {
		n, err := localFile.Read(buf)
		total += uint64(n)

		if err == io.EOF {

			log.Printf("Finished sending file, remaining bytes is %d", n)
            return nil 	// we are finished
        }
		if err != nil {
			return err
		}

		log.Printf("Writing file  %d kb written", total/1024)
		conn.Write(buf[:n])
	}	
}


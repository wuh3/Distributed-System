package util

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"net"
	"path/filepath"
)

var SshClients map[string]*sftp.Client

type clientResult struct {
	Ip string
	Client *sftp.Client
}

func EmptySdfsFolder(sdfsFolder string) error{
	dir, err := os.Open(sdfsFolder)
	if err != nil {
		return err
	}
	defer dir.Close()

	// Read all file names in the folder
	fileNames, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}

	// Remove each file
	for _, fileName := range fileNames {
		filePath := filepath.Join(sdfsFolder, fileName)
		err := os.Remove(filePath)
		if err != nil {
			return err
		} 
	}
	return nil
}

func CreateSshClients(serverHostnames []string, sshConfig *ssh.ClientConfig, selfIp string) {
	SshClients = make(map[string]*sftp.Client)
	resultsChan := make(chan clientResult, len(serverHostnames))
	var wg sync.WaitGroup

	for _, hostname := range serverHostnames {
		wg.Add(1)
		go func(hostname string){
			
			ips, _ := net.LookupHost(hostname)
			ip := ips[0]
			if ip == selfIp {
				wg.Done()
				return
			}
			conn, connErr := ssh.Dial("tcp", ip + ":22", sshConfig)
			if connErr != nil {
				wg.Done()
				return
			} 

			client, err := sftp.NewClient(conn)
			if err != nil {
				wg.Done()
				return
			}

			resultsChan <- clientResult {
				Ip: ip,
				Client: client,
			}
			wg.Done()

		}(hostname)
	}

	wg.Wait()
	close(resultsChan)

	for result := range resultsChan {
		SshClients[result.Ip] = result.Client
	}
	//fmt.Println(this.SshClients)
}

// copy file to a remote location using sftp
func CopyFileToRemote(localFilePath string, remoteFilePath string, remoteAddr string, sshConfig *ssh.ClientConfig) error {

	client, ok := SshClients[remoteAddr]

	if (!ok) {
		log.Printf("Ssh client not found, re-creating...")
		conn, err := ssh.Dial("tcp", remoteAddr + ":22", sshConfig)
		if err != nil {
			return(err)
		}
		
		client, err = sftp.NewClient(conn)
		if err != nil {
			log.Printf("Failed to create SFTP client: %s", err.Error())
			return(fmt.Errorf("Failed to create SFTP client: %w", err))
		}
	} else {
		log.Println("using buffered client")
	}
		

	localFile, err := os.Open(localFilePath)
	if err != nil {
		return(fmt.Errorf("Failed to open local file: %w", err))
	}
	defer localFile.Close()

	remoteFile, err := client.Create(remoteFilePath)
	if err != nil {
		return(fmt.Errorf("Failed to create remote file: %w", err))
	}
	defer remoteFile.Close()

	_, err = io.Copy(remoteFile, localFile)
	if err != nil {
		return(fmt.Errorf("Failed to upload file: %w", err))
	}


	//client.Close()
	return nil
}

func DeleteFile(filename string, sdfsFolder string) error{
	filePath := sdfsFolder + filename
	err := os.Remove(filePath)
    if err != nil {
        return err
    }
    return nil
}
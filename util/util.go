package util

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

type KeyValue struct {
	Key   string
	Value []string
}

const (
	RPC_PORT string = ":8008"
)

func Prompt(title string, cmd *string, args *[]string, isValidCmd func(string) bool) {
	var input string
	for {
		fmt.Println(title)
		in := bufio.NewReader(os.Stdin)
		input, _ = in.ReadString('\n')

		input = strings.Trim(input, " \n\r")
		splitted := strings.Split(input, " ")
		if len(splitted) == 0 {
			fmt.Println("Invalid input, please try again.")
			continue
		}

		cmdTmp := splitted[0]
		argsTmp := splitted[1:]

		if isValidCmd(cmdTmp) {
			*cmd = cmdTmp
			*args = argsTmp
			fmt.Println()
			return
		}
		fmt.Println("Invalid input, please try again.")
	}
}

func IsValidAddress(in string) bool {
	elems := strings.Split(in, ":")
	if len(elems) != 2 {
		return false
	}
	return IsValidIp(elems[0]) && IsValidPort(elems[1])
}

func IsValidPort(in string) bool {
	n, err := strconv.Atoi(in)
	return err == nil && n >= 0 && n <= 65535
}

func IsValidDropRate(in string) bool {
	n, err := strconv.ParseFloat(in, 64)
	return err == nil && n >= 0 && n <= 1
}

func IsValidIp(in string) bool {
	elems := strings.Split(in, ".")
	if len(elems) != 4 {
		return false
	}
	for _, e := range elems {
		n, err := strconv.Atoi(e)
		if err != nil || n < 0 || n > 255 {
			return false
		}
	}
	return true
}

func GetOutboundIp() [4]uint8 {
	var ret [4]uint8
	conn, _ := net.Dial("udp4", "8.8.8.8:80")
	defer conn.Close()
	ip := strings.Split(conn.LocalAddr().String(), ":")[0]

	elems := strings.Split(ip, ".")
	for i := range ret {
		v, err := strconv.Atoi(elems[i])
		if err != nil {
			log.Println("Error parsing outbound ip address", err)
		}
		ret[i] = uint8(v)
	}
	return ret
}

func ExtractLineCount(str string) (int32, error) {
	str = strings.Trim(str, " \r\n")
	if len(str) == 0 {
		return 0, nil
	}
	values := strings.Split(str, ":")
	if len(values) < 2 {
		return -1, errors.New("Incorrect input format")
	}
	countVal := strings.Trim(values[1], " \r\n")
	ret, err := strconv.Atoi(countVal)
	if err != nil {
		return -1, err
	}
	return int32(ret), nil
}

func GetServantIps(fileToClusters *FileNameToCluster, fileName string) []string {
	cluster := (*fileToClusters)[fileName]
	servantIps := make([]string, len(cluster.Servants))
	for i, servantFileInfo := range cluster.Servants {
		ip := strings.Split(servantFileInfo.NodeId, ":")[0]
		servantIps[i] = ip
	}
	return servantIps
}

func HandleError(err error, source string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n[%s ERROR] %s\n", source, err.Error())
	}
}

func RemoveStringFromSlice(s []int, r int) []int {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

package routines

import (
	"hash/fnv"
	"log"
	"strings"
)

const MapleJuicePort = "1025" // TODO move to config

// leader
var mapleTasks map[string][]string

// leader has a VM to inputLines mapping
var juiceTasks map[int][]string

func hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func shuffleRange(numJuices int, sdfsIntermediateFilenamePrefix string, inputFiles []string) {
	size := len(inputFiles)
	groupSize := size / numJuices
	for i, filename := range inputFiles {
		key := filename
		juiceNum := i / groupSize
		if juiceNum >= numJuices {
			juiceNum = numJuices
		}
		juiceTasks[juiceNum] = append(juiceTasks[juiceNum], key)
		log.Print("[Debug]")
		log.Print(juiceNum)
		log.Println(key)
	}
}

func shuffleHash(numJuices int, sdfsIntermediateFilenamePrefix string, inputFiles []string) {
	for _, filename := range inputFiles {
		key := filename
		juiceJobNum := hash(key) % uint64(numJuices)
		juiceNum := int(juiceJobNum)
		if juiceNum < 0 {
			log.Fatal("juice negative")
		}
		juiceTasks[juiceNum] = append(juiceTasks[juiceNum], key)
		log.Print("[Debug]")
		log.Print(juiceNum)
		log.Println(key)
	}
}

func InitMap() {
	mapleTasks = map[string][]string{}
	juiceTasks = map[int][]string{}
}

func getIndex(field string, schema string) int { // TODO
	index := 0
	attributes := strings.Split(schema, ",")
	for i, attribute := range attributes {
		if strings.Compare(field, attribute) == 0 {
			index = i
			break
		}
	}
	return index
}

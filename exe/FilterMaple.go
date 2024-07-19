// Ran `go build -o FilterMaple.so -buildmode=plugin FilterMaple.go` and built FilterMaple.so
package main

import (
	"bufio"
	"cs425-mp2/config"
	"cs425-mp2/util"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func FilterMaple(filePath string, numOfMaple int, destDir string, inputPrefix string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read the header line
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}
	objectIDIndex := -1
	for i, col := range header {
		if col == "OBJECTID" {
			objectIDIndex = i
			break
		}
	}

	if objectIDIndex == -1 {
		return nil, fmt.Errorf("OBJECTID column not found")
	}

	// Initialize a slice of maps to hold the divided data
	dividedMaps := make([]map[string]util.KeyValue, numOfMaple)
	for i := range dividedMaps {
		dividedMaps[i] = make(map[string]util.KeyValue)
	}

	// Process each line and distribute them among the maps
	var rowCount int
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// Determine which map to put the data into
		mapIndex := rowCount % numOfMaple
		rowCount++

		// Extract the OBJECTID as key
		key := record[objectIDIndex]
		valuePairs := make([]string, 0, len(header))

		// Create value pairs for each column
		for i, colValue := range record {
			valuePairs = append(valuePairs, fmt.Sprintf("%s: %s", header[i], colValue))
		}

		dividedMaps[mapIndex][key] = util.KeyValue{Key: key, Value: valuePairs}
	}

	// Write each map to a separate file
	for i, dm := range dividedMaps {
		fileName := fmt.Sprintf("maple_input_%d", i)
		if err := writeMapToFile(dm, fileName, destDir); err != nil {
			return nil, err
		}
	}

	fileNames := make([]string, 0)
	err = filepath.Walk(config.Homedir+destDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasPrefix(info.Name(), inputPrefix) {
			fullPath := filepath.Join(config.Homedir+destDir, info.Name())
			//fmt.Println("Loading file:", fullPath)
			fileNames = append(fileNames, fullPath)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return fileNames, nil
}

// writeMapToFile writes the given map to a file in a readable format
func writeMapToFile(m map[string]util.KeyValue, fileName string, destDir string) error {
	file, err := os.Create(config.Homedir + destDir + "/" + fileName)
	util.HandleError(err, "Write maple input files")
	if err != nil {
		return err
	}
	defer file.Close()

	for _, kv := range m {
		_, err := file.WriteString(fmt.Sprintf("%s: %v\n", kv.Key, kv.Value))
		util.HandleError(err, "Write map to file")
		if err != nil {
			return err
		}
	}

	return nil
}

func LoadMapleInput(filePath string) (map[string]util.KeyValue, error) {
	file, err := os.Open("/home/haozhew3/cs425/mp4/sdfs/" + filePath)
	util.HandleError(err, "Open maple inputs in filter")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	resultMap := make(map[string]util.KeyValue)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 {
			continue // or handle the error
		}

		key := parts[0]
		values := strings.Split(parts[1], ", ")

		resultMap[key] = util.KeyValue{Key: key, Value: values}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return resultMap, nil
}

/*
Call this method for each maple task
*/
func LoadAllMapleInputs(fileNames []string, inputDir string) ([]map[string]util.KeyValue, error) {
	var allMaps []map[string]util.KeyValue

	for i := 0; i < len(fileNames); i++ {
		fileName := inputDir + fileNames[i]
		loadedMap, err := LoadMapleInput(fileName)
		if err != nil {
			return nil, err
		}
		allMaps = append(allMaps, loadedMap)
	}

	return allMaps, nil
}

// filterjuice.go
package main

import (
	"bufio"
	"cs425-mp2/util"
	"os"
	"path/filepath"
	"strings"
)

// FilterJuice scans the given directory for files matching the pattern and loads their contents.
func FilterJuice(inputDirectory string, reduceInputPrefix string) ([]map[string]util.KeyValue, error) {
	fileNames, err := GetAllFileListsByPrefix(reduceInputPrefix, inputDirectory)
	if err != nil {
		return nil, err
	}
	//
	var allMaps []map[string]util.KeyValue
	for _, fileName := range fileNames {
		loadedMap, err := LoadJuiceInput(fileName)
		if err != nil {
			return nil, err
		}
		allMaps = append(allMaps, loadedMap)
	}
	return allMaps, nil
}

// loadFile loads the key-value pairs from a file into a map.
func loadFile(filePath string) (map[string]util.KeyValue, error) {
	file, err := os.Open(filePath)
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
		valueStr := parts[1]
		values := strings.Split(valueStr, ", ") // Assuming values are comma-separated

		resultMap[key] = util.KeyValue{Key: key, Value: values}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return resultMap, nil
}

func GetAllFileListsByPrefix(inputPrefix string, inputDirectory string) ([]string, error) {
	fileNames := make([]string, 0)
	err := filepath.Walk(inputDirectory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasPrefix(info.Name(), inputPrefix) {
			fullPath := filepath.Join(inputDirectory, info.Name())
			fileNames = append(fileNames, fullPath)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fileNames, nil
}

func LoadJuiceInput(filePath string) (map[string]util.KeyValue, error) {
	file, err := os.Open(filePath)
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

// TODO: UNDER DEVELOPMENT
package utils

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type LazyLoader struct {
	file   *os.File
	reader *bufio.Reader
}

func NewLazyLoader(filepath string) (*LazyLoader, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}

	return &LazyLoader{
		file:   file,
		reader: bufio.NewReader(file),
	}, nil
}

func (ll *LazyLoader) GetRowsInRange(start, end int) ([][]string, error) {
	if start > end || start < 0 {
		return nil, fmt.Errorf("invalid range: start=%d, end=%d", start, end)
	}

	var rows [][]string
	currentRow := 0 

	_, err := ll.reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	for {
		line, err := ll.reader.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}

		currentRow++

		if currentRow < start {
			continue 
		}

		if currentRow > end {
			break 
		}

		row := strings.Split(strings.TrimSpace(line), ",")
		rows = append(rows, row)
	}

	return rows, nil
}


func (ll *LazyLoader) Close() error {
	return ll.file.Close()
}

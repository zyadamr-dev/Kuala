package io

import (
	"github.com/zyadamr-dev/Kuala/dataframe/coretypes"
	"github.com/zyadamr-dev/Kuala/dataframe/ops"
	"github.com/zyadamr-dev/Kuala/dataframe/dataframe"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func ReadCSV(filepath string) (*dataframe.DataFrame, error) {
	headers, err := ops.GetColumns(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to get headers: %w", err)
	}

	df := dataframe.New()
	for _, header := range headers {
		df.Columns = append(df.Columns, coretypes.Column[string]{
			Name: header,
			Data: make([]string, 0),
		})
	}

	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	if scanner.Scan() {
		// Discard header line
	}

	rowNum := 0
	for scanner.Scan() {
		rowNum++
		line := scanner.Text()
		fields := strings.Split(line, ",")

		if len(fields) != len(df.Columns) {
			return nil, fmt.Errorf("row %d has %d columns, expected %d",
				rowNum, len(fields), len(df.Columns))
		}

		for i := range df.Columns {
			df.Columns[i].Data = append(df.Columns[i].Data, fields[i])
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return df, nil
}
package ops

import (
	"github.com/zyadamr-dev/Kuala/dataframe/coretypes"
	"fmt"
	"strconv"
	"strings"
)

// Col returns the data of a specific column in the DataFrame by name.
// It checks if the column exists (case-insensitive), then returns its values as a string slice.
// Returns an error if the column does not exist or its data is not found.
func Col(df *coretypes.DataFrame[string], colName string) ([]string, error) {
	headers, err := GetColumnsDataFrame(df)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	exists := false
	for _, header := range headers {
		if strings.EqualFold(header, colName) {
			exists = true
			break
		}
	}

	if !exists {
		return nil, fmt.Errorf("column '%s' not found", colName)
	}

	for _, col := range df.Columns {
		if col.Name == colName {
			return col.Data, nil
		}
	}

	return nil, fmt.Errorf("column '%s' data not found in DataFrame", colName)
}

// Loc returns a slice of rows from the DataFrame using row indices.
// The function accepts a start index and an optional end index. If only one index is provided,
// it returns the row at that index. If a range is given, it returns rows in [start, end] inclusive.
// Returns an error if the range is invalid or out of bounds.
func Loc(df *coretypes.DataFrame[string], start int, end ...int) ([][]string, error) {
	st := start
	ed := start
	if len(end) > 0 {
		ed = end[0]
	}

	if st > ed {
		return nil, fmt.Errorf("start index %d cannot be greater than end index %d", st, ed)
	}	

	if st < 0 || ed >= len(df.Columns[0].Data) {
		return nil, fmt.Errorf("index out of range: start=%d, end=%d", st, ed)
	}

	var result [][]string
	for i := st; i <= ed; i++ {
		row := make([]string, len(df.Columns))
		for j, col := range df.Columns {
			row[j] = col.Data[i]
		}
		result = append(result, row)
	}

	return result, nil
}

func GroupBy(df *coretypes.DataFrame[string], groupCol string, target string) *coretypes.DataFrame[string] {
	grouped := make(map[string][]float64)

	var groupColIndex, targetColIndex int
	foundGroupCol, foundTargetCol := false, false

	for i, col := range df.Columns {
		if col.Name == groupCol {
			groupColIndex = i
			foundGroupCol = true
		}
		if col.Name == target {
			targetColIndex = i
			foundTargetCol = true
		}
	}

	if !foundGroupCol || !foundTargetCol {
		return nil
	}

	groupColData := df.Columns[groupColIndex].Data
	targetColData := df.Columns[targetColIndex].Data

	for i := 0; i < len(groupColData); i++ {
		groupKey := groupColData[i]
		value, err := strconv.ParseFloat(targetColData[i], 64)
		if err != nil {
			continue
		}
		grouped[groupKey] = append(grouped[groupKey], value)
	}

	newCols := []coretypes.Column[string]{{
		Name: groupCol,
		Data: []string{},
	}, {
		Name: target + "_grouped",
		Data: []string{},
	}}

	for k, values := range grouped {
		newCols[0].Data = append(newCols[0].Data, k)
		newCols[1].Data = append(newCols[1].Data, fmt.Sprintf("%v", values))
	}

	return &coretypes.DataFrame[string]{Columns: newCols}
}

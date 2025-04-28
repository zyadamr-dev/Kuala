package ops

import (
	"github.com/zyadamr-dev/Kuala/dataframe/coretypes"
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ColumnCount returns the number of columns in the DataFrame.
func ColumnCount(df *coretypes.DataFrame[string]) int {
	return len(df.Columns)
}

// RowCount returns the number of rows in the DataFrame.
// It assumes that all columns have equal length.
func RowCount(df *coretypes.DataFrame[string]) int {
	return len(df.Columns[0].Data)
}

// Shape returns the shape of the DataFrame in the form (rows, columns).
func Shape(df *coretypes.DataFrame[string]) string {
	return fmt.Sprintf("(%v, %v)", RowCount(df), ColumnCount(df))
}

// Head returns the first n rows of the DataFrame.
// If n is not provided, it defaults to 5.
// Returns an error if the DataFrame is empty.
func Head(df *coretypes.DataFrame[string], n ...int) ([][]string, error) {
	rowNum := 5
	if len(n) > 0 {
		rowNum = n[0]
	}

	if len(df.Columns) == 0 || len(df.Columns[0].Data) == 0 {
		return nil, fmt.Errorf("dataframe is empty")
	}

	maxRows := len(df.Columns[0].Data)
	if rowNum > maxRows {
		rowNum = maxRows
	}

	headRows := make([][]string, rowNum)
	for rowIndex := 0; rowIndex < rowNum; rowIndex++ {
		row := make([]string, len(df.Columns))
		for colIndex, col := range df.Columns {
			row[colIndex] = col.Data[rowIndex]
		}
		headRows[rowIndex] = row
	}

	return headRows, nil
}

// Tail returns the last n rows of the DataFrame.
// If n is not provided, it defaults to 5.
// Returns an error if the DataFrame is empty.
func Tail(df *coretypes.DataFrame[string], n ...int) ([][]string, error) {
	rowNum := 5
	if len(n) > 0 {
		rowNum = n[0]
	}

	if len(df.Columns) == 0 || len(df.Columns[0].Data) == 0 {
		return nil, fmt.Errorf("dataframe is empty")
	}

	totalRows := len(df.Columns[0].Data)
	if rowNum > totalRows {
		rowNum = totalRows
	}

	start := totalRows - rowNum
	tailRows := [][]string{}

	for i := start; i < totalRows; i++ {
		row := []string{}
		for _, col := range df.Columns {
			row = append(row, col.Data[i])
		}
		tailRows = append(tailRows, row)
	}

	return tailRows, nil
}

// GetColumnsDataFrame returns the column names of the DataFrame.
func GetColumnsDataFrame(df *coretypes.DataFrame[string]) ([]string, error) {
	var cols []string
	for _, col := range df.Columns {
		cols = append(cols, col.Name)
	}
	return cols, nil
}

// GetColumns reads the first line of a CSV file and returns it as a slice of column names.
// Returns an error if the file is empty or can't be read.
func GetColumns(filepath string) ([]string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return nil, fmt.Errorf("empty file")
	}

	line := scanner.Text()
	fields := strings.Split(line, ",")

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return fields, nil
}

// DType prints the inferred data type for each column in the DataFrame.
// It checks whether the values are integers, floats, or strings.
func DType(df *coretypes.DataFrame[string]) {
	for _, col := range df.Columns {
		detected := "string"

		for _, val := range col.Data {
			if _, err := strconv.Atoi(val); err == nil {
				detected = "int"
				continue
			} else if _, err := strconv.ParseFloat(val, 64); err == nil {
				detected = "float"
				break
			} else {
				detected = "string"
				break
			}
		}

		fmt.Printf("%s ---> %s\n", col.Name, detected)
	}
}

func Apply(df *coretypes.DataFrame[string], colName string, transform func(string)string) ([]string, error) {
	for _, col := range(df.Columns) {
		if strings.EqualFold(col.Name, colName) {
			transformedData := make([]string, len(col.Data)) 
			for i, val := range(col.Data) {
				transformedData[i] = transform(val)
			}
			return transformedData, nil
		}
	}
	return nil, fmt.Errorf("column '%s' not found", colName)
}
package ops

import (
	"github.com/zyadamr-dev/Kuala/dataframe/coretypes"
	"fmt"
	"math"
	"sort"
	"strconv"
)

func reduction(df *coretypes.DataFrame[string], colName string, init float64, fn func(prev, cur float64) float64) (float64, error) {
	colData, err := Col(df, colName)
	if err != nil {
		return init, fmt.Errorf("error getting column data: %w", err)
	}

	res := init
	for _, val := range colData {
		num, err := strconv.ParseFloat(val, 64)
		if err != nil {
			continue
		}
		res = fn(res, num)
	}

	return res, nil
}

// Sum calculates the total sum of a column's numeric values in the DataFrame.
// You can optionally provide an initial value to start summing from.
func Sum(df *coretypes.DataFrame[string], colName string, init ...float64) (float64, error) {
	acc := 0.0
	if len(init) > 0 {
		acc = init[0]
	}
	return reduction(df, colName, acc, func(prev, cur float64) float64 {
		return prev + cur
	})
}

// Mean returns the arithmetic mean (average) of the numeric values in a column.
// You can optionally provide an initial sum to include in the calculation.
func Mean(df *coretypes.DataFrame[string], colName string, init ...float64) (float64, error) {
	acc := 0.0
	if len(init) > 0 {
		acc = init[0]
	}

	sum, err := Sum(df, colName, acc)
	if err != nil {
		return math.Inf(-1), fmt.Errorf("error getting column data: %w", err)
	}

	return sum / float64(RowCount(df)), nil
}

// Median computes the median value of the numeric data in a column.
// Returns an error if no numeric values are present.
func Median(df *coretypes.DataFrame[string], colName string) (float64, error) {
	colData, err := Col(df, colName)
	if err != nil {
		return 0, fmt.Errorf("error getting column data: %w", err)
	}

	var floatData []float64
	for _, val := range colData {
		num, err := strconv.ParseFloat(val, 64)
		if err != nil {
			continue
		}
		floatData = append(floatData, num)
	}

	if len(floatData) == 0 {
		return 0, fmt.Errorf("no numeric values found in column '%s'", colName)
	}

	sort.Float64s(floatData)
	n := len(floatData)
	if n%2 == 0 {
		return (floatData[n/2-1] + floatData[n/2]) / 2, nil
	}
	return floatData[n/2], nil
}

// Max returns the maximum numeric value for the specified columns in the DataFrame.
// If no column names are provided, it calculates for all columns.
// Returns a float64 if one column is passed, or a slice of float64 for multiple.
func Max(df *coretypes.DataFrame[string], colNames ...string) (any, error) {
	var colsToProcess []coretypes.Column[string]

	if len(colNames) == 0 {
		colsToProcess = df.Columns
	} else {
		for _, name := range colNames {
			found := false
			for _, col := range df.Columns {
				if col.Name == name {
					colsToProcess = append(colsToProcess, col)
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column '%s' not found", name)
			}
		}
	}

	results := make([]float64, len(colsToProcess))
	for i, col := range colsToProcess {
		res, err := reduction(df, col.Name, math.Inf(-1), func(prev, cur float64) float64 {
			if cur > prev {
				return cur
			}
			return prev
		})
		if err != nil {
			return nil, err
		}
		if res == math.Inf(-1) {
			return nil, fmt.Errorf("no valid numeric data in column '%s'", col.Name)
		}
		results[i] = res
	}

	if len(results) == 1 {
		return results[0], nil
	}
	return results, nil
}

// Min returns the minimum numeric value for the specified columns in the DataFrame.
// If no column names are provided, it calculates for all columns.
// Returns a float64 if one column is passed, or a slice of float64 for multiple.
func Min(df *coretypes.DataFrame[string], colNames ...string) (any, error) {
	var colsToProcess []coretypes.Column[string]

	if len(colNames) == 0 {
		colsToProcess = df.Columns
	} else {
		for _, name := range colNames {
			found := false
			for _, col := range df.Columns {
				if col.Name == name {
					colsToProcess = append(colsToProcess, col)
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("column '%s' not found", name)
			}
		}
	}

	results := make([]float64, len(colsToProcess))
	for i, col := range colsToProcess {
		res, err := reduction(df, col.Name, math.Inf(1), func(prev, cur float64) float64 {
			if cur < prev {
				return cur
			}
			return prev
		})
		if err != nil {
			return nil, err
		}
		if res == math.Inf(1) {
			return nil, fmt.Errorf("no valid numeric data in column '%s'", col.Name)
		}
		results[i] = res
	}

	if len(results) == 1 {
		return results[0], nil
	}
	return results, nil
}


func Variance(df *coretypes.DataFrame[string], colName string) float64 {
	mean, _ := Mean(df, colName)

	var column []float64
	for _, col := range df.Columns {
		if col.Name == colName {
			for _, val := range col.Data {
				num, err := strconv.ParseFloat(val, 64)
				if err == nil {
					column = append(column, num)
				}
			}
			break
		}
	}

	if len(column) == 0 {
		return 0 
	}

	var sum float64
	for _, value := range column {
		diff := value - mean
		sum += diff * diff
	}

	return sum / float64(len(column)-1) 
}

func Std(df *coretypes.DataFrame[string], colName string) float64 {
	return math.Sqrt(Variance(df, colName))
}
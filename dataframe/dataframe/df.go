package dataframe

import (
	"github.com/zyadamr-dev/Kuala/dataframe/coretypes"
	"github.com/zyadamr-dev/Kuala/dataframe/ops"
)

// DataFrame wraps a core DataFrame of strings and provides higher-level methods.
type DataFrame struct {
	*coretypes.DataFrame[string]
}

// New creates and returns a new instance of DataFrame.
// It initializes an empty core DataFrame of strings.
func New() *DataFrame {
	return &DataFrame{
		DataFrame: coretypes.New[string](),
	}
}

// Sum calculates the sum of the numeric values in the specified column.
// It delegates the calculation to the ops.Sum function.
func (d *DataFrame) Sum(colName string) (float64, error) {
	return ops.Sum(d.DataFrame, colName)
}

// Mean calculates the arithmetic mean (average) of the numeric values in the specified column.
// It delegates the calculation to the ops.Mean function.
func (d *DataFrame) Mean(colName string) (float64, error) {
	return ops.Mean(d.DataFrame, colName)
}

// Median calculates the median of the numeric values in the specified column.
// It delegates the calculation to the ops.Median function.
func (d *DataFrame) Median(colName string) (float64, error) {
	return ops.Median(d.DataFrame, colName)
}

// Max returns the maximum numeric value for the specified column(s) in the DataFrame.
// If multiple columns are specified, it returns a slice of float64; otherwise, a single float64.
// It delegates the calculation to the ops.Max function.
func (d *DataFrame) Max(colNames ...string) (any, error) {
	return ops.Max(d.DataFrame, colNames...)
}

// Min returns the minimum numeric value for the specified column(s) in the DataFrame.
// If multiple columns are specified, it returns a slice of float64; otherwise, a single float64.
// It delegates the calculation to the ops.Min function.
func (d *DataFrame) Min(colNames ...string) (any, error) {
	return ops.Min(d.DataFrame, colNames...)
}

// Loc returns a slice of rows from the DataFrame between the start and end indices (inclusive).
// The start index is required; an optional end index may be provided.
// It delegates row extraction to the ops.Loc function.
func (d *DataFrame) Loc(start int, end ...int) ([][]string, error) {
	return ops.Loc(d.DataFrame, start, end...)
}

// Col returns the data (as a slice of strings) from the specified column.
// It delegates the retrieval to the ops.Col function.
func (d *DataFrame) Col(colName string) ([]string, error) {
	return ops.Col(d.DataFrame, colName)
}

// DType prints or returns the data type information of each column in the DataFrame.
// It delegates this action to the ops.DType function.
func (d *DataFrame) DType() {
	ops.DType(d.DataFrame)
}

// Head returns the first n rows of the specified column (or columns) from the DataFrame.
// If n is not provided, a default number of rows is returned.
// It delegates to the ops.Head function.
func (d *DataFrame) Head(n ...int) ([][]string, error) {
	return ops.Head(d.DataFrame, n...)
}

// Tail returns the last n rows of the specified column (or columns) from the DataFrame.
// If n is not provided, a default number of rows is returned.
// It delegates to the ops.Tail function.
func (d *DataFrame) Tail(n ...int) ([][]string, error) {
	return ops.Tail(d.DataFrame, n...)
}

// Shape returns a string representing the dimensions (rows x columns) of the DataFrame.
// It delegates the calculation to the ops.Shape function.
func (d *DataFrame) Shape() string {
	return ops.Shape(d.DataFrame)
}

// RowCount returns the number of rows in the DataFrame.
// It delegates the count to the ops.RowCount function.
func (d *DataFrame) RowCount() int {
	return ops.RowCount(d.DataFrame)
}

// ColumnCount returns the number of columns in the DataFrame.
// It delegates the count to the ops.ColumnCount function.
func (d *DataFrame) ColumnCount() int {
	return ops.ColumnCount(d.DataFrame)
}

// Drop removes the specified column from the DataFrame.
// If inplace is provided and set to true, the column is removed from the current DataFrame.
// Otherwise, a new DataFrame with the column removed is returned.
// It delegates to the ops.Drop function.
func (d *DataFrame) Drop(colName string, inplace ...bool) any {
	return ops.Drop(d.DataFrame, colName, inplace...)
}

// Variance calculates the variance of the specified column in the DataFrame.
// It delegates to the ops.Variance function.
func (d *DataFrame) Variance(colName string) float64 {
	return ops.Variance(d.DataFrame, colName)
}

// Std calculates the standard deviation of the specified column in the DataFrame.
// It delegates to the ops.Std function.
func (d *DataFrame) Std(colName string) float64 {
	return ops.Std(d.DataFrame, colName)
}

func (d *DataFrame) Apply(colName string, transform func(string)string) ([]string, error) {
	return ops.Apply(d.DataFrame, colName, transform)
}


func (d *DataFrame) GroupBy(groupCol string, targetCol string) *DataFrame {
	groupedCoreDF := ops.GroupBy(d.DataFrame, groupCol, targetCol)
	return &DataFrame{DataFrame: groupedCoreDF}
}
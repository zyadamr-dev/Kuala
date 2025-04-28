package main

import (
	"github.com/zyadamr-dev/Kuala/dataframe/io"
	"fmt"
	"strconv"
)

func main() {
	df, _ := io.ReadCSV("test.csv")
	fmt.Println(df.Sum("Age"))
	// fmt.Println(df.Col("Score"))
	// fmt.Println(df.Head())
	// fmt.Println(df.Shape())
	// fmt.Println(df.Col("Name"))
	// df.DType()
	// fmt.Println(df.Tail(2))
	// fmt.Println(df.Loc(6, 10))
	// fmt.Println(df.Max("Age"))
	// fmt.Println(df.Variance("Age"))
	// fmt.Println(df.Std("Age"))
	fmt.Println(df.Apply("Age", func(s string) string {
		parsed, err := strconv.ParseInt(s, 10, 64)
		if err != nil {

		}
		if parsed > 30 {
			return s
		}

		return s + " Under 30"
	}))
}

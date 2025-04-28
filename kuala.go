package Kuala

import (
	"github.com/zyadamr-dev/Kuala/dataframe/dataframe"
	"github.com/zyadamr-dev/Kuala/dataframe/io"
)

func ReadCSV(path string) (*dataframe.DataFrame, error) {
    return io.ReadCSV(path)
}

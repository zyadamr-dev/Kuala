package ops

import "github.com/zyadamr-dev/Kuala/dataframe/coretypes"

func Drop(df *coretypes.DataFrame[string], colName string, inplace ...bool) *coretypes.DataFrame[string] {
	erased := false
	if len(inplace) != 0 {
		erased = inplace[0]
	}
	
	target := df
	if !erased {
		target = coretypes.New[string]()
		for _, col := range df.Columns {
			if col.Name != colName {
				target.Columns = append(target.Columns, col)
			}
		}
		return target
	}

	newCols := make([]coretypes.Column[string], 0)
	for _, col := range df.Columns {
		if col.Name != colName {
			newCols = append(newCols, col)
		}
	}
	df.Columns = newCols
	return df
}
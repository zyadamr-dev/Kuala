package tests

import (
	"github.com/zyadamr-dev/Kuala/dataframe/coretypes"
	"github.com/zyadamr-dev/Kuala/dataframe/dataframe"
	"strings"
	"testing"
)


func TestCol(t *testing.T) {
	df := dataframe.New()
	df.Columns = []coretypes.Column[string]{
		{
			Name: "Name",
			Data: []string{"Alice", "Bob", "Charlie"},
		},
		{
			Name: "Age",
			Data: []string{"25", "40", "20"},
		},
	}

	result, err := df.Col("Name")
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	expected := []string{"Alice", "Bob", "Charlie"}

	for i, v := range expected {
		if !strings.EqualFold(result[i], v) {
			t.Errorf("Expected '%v' at index %d, got: '%v'", v, i, result[i])
		}
	}

	t.Log("TestCol passed!")
}


// func TestLoc(t *testing.T) {
// 	df := dataframe.New()
// 	df.Columns = []coretypes.Column[string]{
// 		{
// 			Name: "Name",
// 			Data: []string{"Alice", "Bob", "Charlie"},
// 		},
// 		{
// 			Name: "Age",
// 			Data: []string{"25", "40", "20"},
// 		},
// 	}

// 	result, err := df.Loc(0)
// 	if err != nil {
// 		t.Fatalf("Loc failed: %v", err)
// 	}

// 	expected := []string{"Alice", "25"}

// 	for i, v := range expected {
// 		if !strings.EqualFold(result[i][i], v) {
// 			t.Errorf("Expected '%v' at index %d, got: '%v'", v, i, result[i])
// 		}
// 	}

// 	t.Log("TestLoc passed!")
// }

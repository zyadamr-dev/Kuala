package tests

import (
	"github.com/zyadamr-dev/Kuala/dataframe/coretypes"
	"github.com/zyadamr-dev/Kuala/dataframe/dataframe"
	"math"
	"testing"
)

func floatsEqual(a, b float64) bool {
	const epsilon = 1e-4
	return math.Abs(a-b) < epsilon
}

func TestMean(t *testing.T) {
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

	result, err := df.Mean("Age")
	if err != nil {
		t.Fatalf("Mean failed: %v", err)
	}

	if !floatsEqual(result, 28.3333333333) {
		t.Errorf("Expected 28, got %v", result)
	} else {
		t.Log("TestMean passed!")
	}
}

func TestMedian(t *testing.T) {
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

	result, err := df.Median("Age")
	if err != nil {
		t.Fatalf("Median failed: %v", err)
	}

	if !floatsEqual(result, 25) {
		t.Errorf("Expected 25, got %v", result)
	} else {
		t.Log("TestMedian passed!")
	}
}

func TestSum(t *testing.T) {
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

	result, err := df.Sum("Age")
	if err != nil {
		t.Fatalf("Sum failed: %v", err)
	}

	if !floatsEqual(result, 85) {
		t.Errorf("Expected 85, got %v", result)
	} else {
		t.Log("TestSum passed!")
	}
}

func TestMin(t *testing.T) {
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

	result, err := df.Min("Age")
	if err != nil {
		t.Fatalf("Min failed: %v", err)
	}

	if !floatsEqual(result.(float64), 20) {
		t.Errorf("Expected 20, got %v", result)
	} else {
		t.Log("TestMin passed!")
	}
}

func TestMax(t *testing.T) {
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

	result, err := df.Max("Age")
	if err != nil {
		t.Fatalf("Max failed: %v", err)
	}

	if !floatsEqual(result.(float64), 40) {
		t.Errorf("Expected 40, got %v", result)
	} else {
		t.Log("TestMax passed!")
	}
}

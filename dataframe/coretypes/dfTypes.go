package coretypes

type Column[T any] struct {
	Name string
	Data []T
}

type DataFrame[T any] struct {
	Columns []Column[T]
}

func (d *DataFrame[T]) Sum() any {
	panic("unimplemented")
}

func New[T any]() *DataFrame[T] {
	return &DataFrame[T]{
		Columns: make([]Column[T], 0),
	}
}

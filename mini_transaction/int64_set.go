package mini_transaction

// Int64Set and its iteration interface are retained from gofonia. A set is
// local to its caller; concurrent mutation requires external synchronization.
type Int64Set map[int64]bool

func NewInt64Set() *Int64Set {
	return &Int64Set{}
}

func (is Int64Set) Len() int {
	return len(is)
}

func (is Int64Set) Add(val int64) {
	is[val] = true
}

func (is Int64Set) Remove(val int64) {
	delete(is, val)
}

func (is Int64Set) ToInt64Slice() []int64 {
	result := []int64{}
	for k := range is {
		result = append(result, k)
	}
	return result
}

func (is Int64Set) Each(iter func(int64) bool) {
	for k := range is {
		result := iter(k)
		if result == false {
			break
		}
	}
}

package mapreduce

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeRow struct {
	keySwitch bool
	value     any
}

type fakeReader []fakeRow

func (*fakeReader) TableIndex() int {
	panic("implement me")
}

func (r *fakeReader) KeySwitch() bool {
	return (*r)[0].keySwitch
}

func (*fakeReader) RowIndex() int64 {
	panic("implement me")
}

func (*fakeReader) RangeIndex() int {
	panic("implement me")
}

func (r *fakeReader) Scan(value any) error {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	v.Set(reflect.ValueOf((*r)[0].value))
	return nil
}

func (*fakeReader) MustScan(value any) {
	panic("implement me")
}

func (r *fakeReader) Next() bool {
	if len(*r) <= 1 {
		return false
	}

	*r = (*r)[1:]
	return true
}

func TestGroupKeys(t *testing.T) {
	r := fakeReader{
		{},
		{false, 1},
		{false, 2},
		{true, 3},
		{true, 4},
		{false, 5},
	}

	output := [][]int{}

	_ = GroupKeys(&r, func(r Reader) error {
		var values []int
		for r.Next() {
			var v int
			_ = r.Scan(&v)
			values = append(values, v)
		}
		output = append(output, values)
		return nil
	})

	require.Equal(t, [][]int{{1, 2}, {3}, {4, 5}}, output)
}

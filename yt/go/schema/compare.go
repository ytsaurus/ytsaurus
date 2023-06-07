package schema

import (
	"bytes"
	"fmt"
	"reflect"
)

func typeOrder(v any) int {
	switch v.(type) {
	case nil:
		return 0x2

	case int, int8, int16, int32, int64:
		return 0x3

	case uint, uint8, uint16, uint32, uint64:
		return 0x4

	case float64, float32:
		return 0x5

	case bool:
		return 0x6

	case []byte, string:
		return 0x10

	default:
		panic(fmt.Sprintf("order is not defined for type %T", v))
	}
}

// CompareRows orders two set of row keys according to schema.
//
// Keys may contain:
//   - nil
//   - bool
//   - string
//   - []byte
//   - float32, float64
//   - int, int32, int64, int16, int8
//   - uint, uint32, uint64, uint16, uint8
//
// Passing any other value to this function results in panic.
//
// Currently, this function does not handle NaN-s correctly.
//
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func CompareRows(a, b []any, s *Schema) int {
	for i := 0; i < len(a) || i < len(b); i++ {
		if i >= len(a) {
			return -1
		}

		if i >= len(b) {
			return 1
		}

		aType := typeOrder(a[i])
		bType := typeOrder(b[i])

		if aType < bType {
			return -1
		} else if aType > bType {
			return 1
		}

		switch aType {
		case 0x2:

		case 0x3:
			aValue := reflect.ValueOf(a[i]).Int()
			bValue := reflect.ValueOf(b[i]).Int()

			if aValue < bValue {
				return -1
			} else if bValue < aValue {
				return 1
			}

		case 0x4:
			aValue := reflect.ValueOf(a[i]).Uint()
			bValue := reflect.ValueOf(b[i]).Uint()

			if aValue < bValue {
				return -1
			} else if bValue < aValue {
				return 1
			}

		case 0x5:
			aValue := reflect.ValueOf(a[i]).Float()
			bValue := reflect.ValueOf(b[i]).Float()

			if aValue < bValue {
				return -1
			} else if bValue < aValue {
				return 1
			}

		case 0x6:
			aValue := reflect.ValueOf(a[i]).Bool()
			bValue := reflect.ValueOf(b[i]).Bool()

			if !aValue && bValue {
				return -1
			} else if !bValue && aValue {
				return 1
			}

		case 0x10:
			asBytes := func(v any) []byte {
				switch v := v.(type) {
				case string:
					return []byte(v)

				case []byte:
					return v

				default:
					panic("unreachable")
				}

			}

			aValue := asBytes(a[i])
			bValue := asBytes(b[i])

			result := bytes.Compare(aValue, bValue)
			if result == -1 {
				return -1
			} else if result == 1 {
				return 1
			}

		default:
			panic("unreachable")
		}
	}

	return 0
}

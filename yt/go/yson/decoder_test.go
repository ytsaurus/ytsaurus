package yson

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"testing"
)

func testDecoder(t *testing.T, input []byte, expected any, generic bool) {
	t.Helper()

	t.Logf("testing input: %#v", string(input))
	t.Logf("expected output: %#v", expected)

	var copyPtr any
	if !generic {
		copyPtr = reflect.New(reflect.TypeOf(expected)).Interface()
	} else {
		var value any
		copyPtr = &value
	}

	err := Unmarshal(input, copyPtr)
	if err != nil {
		t.Fatalf("decoder error: %v", err)
	}

	copy := reflect.ValueOf(copyPtr).Elem().Interface()
	if !reflect.DeepEqual(copy, expected) {
		t.Logf("decoder returned incorrect value")
		t.Logf("expected: %#v (%T)", expected, expected)
		t.Logf("actual: %#v (%T))", copy, copy)
		t.Fail()
	}
}

type testCase struct {
	input    string
	expected any
}

func runTests(t *testing.T, tests []testCase) {
	t.Helper()

	for i, testCase := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Helper()

			testDecoder(t, []byte(testCase.input), testCase.expected, false)
		})
	}
}

func runBadInputTests(t *testing.T, tests []testCase) {
	t.Helper()

	for i, testCase := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Logf("checking input %q", testCase.input)

			err := Unmarshal([]byte(testCase.input), testCase.expected)
			if err == nil {
				t.Fatalf("bad input accepted without error")
			}
		})
	}
}

func TestDecodeIntTypes(t *testing.T) {
	runTests(t, []testCase{
		{"1", 1},
		{" 1 ", 1},
		{"1u", uint(1)},
		{" 1u ", uint(1)},
		{" -1 ", -1},
		{"1u", uint(1)},

		{"<a=1>1", 1},
		{" < a = 1 > 1", 1},

		{"1u", uint8(1)},
		{"1u", uint16(1)},
		{"1u", uint32(1)},
		{"1u", uint64(1)},

		{"-1", int8(-1)},
		{"-1", int16(-1)},
		{"-1", int32(-1)},
		{"-1", int64(-1)},

		{"\x02\x00", 0},

		{
			string(append([]byte{0x02}, testBigVarint...)),
			int64(math.MaxInt64),
		},
		{
			string(append([]byte{0x02}, testMediumVarint...)),
			int64(math.MaxInt32),
		},

		{"\x06\x00", uint(0)},

		{
			string(append([]byte{0x06}, testBigUvarint...)),
			uint64(math.MaxUint64),
		},

		{
			string(append([]byte{0x06}, testMediumUvarint...)),
			uint64(math.MaxUint32),
		},
	})
}

func TestDecodeInvalidInts(t *testing.T) {
	p := new(int)

	runBadInputTests(t, []testCase{
		{"", p},
		{" 1uu", p},
		{" 112341234123412341234132", p},
		{" 1 1", p},
		{"-1u", p},
	})
}

func TestDecodeStringTypes(t *testing.T) {
	binaryString := make([]byte, 11)
	binaryString[0] = 0x1
	binaryString = binaryString[:1+binary.PutVarint(binaryString[1:], 12345)]

	data := make([]byte, 12345)
	for i := range data {
		data[i] = 'z'
	}

	binaryString = append(binaryString, data...)

	runTests(t, []testCase{
		{"a", "a"},
		{"a", []byte("a")},
		{` "\n" `, "\n"},
		{string(binaryString), data},
	})
}

func TestDecodeInvalidStrings(t *testing.T) {
	p := new(string)

	runBadInputTests(t, []testCase{
		{"-a", p},
		{"a b", p},
		{`"\"`, p},
		{`"\"`, p},
	})
}

func TestDecodeBools(t *testing.T) {
	runTests(t, []testCase{
		{"%true", true},
		{"%false", false},
		{string([]byte{0x04}), false},
		{string([]byte{0x05}), true},
	})
}

func TestDecodeFloats(t *testing.T) {
	runTests(t, []testCase{
		{"1.25", 1.25},
		{"1.25", float32(1.25)},
		{"1e9", 1e9},
		{"%inf", math.Inf(1)},
		{"%+inf", math.Inf(1)},
		{"%-inf", math.Inf(-1)},
		{
			string([]byte{0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}),
			0.0,
		},
	})
}

func TestDecodeList(t *testing.T) {
	runTests(t, []testCase{
		{"[]", []int(nil)},
		{"[1]", []int{1}},
		{"[1;]", []int{1}},
		{"[1; 2; 3; 4; 5]", []int{1, 2, 3, 4, 5}},
		{"[1;2;3;4;5;]", []int{1, 2, 3, 4, 5}},
	})
}

func TestDecodeRawValue(t *testing.T) {
	runTests(t, []testCase{
		{"[]", RawValue("[]")},
		{"<a=1>{b=2}", RawValue("<a=1>{b=2}")},
	})
}

func TestDecodeNan(t *testing.T) {
	var f float64

	err := Unmarshal([]byte("%nan"), &f)
	if err != nil {
		t.Fatalf("decoder error: %s", err)
	}

	if !math.IsNaN(f) {
		t.Fatalf("failed to decode NaN value")
	}
}

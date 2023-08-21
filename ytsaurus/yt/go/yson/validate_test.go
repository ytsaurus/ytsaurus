package yson

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

func testValidation(t *testing.T, expectValid bool, validate func([]byte) error, inputs []string) {
	t.Helper()

	for i, input := range inputs {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Logf("testing input %q", input)
			err := validate([]byte(input))
			if expectValid {
				if err != nil {
					t.Fatalf("validation failed: %s", err.Error())
				}
			} else {
				if err == nil {
					t.Fatalf("validation accepted bad input")
				}
			}
		})
	}
}

func TestValidateSmallValues(t *testing.T) {
	testValidation(t, true, Valid, []string{
		"1",
		"<>1",
		"<a=1>1",
		"<a=1;>1",
		"<>{}",
		"<>[]",
		"[{}; []]",
		"[<>1]",
		"[<>1;<>2]",
	})
}

func TestValidateListFragment(t *testing.T) {
	testValidation(t, true, ValidListFragment, []string{
		"1",
		"<>1;",
		"<a=1>1;<a=1;>1;<>{};<>[];[{}; []];[<>1];[<>1;<>2];",
	})
}

func TestValidateBadListFragment(t *testing.T) {
	testValidation(t, false, ValidListFragment, []string{
		"1;<>",
		"1;\"",
	})
}

func TestValidateMapFragment(t *testing.T) {
	testValidation(t, true, ValidMapFragment, []string{
		"k=1",
		"k=<>1;",
		"k=<a=1>1;k=<a=1;>1;k=<>{};k=<>[];k=[{}; []];k=[<>1];k=[<>1;<>2];",
	})
}

func TestValidateBadMapFragment(t *testing.T) {
	testValidation(t, false, ValidMapFragment, []string{
		"k=",
		"k=1;b",
		"k=1;b=",
		"k=1;c",
		"k=1;c;",
	})
}

func TestValidateCatchesErrors(t *testing.T) {
	testValidation(t, false, Valid, []string{
		"",
		"  ",
		"0>0",
		"<><>1",
		"<{}>",
		"<[]>",
		"{>",
		"<a=;>1",
		"<a=1;;>1",
		"1;",
		"{1=1}",
		"<1=1>1",
	})
}

var testBigVarint []byte
var testMediumVarint []byte

var testBigUvarint []byte
var testMediumUvarint []byte

func init() {
	testBigVarint = make([]byte, 10)
	binary.PutVarint(testBigVarint, math.MaxInt64)

	testMediumVarint = make([]byte, 10)
	testMediumVarint = testMediumVarint[:binary.PutVarint(testMediumVarint, math.MaxInt32)]

	testBigUvarint = make([]byte, 10)
	binary.PutUvarint(testBigUvarint, math.MaxUint64)

	testMediumUvarint = make([]byte, 10)
	testMediumUvarint = testMediumUvarint[:binary.PutUvarint(testMediumUvarint, math.MaxUint32)]
}

func TestValidateBinaryLiterals(t *testing.T) {
	testValidation(t, true, Valid, []string{
		"\x03\x00\x00\x00\x00\x00\x00\x00\x00",

		"\x02\x00",
		string(append([]byte{0x02}, testBigVarint...)),
		string(append([]byte{0x02}, testMediumVarint...)),

		"\x06\x00",
		string(append([]byte{0x06}, testBigUvarint...)),
		string(append([]byte{0x06}, testMediumUvarint...)),

		"\x01\x00",
		"\x01\x02\xff",
	})
}

func TestValidateCatchesErrorsInBinaryLiterals(t *testing.T) {
	testValidation(t, false, Valid, []string{
		"\x03",
		"\x03\x00\x00\x00\x00\x00\x00\x00",

		"\x01\x02",
		"\x02\x00\x00",
	})
}

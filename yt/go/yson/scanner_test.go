package yson

import (
	"testing"
)

func checkScannerRaw(t *testing.T, input []byte, opcodes []opcode, literals []literalKind) {
	t.Helper()

	var s scanner
	s.reset(StreamNode)

	for i, c := range input {
		op := s.step(&s, c)

		if op != opcodes[i] {
			if op == scanError {
				t.Logf("error in scanner: %s", s.err.Error())
			}
			t.Fatalf("invalid opcode in string %q at position %d (expected %v, actual %v)",
				string(input), i, string(opcodes[i]), string(op))

		}
		if op == scanError {
			return
		}

		if literals[i] != ' ' && literals[i] != s.lastLiteral {
			t.Fatalf("invalid literal in string %q at position %d (expected %v, actual %v)",
				string(input), i, string(literals[i]), string(s.lastLiteral))
		}
	}

	op := s.eof()
	if op == scanError && opcodes[len(input)] != scanError {
		t.Logf("error in scanner: %s", s.err.Error())
	}

	if op != opcodes[len(input)] {
		t.Fatalf("invalid opcode in string %q after eof (expected %v, actual %v)",
			string(input), string(opcodes[len(input)]), string(op))
	}

	if literals[len(input)] != ' ' && literals[len(input)] != s.lastLiteral {
		t.Fatalf("invalid literal in string %q after eof (expected %v, actual %v)",
			string(input), string(literals[len(input)]), string(s.lastLiteral))
	}
}

func checkScanner(t *testing.T, input, opcodes, literals string) {
	t.Helper()

	var intOpcodes []opcode
	for _, op := range opcodes {
		intOpcodes = append(intOpcodes, opcode(op))
	}

	var intLiterals []literalKind
	for _, literal := range literals {
		intLiterals = append(intLiterals, literalKind(literal))
	}

	t.Logf("Checking string    %q", input)
	t.Logf("Expecting opcodes  %q", opcodes)
	t.Logf("Expecting literals %q", literals)
	checkScannerRaw(t, []byte(input), intOpcodes, intLiterals)
}

func TestScanBool(t *testing.T) {
	checkScanner(t,
		"%true",
		"occcce",
		"     b",
	)

	checkScanner(t,
		"%false",
		"occccce",
		"      b",
	)

	checkScanner(t,
		"\x04",
		"oe",
		" 4",
	)

	checkScanner(t,
		"\x05",
		"oe",
		" 5",
	)
}

func TestScanIntegers(t *testing.T) {
	checkScanner(t,
		"1",
		"oe",
		" i",
	)

	checkScanner(t,
		" 1 ",
		"soee",
		"  i ",
	)

	checkScanner(t,
		" 1u ",
		"socee",
		"   u ",
	)

	checkScanner(t,
		"+1",
		"oce",
		"  i",
	)

	checkScanner(t,
		"-1",
		"oce",
		"  i",
	)

	checkScanner(t,
		"1u",
		"oce",
		"  u",
	)

	checkScanner(t,
		"+1u",
		"occe",
		"   u",
	)

	// this error is validated in decoder
	checkScanner(t,
		"-1u",
		"occe",
		"   u",
	)
}

func TestScanFloats(t *testing.T) {
	checkScanner(t,
		"1.",
		"oce",
		"  f",
	)

	checkScanner(t,
		"+1.",
		"occe",
		"   f",
	)

	checkScanner(t,
		"-1.",
		"occe",
		"   f",
	)

	checkScanner(t,
		"1e",
		"ocx",
		"   ",
	)

	checkScanner(t,
		"1e1",
		"occe",
		"   f",
	)

	checkScanner(t,
		"1e+1",
		"occce",
		"    f",
	)

	checkScanner(t,
		"1e-1",
		"occce",
		"    f",
	)

	checkScanner(t,
		"1.e-1",
		"occcce",
		"     f",
	)

	checkScanner(t,
		"1.1e-1",
		"occccce",
		"      f",
	)
}

func TestScanSpecialFloats(t *testing.T) {
	checkScanner(t,
		"%nan",
		"occce",
		"    f",
	)

	checkScanner(t,
		"%inf",
		"occce",
		"    f",
	)

	checkScanner(t,
		"%+inf",
		"occcce",
		"     f",
	)

	checkScanner(t,
		"%-inf",
		"occcce",
		"     f",
	)
}

func TestScanIdentifier(t *testing.T) {
	checkScanner(t,
		"a",
		"oe",
		" a",
	)

	checkScanner(t,
		"a1234",
		"occcce",
		"     a",
	)

	checkScanner(t,
		"a.-_",
		"occce",
		"    a",
	)
}

func TestScanLiteralString(t *testing.T) {
	checkScanner(t,
		`""`,
		"oce",
		`  "`,
	)

	checkScanner(t,
		`"a"`,
		"occe",
		`   "`,
	)

	checkScanner(t,
		`"\""`,
		"occce",
		`    "`,
	)

	checkScanner(t,
		`"\\"`,
		"occce",
		`    "`,
	)
}

func TestScanCompositeNodes(t *testing.T) {
	checkScanner(t,
		"<>{a=<>[1;2];b=1;}",
		"<>{ks<>[oso]sksos}e",
		"    a    i i  a i  ",
	)

	checkScanner(t,
		"[[[c]0",
		"[[[o]x",
		"    a ",
	)

	checkScanner(t,
		"[[[[[[[[[[[[{[",
		"[[[[[[[[[[[[{x",
		"               ",
	)

	checkScanner(t,
		"[[[[<[",
		"[[[[<x",
		"      ",
	)

	checkScanner(t,
		"{S2=<>",
		"{kcs<>x",
		"       ")
}

func TestScanBinaryFloat(t *testing.T) {
	checkScanner(t,
		string([]byte{0x03}),
		"ox",
		"   3",
	)
}

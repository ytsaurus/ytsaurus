package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"go/parser"
	"os"
	"strings"
)

type stringList []string

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

var (
	flagInterfaces stringList
	flagOutput     = flag.String("output", "", "path to output file")
)

func fatalf(msg string, args ...any) {
	line := fmt.Sprintf(msg, args...)
	if line[len(line)-1] != '\n' {
		line += "\n"
	}

	_, _ = os.Stderr.WriteString(line)
	os.Exit(2)
}

func main() {
	flag.Var(&flagInterfaces, "interface", "path to interface.go file (can be specified multiple times)")
	flag.Parse()
	if len(flagInterfaces) == 0 || *flagOutput == "" {
		flag.Usage()
		os.Exit(2)
	}

	var buf bytes.Buffer

	emitHeader(&buf)
	for _, filePath := range flagInterfaces {
		node, err := parser.ParseFile(fset, filePath, nil, parser.ParseComments)
		if err != nil {
			fatalf("ast parsing file %q error: %v", filePath, err)
		}

		f, err := parseFile(node)
		if err != nil {
			fatalf("parsing file %q error: %v", filePath, err)
		}

		if err = emit(f, &buf); err != nil {
			fatalf("emit file %q error: %v", filePath, err)
		}
	}

	fmtbuf, err := format.Source(buf.Bytes())
	if err != nil {
		fatalf("source error: %v", err)
	}

	if err = os.WriteFile(*flagOutput, fmtbuf, 0644); err != nil {
		fatalf("write file error: %v", err)
	}
}

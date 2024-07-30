package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"go/parser"
	"os"
)

var (
	flagInterface = flag.String("interface", "", "path to interface.go file")
	flagOutput    = flag.String("output", "", "path to output file")
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
	flag.Parse()
	if *flagInterface == "" || *flagOutput == "" {
		flag.Usage()
		os.Exit(2)
	}

	node, err := parser.ParseFile(fset, *flagInterface, nil, parser.ParseComments)
	if err != nil {
		fatalf("%v", err)
	}

	f, err := parseFile(node)
	if err != nil {
		fatalf("%v", err)
	}

	var buf bytes.Buffer
	if err = emit(f, &buf); err != nil {
		fatalf("emit error: %v", err)
	}

	fmtbuf, err := format.Source(buf.Bytes())
	if err != nil {
		fatalf("source error: %v", err)
	}

	if err = os.WriteFile(*flagOutput, fmtbuf, 0644); err != nil {
		fatalf("write file error: %v", err)
	}
}

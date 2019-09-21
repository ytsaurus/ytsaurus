package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var (
	ytRoot = flag.String("yt-root", "", "root of yt source code directory")
)

func findHeaders() (headers []string) {
	err := filepath.Walk(*ytRoot,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if !info.IsDir() && strings.HasSuffix(path, ".h") {
				headers = append(headers, path)
			}

			return nil
		})

	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "walk: %+v\n", err)
		os.Exit(1)
	}

	return
}

type errorCode struct {
	name  string
	value int
}

var (
	reStart = regexp.MustCompile(`DEFINE_ENUM\(EErrorCode,`)
	reValue = regexp.MustCompile(`\(\((\w+)\)\s*\((\d+)\)\)`)
	reEnd   = regexp.MustCompile(`\);`)
)

func scanErrorCodes(lines []string) (ec []errorCode) {
	inside := false

	for _, l := range lines {
		if inside {
			if match := reValue.FindStringSubmatch(l); match != nil {
				code, _ := strconv.Atoi(match[2])

				ec = append(ec, errorCode{
					name:  match[1],
					value: code,
				})
			} else if reEnd.MatchString(l) {
				inside = false
			}
		} else {
			if reStart.MatchString(l) {
				inside = true
			}
		}
	}

	return
}

func readLines(path string) (lines []string) {
	file, err := os.Open(path)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "open: %+v\n", err)
		os.Exit(1)
	}
	defer func() { _ = file.Close() }()

	reader := bufio.NewReader(file)

	var line string
	for {
		line, err = reader.ReadString('\n')
		if err != nil && err != io.EOF {
			_, _ = fmt.Fprintf(os.Stderr, "readline: %+v\n", err)
			os.Exit(1)
		} else if err == io.EOF {
			break
		}

		lines = append(lines, line)
	}

	return
}

func main() {
	flag.Parse()

	var ec []errorCode
	for _, h := range findHeaders() {
		lines := readLines(h)
		ec = append(ec, scanErrorCodes(lines)...)
	}

	rename := func(e errorCode) errorCode {
		e.name = strings.Replace(e.name, "RPC", "Rpc", -1)

		switch e.value {
		case 108:
			return errorCode{"RPCRequestQueueSizeLimitExceeded", e.value}
		case 109:
			return errorCode{"RPCAuthenticationError", e.value}
		case 802:
			return errorCode{"InvalidElectionEpoch", e.value}
		case 800:
			return errorCode{"InvalidElectionState", e.value}
		case 1915:
			return errorCode{"APINoSuchOperation", e.value}
		default:
			return e
		}
	}

	for i, e := range ec {
		ec[i] = rename(e)
	}

	sort.Slice(ec, func(i, j int) bool {
		return ec[i].value < ec[j].value
	})

	fmt.Println("package yterrors")
	fmt.Println("import \"fmt\"")

	fmt.Println("const (")
	for _, e := range ec {
		fmt.Printf("Code%s ErrorCode = %d\n", e.name, e.value)
	}
	fmt.Println(")")

	fmt.Println("func (e ErrorCode) String() string {")
	fmt.Println("switch e {")

	for _, e := range ec {
		fmt.Printf("case Code%s: return %q\n", e.name, e.name)
	}

	fmt.Println("default:")

	unknown := "return fmt.Sprintf(\"UnknownCode%d\", int(e))"
	fmt.Println(unknown)

	fmt.Println("}")
	fmt.Println("}")
}

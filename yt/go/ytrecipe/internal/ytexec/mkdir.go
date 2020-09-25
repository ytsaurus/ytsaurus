package ytexec

import (
	"fmt"
	"os"
)

func IsMkdir() bool {
	return len(os.Args) > 2 && os.Args[1] == "-mkdir"
}

func Mkdir() int {
	err := os.MkdirAll(os.Args[2], 0777)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%+v\n", err)
		return 1
	}

	return 0
}

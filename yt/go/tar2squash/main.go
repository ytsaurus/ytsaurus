package main

import (
	"archive/tar"
	"fmt"
	"os"
	"time"

	"a.yandex-team.ru/yt/go/tar2squash/internal/squashfs"
	"a.yandex-team.ru/yt/go/tar2squash/internal/tar2squash"
)

func doConvert(outputPath string) error {
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}

	w, err := squashfs.NewWriter(f, time.Now())
	if err != nil {
		return err
	}

	tr := tar.NewReader(os.Stdin)
	if err := tar2squash.Convert(w, tr); err != nil {
		return err
	}

	return f.Close()
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: tar2squash IMG\n")
		os.Exit(1)
	}

	if err := doConvert(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "tar2squash: %v\n", err)
		os.Exit(1)
	}
}

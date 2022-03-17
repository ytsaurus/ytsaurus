package main

import (
	"archive/tar"
	"compress/gzip"
	"flag"
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

	gz, err := gzip.NewReader(os.Stdin)
	if err != nil {
		return err
	}

	tr := tar.NewReader(gz)
	if err := tar2squash.Convert(w, tr); err != nil {
		return err
	}

	if *flagWeakSync {
		if err := weakSync(f); err != nil {
			return err
		}
	}

	return f.Close()
}

var (
	flagWeakSync = flag.Bool("weak-sync", false, "")
)

func main() {
	flag.Parse()

	if len(flag.Args()) != 1 {
		fmt.Fprintf(os.Stderr, "usage: tar2squash IMG\n")
		os.Exit(1)
	}

	if err := doConvert(flag.Arg(0)); err != nil {
		fmt.Fprintf(os.Stderr, "tar2squash: %v\n", err)
		os.Exit(1)
	}
}

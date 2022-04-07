package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"

	"a.yandex-team.ru/yt/go/tar2squash/internal/squashfs"
	"a.yandex-team.ru/yt/go/tar2squash/internal/tar2squash"
)

type bufferedFile struct {
	ws io.WriteSeeker

	buf *bufio.Writer
	off int64
}

func (bf *bufferedFile) Write(p []byte) (int, error) {
	n, err := bf.buf.Write(p)
	bf.off += int64(n)
	return n, err
}

func (bf *bufferedFile) Seek(offset int64, whence int) (int64, error) {
	if offset == 0 && whence == io.SeekCurrent {
		return bf.off, nil
	}

	if err := bf.buf.Flush(); err != nil {
		return 0, err
	}

	off, err := bf.ws.Seek(offset, whence)
	if err != nil {
		return off, err
	}

	bf.off = off
	return off, nil
}

func (bf *bufferedFile) Flush() error {
	return bf.buf.Flush()
}

func doConvert(outputPath string) error {
	f, err := os.Create(outputPath)
	if err != nil {
		return err
	}

	bf := &bufferedFile{
		ws:  f,
		buf: bufio.NewWriterSize(f, 1<<20),
	}

	w, err := squashfs.NewWriter(bf, time.Now())
	if err != nil {
		return err
	}

	gz, err := gzip.NewReader(bufio.NewReaderSize(os.Stdin, 1<<18))
	if err != nil {
		return err
	}

	opts := tar2squash.Options{}
	if *flagXattrFilter != "" {
		re, err := regexp.Compile(*flagXattrFilter)
		if err != nil {
			return err
		}

		opts.XattrFilter = func(name string) bool {
			return re.MatchString(name)
		}
	}

	pr, pw := io.Pipe()
	go func() {
		_, err := io.Copy(pw, gz)
		if err != nil {
			_ = pw.CloseWithError(err)
		} else {
			_ = pw.Close()
		}
	}()

	tr := tar.NewReader(bufio.NewReaderSize(pr, 1<<18))
	if err := tar2squash.Convert(w, tr, opts); err != nil {
		return err
	}

	if *flagWeakSync {
		if err := weakSync(f); err != nil {
			return err
		}
	}

	if err := bf.Flush(); err != nil {
		return err
	}

	return f.Close()
}

var (
	flagWeakSync    = flag.Bool("weak-sync", false, "")
	flagXattrFilter = flag.String("xattr-filter", "", "")
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

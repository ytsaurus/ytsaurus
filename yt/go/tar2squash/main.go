package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"time"

	"a.yandex-team.ru/library/go/squashfs"
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

func readAtMost(r io.Reader, buf []byte) (int, error) {
	n := 0

	for n != len(buf) {
		d, err := r.Read(buf[n:])
		n += d

		if err == io.EOF {
			return n, nil
		} else if err != nil {
			return n, err
		}
	}

	return n, nil
}

func readInput() (io.Reader, error) {
	fileHead := make([]byte, 257+5)
	n, err := readAtMost(os.Stdin, fileHead)
	if err != nil {
		return nil, err
	}
	fileHead = fileHead[:n]

	checkSignature := func(offset int, magic []byte) bool {
		if offset+len(magic) > len(fileHead) {
			return false
		}

		return bytes.Equal(fileHead[offset:offset+len(magic)], magic)
	}

	in := io.MultiReader(bytes.NewBuffer(fileHead), bufio.NewReaderSize(os.Stdin, 1<<18))
	pipeInput := func(name string, args ...string) (io.Reader, error) {
		pr, pw := io.Pipe()

		cmd := exec.Command(name, args...)
		cmd.Stdin = in
		cmd.Stdout = pw
		cmd.Stderr = os.Stderr

		go func() {
			if err := cmd.Run(); err != nil {
				_ = pw.CloseWithError(err)
			} else {
				_ = pw.Close()
			}
		}()

		return pr, nil
	}

	if checkSignature(0, []byte("\xFD7zXZ\x00")) {
		return pipeInput("/usr/bin/xz", "-d")
	} else if checkSignature(0, []byte("\x1F\x8B\x08")) {
		return pipeInput("/bin/gzip", "-d")
	} else if checkSignature(0, []byte("\x28\xB5\x2F\xFD")) {
		return pipeInput("/usr/bin/zstd", "-d")
	} else if checkSignature(0, []byte("hsqs")) {
		return nil, fmt.Errorf("squashfs format is not supported")
	} else if checkSignature(257, []byte("ustar")) {
		return in, nil
	} else {
		return nil, fmt.Errorf("can't detect archive signature")
	}
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

	in, err := readInput()
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

	tr := tar.NewReader(bufio.NewReaderSize(in, 1<<18))
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

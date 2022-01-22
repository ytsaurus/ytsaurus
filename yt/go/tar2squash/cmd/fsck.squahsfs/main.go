package main

import (
	"fmt"
	"os"

	"a.yandex-team.ru/yt/go/tar2squash/internal/squashfs"
)

func do() error {
	f, err := os.Open(os.Args[1])
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := squashfs.NewReader(f)
	if err != nil {
		return err
	}

	var walkFS func(squashfs.Inode) error
	walkFS = func(inode squashfs.Inode) error {
		st, err := r.Stat("", inode)
		if err != nil {
			return err
		}

		fmt.Fprintf(os.Stderr, "inode: %#v\n", st)

		dir, err := r.ReaddirNoStat(inode)
		if err != nil {
			return err
		}

		for _, entry := range dir {
			fmt.Fprintf(os.Stderr, "entry: name=%s %#v\n", entry.Name(), entry)

			if entry.IsDir() {
				if err := walkFS(entry.(*squashfs.FileInfo).Inode); err != nil {
					return err
				}
			}
		}

		return nil
	}

	return walkFS(r.RootInode())
}

func main() {
	if err := do(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

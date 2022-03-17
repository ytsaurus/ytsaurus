//go:build !linux

package main

import "os"

func weakSync(f *os.File) error {
	return nil
}

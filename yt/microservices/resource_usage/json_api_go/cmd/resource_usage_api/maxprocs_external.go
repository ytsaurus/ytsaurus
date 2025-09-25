//go:build !internal
// +build !internal

package main

import "runtime"

func AdjustMaxProcs() int {
	n := runtime.NumCPU()
	runtime.GOMAXPROCS(n)

	return n
}

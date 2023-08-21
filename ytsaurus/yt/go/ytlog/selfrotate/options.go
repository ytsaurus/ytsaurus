package selfrotate

import (
	"fmt"
	"time"
)

type Compress int

const (
	// Never compress output files.
	CompressNone Compress = iota

	// Compress file in background after rotation.
	//
	// This policy is useful when used together with default zap core, because log file is guaranteed to be consistent,
	// even if go program crashes in the middle of file operation.
	CompressDelayed

	// Compress output lines before writing them to file.
	//
	// This policy applies gzip compression to each individual buffer passed to Write(). Only useful together with
	// async zap core that does log line batching.
	CompressAtomic
)

type RotateInterval time.Duration

const (
	RotateHourly = RotateInterval(time.Hour)
	RotateDaily  = RotateInterval(time.Hour * 24)
)

type Options struct {
	// Name is path to first output file.
	Name string

	// MaxKeep is maximal number of log files to keep.
	//
	// If MaxKeep == 0, count based rotation is disabled.
	MaxKeep int

	// MaxSize limits number of log files to keep, based of total file size in bytes.
	//
	// If MaxSize == 0, size based rotation is disabled.
	MaxSize int64

	// MinFreeSpace limits number of log files based of free space available on log file partition.
	MinFreeSpace float64

	// Compress configures log compression policy.
	Compress Compress

	// RotateInterval specifies rotation interval for logs.
	//
	// By default, rotate every hour.
	RotateInterval RotateInterval
}

func (o *Options) valid() error {
	if o.Name == "" {
		return fmt.Errorf("log name not specified")
	}

	return nil
}

func (o *Options) setDefault() {
	if o.RotateInterval == 0 {
		o.RotateInterval = RotateHourly
	}
}

package yt

import (
	"errors"
)

var (
	ErrTxCommitted = errors.New("transaction is already committed")
	ErrTxAborted   = errors.New("transaction is already aborted")
)

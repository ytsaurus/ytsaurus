package yt

import "errors"

var (
	// ErrTxCommitted is returned from transaction methods after Commit() was called.
	//
	// Seeing this error does not guarantee that transaction is really committed.
	ErrTxCommitted = errors.New("transaction is already committed")

	// ErrTxAborted is returned from transaction methods after Abort() was called.
	//
	// Seeing this error does not guarantee that transaction is really aborted.
	ErrTxAborted = errors.New("transaction is already aborted")
)

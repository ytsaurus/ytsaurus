package yt

import (
	"fmt"
	"io"

	"a.yandex-team.ru/yt/go/yt"
)

type fileWriter struct {
	io.WriteCloser
	tx        yt.Tx
	size      int64
	closed    bool
	cancelled bool
	committed bool
}

func (fw *fileWriter) Write(p []byte) (int, error) {
	if fw.closed {
		return 0, fmt.Errorf("already closed")
	} else if fw.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	n, err := fw.WriteCloser.Write(p)
	fw.size += int64(n)
	return n, err
}

func (fw *fileWriter) Size() int64 {
	return fw.size
}

func (fw *fileWriter) Cancel() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	} else if fw.cancelled {
		return fmt.Errorf("already cancelled")
	} else if fw.committed {
		return fmt.Errorf("already committed")
	}

	fw.cancelled = true
	if err := fw.tx.Abort(); err != nil {
		return err
	}

	return nil
}

// Commit is some kind of a strange state.
// Sometimes tests call Commit() only with expectation that a file or a chunk of a file was uploaded and do nothing more with Writer.
// And sometimes tests call Commit() and then Close() as a next action.
func (fw *fileWriter) Commit() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	} else if fw.cancelled {
		return fmt.Errorf("already cancelled")
	} else if fw.committed {
		return fmt.Errorf("already committed")
	}

	fw.committed = true
	if err := fw.closeAndCommit(); err != nil {
		return err
	}

	return nil
}

// Close() is a final state of Writer, no further actions expected after Close() was called
func (fw *fileWriter) Close() error {
	if fw.committed {
		return nil
	}

	if fw.closed {
		return fmt.Errorf("already closed")
	} else if fw.cancelled {
		return fmt.Errorf("already cancelled")
	}

	fw.closed = true
	if err := fw.closeAndCommit(); err != nil {
		return err
	}

	return nil
}

func (fw *fileWriter) closeAndCommit() error {
	if err := fw.WriteCloser.Close(); err != nil {
		return err
	}
	if err := fw.tx.Commit(); err != nil {
		return err
	}
	return nil
}

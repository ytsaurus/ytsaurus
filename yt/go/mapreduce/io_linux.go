//go:build linux
// +build linux

package mapreduce

import (
	"fmt"
	"io"
	"os"
	"syscall"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/yson"
)

func (c *jobContext) initPipes(nOutputPipes int) error {
	c.in = os.Stdin

	// Hide stdin from user code, just in case.
	os.Stdin = nil

	for i := 0; i < nOutputPipes; i++ {
		var pipe *os.File

		fd := uintptr(3*i + 1)
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFD, 0)
		if errno != 0 {
			return xerrors.Errorf("output pipe #%d is missing: %w", i, errno)
		}

		if i == 0 {
			pipe = os.Stdout

			// Hide stdout from user code to avoid format errors caused by fmt.Println calls.
			os.Stdout = nil
		} else {
			pipe = os.NewFile(fd, fmt.Sprintf("yt-output-pipe-%d", i))
		}

		c.out = append(c.out, pipe)
	}

	return nil
}

func (c *jobContext) createReader(state *jobState) (Reader, error) {
	if len(state.InputTablesInfo) > 0 {
		skiffSchemas := make([]any, len(state.InputTablesInfo))
		tableSchemas := make([]*schema.Schema, len(state.InputTablesInfo))
		for i, info := range state.InputTablesInfo {
			if info.SkiffSchema == nil {
				return nil, xerrors.Errorf("input table at index %d is configured for skiff format but schema is missing", i)
			}
			skiffSchemas[i] = info.SkiffSchema
			tableSchemas[i] = info.TableSchema
		}

		inputFormat := skiff.Format{
			Name:         "skiff",
			TableSchemas: skiffSchemas,
		}
		return newSkiffReader(c.in, c, &inputFormat, tableSchemas)
	}

	return newYSONReader(c.in, c), nil
}

func (c *jobContext) createWriter(out io.WriteCloser, skiffSchema *skiff.Schema) (Writer, error) {
	if skiffSchema != nil {
		encoder, err := skiff.NewEncoder(out, *skiffSchema)
		if err != nil {
			return nil, xerrors.Errorf("failed to create skiff encoder: %w", err)
		}
		return newWriter(&skiffEncoder{encoder: encoder}, c, out), nil
	}

	ysonWriter := yson.NewWriterConfig(out, yson.WriterConfig{
		Format: yson.FormatBinary,
		Kind:   yson.StreamListFragment,
	})
	return newWriter(&ysonEncoder{writer: ysonWriter}, c, out), nil
}

func (c *jobContext) createWriters(state *jobState) ([]Writer, error) {
	useSkiffFormat := len(state.OutputTablesInfo) > 0 && state.OutputTablesInfo[0].SkiffSchema != nil
	var schemas []*skiff.Schema
	if useSkiffFormat {
		schemas = make([]*skiff.Schema, len(state.OutputTablesInfo))
		for i, info := range state.OutputTablesInfo {
			if info.SkiffSchema == nil {
				return nil, xerrors.Errorf("output table at index %d is configured for skiff format but schema is missing", i)
			}
			schemas[i] = info.SkiffSchema
		}
		if len(schemas) != len(c.out) {
			return nil, xerrors.Errorf(
				"number of output types (%d) does not match number of output tables (%d)",
				len(schemas), len(c.out),
			)
		}
	}

	writers := make([]Writer, len(c.out))
	for i, file := range c.out {
		var skiffSchema *skiff.Schema
		if useSkiffFormat {
			skiffSchema = schemas[i]
		}
		writer, err := c.createWriter(file, skiffSchema)
		if err != nil {
			return nil, xerrors.Errorf("failed to create writer (index: %d): %w", i, err)
		}
		writers[i] = writer
	}
	return writers, nil
}

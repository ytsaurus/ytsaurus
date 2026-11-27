package rpcclient

import (
	"bytes"
	"io"
	"time"

	"github.com/golang/protobuf/proto"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

const (
	// defaultStreamingStallTimeout is similar to default_streaming_stall_timeout of C++ rpc proxy client.
	defaultStreamingStallTimeout = time.Minute
	// defaultTotalStreamingTimeout is similar to default_total_streaming_timeout of C++ rpc proxy client,
	// that is used for heavy reads.
	defaultTotalStreamingTimeout = 15 * time.Minute

	// filePacketSize is the maximum size of a single attachment sent by the file writer.
	filePacketSize = bus.DefaultStreamingWindowSize / 2
)

// convertRichYPath converts path to the text form of rich YPath: <attributes>path.
func convertRichYPath(path ypath.YPath) ([]byte, error) {
	var rich ypath.Rich
	switch p := path.(type) {
	case *ypath.Rich:
		rich = *p
	case ypath.Rich:
		rich = p
	default:
		return []byte(path.YPath().String()), nil
	}

	plain := rich.Path.String()
	rich.Path = ""

	attrs, err := yson.MarshalFormat(rich, yson.FormatText)
	if err != nil {
		return nil, xerrors.Errorf("unable to serialize path attributes: %w", err)
	}

	// NB: Path with empty value is serialized as <attributes>"".
	attrs = bytes.TrimSuffix(attrs, []byte(`""`))
	if bytes.Equal(attrs, []byte("<>")) {
		attrs = nil
	}

	return append(attrs, plain...), nil
}

func marshalConfig(config any) ([]byte, error) {
	if config == nil {
		return nil, nil
	}
	return yson.Marshal(config)
}

func buildReadFileRequest(path ypath.YPath, opts *yt.ReadFileOptions) (*rpc_proxy.TReqReadFile, error) {
	config, err := marshalConfig(opts.FileReader)
	if err != nil {
		return nil, xerrors.Errorf("unable to serialize file reader config: %w", err)
	}

	return &rpc_proxy.TReqReadFile{
		Path:                              []byte(path.YPath().String()),
		Offset:                            opts.Offset,
		Length:                            opts.Length,
		Config:                            config,
		TransactionalOptions:              convertTransactionOptions(opts.TransactionOptions),
		SuppressableAccessTrackingOptions: convertAccessTrackingOptions(opts.AccessTrackingOptions),
	}, nil
}

func buildWriteFileRequest(path ypath.YPath, opts *yt.WriteFileOptions) (*rpc_proxy.TReqWriteFile, error) {
	richPath, err := convertRichYPath(path)
	if err != nil {
		return nil, err
	}

	config, err := marshalConfig(opts.FileWriter)
	if err != nil {
		return nil, xerrors.Errorf("unable to serialize file writer config: %w", err)
	}

	return &rpc_proxy.TReqWriteFile{
		Path:                 richPath,
		ComputeMd5:           &opts.ComputeMD5,
		Config:               config,
		TransactionalOptions: convertTransactionOptions(opts.TransactionOptions),
		PrerequisiteOptions:  convertPrerequisiteOptions(opts.PrerequisiteOptions),
	}, nil
}

// finalError returns the final error of the call, if any, and err otherwise.
//
// NB: Must be called only when the stream is known to be finished or failed, so that Wait does not block.
func finalError(stream ClientStream, err error) error {
	if waitErr := stream.Wait(); waitErr != nil {
		return waitErr
	}
	return err
}

// fileReader reads file data from the response attachments of a ReadFile call.
type fileReader struct {
	stream ClientStream

	buf []byte
	// err is io.EOF when the file is read successfully.
	err error
}

func newFileReader(stream ClientStream) (*fileReader, error) {
	// NB: Server checks the end of the request stream before sending data.
	if err := stream.CloseSend(); err != nil {
		err = finalError(stream, err)
		stream.Abort(err)
		return nil, err
	}

	metaRef, err := stream.Read()
	if err == io.EOF {
		err = yterrors.Err(yterrors.CodeProtocolError, "File stream ended before meta")
	}
	if err != nil {
		err = finalError(stream, err)
		stream.Abort(err)
		return nil, err
	}

	var meta rpc_proxy.TReadFileMeta
	if err := proto.Unmarshal(metaRef, &meta); err != nil {
		err = yterrors.Err(yterrors.CodeProtocolError, "Failed to deserialize file stream header", err)
		stream.Abort(err)
		return nil, err
	}

	return &fileReader{stream: stream}, nil
}

func (r *fileReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		if r.err != nil {
			return 0, r.err
		}

		data, err := r.stream.Read()
		switch {
		case err == io.EOF:
			r.err = finalError(r.stream, io.EOF)
		case err != nil:
			r.err = finalError(r.stream, err)
		default:
			r.buf = data
		}
	}

	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *fileReader) Close() error {
	if r.err == nil {
		r.err = xerrors.New("file reader is closed")
		r.stream.Abort(yterrors.Err(yterrors.CodeCanceled, "File reader closed"))
	}
	return nil
}

// fileWriter writes file data as request attachments of a WriteFile call.
type fileWriter struct {
	stream     ClientStream
	packetSize int

	closed bool
	err    error
}

func newFileWriter(stream ClientStream, packetSize int) (*fileWriter, error) {
	// NB: Server closes the response stream before reading data, that is the handshake.
	data, err := stream.Read()
	if err == nil {
		err = yterrors.Err(yterrors.CodeProtocolError, "Unexpected data in file writer handshake",
			yterrors.Attr("size", len(data)))
		stream.Abort(err)
		return nil, err
	}
	if err != io.EOF {
		err = finalError(stream, err)
		stream.Abort(err)
		return nil, err
	}

	return &fileWriter{stream: stream, packetSize: packetSize}, nil
}

func (w *fileWriter) Write(p []byte) (n int, err error) {
	if w.closed {
		return 0, xerrors.New("file writer is closed")
	}
	if w.err != nil {
		return 0, w.err
	}

	for len(p) > 0 {
		size := min(len(p), w.packetSize)

		// NB: Data is sent asynchronously and the caller may reuse p after Write returns.
		packet := make([]byte, size)
		copy(packet, p[:size])

		if err := w.stream.Write(packet); err != nil {
			w.err = finalError(w.stream, err)
			return n, w.err
		}

		n += size
		p = p[size:]
	}

	return n, nil
}

func (w *fileWriter) Close() error {
	if w.closed {
		return w.err
	}
	w.closed = true

	if w.err != nil {
		return w.err
	}

	if err := w.stream.CloseSend(); err != nil {
		w.err = finalError(w.stream, err)
		return w.err
	}

	w.err = w.stream.Wait()
	return w.err
}

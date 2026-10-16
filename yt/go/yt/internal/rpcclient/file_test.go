package rpcclient

import (
	"bytes"
	"io"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
	"go.ytsaurus.tech/yt/go/proto/core/misc"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func readFileMeta(t *testing.T) []byte {
	t.Helper()

	meta, err := proto.Marshal(&rpc_proxy.TReadFileMeta{
		Revision: ptr.Uint64(42),
		Id:       misc.NewProtoFromGUID(guid.New()),
	})
	require.NoError(t, err)
	return meta
}

func TestConvertRichYPath(t *testing.T) {
	for _, tc := range []struct {
		name     string
		path     ypath.YPath
		expected string
	}{
		{"Path", ypath.Path("//tmp/file"), "//tmp/file"},
		{"RichWithoutAttributes", ypath.NewRich("//tmp/file"), "//tmp/file"},
		{"RichAppend", ypath.NewRich("//tmp/file").SetAppend(), "<append=%true;>//tmp/file"},
		{"RichValue", *ypath.NewRich("//tmp/file").SetAppend(), "<append=%true;>//tmp/file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, err := convertRichYPath(tc.path)
			require.NoError(t, err)
			require.Equal(t, tc.expected, string(path))
		})
	}
}

func TestBuildReadFileRequest(t *testing.T) {
	txID := yt.TxID(guid.New())
	opts := &yt.ReadFileOptions{
		Offset:     ptr.Int64(10),
		Length:     ptr.Int64(20),
		FileReader: map[string]any{"workload_descriptor": map[string]any{"category": "user_batch"}},
		TransactionOptions: &yt.TransactionOptions{
			TransactionID: txID,
		},
		AccessTrackingOptions: &yt.AccessTrackingOptions{
			SuppressAccessTracking: true,
		},
	}

	req, err := buildReadFileRequest(ypath.Path("//tmp/file"), opts)
	require.NoError(t, err)

	require.Equal(t, "//tmp/file", string(req.GetPath()))
	require.Equal(t, int64(10), req.GetOffset())
	require.Equal(t, int64(20), req.GetLength())
	require.Equal(t, guid.GUID(txID), makeGUID(req.GetTransactionalOptions().GetTransactionId()))
	require.True(t, req.GetSuppressableAccessTrackingOptions().GetSuppressAccessTracking())

	var config map[string]any
	require.NoError(t, yson.Unmarshal(req.GetConfig(), &config))
	require.Equal(t, map[string]any{"workload_descriptor": map[string]any{"category": "user_batch"}}, config)

	t.Run("Defaults", func(t *testing.T) {
		req, err := buildReadFileRequest(ypath.Path("//tmp/file"), &yt.ReadFileOptions{})
		require.NoError(t, err)
		require.Nil(t, req.Offset)
		require.Nil(t, req.Length)
		require.Nil(t, req.Config)
		require.Nil(t, req.TransactionalOptions)
		require.Nil(t, req.SuppressableAccessTrackingOptions)
	})
}

func TestBuildWriteFileRequest(t *testing.T) {
	txID := yt.TxID(guid.New())
	prerequisiteTxID := yt.TxID(guid.New())
	opts := &yt.WriteFileOptions{
		ComputeMD5: true,
		FileWriter: map[string]any{"upload_replication_factor": 3},
		TransactionOptions: &yt.TransactionOptions{
			TransactionID: txID,
		},
		PrerequisiteOptions: &yt.PrerequisiteOptions{
			TransactionIDs: []yt.TxID{prerequisiteTxID},
		},
	}

	req, err := buildWriteFileRequest(ypath.NewRich("//tmp/file").SetAppend(), opts)
	require.NoError(t, err)

	require.Equal(t, "<append=%true;>//tmp/file", string(req.GetPath()))
	require.True(t, req.GetComputeMd5())
	require.Equal(t, guid.GUID(txID), makeGUID(req.GetTransactionalOptions().GetTransactionId()))
	require.Len(t, req.GetPrerequisiteOptions().GetTransactions(), 1)
	require.Equal(t, guid.GUID(prerequisiteTxID), makeGUID(req.GetPrerequisiteOptions().GetTransactions()[0].GetTransactionId()))

	var config map[string]any
	require.NoError(t, yson.Unmarshal(req.GetConfig(), &config))
	require.Equal(t, map[string]any{"upload_replication_factor": int64(3)}, config)
}

func TestFileReader(t *testing.T) {
	t.Run("ReadAll", func(t *testing.T) {
		stream := &fakeStream{reads: []fakeRead{
			{data: readFileMeta(t)},
			{data: []byte("hello, ")},
			{data: []byte{}},
			{data: []byte("world")},
		}}

		r, err := newFileReader(stream)
		require.NoError(t, err)
		require.Equal(t, [][]byte{nil}, stream.writes, "request stream must be closed")

		data, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, "hello, world", string(data))

		n, err := r.Read(make([]byte, 1))
		require.Equal(t, 0, n)
		require.ErrorIs(t, err, io.EOF)

		require.NoError(t, r.Close())
		require.False(t, stream.aborted, "Close after the end must not abort")
	})

	t.Run("SmallBuffer", func(t *testing.T) {
		stream := &fakeStream{reads: []fakeRead{
			{data: readFileMeta(t)},
			{data: []byte("abcdef")},
		}}

		r, err := newFileReader(stream)
		require.NoError(t, err)

		var got bytes.Buffer
		buf := make([]byte, 4)
		for {
			n, err := r.Read(buf)
			got.Write(buf[:n])
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
		require.Equal(t, "abcdef", got.String())
	})

	t.Run("ErrorBeforeMeta", func(t *testing.T) {
		rspErr := yterrors.Err(yterrors.ErrorCode(500), "No such node")
		stream := &fakeStream{
			reads:   []fakeRead{{err: rspErr}},
			waitErr: rspErr,
		}

		_, err := newFileReader(stream)
		require.Error(t, err)
		require.True(t, yterrors.ContainsErrorCode(err, 500))
	})

	t.Run("MissingMeta", func(t *testing.T) {
		stream := &fakeStream{}

		_, err := newFileReader(stream)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeProtocolError))
		require.True(t, stream.aborted)
	})

	t.Run("BadMeta", func(t *testing.T) {
		stream := &fakeStream{reads: []fakeRead{{data: []byte{0xff, 0xff}}}}

		_, err := newFileReader(stream)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeProtocolError))
		require.True(t, stream.aborted)
	})

	t.Run("ErrorAfterData", func(t *testing.T) {
		rspErr := yterrors.Err(yterrors.CodeTimeout, "server failed")
		stream := &fakeStream{reads: []fakeRead{
			{data: readFileMeta(t)},
			{data: []byte("partial")},
			{err: rspErr},
		}}

		r, err := newFileReader(stream)
		require.NoError(t, err)

		stream.waitErr = rspErr
		data, err := io.ReadAll(r)
		require.Equal(t, "partial", string(data))
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout), "error must not be io.EOF")
	})

	t.Run("FinalErrorAfterEOF", func(t *testing.T) {
		rspErr := yterrors.Err(yterrors.CodeTimeout, "server failed")
		stream := &fakeStream{
			reads:   []fakeRead{{data: readFileMeta(t)}, {data: []byte("data")}},
			waitErr: rspErr,
		}

		r, err := newFileReader(stream)
		require.NoError(t, err)

		_, err = io.ReadAll(r)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout), "io.EOF only after successful response")
	})

	t.Run("CloseBeforeEnd", func(t *testing.T) {
		stream := &fakeStream{reads: []fakeRead{
			{data: readFileMeta(t)},
			{data: []byte("first")},
			{data: []byte("second")},
		}}

		r, err := newFileReader(stream)
		require.NoError(t, err)

		_, err = r.Read(make([]byte, 5))
		require.NoError(t, err)

		require.NoError(t, r.Close())
		require.True(t, stream.aborted)
		require.True(t, yterrors.ContainsErrorCode(stream.abortErr, yterrors.CodeCanceled))

		_, err = r.Read(make([]byte, 5))
		require.Error(t, err)
		require.NotErrorIs(t, err, io.EOF)
	})
}

func TestFileWriter(t *testing.T) {
	t.Run("WriteAndClose", func(t *testing.T) {
		stream := &fakeStream{}

		w, err := newFileWriter(stream, 4)
		require.NoError(t, err)

		buf := []byte("hello, world")
		n, err := w.Write(buf)
		require.NoError(t, err)
		require.Equal(t, len(buf), n)

		// The caller may reuse the buffer after Write returns.
		copy(buf, "XXXXXXXXXXXX")

		require.NoError(t, w.Close())
		require.NoError(t, w.Close(), "second Close returns the same result")

		require.Equal(t, [][]byte{[]byte("hell"), []byte("o, w"), []byte("orld"), nil}, stream.writes)
		require.Equal(t, 1, stream.waitCalls)

		_, err = w.Write([]byte("late"))
		require.Error(t, err)
	})

	t.Run("HandshakeError", func(t *testing.T) {
		rspErr := yterrors.Err(yterrors.ErrorCode(500), "No such node")
		stream := &fakeStream{
			reads:   []fakeRead{{err: rspErr}},
			waitErr: rspErr,
		}

		_, err := newFileWriter(stream, 4)
		require.True(t, yterrors.ContainsErrorCode(err, 500))
		require.True(t, stream.aborted)
	})

	t.Run("UnexpectedHandshakeData", func(t *testing.T) {
		stream := &fakeStream{reads: []fakeRead{{data: []byte("unexpected")}}}

		_, err := newFileWriter(stream, 4)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeProtocolError))
		require.True(t, stream.aborted)
	})

	t.Run("ErrorDuringWrite", func(t *testing.T) {
		rspErr := yterrors.Err(yterrors.CodeTimeout, "server failed")
		stream := &fakeStream{}

		w, err := newFileWriter(stream, 4)
		require.NoError(t, err)

		stream.writeErr = rspErr
		stream.waitErr = rspErr

		n, err := w.Write([]byte("data"))
		require.Equal(t, 0, n)
		require.True(t, yterrors.ContainsErrorCode(err, yterrors.CodeTimeout))

		require.True(t, yterrors.ContainsErrorCode(w.Close(), yterrors.CodeTimeout))
	})

	t.Run("ErrorOnClose", func(t *testing.T) {
		rspErr := yterrors.Err(yterrors.CodeTimeout, "commit failed")
		stream := &fakeStream{waitErr: rspErr}

		w, err := newFileWriter(stream, 4)
		require.NoError(t, err)

		_, err = w.Write([]byte("data"))
		require.NoError(t, err)

		require.True(t, yterrors.ContainsErrorCode(w.Close(), yterrors.CodeTimeout))
	})
}

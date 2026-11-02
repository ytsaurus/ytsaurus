package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/core/log/ctxlog"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/internal/httpclient"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yttest"
)

func TestClientCompression(t *testing.T) {
	t.Parallel()

	env := yttest.New(t)

	for _, codec := range []yt.ClientCompressionCodec{
		yt.ClientCodecDefault,
		yt.ClientCodecGZIP,
		yt.ClientCodecNone,
		yt.ClientCodecSnappy,
		yt.ClientCodecZSTDFastest,
		yt.ClientCodecZSTDDefault,
		yt.ClientCodecZSTDBetterCompression,
		yt.ClientCodecBrotliFastest,
		yt.ClientCodecBrotliDefault,
		// TODO(prime@): fix codec select in HTTP proxy
		// yt.ClientCodecBrotliBestCompression,
	} {
		t.Run(fmt.Sprint(codec), func(t *testing.T) {
			yc, err := ythttp.NewClient(&yt.Config{
				CompressionCodec: codec,
				Logger:           env.L,
			})
			require.NoError(t, err)

			recorder := &uploadEncodingRecorder{}
			ctx := httpclient.WithRoundTripper(ctxlog.WithFields(env.Ctx, log.String("subtest_name", t.Name())), recorder)

			tmpPath := env.TmpPath()
			_, err = yc.CreateNode(ctx, tmpPath, yt.NodeTable, nil)
			require.NoError(t, err)

			w, err := yc.WriteTable(ctx, tmpPath, nil)
			require.NoError(t, err)
			defer w.Rollback()

			require.NoError(t, w.Write(testRow{Key: "a", Value: "b"}))
			require.NoError(t, w.Commit())

			r, err := yc.ReadTable(ctx, tmpPath, nil)
			require.NoError(t, err)
			defer r.Close()

			assert.True(t, r.Next())
			require.NoError(t, r.Err())
			require.NoError(t, r.Scan(new(testRow)))
			assert.False(t, r.Next())
			require.NoError(t, r.Err())

			rw, err := yt.WriteTable(ctx, yc, tmpPath, yt.WithExistingTable(), yt.WithRawRows("json"), yt.WithBatchSize(16))
			require.NoError(t, err)
			defer func() { _ = rw.Rollback() }()

			require.NoError(t, rw.Write([]byte(`{"table_key":"a","value":"b"}`)))
			require.NoError(t, rw.Write([]byte(`{"table_key":"c","value":"d"}`)))
			require.NoError(t, rw.Commit())

			rows, err := readRowsFormat[testRow](ctx, yc, tmpPath, nil)
			require.NoError(t, err)
			require.Equal(t, []testRow{{Key: "a", Value: "b"}, {Key: "c", Value: "d"}}, rows)

			rawWriter, ok := yc.(rawTableWriter)
			require.True(t, ok)
			require.NoError(t, rawWriter.WriteTableRaw(ctx, tmpPath, &yt.WriteTableOptions{Format: "json"}, new(bytes.Buffer)))
			rows, err = readRowsFormat[testRow](ctx, yc, tmpPath, nil)
			require.NoError(t, err)
			require.Empty(t, rows)

			filePath := env.TmpPath()
			fw, err := yt.WriteFile(ctx, yc, filePath)
			require.NoError(t, err)
			_, err = fw.Write([]byte("raw file content"))
			require.NoError(t, err)
			require.NoError(t, fw.Close())

			fr, err := yc.ReadFile(ctx, filePath, nil)
			require.NoError(t, err)
			defer fr.Close()

			content, err := io.ReadAll(fr)
			require.NoError(t, err)
			require.Equal(t, "raw file content", string(content))

			expectedEncoding := ""
			if block, ok := (&yt.Config{CompressionCodec: codec}).GetClientCompressionCodec().BlockCodec(); ok {
				expectedEncoding = "z-" + block
			}
			require.NotEmpty(t, recorder.encodings)
			for _, encoding := range recorder.encodings {
				require.Equal(t, expectedEncoding, encoding)
			}
		})
	}
}

type rawTableWriter interface {
	WriteTableRaw(ctx context.Context, path ypath.YPath, options *yt.WriteTableOptions, body *bytes.Buffer) error
}

type uploadEncodingRecorder struct {
	mu        sync.Mutex
	encodings []string
}

func (r *uploadEncodingRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasSuffix(req.URL.Path, "/write_table") || strings.HasSuffix(req.URL.Path, "/write_file") {
		r.mu.Lock()
		r.encodings = append(r.encodings, req.Header.Get("Content-Encoding"))
		r.mu.Unlock()
	}
	return http.DefaultTransport.RoundTrip(req)
}

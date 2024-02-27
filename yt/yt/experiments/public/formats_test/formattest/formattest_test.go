package formattest

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yson"
)

type testRow struct {
	Key   string
	Value int
}

func TestRoundTrip(t *testing.T) {
	schema := schema.MustInfer(testRow{})

	var b bytes.Buffer

	w := yson.NewWriterConfig(&b, yson.WriterConfig{
		Format: yson.FormatPretty,
		Kind:   yson.StreamListFragment,
	})

	w.Any(testRow{"foo", 1})
	w.Any(testRow{"bar", 2})

	require.NoError(t, w.Finish())

	attachments, descriptor, err := EncodeToWire(b.Bytes(), &schema, false, false)
	require.NoError(t, err)

	t.Logf("descriptor %v", proto.MarshalTextString(descriptor))
	for _, blob := range attachments {
		t.Logf("wire\n%s", hex.Dump(blob))
	}

	data, err := DecodeFromWire(attachments, false, &schema, descriptor)
	require.NoError(t, err)
	t.Logf("decoded\n%s", data)
}

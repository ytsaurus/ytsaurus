package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestTableSchemaPreservesRichPathAttributes(t *testing.T) {
	invokeErr := errors.New("stop after recording call")
	var requestedPath ypath.YPath
	encoder := &Encoder{
		StartCall: func() *Call { return &Call{} },
		Invoke: func(_ context.Context, call *Call) (*CallResult, error) {
			var ok bool
			requestedPath, ok = call.Params.YPath()
			require.True(t, ok)
			return nil, invokeErr
		},
	}

	_, err := encoder.tableSchema(context.Background(), ypath.NewRich("//tmp/input").SetCluster("other"))
	require.ErrorIs(t, err, invokeErr)
	rich, ok := requestedPath.(*ypath.Rich)
	require.True(t, ok)
	require.Equal(t, ypath.Path("//tmp/input/@"), rich.Path)
	require.Equal(t, "other", rich.Cluster)
}

func TestReadTablePartitionPassesSkiffSchema(t *testing.T) {
	tableSchema := &schema.Schema{Columns: []schema.Column{{Name: "value", Type: schema.TypeInt64}}}
	format := skiff.Format{Name: "skiff"}
	called := false
	encoder := &Encoder{
		StartCall: func() *Call { return &Call{} },
		InvokeReadRow: func(_ context.Context, call *Call) (yt.TableReader, error) {
			called = true
			require.Equal(t, format, call.Format)
			require.Same(t, tableSchema, call.TableSchema)
			return nil, nil
		},
	}

	_, err := encoder.ReadTablePartition(context.Background(), []byte("partition-cookie"), &yt.ReadTablePartitionOptions{
		Format:      format,
		TableSchema: tableSchema,
	})

	require.NoError(t, err)
	require.True(t, called)
}

func TestReadTablePartitionRejectsSchemaWithoutSkiff(t *testing.T) {
	for _, tc := range []struct {
		name   string
		format any
	}{
		{name: "default YSON"},
		{name: "explicit YSON", format: "yson"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoder := &Encoder{
				StartCall: func() *Call { return &Call{} },
				InvokeReadRow: func(_ context.Context, _ *Call) (yt.TableReader, error) {
					t.Fatal("reader must not be invoked for a non-Skiff format with a table schema")
					return nil, nil
				},
			}

			_, err := encoder.ReadTablePartition(context.Background(), []byte("partition-cookie"), &yt.ReadTablePartitionOptions{
				Format:      tc.format,
				TableSchema: &schema.Schema{},
			})

			require.ErrorContains(t, err, "unexpected output format")
		})
	}
}

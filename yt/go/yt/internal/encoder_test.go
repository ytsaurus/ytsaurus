package internal

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/skiff"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestVersionedReadOptionsHTTP(t *testing.T) {
	for _, verb := range []Verb{VerbSelectRows, VerbLookupRows} {
		for _, tc := range []struct {
			name     string
			options  *yt.VersionedReadOptions
			expected map[string]any
		}{
			{name: "omitted"},
			{
				name:     "empty",
				options:  &yt.VersionedReadOptions{},
				expected: map[string]any{},
			},
			{
				name:     "default",
				options:  &yt.VersionedReadOptions{ReadMode: yt.VersionedReadModeDefault},
				expected: map[string]any{"read_mode": "default"},
			},
			{
				name:     "latest timestamp",
				options:  &yt.VersionedReadOptions{ReadMode: yt.VersionedReadModeLatestTimestamp},
				expected: map[string]any{"read_mode": "latest_timestamp"},
			},
		} {
			t.Run(string(verb)+"/"+tc.name, func(t *testing.T) {
				called := false
				encoder := &Encoder{
					StartCall: func() *Call { return &Call{} },
					InvokeReadRow: func(_ context.Context, call *Call) (yt.TableReader, error) {
						called = true
						require.Equal(t, verb, call.Params.HTTPVerb())

						var buffer bytes.Buffer
						writer := yson.NewWriterFormat(&buffer, yson.FormatText)
						writer.BeginMap()
						call.Params.MarshalHTTP(writer)
						writer.EndMap()
						require.NoError(t, writer.Finish())

						var params map[string]any
						require.NoError(t, yson.Unmarshal(buffer.Bytes(), &params))
						if tc.options == nil {
							require.NotContains(t, params, "versioned_read_options")
						} else {
							require.Equal(t, tc.expected, params["versioned_read_options"])
						}
						return nil, nil
					},
				}

				var err error
				switch verb {
				case VerbSelectRows:
					_, err = encoder.SelectRows(context.Background(), "* from [//tmp/table]", &yt.SelectRowsOptions{
						VersionedReadOptions: tc.options,
					})
				case VerbLookupRows:
					_, err = encoder.LookupRows(context.Background(), "//tmp/table", []any{map[string]any{"key": int64(1)}}, &yt.LookupRowsOptions{
						VersionedReadOptions: tc.options,
					})
				}
				require.NoError(t, err)
				require.True(t, called)
			})
		}
	}
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

func TestWriteTableFragmentPassesSkiffFormat(t *testing.T) {
	format := skiff.Format{Name: "skiff", TableSchemas: []any{&skiff.Schema{Type: skiff.TypeTuple}}}
	called := false
	encoder := &Encoder{
		StartCall: func() *Call { return &Call{} },
		InvokeWriteRow: func(_ context.Context, call *Call) (yt.TableWriter, error) {
			called = true
			require.Equal(t, format, call.Format)
			return nil, nil
		},
	}

	_, err := encoder.WriteTableFragment(context.Background(), yt.WriteFragmentCookie{}, &yt.TableFragmentWriterOptions{
		Format: format,
	})

	require.NoError(t, err)
	require.True(t, called)
}

func TestWriteTableFragmentRejectsUnsupportedFormat(t *testing.T) {
	for _, tc := range []struct {
		name     string
		format   any
		errorMsg string
	}{
		{name: "YSON", format: "yson", errorMsg: "unexpected format"},
		{name: "skiff schema by value", format: skiff.Format{Name: "skiff", TableSchemas: []any{skiff.Schema{Type: skiff.TypeTuple}}}, errorMsg: "*skiff.Schema"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoder := &Encoder{
				StartCall: func() *Call { return &Call{} },
				InvokeWriteRow: func(_ context.Context, _ *Call) (yt.TableWriter, error) {
					t.Fatal("writer must not be invoked for an unsupported format")
					return nil, nil
				},
			}

			_, err := encoder.WriteTableFragment(context.Background(), yt.WriteFragmentCookie{}, &yt.TableFragmentWriterOptions{
				Format: tc.format,
			})

			require.ErrorContains(t, err, tc.errorMsg)
		})
	}
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

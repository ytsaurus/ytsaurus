package yson

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTextWriter(t *testing.T) {
	for _, testCase := range []struct {
		output string
		fn     func(w *Writer)
	}{
		{
			"#",
			func(w *Writer) { w.Entity() },
		},
		{
			"1",
			func(w *Writer) { w.Int64(1) },
		},
		{
			"1u",
			func(w *Writer) { w.Uint64(1) },
		},
		{
			"1.000000",
			func(w *Writer) { w.Float64(1) },
		},
		{
			"%inf",
			func(w *Writer) { w.Float64(math.Inf(1)) },
		},
		{
			"%-inf",
			func(w *Writer) { w.Float64(math.Inf(-1)) },
		},
		{
			"%nan",
			func(w *Writer) { w.Float64(math.NaN()) },
		},
		{
			"%true",
			func(w *Writer) { w.Bool(true) },
		},
		{
			"%false",
			func(w *Writer) { w.Bool(false) },
		},
		{
			"<a=1;>#",
			func(w *Writer) {
				w.BeginAttrs()
				w.MapKeyString("a")
				w.Int64(1)
				w.EndAttrs()
				w.Entity()
			},
		},
		{
			"<a=1;>{b=[2;3;];d=<f=4;>{g=g;};}",
			func(w *Writer) {
				w.BeginAttrs()
				w.MapKeyString("a")
				w.Int64(1)
				w.EndAttrs()

				w.BeginMap()

				w.MapKeyString("b")
				w.BeginList()
				w.Int64(2)
				w.Int64(3)
				w.EndList()

				w.MapKeyString("d")
				w.BeginAttrs()
				w.MapKeyString("f")
				w.Int64(4)
				w.EndAttrs()

				w.BeginMap()
				w.MapKeyString("g")
				w.String("g")
				w.EndMap()

				w.EndMap()
			},
		},
		{
			`""`,
			func(w *Writer) { w.String("") },
		},
	} {
		var buf bytes.Buffer
		w := NewWriter(&buf)

		testCase.fn(w)

		assert.NoError(t, w.Finish())
		assert.Equal(t, testCase.output, buf.String())
	}
}

func TestBinaryWriter(t *testing.T) {
	for _, testCase := range []struct {
		output []byte
		fn     func(w *Writer)
	}{
		{
			[]byte{'#'},
			func(w *Writer) { w.Entity() },
		},
		{
			[]byte{0x04},
			func(w *Writer) { w.Bool(false) },
		},
		{
			[]byte{0x05},
			func(w *Writer) { w.Bool(true) },
		},
	} {
		var buf bytes.Buffer
		w := NewWriterFormat(&buf, FormatBinary)

		testCase.fn(w)

		assert.NoError(t, w.Finish())
		assert.Equal(t, testCase.output, buf.Bytes())
	}
}

func TestPrettyWriter(t *testing.T) {
	for _, testCase := range []struct {
		output []byte
		fn     func(w *Writer)
	}{
		{
			[]byte(`
{
    a=[
        <
            b=0;
        >
        1;
    ];
}`)[1:],
			func(w *Writer) {
				w.BeginMap()
				w.MapKeyString("a")

				w.BeginList()

				w.BeginAttrs()
				w.MapKeyString("b")
				w.Int64(0)
				w.EndAttrs()

				w.Int64(1)
				w.EndList()
				w.EndMap()
			},
		},
		{
			[]byte(`"<\\"`),
			func(w *Writer) {
				w.String(`<\`)
			},
		},
	} {
		var buf bytes.Buffer
		w := NewWriterFormat(&buf, FormatPretty)

		testCase.fn(w)

		assert.NoError(t, w.Finish())
		assert.Equal(t, string(testCase.output), buf.String())
	}
}

func TestListFragmentWriter(t *testing.T) {
	for _, testCase := range []struct {
		output string
		fn     func(w *Writer)
	}{
		{
			"#;",
			func(w *Writer) { w.Entity() },
		},
		{
			"1;a;<>#;{};[];",
			func(w *Writer) {
				w.Int64(1)
				w.String("a")

				w.BeginAttrs()
				w.EndAttrs()
				w.Entity()

				w.BeginMap()
				w.EndMap()

				w.BeginList()
				w.EndList()
			},
		},
	} {
		var buf bytes.Buffer
		w := NewWriterConfig(&buf, WriterConfig{FormatText, StreamListFragment})

		testCase.fn(w)

		assert.NoError(t, w.Finish())
		assert.Equal(t, testCase.output, buf.String())
	}
}

func TestMapFragmentWriter(t *testing.T) {
	for _, testCase := range []struct {
		output string
		fn     func(w *Writer)
	}{
		{
			"k=#;",
			func(w *Writer) {
				w.MapKeyString("k")
				w.Entity()
			},
		},
		{
			"k=1;k=a;k=<>#;k={};k=[];",
			func(w *Writer) {
				w.MapKeyString("k")
				w.Int64(1)

				w.MapKeyString("k")
				w.String("a")

				w.MapKeyString("k")
				w.BeginAttrs()
				w.EndAttrs()
				w.Entity()

				w.MapKeyString("k")
				w.BeginMap()
				w.EndMap()

				w.MapKeyString("k")
				w.BeginList()
				w.EndList()
			},
		},
	} {
		var buf bytes.Buffer
		w := NewWriterConfig(&buf, WriterConfig{FormatText, StreamMapFragment})

		testCase.fn(w)

		assert.NoError(t, w.Finish())
		assert.Equal(t, testCase.output, buf.String())
	}
}

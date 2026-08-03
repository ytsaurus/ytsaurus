package flow

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/wire"
)

func testSchema() Schema {
	return NewSchema(schema.Schema{Columns: []schema.Column{
		{Name: "word", Type: schema.TypeString},
		{Name: "blob", Type: schema.TypeBytes},
		{Name: "count", Type: schema.TypeInt64},
		{Name: "ratio", Type: schema.TypeFloat64},
		{Name: "flag", Type: schema.TypeBoolean},
		{Name: "shard", Type: schema.TypeUint64},
		{Name: "extra", Type: schema.TypeAny},
	}})
}

func TestSchemaColumnTypes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		column schema.Column
		want   wire.ValueType
	}{
		{"int64", schema.Column{Type: schema.TypeInt64}, wire.TypeInt64},
		{"int32", schema.Column{Type: schema.TypeInt32}, wire.TypeInt64},
		{"uint32", schema.Column{Type: schema.TypeUint32}, wire.TypeUint64},
		{"float32", schema.Column{Type: schema.TypeFloat32}, wire.TypeFloat64},
		{"boolean", schema.Column{Type: schema.TypeBoolean}, wire.TypeBool},
		{"bool_v3_spelling", schema.Column{Type: schema.Type("bool")}, wire.TypeBool},
		{"utf8", schema.Column{Type: schema.TypeString}, wire.TypeBytes},
		{"string", schema.Column{Type: schema.TypeBytes}, wire.TypeBytes},
		{"any", schema.Column{Type: schema.TypeAny}, wire.TypeAny},
		{"yson_v3_spelling", schema.Column{Type: schema.Type("yson")}, wire.TypeAny},

		{"date", schema.Column{Type: schema.TypeDate}, wire.TypeUint64},
		{"datetime", schema.Column{Type: schema.TypeDatetime}, wire.TypeUint64},
		{"timestamp", schema.Column{Type: schema.TypeTimestamp}, wire.TypeUint64},
		{"interval", schema.Column{Type: schema.TypeInterval}, wire.TypeInt64},

		{
			"optional_unwrapped",
			schema.Column{ComplexType: schema.Optional{Item: schema.TypeInt64}},
			wire.TypeInt64,
		},
		{
			"tagged_unwrapped",
			schema.Column{ComplexType: schema.Tagged{Tag: "url", Item: schema.TypeString}},
			wire.TypeBytes,
		},
		{
			"list_is_composite",
			schema.Column{ComplexType: schema.List{Item: schema.TypeInt64}},
			wire.TypeComposite,
		},
		{
			"decimal_is_string",
			schema.Column{ComplexType: schema.Decimal{Precision: 10, Scale: 2}},
			wire.TypeBytes,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.column.Name = "c"
			s := NewSchema(schema.Schema{Columns: []schema.Column{tc.column}})
			typ, ok := s.ColumnType(0)
			require.True(t, ok)
			require.Equal(t, tc.want, typ)
		})
	}
}

func TestSchemaRejectsUnsupportedColumnType(t *testing.T) {
	s := NewSchema(schema.Schema{Columns: []schema.Column{{
		Name: "created_at",
		Type: schema.Type("timestamp64"),
	}}})

	_, ok := s.ColumnType(0)
	require.False(t, ok)

	_, err := NewPayloadBuilder(s).Set("created_at", uint64(42)).Finish()
	require.ErrorIs(t, err, ErrUnsupportedSchemaType)
}

func TestSchemaSnapshotsTable(t *testing.T) {
	strict := true
	table := schema.Schema{
		Strict:  &strict,
		Columns: []schema.Column{{Name: "word", Type: schema.TypeString}},
	}
	s := NewSchema(table)

	table.Columns[0].Name = "changed"
	*table.Strict = false
	require.Equal(t, "word", s.Table().Columns[0].Name)
	require.True(t, *s.Table().Strict)

	tableCopy := s.Table()
	tableCopy.Columns[0].Name = "changed"
	*tableCopy.Strict = false
	columnsCopy := s.Columns()
	columnsCopy[0].Name = "changed"

	name, ok := s.ColumnName(0)
	require.True(t, ok)
	require.Equal(t, "word", name)
	id, ok := s.FindColumn("word")
	require.True(t, ok)
	require.Zero(t, id)
	require.True(t, *s.Table().Strict)
}

func TestZeroSchemaIsEmpty(t *testing.T) {
	var s Schema

	require.Equal(t, 0, s.Len())
	_, ok := s.FindColumn("word")
	require.False(t, ok)
	_, ok = s.ColumnName(0)
	require.False(t, ok)

	p := NewPayload(wire.Row{wire.NewInt64(0, 42)}, s)
	require.Empty(t, p.toMap())
}

func finish(t *testing.T, b *PayloadBuilder) Payload {
	t.Helper()

	p, err := b.Finish()
	require.NoError(t, err)
	return p
}

type doc struct {
	Name  string `yson:"name"`
	Score int64  `yson:"score"`
}

type testRow struct {
	Word  string  `yson:"word"`
	Blob  []byte  `yson:"blob"`
	Count int64   `yson:"count"`
	Ratio float64 `yson:"ratio"`
	Flag  bool    `yson:"flag"`
	Shard uint64  `yson:"shard"`
	Extra doc     `yson:"extra"`
}

func TestPayloadTypedAccess(t *testing.T) {
	p := finish(t, NewPayloadBuilder(testSchema()).
		Set("word", "hello").
		Set("blob", []byte{0x00, 0xff}).
		Set("count", 42).
		Set("ratio", 0.5).
		Set("flag", true).
		Set("shard", uint32(7)))

	word, err := p.String("word")
	require.NoError(t, err)
	require.Equal(t, "hello", word)

	blob, err := p.Bytes("blob")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00, 0xff}, blob)

	count, err := p.Int64("count")
	require.NoError(t, err)
	require.Equal(t, int64(42), count)

	ratio, err := p.Float64("ratio")
	require.NoError(t, err)
	require.Equal(t, 0.5, ratio)

	flag, err := p.Bool("flag")
	require.NoError(t, err)
	require.True(t, flag)

	shard, err := p.Uint64("shard")
	require.NoError(t, err)
	require.Equal(t, uint64(7), shard)
}

func TestPayloadOwnsByteSlices(t *testing.T) {
	source := []byte("blob")
	p := finish(t, NewPayloadBuilder(testSchema()).Set("blob", source))
	source[0] = 'X'

	read, err := p.Bytes("blob")
	require.NoError(t, err)
	require.Equal(t, []byte("blob"), read)

	read[0] = 'Y'
	read, err = p.Bytes("blob")
	require.NoError(t, err)
	require.Equal(t, []byte("blob"), read)

	row := p.Row()
	row[1].Bytes()[0] = 'Z'
	read, err = p.Bytes("blob")
	require.NoError(t, err)
	require.Equal(t, []byte("blob"), read)
}

func TestPayloadUnknownColumn(t *testing.T) {
	p := finish(t, NewPayloadBuilder(testSchema()))

	_, err := p.Int64("nope")
	require.ErrorIs(t, err, ErrColumnNotFound)

	require.ErrorIs(t, p.Any("nope", &struct{}{}), ErrColumnNotFound)

	_, ok := p.Value("nope")
	require.False(t, ok)
	require.False(t, p.Has("nope"))
}

func TestPayloadNullAndMissingCells(t *testing.T) {
	full := finish(t, NewPayloadBuilder(testSchema()).Set("count", 1).Set("word", nil))

	short := NewPayload(wire.Row{wire.NewInt64(2, 1)}, testSchema())

	for name, p := range map[string]Payload{"explicit_null": full, "missing_cell": short} {
		t.Run(name, func(t *testing.T) {
			require.False(t, p.Has("word"))
			require.False(t, p.Has("ratio"))

			_, err := p.String("word")
			require.ErrorIs(t, err, ErrNullValue)

			require.Equal(t, []string{"count"}, p.Columns())
			require.Equal(t, map[string]any{"count": int64(1)}, p.toMap())
		})
	}
}

func TestPayloadAddressesCellsByColumnID(t *testing.T) {
	p := NewPayload(wire.Row{
		wire.NewInt64(2, 42),
		wire.NewBytes(0, []byte("hello")),
	}, testSchema())

	count, err := p.Int64("count")
	require.NoError(t, err)
	require.Equal(t, int64(42), count)

	word, err := p.String("word")
	require.NoError(t, err)
	require.Equal(t, "hello", word)

	require.False(t, p.Has("blob"))
}

func TestPayloadTypeMismatch(t *testing.T) {
	p := finish(t, NewPayloadBuilder(testSchema()).Set("count", 42))

	_, err := p.String("count")
	require.ErrorIs(t, err, ErrTypeMismatch)

	_, err = p.Uint64("count")
	require.ErrorIs(t, err, ErrTypeMismatch)

	require.ErrorIs(t, p.Any("count", &struct{}{}), ErrTypeMismatch)
}

func TestPayloadAnyColumn(t *testing.T) {
	p := finish(t, NewPayloadBuilder(testSchema()).Set("extra", doc{Name: "x", Score: 3}))

	var decoded doc
	require.NoError(t, p.Any("extra", &decoded))
	require.Equal(t, doc{Name: "x", Score: 3}, decoded)

	value, ok := p.Value("extra")
	require.True(t, ok)
	copied := finish(t, NewPayloadBuilder(testSchema()).Set("extra", value.Bytes()))
	require.Equal(t, p.Row(), copied.Row())
}

func TestPayloadConvertTo(t *testing.T) {
	p := finish(t, NewPayloadBuilder(testSchema()).
		Set("word", "hello").
		Set("blob", []byte{0x00, 0xff}).
		Set("count", 42).
		Set("ratio", 0.5).
		Set("flag", true).
		Set("shard", uint64(7)).
		Set("extra", doc{Name: "x", Score: 3}))

	var scanned testRow
	require.NoError(t, p.ConvertTo(&scanned))
	require.Equal(t, testRow{
		Word:  "hello",
		Blob:  []byte{0x00, 0xff},
		Count: 42,
		Ratio: 0.5,
		Flag:  true,
		Shard: 7,
		Extra: doc{Name: "x", Score: 3},
	}, scanned)
}

func TestPayloadConvertToRoundTripsThroughSetStruct(t *testing.T) {
	original := testRow{
		Word:  "hello",
		Blob:  []byte{0x00, 0xff},
		Count: -1,
		Ratio: 2.5,
		Flag:  true,
		Shard: 1 << 63,
		Extra: doc{Name: "x", Score: 3},
	}

	p := finish(t, NewPayloadBuilder(testSchema()).SetStruct(original))

	var restored testRow
	require.NoError(t, p.ConvertTo(&restored))
	require.Equal(t, original, restored)
}

func TestPayloadConvertToKeepsWhatTheRowDoesNotCarry(t *testing.T) {
	p := finish(t, NewPayloadBuilder(testSchema()).Set("count", 42))

	scanned := struct {
		Count int64  `yson:"count"`
		Word  string `yson:"word"`
		Nope  string `yson:"nope"`
	}{Word: "kept", Nope: "kept"}

	require.NoError(t, p.ConvertTo(&scanned))
	require.Equal(t, int64(42), scanned.Count)
	require.Equal(t, "kept", scanned.Word)
	require.Equal(t, "kept", scanned.Nope)
}

func TestPayloadConvertToTypeMismatch(t *testing.T) {
	p := finish(t, NewPayloadBuilder(testSchema()).Set("count", 42))

	var scanned struct {
		Count string `yson:"count"`
	}
	require.ErrorIs(t, p.ConvertTo(&scanned), ErrTypeMismatch)
}

func TestPayloadConvertToRejectsFloat32Overflow(t *testing.T) {
	p := NewPayload(
		wire.Row{wire.NewFloat64(0, math.MaxFloat64)},
		NewSchema(schema.Schema{Columns: []schema.Column{{Name: "ratio", Type: schema.TypeFloat64}}}),
	)

	var scanned struct {
		Ratio float32 `yson:"ratio"`
	}
	require.ErrorIs(t, p.ConvertTo(&scanned), ErrTypeMismatch)
	require.Zero(t, scanned.Ratio)
}

func TestPayloadConvertToRequiresAPointer(t *testing.T) {
	p := finish(t, NewPayloadBuilder(testSchema()).Set("count", 42))

	require.Error(t, p.ConvertTo(testRow{}))
	require.Error(t, p.ConvertTo(nil))
	require.Error(t, p.ConvertTo(&map[string]any{}))
}

func TestPayloadBuilderSetStruct(t *testing.T) {
	p := finish(t, NewPayloadBuilder(testSchema()).SetStruct(&testRow{Word: "hello", Count: 42}))

	word, err := p.String("word")
	require.NoError(t, err)
	require.Equal(t, "hello", word)

	count, err := p.Int64("count")
	require.NoError(t, err)
	require.Equal(t, int64(42), count)
}

func TestPayloadBuilderSetStructRejectsUnknownColumn(t *testing.T) {
	_, err := NewPayloadBuilder(testSchema()).SetStruct(struct {
		Nope string `yson:"nope"`
	}{}).Finish()
	require.ErrorIs(t, err, ErrColumnNotFound)
}

func TestPayloadBuilderSetStructTypeMismatch(t *testing.T) {
	_, err := NewPayloadBuilder(testSchema()).SetStruct(struct {
		Count string `yson:"count"`
	}{Count: "not a number"}).Finish()
	require.ErrorIs(t, err, ErrTypeMismatch)
}

func TestStructColumnsFollowYSONTags(t *testing.T) {
	type header struct {
		Word string `yson:"word"`
	}
	type tagged struct {
		header
		Count  *int64 `yson:"count"`
		Hidden string `yson:"-"`
	}

	count := int64(7)
	p := finish(t, NewPayloadBuilder(testSchema()).
		SetStruct(tagged{header: header{Word: "hello"}, Count: &count, Hidden: "dropped"}))
	require.Equal(t, []string{"word", "count"}, p.Columns())

	var scanned tagged
	require.NoError(t, p.ConvertTo(&scanned))
	require.Equal(t, "hello", scanned.Word)
	require.Equal(t, count, *scanned.Count)

	empty := finish(t, NewPayloadBuilder(testSchema()).SetStruct(tagged{}))
	require.False(t, empty.Has("count"))
}

func TestPayloadBuilderRejectedValues(t *testing.T) {
	for name, tc := range map[string]struct {
		column string
		value  any
		want   error
	}{
		"unknown_column":    {"nope", 1, ErrColumnNotFound},
		"string_into_int64": {"count", "not a number", ErrTypeMismatch},
		"int_into_string":   {"word", 1, ErrTypeMismatch},
		"int_into_boolean":  {"flag", 1, ErrTypeMismatch},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewPayloadBuilder(testSchema()).Set(tc.column, tc.value).Finish()
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestPayloadBuilderRemembersTheFirstRejectedValue(t *testing.T) {
	b := NewPayloadBuilder(testSchema())

	_, err := b.Set("count", "not a number").Set("word", "hello").Finish()
	require.ErrorIs(t, err, ErrTypeMismatch)

	_, err = b.Set("word", "hello").Finish()
	require.ErrorIs(t, err, ErrTypeMismatch)
}

func TestPayloadBuilderNilStoresNullCell(t *testing.T) {
	p := finish(t, NewPayloadBuilder(testSchema()).Set("count", 7).Set("count", nil))

	v, ok := p.Value("count")
	require.True(t, ok)
	require.Equal(t, wire.TypeNull, v.Type)
	require.False(t, p.Has("count"))
}

func TestPayloadBuilderFinishDoesNotMutateTheBuilder(t *testing.T) {
	b := NewPayloadBuilder(testSchema())
	first := finish(t, b.Set("count", 1))
	second := finish(t, b.Set("word", "second"))

	require.Equal(t, []string{"word", "count"}, second.Columns())
	require.Equal(t, []string{"count"}, first.Columns())

	count, err := first.Int64("count")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestPayloadToBuilder(t *testing.T) {
	original := finish(t, NewPayloadBuilder(testSchema()).Set("word", "hello").Set("count", 1))
	updated := finish(t, original.ToBuilder().Set("count", 2))

	word, err := updated.String("word")
	require.NoError(t, err)
	require.Equal(t, "hello", word)

	count, err := updated.Int64("count")
	require.NoError(t, err)
	require.Equal(t, int64(2), count)

	originalCount, err := original.Int64("count")
	require.NoError(t, err)
	require.Equal(t, int64(1), originalCount)
}

func TestPayloadRoundTripsThroughWire(t *testing.T) {
	original := finish(t, NewPayloadBuilder(testSchema()).
		Set("word", "hello").
		Set("count", -1).
		Set("ratio", 2.5).
		Set("flag", true).
		Set("shard", uint64(1<<63)).
		Set("extra", map[string]int64{"a": 1}))

	data, err := wire.MarshalRowProto(original.Row())
	require.NoError(t, err)
	decoded, err := wire.UnmarshalRowProto(data)
	require.NoError(t, err)

	restored := NewPayload(decoded, testSchema())
	require.Equal(t, original.toMap(), restored.toMap())
}

func TestMessageBuilder(t *testing.T) {
	b := newMessageBuilder("output", testSchema())
	require.Equal(t, "output", b.StreamID())

	first, err := b.
		SetEventTimestamp(100).
		SetSystemTimestamp(200).
		Set("word", "hello").
		Finish()
	require.NoError(t, err)

	require.Equal(t, "output", first.StreamID)
	require.Equal(t, uint64(100), first.EventTimestamp)
	require.Equal(t, uint64(200), first.SystemTimestamp)

	require.Empty(t, first.ID)
	require.Zero(t, first.StreamSpecID)

	word, err := first.Payload.String("word")
	require.NoError(t, err)
	require.Equal(t, "hello", word)

	second, err := b.Set("word", "next").Finish()
	require.NoError(t, err)
	require.Equal(t, uint64(100), second.EventTimestamp)
	require.Equal(t, uint64(200), second.SystemTimestamp)
}

func TestMessageBuilderReportsRejectedValues(t *testing.T) {
	b := newMessageBuilder("output", testSchema())

	_, err := b.Set("nope", 1).Finish()
	require.ErrorIs(t, err, ErrColumnNotFound)

	_, err = b.SetStruct(testRow{Word: "hello"}).Finish()
	require.ErrorIs(t, err, ErrColumnNotFound)
}

func TestKeyedInputs(t *testing.T) {
	key := finish(t, NewPayloadBuilder(testSchema()).Set("word", "key"))

	meta := Meta{ID: "1", StreamID: "input", StreamSpecID: 3}

	for name, input := range map[string]Input{
		"extended_message": ExtendedMessage{Message: Message{Meta: meta}, Key: key},
		"timer":            Timer{Meta: meta, TriggerTimestamp: 10, Key: key},
		"visit":            NewVisit(meta, key),
	} {
		t.Run(name, func(t *testing.T) {
			word, err := input.PartitionKey().String("word")
			require.NoError(t, err)
			require.Equal(t, "key", word)
		})
	}
}

func TestVisitStreamSpecID(t *testing.T) {
	visit := NewVisit(Meta{ID: "1", StreamID: "input", StreamSpecID: 3}, Payload{})
	require.Equal(t, NoStreamSpecID, visit.StreamSpecID)
	require.Equal(t, "input", visit.StreamID)
}

package flow

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/schema"
)

type typedClick struct {
	YSONMessage

	URL   string `yson:"url"`
	Count int64  `yson:"count"`
}

func TestYSONMessageSchemaExcludesMetadata(t *testing.T) {
	s := YSONMessageSchema[typedClick]()
	require.Equal(t, []schema.Column{
		{Name: "url", Type: schema.TypeBytes},
		{Name: "count", Type: schema.TypeInt64},
	}, s.Columns())
}

func TestYSONMessageRoundTrip(t *testing.T) {
	rt := testRuntime(t)
	rt.SetStreamSpecs(NewStreamSpecs(nil, []Stream{NewYSONStream[typedClick]("clicks")}))

	written := NewYSONMessage[typedClick]("clicks")
	written.Meta.EventTimestamp = 123
	written.URL = "https://ya.ru"
	written.Count = 7

	message, err := ConvertFrom(rt, written)
	require.NoError(t, err)
	require.Equal(t, "clicks", message.StreamID)
	require.Equal(t, uint64(123), message.EventTimestamp)

	message.ID = "message-id"
	var decoded typedClick
	require.NoError(t, message.ConvertTo(&decoded))
	require.Equal(t, written.URL, decoded.URL)
	require.Equal(t, written.Count, decoded.Count)
	require.Equal(t, "message-id", decoded.Meta.ID)
	require.Equal(t, "clicks", decoded.Meta.StreamID)
}

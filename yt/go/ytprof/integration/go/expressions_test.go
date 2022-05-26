package integration

import (
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/ytprof"
	"a.yandex-team.ru/yt/go/ytprof/internal/expressions"
)

type MetadataQueryTest struct {
	Metadata ytprof.Metadata
	Query    string
	Result   bool
}

var (
	MetadataQueries = []MetadataQueryTest{{
		Metadata: ytprof.Metadata{MapData: map[string]string{
			"DefaultSampleType": "cpu",
		}},
		Query:  "Metadata['DefaultSampleType'] == 'cpu'",
		Result: true,
	}, {
		Metadata: ytprof.Metadata{MapData: map[string]string{
			"DefaultSampleType": "memory",
		}},
		Query:  "Metadata['DefaultSampleType'] == 'cpu'",
		Result: false,
	}, {
		Metadata: ytprof.Metadata{MapData: map[string]string{
			"DefaultSampleType": "cpu",
		}},
		Query:  "Metadata['DefaultSampleType'] != 'cpu'",
		Result: false,
	}, {
		Metadata: ytprof.Metadata{MapData: map[string]string{
			"DefaultSampleType": "memory",
		}},
		Query:  "Metadata['DefaultSampleType'] != 'cpu'",
		Result: true,
	}}
)

func TestQueries(t *testing.T) {
	for _, value := range MetadataQueries {
		expr, err := expressions.NewExpression(value.Query)
		require.NoError(t, err)
		result, err := expr.Evaluate(value.Metadata)
		require.NoError(t, err)
		require.Equal(t, result, value.Result)
	}
}

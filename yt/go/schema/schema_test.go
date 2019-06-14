package schema

import (
	"testing"

	"a.yandex-team.ru/yt/go/yson"
	"github.com/stretchr/testify/require"
)

func TestSchemaMarshalYSON(t *testing.T) {
	s := MustInfer(&testBasicTypes{})

	ys, err := yson.Marshal(&s)
	require.NoError(t, err)

	var s1 Schema
	err = yson.Unmarshal(ys, &s1)
	require.NoError(t, err)

	require.Equal(t, s, s1)
}

func TestSchemaEquality(t *testing.T) {
	s0 := MustInfer(&testBasicTypes{})

	strict := true
	s1 := s0
	s1.Strict = &strict

	require.True(t, s0.Equal(s1))
	require.False(t, s0.Equal(s1.WithUniqueKeys()))
}

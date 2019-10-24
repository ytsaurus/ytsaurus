package generator_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yp/go/yson/internal/proto-yson-gen/generator"
)

func TestSnakeCase(t *testing.T) {
	cases := []struct {
		Name     string
		Expected string
	}{
		{
			Name:     "MFieldName_2",
			Expected: "m_field_name_2",
		},
		{
			Name:     "TEST",
			Expected: "t_e_s_t",
		},
		{
			Name:     "T_E_S_T",
			Expected: "t_e_s_t",
		},
	}

	for _, cs := range cases {
		t.Run(cs.Name, func(t *testing.T) {
			actual := generator.SnakeCase(cs.Name)
			require.Equal(t, cs.Expected, actual)
		})
	}
}

package output

import (
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/ytrecipe"
)

func TestYTOutput(t *testing.T) {
	path := filepath.Join(ytrecipe.YTRecipeOutput, "test.txt")
	require.NoError(t, ioutil.WriteFile(path, []byte("hello"), 0666))
}

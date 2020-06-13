package output

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/test/yatest"
)

func TestYTOutput(t *testing.T) {
	require.NoError(t, os.MkdirAll(yatest.WorkPath("ytrecipe_output"), 0777))
	path := filepath.Join(yatest.WorkPath("ytrecipe_output"), "test.txt")
	require.NoError(t, ioutil.WriteFile(path, []byte("hello"), 0666))
}

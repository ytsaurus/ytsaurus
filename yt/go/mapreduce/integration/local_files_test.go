package integration

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/mapreduce"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yterrors"
	"a.yandex-team.ru/yt/go/yttest"
)

type CheckFilesJob struct {
	mapreduce.Untyped
}

func saveError(out mapreduce.Writer, fn func() error) error {
	err := fn()
	if err != nil {
		out.MustWrite(ErrorRow{
			Error: yterrors.FromError(err).(*yterrors.Error),
		})
	}

	return nil
}

func (c *CheckFilesJob) Do(ctx mapreduce.JobContext, in mapreduce.Reader, out []mapreduce.Writer) error {
	checkFile := func(path string, executable bool) error {
		if st, err := os.Stat(path); err != nil {
			return err
		} else if executable && st.Mode()&0100 == 0 {
			return fmt.Errorf("%s file mode is invalid: mode=%#o", path, st.Mode())
		} else if !executable && st.Mode()&0100 != 0 {
			return fmt.Errorf("%s file mode is invalid: mode=%#o", path, st.Mode())
		}

		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		if !bytes.Equal(content, []byte(path)) {
			return fmt.Errorf("%s file content is invalid: got %q", path, content)
		}

		return nil
	}

	return saveError(out[0], func() error {
		if err := checkFile("test.txt", false); err != nil {
			return err
		}

		if err := checkFile("test.bin", true); err != nil {
			return err
		}

		if err := checkFile("deep.txt", false); err != nil {
			return err
		}

		return nil
	})
}

func init() {
	mapreduce.Register(&CheckFilesJob{})
}

func requireNoErrorsInTable(t *testing.T, env *yttest.Env, errTable ypath.Path) {
	t.Helper()

	var errs []ErrorRow
	require.NoError(t, env.DownloadSlice(errTable, &errs))

	for _, r := range errs {
		assert.NoError(t, r.Error)
	}
}

func TestLocalFiles(t *testing.T) {
	t.Parallel()

	env, cancel := yttest.NewEnv(t)
	defer cancel()

	table := env.TmpPath()
	errTable := env.TmpPath()

	require.NoError(t, env.UploadSlice(table, []struct{ A int }{
		{A: 1},
	}))

	require.NoError(t, os.WriteFile("test.txt", []byte("test.txt"), 0666))
	require.NoError(t, os.WriteFile("test.bin", []byte("test.bin"), 0777))
	require.NoError(t, os.MkdirAll("dir", 0777))
	require.NoError(t, os.WriteFile("dir/deep.txt", []byte("deep.txt"), 0666))

	s := spec.Map().
		AddInput(table).
		AddOutput(errTable)

	op, err := env.MR.Map(&CheckFilesJob{}, s,
		mapreduce.WithLocalFiles([]string{"test.txt", "test.bin", "dir/deep.txt"}))
	require.NoError(t, err)
	require.NoError(t, op.Wait())

	requireNoErrorsInTable(t, env, errTable)
}

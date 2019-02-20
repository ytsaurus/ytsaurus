package mapreduce

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestJobArgs(t *testing.T) {
	args, err := parseJobArgs([]string{"-job", "a.yandex-team.ru/junk/imperator/mr/job.CatJob"})

	require.NoError(t, err)
	assert.Equal(t, "a.yandex-team.ru/junk/imperator/mr/job.CatJob", args.job)
}

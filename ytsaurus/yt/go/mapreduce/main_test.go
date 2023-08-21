package mapreduce

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobArgs(t *testing.T) {
	args, err := parseJobArgs([]string{"-job", "go.ytsaurus.tech/junk/imperator/mr/job.CatJob"})

	require.NoError(t, err)
	assert.Equal(t, "go.ytsaurus.tech/junk/imperator/mr/job.CatJob", args.job)
}

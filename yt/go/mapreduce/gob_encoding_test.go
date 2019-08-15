package mapreduce

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type SubStruct struct {
	Field string
}

type ReduceJobTest struct {
	Field string
	Str   SubStruct
}

func (j ReduceJobTest) Do(JobContext, Reader, []Writer) error {
	return nil
}

func TestGobEncoding(t *testing.T) {
	Register(ReduceJobTest{})

	rjob := ReduceJobTest{"test-string-1", SubStruct{"test-string-2"}}

	var jobSend Job = rjob

	b, err := encodeJob(jobSend)

	require.NoError(t, err)

	var jobReceive Job

	err = decodeJob(&b, &jobReceive)
	require.NoError(t, err)

	require.Equal(t, jobSend, jobReceive)
}

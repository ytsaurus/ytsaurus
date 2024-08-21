//go:build internal

package integration

import (
	"os"
	"testing"

	"go.ytsaurus.tech/yt/go/mapreduce"
)

func TestMain(m *testing.M) {
	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}

	os.Exit(m.Run())
}

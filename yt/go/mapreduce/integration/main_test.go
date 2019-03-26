package integration

import (
	"os"
	"testing"

	"a.yandex-team.ru/yt/go/mapreduce"
)

func TestMain(m *testing.M) {
	if mapreduce.InsideJob() {
		os.Exit(mapreduce.JobMain())
	}

	os.Exit(m.Run())
}

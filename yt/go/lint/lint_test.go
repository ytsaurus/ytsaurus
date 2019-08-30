package lint

import (
	"testing"

	"a.yandex-team.ru/library/go/test/lint"
)

func TestLint(t *testing.T) {
	lint.RunLinterPaths(
		t,
		[]string{
			"yt/go/yterrors/...",
			"yt/go/yson/...",
			"yt/go/ypath/...",
			"yt/go/discovery/...",
			"yt/go/bench/...",
			"yt/go/guid/...",
			"yt/go/wire/...",
			"yt/go/ytlock/...",
			"yt/go/yttest/...",
			"yt/go/schema/...",
			"yt/go/mapreduce/...",
			"yt/go/yt",
			"yt/go/yt/ythttp/...",
			"yt/go/yt/integration/...",
			"yt/go/yt/internal",
			"yt/go/yt/internal/httpclient",
		},
		[]string{
			"library/go",
			"yt/go",
		})
}

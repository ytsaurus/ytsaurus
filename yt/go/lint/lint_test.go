package lint

import (
	"testing"

	"a.yandex-team.ru/library/go/test/lint"
)

func TestLint(t *testing.T) {
	lint.RunLinter(
		t,
		"yt/go",
		[]string{
			"library/go",
		})
}

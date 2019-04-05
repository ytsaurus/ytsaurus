package ytlock

import (
	"testing"

	"a.yandex-team.ru/yt/go/ypath"

	"github.com/stretchr/testify/require"
)

func TestLockWithOptions(t *testing.T) {
	initialOpt := Options{FailIfMissing: true}
	lock := NewLockWithOptions(nil, nil, ypath.Path(""), initialOpt)
	require.Equal(t, lock.Options, initialOpt, "Lock should have given options")
}

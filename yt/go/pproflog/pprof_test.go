package pproflog

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/yttest"
)

func TestLogProfile(t *testing.T) {
	tmp, err := os.MkdirTemp("", "pprof")
	require.NoError(t, err)

	t.Logf("logging profiles to %s", tmp)

	go func() {
		for {
			_ = fmt.Sprintf("%d %s", 1, "Hello")
		}
	}()

	opts := Options{
		Dir:               tmp,
		Keep:              3,
		ProfilingPeriod:   time.Second,
		ProfilingDuration: 100 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	l, stopLogger := yttest.NewLogger(t)
	defer stopLogger()

	err = LogProfile(ctx, l, opts)
	require.NoError(t, err)
}

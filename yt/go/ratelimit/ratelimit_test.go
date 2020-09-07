package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestNoRateLimit(t *testing.T) {
	limit := NewLimiter(1, 0)

	ctx := context.Background()

	require.NoError(t, limit.Acquire(ctx))
	require.NoError(t, limit.Acquire(ctx))
}

func TestBlockedRateLimit(t *testing.T) {
	limit := NewLimiter(0, time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := limit.Acquire(ctx)
	require.Equal(t, context.DeadlineExceeded, err)
}

func TestSimpleLimitCancel(t *testing.T) {
	limit := NewLimiter(1, time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	require.NoError(t, limit.Acquire(ctx))

	err := limit.Acquire(ctx)
	require.Equal(t, context.DeadlineExceeded, err)
}

func TestStressBlocking(t *testing.T) {
	const (
		N = 100
		G = 100
	)

	limit := NewLimiter(N, time.Millisecond*10)

	var eg errgroup.Group
	for i := 0; i < G; i++ {
		eg.Go(func() error {
			for j := 0; j < N; j++ {
				if err := limit.Acquire(context.Background()); err != nil {
					return err
				}
			}

			return nil
		})
	}

	require.NoError(t, eg.Wait())
}

func TestStressNoBlocking(t *testing.T) {
	const (
		N = 100
		G = 100
	)

	limit := NewLimiter(N, time.Millisecond*10)

	var eg errgroup.Group
	for i := 0; i < G; i++ {
		eg.Go(func() error {
			for j := 0; j < N; j++ {
				if err := limit.Acquire(context.Background()); err != nil {
					return err
				}

				time.Sleep(time.Millisecond * 11)
			}

			return nil
		})
	}

	require.NoError(t, eg.Wait())
}

func BenchmarkNoBlocking(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(1)

	limit := NewLimiter(1, 0)

	ctx := context.Background()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := limit.Acquire(ctx); err != nil {
				b.Errorf("acquire failed: %v", err)
			}
		}
	})
}

func BenchmarkReferenceMutex(b *testing.B) {
	var mu sync.Mutex

	var j int
	for i := 0; i < b.N; i++ {
		mu.Lock()
		j++
		mu.Unlock()
	}
}

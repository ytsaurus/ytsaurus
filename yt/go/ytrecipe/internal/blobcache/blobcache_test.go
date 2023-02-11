package blobcache

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yttest"
)

func TestBlobCache(t *testing.T) {
	env, stop := yttest.NewEnv(t)
	defer stop()

	cache := NewCache(env.L, env.YT, Config{
		Root:             "//home/cache",
		ProcessName:      "test",
		UploadPingPeriod: time.Second * 5,
		UploadTimeout:    time.Second * 15,
		EntryTTL:         time.Hour,
		ExpirationDelay:  time.Minute,
	})

	require.NoError(t, cache.Migrate(env.Ctx))

	t.Run("SimpleUpload", func(t *testing.T) {
		testBlob := []byte("1234")

		newTestBlob := func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewBuffer(testBlob)), nil
		}

		path, err := cache.Upload(env.Ctx, "1", newTestBlob)
		require.NoError(t, err)

		r, err := env.YT.ReadFile(env.Ctx, path, nil)
		require.NoError(t, err)
		defer r.Close()

		blob, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, testBlob, blob)

		secondPath, err := cache.Upload(env.Ctx, "1", newTestBlob)
		require.NoError(t, err)
		require.Equal(t, secondPath, path)
	})

	t.Run("ConcurrentUpload", func(t *testing.T) {
		const (
			uploadDuration = time.Second * 20
			concurrency    = 16
		)

		testBlob := []byte("1234")
		first := make(chan struct{}, 1)

		newTestBlob := func() (io.ReadCloser, error) {
			select {
			case first <- struct{}{}:
			default:
				t.Errorf("blob uploaded more than once")
			}

			time.Sleep(uploadDuration)
			return io.NopCloser(bytes.NewBuffer(testBlob)), nil
		}

		var wg sync.WaitGroup
		wg.Add(concurrency)

		paths := make([]ypath.Path, concurrency)
		for i := 0; i < concurrency; i++ {
			go func(i int) {
				defer wg.Done()

				path, err := cache.Upload(env.Ctx, "2", newTestBlob)
				assert.NoError(t, err)
				paths[i] = path
			}(i)
		}

		wg.Wait()
		for i := 0; i < concurrency; i++ {
			require.Equal(t, paths[0], paths[i])
		}
	})

	t.Run("UploadTimeout", func(t *testing.T) {
		testBlob := []byte("1234")
		newTestBlob := func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewBuffer(testBlob)), nil
		}

		errBadBlob := errors.New("bad blob")
		newErrBlob := func() (io.ReadCloser, error) {
			return nil, errBadBlob
		}

		_, err := cache.Upload(env.Ctx, "3", newErrBlob)
		require.Equal(t, err, errBadBlob)

		_, err = cache.Upload(env.Ctx, "3", newTestBlob)
		require.NoError(t, err)
	})
}

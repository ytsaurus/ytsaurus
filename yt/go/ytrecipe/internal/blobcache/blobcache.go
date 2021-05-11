package blobcache

import (
	"context"
	"io"
	"sync"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
)

const (
	CacheTableName = "blob_cache_v2"
	BlobPath       = "blob"
)

var (
	CacheTableSchema = schema.MustInfer(&Entry{})
)

type (
	EntryKey struct {
		Key string
	}

	Entry struct {
		Key        string      `yson:",key"`
		Path       *ypath.Path `yson:",omitempty"`
		LockedAt   schema.Timestamp
		ExpiresAt  schema.Timestamp
		UploadedBy string
	}

	Config struct {
		Root             ypath.Path
		UploadPingPeriod time.Duration
		UploadTimeout    time.Duration
		EntryTTL         time.Duration
		ExpirationDelay  time.Duration
		ProcessName      string
	}

	Cache struct {
		l      log.Structured
		yc     yt.Client
		config Config
	}
)

func NewCache(l log.Structured, yc yt.Client, config Config) *Cache {
	return &Cache{
		l:      l,
		yc:     yc,
		config: config,
	}
}

func (f *Cache) getEntry(ctx context.Context, tx yt.TabletTx, key string) (*Entry, error) {
	r, err := tx.LookupRows(ctx, f.config.Root.Child(CacheTableName), []interface{}{EntryKey{Key: key}}, nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	if !r.Next() {
		return nil, r.Err()
	}

	var e Entry
	if err := r.Scan(&e); err != nil {
		return nil, err
	}
	return &e, nil
}

func (f *Cache) putEntry(ctx context.Context, tx yt.TabletTx, entry *Entry) error {
	return tx.InsertRows(ctx, f.config.Root.Child(CacheTableName), []interface{}{entry}, nil)
}

func (f *Cache) isDead(e *Entry) bool {
	return time.Since(e.LockedAt.Time()) > f.config.UploadTimeout
}

func (f *Cache) isFresh(e *Entry) bool {
	return time.Until(e.ExpiresAt.Time()) > 0
}

func (f *Cache) pingKey(ctx context.Context, key string) error {
	tx, err := f.yc.BeginTabletTx(ctx, nil)
	if err != nil {
		return err
	}

	entry, err := f.getEntry(ctx, tx, key)
	if err != nil {
		return err
	}

	entry.LockedAt = schema.NewTimestamp(time.Now())
	if err = f.putEntry(ctx, tx, entry); err != nil {
		return err
	}

	return tx.Commit()
}

func (f *Cache) startPingKey(ctx context.Context, key string) (stop func()) {
	var once sync.Once
	gracefulStop := make(chan struct{})
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		defer close(stopped)

		for {
			select {
			case <-gracefulStop:
				return
			case <-ctx.Done():
				return
			case <-time.After(f.config.UploadPingPeriod):
			}

			f.l.Debug("pinging key", log.String("key", key))
			if err := f.pingKey(ctx, key); err != nil {
				f.l.Warn("failed to ping upload key", log.Error(err))
			}
		}
	}()

	return func() {
		once.Do(func() {
			close(gracefulStop)
			select {
			case <-stopped:
				return

			case <-time.After(f.config.UploadPingPeriod):
			}

			cancel()
			<-stopped
		})
	}
}

func (f *Cache) doUpload(ctx context.Context, key string, openBlob func() (io.ReadCloser, error), expiresAt time.Time) (ypath.Path, error) {
	stop := f.startPingKey(ctx, key)
	defer stop()

	id := guid.New().String()
	path := f.config.Root.Child(BlobPath).Child(id[:2]).Child(id[2:])
	opts := &yt.CreateNodeOptions{Recursive: true, Force: true, Attributes: map[string]interface{}{
		"expiration_time": yson.Time(expiresAt),
	}}
	if _, err := f.yc.CreateNode(ctx, path, yt.NodeFile, opts); err != nil {
		return "", err
	}

	w, err := f.yc.WriteFile(ctx, path, nil)
	if err != nil {
		return "", err
	}
	defer w.Close()

	r, err := openBlob()
	if err != nil {
		return "", err
	}
	defer r.Close()

	if _, err := io.Copy(w, r); err != nil {
		return "", err
	}

	if err := w.Close(); err != nil {
		return "", err
	}

	return path, nil
}

func (f *Cache) tryUpload(ctx context.Context, key string, openBlob func() (io.ReadCloser, error)) (ypath.Path, bool, error) {
	tx, err := f.yc.BeginTabletTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Abort()

	entry, err := f.getEntry(ctx, tx, key)
	if err != nil {
		return "", false, err
	}

	switch {
	case entry != nil && entry.Path != nil && f.isFresh(entry):
		return *entry.Path, false, nil

	case entry == nil || f.isDead(entry):
		entry = &Entry{
			Key:        key,
			LockedAt:   schema.NewTimestamp(time.Now()),
			UploadedBy: f.config.ProcessName,
		}
		if err := f.putEntry(ctx, tx, entry); err != nil {
			return "", false, err
		}
		if err := tx.Commit(); err != nil {
			if yterrors.ContainsErrorCode(err, yterrors.CodeTransactionLockConflict) {
				f.l.Debug("cache entry is concurrently locked by another process",
					log.String("key", key))
				return "", true, nil
			}

			return "", false, err
		}

		expiresAt := time.Now().Add(f.config.EntryTTL)
		path, err := f.doUpload(ctx, key, openBlob, expiresAt.Add(f.config.ExpirationDelay))
		if err != nil {
			return "", false, err
		}

		entry = &Entry{
			Key:        key,
			ExpiresAt:  schema.NewTimestamp(expiresAt),
			UploadedBy: f.config.ProcessName,
			Path:       &path,
		}

		saveErr := func() error {
			tx, err := f.yc.BeginTabletTx(ctx, nil)
			if err != nil {
				return err
			}

			if err := f.putEntry(ctx, tx, entry); err != nil {
				return err
			}

			return tx.Commit()
		}()

		if saveErr != nil {
			return "", false, saveErr
		}

		return path, false, nil

	default:
		f.l.Debug("cache entry is locked by another process",
			log.String("key", key),
			log.Time("locked_at", entry.LockedAt.Time()))
		return "", true, nil
	}
}

func (f *Cache) Upload(ctx context.Context, key string, openBlob func() (io.ReadCloser, error)) (ypath.Path, error) {
	for {
		f.l.Debug("started upload iteration", log.String("key", key))
		if path, locked, err := f.tryUpload(ctx, key, openBlob); err != nil {
			return "", err
		} else if locked {
			select {
			case <-time.After(f.config.UploadPingPeriod):
				continue
			case <-ctx.Done():
				return "", ctx.Err()
			}
		} else {
			return path, nil
		}
	}
}

func (f *Cache) Migrate(ctx context.Context) error {
	plan := map[ypath.Path]migrate.Table{
		f.config.Root.Child(CacheTableName): {
			Schema: CacheTableSchema,
			Attributes: map[string]interface{}{
				"in_memory_mode": "uncompressed",

				"max_data_ttl":      f.config.EntryTTL.Milliseconds(),
				"min_data_versions": 0,
				"min_data_ttl":      time.Hour.Milliseconds(),
			},
		},
	}

	return migrate.EnsureTables(ctx, f.yc, plan, migrate.OnConflictTryAlter(ctx, f.yc))
}

func (f *Cache) Clean() error {
	panic("not implemented")
}

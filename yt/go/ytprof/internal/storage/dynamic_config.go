package storage

import (
	"context"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/ytprof"

	logzap "a.yandex-team.ru/library/go/core/log/zap"
)

type ConfigStorage struct {
	objectConfig ypath.Path

	yc yt.Client
	l  *logzap.Logger
}

func NewConfigStorage(yc yt.Client, path ypath.Path, l *logzap.Logger) *ConfigStorage {
	return &ConfigStorage{
		yc:           yc,
		l:            l,
		objectConfig: path.Child(ytprof.ObjectConfig),
	}
}

func (cs *ConfigStorage) ReadConfig(ctx context.Context, result interface{}) error {
	err := cs.yc.GetNode(ctx, cs.objectConfig, result, nil)
	if err != nil {
		return err
	}

	return nil
}

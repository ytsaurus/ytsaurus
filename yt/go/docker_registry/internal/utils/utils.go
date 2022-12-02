package utils

import (
	"os"
	"path/filepath"
	"strings"

	logzap "a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/ytlog"
)

const (
	logFileName   = "ytclient.log"
	logDirEnvName = "DOCKER_REGISTRY_YT_LOG_DIR"
)

func GetSchedulerHintsDocumentPath(repositoryName string) ypath.Path {
	parts := strings.Split(repositoryName, "/")
	return ypath.Root.JoinChild(parts...).Child("_tags")
}

func GetLogger() (*logzap.Logger, func()) {
	logDir, ok := os.LookupEnv(logDirEnvName)
	if !ok {
		return ytlog.Must(), func() {}
	}

	l, stop, err := ytlog.NewSelfrotate(filepath.Join(logDir, logFileName))
	if err != nil {
		panic(err)
	}

	return l, stop
}

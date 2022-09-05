package utils

import (
	"strings"

	"a.yandex-team.ru/yt/go/ypath"
)

func GetSchedulerHintsDocumentPath(repositoryName string) ypath.Path {
	parts := strings.Split(repositoryName, "/")
	return ypath.Root.JoinChild(parts...).Child("_tags")
}

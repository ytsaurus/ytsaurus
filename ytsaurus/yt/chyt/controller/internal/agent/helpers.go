package agent

import (
	"strings"

	"go.ytsaurus.tech/yt/go/ypath"
)

func tokenize(path ypath.Path) []string {
	parts := strings.Split(string(path), "/")
	var j int
	for i := 0; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			parts[j] = parts[i]
			j++
		}
	}
	return parts[:j]
}

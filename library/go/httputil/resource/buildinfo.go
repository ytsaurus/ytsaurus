package resource

import (
	"fmt"
	"sync"
	"time"

	"go.ytsaurus.tech/library/go/core/buildinfo"
)

var (
	buildTime     time.Time
	buildTimeOnce sync.Once
)

func BuildTime() time.Time {
	buildTimeOnce.Do(func() {
		if buildinfo.Info.Date == "" {
			buildTime = time.Now()
			return
		}

		t, err := time.Parse(time.RFC3339Nano, buildinfo.Info.Date)
		if err != nil {
			panic(fmt.Sprintf("failed to parse build date: %v", err))
		}

		buildTime = t
	})

	return buildTime
}

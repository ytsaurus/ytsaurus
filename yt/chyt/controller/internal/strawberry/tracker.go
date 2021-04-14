package strawberry

import (
	"context"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

func TrackChildren(root ypath.Path, period time.Duration, ytc yt.Client, l log.Logger) (ch <-chan string, stop func()) {
	eventCh := make(chan string)
	stopCh := make(chan struct{})

	revisions := make(map[string]yt.Revision)

	l.Debug("tracking started", log.String("root", root.String()))

	go func() {
		ticker := time.NewTicker(period)
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				l.Debug("listing nodes")
				var nodes []struct {
					Key      string      `yson:",value"`
					Revision yt.Revision `yson:"revision,attr"`
				}

				err := ytc.ListNode(context.TODO(), root, &nodes, &yt.ListNodeOptions{Attributes: []string{"revision"}})
				if err != nil {
					l.Error("error while listing directory", log.Error(err))
				}

				found := make(map[string]struct{})

				for _, node := range nodes {
					found[node.Key] = struct{}{}
					if prevRevision, ok := revisions[node.Key]; !ok || prevRevision != node.Revision {
						l.Debug(
							"node changed revision",
							log.String("key", node.Key),
							log.UInt64("prev_revision", uint64(prevRevision)),
							log.UInt64("revision", uint64(node.Revision)))
						eventCh <- node.Key
						revisions[node.Key] = node.Revision
					}
				}

				for key, revision := range revisions {
					if _, ok := found[key]; !ok {
						l.Debug(
							"node disappeared",
							log.String("key", key),
							log.UInt64("prev_revision", uint64(revision)))
						eventCh <- key
					}
				}
			}
		}
	}()

	stop = func() {
		stopCh <- struct{}{}
	}
	ch = eventCh
	return
}

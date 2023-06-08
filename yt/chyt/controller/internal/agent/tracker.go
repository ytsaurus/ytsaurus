package agent

import (
	"context"
	"strings"
	"time"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/ytwalk"
)

type myNode struct {
	Revision yt.Revision `yson:"revision,attr"`
}

// TrackChildren is a primitive for subscription on changes of Cypress nodes in the given subtree. It emits relative
// paths w.r.t. root of changed nodes.
//
// At the beginning it notifies caller about all nodes in the subtree. After that, it performs
// lightweight "get with attributes" requests with given periodicity and tracks node revision change events including
// node disappearance events. Note that each pass which did not result in any event still produces an empty slice which
// allows building reliable logic assuming the absence of future modifications (see waitForPaths helper from tests
// for an example)
//
// Current implementation does not descend into opaque nodes.
func TrackChildren(ctx context.Context, root ypath.Path, period time.Duration, ytc yt.Client, l log.Logger) <-chan []ypath.Path {
	eventCh := make(chan []ypath.Path)

	l.Debug("tracking started", log.String("root", root.String()))

	go func() {
		revisions := make(map[ypath.Path]yt.Revision)
		ticker := time.NewTicker(period)
		for {
			select {
			case <-ctx.Done():
				l.Debug("tracking finished", log.String("root", root.String()))
				close(eventCh)
				return
			case <-ticker.C:
				l.Debug("walking nodes")

				toEmit := make([]ypath.Path, 0)

				found := make(map[ypath.Path]struct{})

				// First, walk over all nodes in root subtree (respecting opaques) and find those
				// whose revisions have changed since the last time we seen them including newly appeared
				// nodes.
				err := ytwalk.Do(ctx, ytc, &ytwalk.Walk{
					Root:       root,
					Attributes: []string{"revision"},
					Node:       new(myNode),
					OnNode: func(path ypath.Path, node any) error {
						path = ypath.Path(strings.TrimPrefix(string(path), string(root)))
						myNode := node.(*myNode)
						found[path] = struct{}{}
						if prevRevision, ok := revisions[path]; !ok || prevRevision != myNode.Revision {
							l.Debug(
								"node changed revision",
								log.String("path", string(path)),
								log.UInt64("prev_revision", uint64(prevRevision)),
								log.UInt64("revision", uint64(myNode.Revision)))
							toEmit = append(toEmit, path)
							revisions[path] = myNode.Revision
						}
						return nil
					},
					RespectOpaque: true,
				})

				if err != nil {
					l.Error("error while walking", log.Error(err))
				}

				for path, revision := range revisions {
					if _, ok := found[path]; !ok {
						l.Debug(
							"node disappeared",
							log.String("key", string(path)),
							log.UInt64("prev_revision", uint64(revision)))
						toEmit = append(toEmit, path)
						delete(revisions, path)
					}
				}

				l.Debug("walking nodes finished", log.Int("nodes_changed", len(toEmit)))

				eventCh <- toEmit
			}
		}
	}()

	return eventCh
}

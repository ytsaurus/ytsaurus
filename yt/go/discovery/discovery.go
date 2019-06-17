// Package discovery implements service discovery over cypress.
//
// Example usage:
//
//   type MemberMeta struct {
//       Version string
//       Shard   int
//   }
//
//   func Example(yc yt.Client, logger log.Logger) err {
//       g := discovery.NewGroup(yc, logger, discovery.Options{Root: ypath.Path("//home/prime/group")})
//
//       // Start updater goroutine.
//       go g.Update(context.Background())
//
//       // Join the group.
//       leave, err := g.Join(context.Backgrond(), "build01-myt.yt.yandex.net", &MemberMeta{Version: "1.1", Shard: 10})
//       if err != nil {
//           return err
//       }
//       defer leave()
//
//       // See other alive members.
//       members := map[string]MemberMeta{}
//       if err := g.List(context.Background(), &members); err != nil {
//           return err
//       }
//
//       fmt.Println(members)
//       return nil
//   }
package discovery

import (
	"context"
	"sync"
	"time"

	"a.yandex-team.ru/library/go/core/log"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
)

var (
	lockKey               = "lock"
	defaultUpdateInterval = 30 * time.Second
)

type Group struct {
	yc      yt.Client
	logger  log.Logger
	options Options

	updated chan struct{}
	l       sync.Mutex
	members map[string]yson.RawValue
}

type Options struct {
	Root ypath.Path

	UpdateInterval time.Duration
}

func NewGroup(yc yt.Client, logger log.Logger, options Options) *Group {
	if options.UpdateInterval == 0 {
		options.UpdateInterval = defaultUpdateInterval
	}

	g := &Group{
		yc:      yc,
		logger:  log.With(logger, log.String("root", string(options.Root))),
		options: options,

		updated: make(chan struct{}),
		members: make(map[string]yson.RawValue),
	}

	return g
}

// Update runs loop, updating list of group members.
//
//     go g.Update(context.Background())
func (g *Group) Update(ctx context.Context) {
	started := false
	for {
		var memberList []struct {
			Name string `yson:",value"`

			MemberInfo yson.RawValue `yson:"member_info,attr"`
			LockCount  uint64        `yson:"lock_count,attr"`
		}

		opts := &yt.ListNodeOptions{
			Attributes: []string{"member_info", "lock_count"},
		}

		if err := g.yc.ListNode(ctx, g.options.Root, &memberList, opts); err != nil {
			if ctx.Err() != nil {
				return
			}

			g.logger.Error("Group update failed", log.Error(err))
			time.Sleep(g.options.UpdateInterval)
			continue
		}

		members := map[string]yson.RawValue{}
		for _, member := range memberList {
			if member.LockCount == 0 {
				continue
			}

			members[member.Name] = member.MemberInfo
		}

		g.logger.Info("Updated group",
			log.Int("alive_members", len(members)),
			log.Int("dead_members", len(memberList)-len(members)))

		g.l.Lock()
		g.members = members
		g.l.Unlock()

		if !started {
			started = true
			close(g.updated)
		}

		time.Sleep(g.options.UpdateInterval)
	}
}

// List returns caches list of alive group members, along with associated metadata.
//
// List blocks until first successful update.
func (g *Group) List(ctx context.Context, members interface{}) error {
	select {
	case <-g.updated:
	case <-ctx.Done():
		return ctx.Err()
	}

	g.l.Lock()
	rawMembers := g.members
	g.l.Unlock()

	bytes, err := yson.Marshal(rawMembers)
	if err != nil {
		return err
	}

	return yson.Unmarshal(bytes, members)
}

type member struct {
	g     *Group
	name  string
	attrs interface{}

	tx   yt.Tx
	stop chan struct{}
}

func (m *member) join(ctx context.Context) (err error) {
	if m.tx != nil {
		select {
		case <-m.tx.Finished():
			m.tx = nil
		default:
		}
	}

	if m.tx == nil {
		m.tx, err = m.g.yc.BeginTx(ctx, nil)
		if err != nil {
			m.tx = nil
			return
		}
	}

	lockOpts := &yt.LockNodeOptions{ChildKey: &lockKey}
	path := m.g.options.Root.Child(m.name)

	_, err = m.tx.LockNode(ctx, path, yt.LockShared, lockOpts)
	if err != nil && yt.ContainsErrorCode(err, yt.CodeResolveError) {
		_, err = m.g.yc.CreateNode(ctx, path, yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
		if err != nil {
			return
		}

		_, err = m.tx.LockNode(ctx, path, yt.LockShared, lockOpts)
	}

	if err != nil {
		return err
	}

	opts := &yt.SetNodeOptions{
		PrerequisiteOptions: &yt.PrerequisiteOptions{
			TransactionIDs: []yt.TxID{m.tx.ID()},
		},
	}

	err = m.g.yc.SetNode(ctx, path.Attr("member_info"), m.attrs, opts)
	return
}

func (m *member) leave() {
	close(m.stop)
}

// Join registers member of the group.
//
// name is unique identifier of the process.
// attrs is additional information that is stored in /@member_info attribute.
//
// Function returns once member is registered.
func (g *Group) Join(ctx context.Context, name string, attrs interface{}) (leave func(), err error) {
	m := &member{g: g, name: name, attrs: attrs, stop: make(chan struct{})}
	leave = m.leave

	for {
		if err = m.join(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}

			g.logger.Error("Group join failed", log.String("name", name), log.Error(err))
			time.Sleep(yt.DefaultTxTimeout)
			continue
		}

		g.logger.Info("Member joined the group", log.String("name", name))
		go func() {
			defer func() {
				if m.tx != nil {
					_ = m.tx.Abort()
				}

				g.logger.Info("Member is stopped", log.String("name", name))
			}()

			for {
				select {
				case <-m.tx.Finished():
					g.logger.Info("Tx is lost", log.String("name", name))

				case <-m.stop:
					return
				}

			restart:
				if err = m.join(ctx); err != nil {
					if ctx.Err() != nil {
						return
					}

					g.logger.Error("Group join failed", log.String("name", name), log.Error(err))
					time.Sleep(yt.DefaultTxTimeout)
					goto restart
				}

				g.logger.Info("Member joined the group", log.String("name", name))
			}
		}()
		return
	}
}

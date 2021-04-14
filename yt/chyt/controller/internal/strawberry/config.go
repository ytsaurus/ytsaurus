package strawberry

import (
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
)

// Config contains strawberry-specific configuration.
type Config struct {
	// Root points to root directory with operation states.
	Root ypath.Path `yson:"root"`

	// PassPeriod defines how often agent performs its passes.
	PassPeriod yson.Duration `yson:"pass_period"`
	// RevisionCollectPeriod defines how often agent collects Cypress node revisions via batch ListNode.
	RevisionCollectPeriod yson.Duration `yson:"revision_collect_period"`
}

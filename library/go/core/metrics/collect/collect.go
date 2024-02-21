package collect

import (
	"context"

	"go.ytsaurus.tech/library/go/core/metrics"
)

type Func func(ctx context.Context, r metrics.Registry, c metrics.CollectPolicy)

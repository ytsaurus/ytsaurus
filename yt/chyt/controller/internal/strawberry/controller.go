package strawberry

import (
	"context"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

// Controller encapsulates particular application business logic, in particular:
// how operations should be started, which files to bring with them, how to check liveness, etc.
type Controller interface {
	// Prepare builds all necessary operation spec fields.
	Prepare(ctx context.Context, oplet *Oplet) (
		spec map[string]any,
		description map[string]any,
		annotation map[string]any,
		runAsUser bool,
		err error)

	// Family returns short lowercase_with_underscore identifier which is included to all vanilla
	// operation annotations started by this strawberry controller. This allows efficient operation
	// filtering using YT list_operations API.
	Family() string

	// Root returns path to the cypress directory containing strawberry nodes.
	Root() ypath.Path

	ParseSpeclet(specletYson yson.RawValue) (parsedSpeclet any, err error)

	// UpdateState updates the controller's state.
	// Returns true if the state has been changed and all oplets should be restarted.
	UpdateState() (changed bool, err error)

	// GetControllerSnapshot returns snapshot of controller state for tracking oplet coherence.
	GetControllerSnapshot() (yson.RawValue, error)

	// DescribeOptions returns human-readable descriptors for controller-related speclet options.
	// Some options can be missing in the result if they are not intended to be visible through user interfaces.
	//
	// Given speclet should have suitable type for the specific controller.
	// Otherwise, DescribeOptions may panic.
	DescribeOptions(parsedSpeclet any) []OptionGroupDescriptor

	// GetOpBriefAttributes returns map with controller-related speclet options,
	// which can be requested from API.
	//
	// Given speclet should have suitable type for the specific controller.
	// Otherwise, GetOpBriefAttributes may panic.
	GetOpBriefAttributes(parsedSpeclet any) map[string]any

	// GetScalerTarget checks whether YT operation should be scaled and returns required scaling parameters.
	// Returns `nil` if scaling is not required.
	// May be called concurrently since it is accessed from `runScaler`, not from `background` goroutine.
	GetScalerTarget(ctx context.Context, opletInfo OpletInfoForScaler) (*ScalerTarget, error)

	// COMPAT(buyval01): Temporary auxiliary method that is only needed to modify persistent state.
	GetOpletInfo(ctx context.Context, oplet *Oplet) (any, error)

	// CheckState gets family specific info about oplet health state and if it needs to be restarted.
	CheckState(ctx context.Context, oplet *Oplet) (ControllerOpletState, error)
}

type ControllerFactory struct {
	Ctor   func(l log.Logger, ytc yt.Client, root ypath.Path, cluster string, config yson.RawValue) Controller
	Config yson.RawValue
	// TODO(max): extra commands is actually of type []api.CmdDescriptor, but we can't import it here
	// without creating a circular dependency. Come up with a better solution.
	ExtraCommands any
}

type ScalerTarget struct {
	InstanceCount int
	Reason        string
}

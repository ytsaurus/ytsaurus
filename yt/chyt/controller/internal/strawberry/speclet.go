package strawberry

import (
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

// Change of these parameters leads to operation restart.
type RestartRequiredOptions struct {
	Stage          *string      `yson:"stage"`
	NetworkProject *string      `yson:"network_project"`
	PreemptionMode *string      `yson:"preemption_mode"`
	LayerPaths     []ypath.Path `yson:"layer_paths"`
}

type Speclet struct {
	RestartRequiredOptions
	Active                 *bool   `yson:"active"`
	Family                 *string `yson:"family"`
	RestartOnSpecletChange *bool   `yson:"restart_on_speclet_change"`
	// MinSpecletRevision is a minimum speclet revision with which an operation does not require a force restart.
	// If the speclet revision of the running yt operation is less than that,
	// it will be restarted despite the RestartOnSpecletChange option.
	MinSpecletRevision yt.Revision `yson:"min_speclet_revision"`

	Pool *string `yson:"pool"`

	// TODO(dakovalkov): Does someone need it?
	// // OperationDescription is visible in clique operation UI.
	// OperationDescription map[string]interface{} `yson:"operation_description"`
	// // OperationTitle is YT operation title visible in operation UI.
	// OperationTitle *string `yson:"operation_title"`
	// // OperationAnnotations allows adding arbitrary human-readable annotations visible via YT list_operations API.
	// OperationAnnotations map[string]interface{} `yson:"operation_annotations"`
}

const (
	DefaultActive                 = false
	DefaultFamily                 = "none"
	DefaultStage                  = "production"
	DefaultRestartOnSpecletChange = true
	DefaultMinIncarnationIndex    = -1
)

func (speclet *Speclet) ActiveOrDefault() bool {
	if speclet.Active != nil {
		return *speclet.Active
	}
	return DefaultActive
}

func (speclet *Speclet) FamilyOrDefault() string {
	if speclet.Family != nil {
		return *speclet.Family
	}
	return DefaultFamily
}

func (speclet *Speclet) StageOrDefault() string {
	if speclet.Stage != nil {
		return *speclet.Stage
	}
	return DefaultStage
}

func (speclet *Speclet) RestartOnSpecletChangeOrDefault() bool {
	if speclet.RestartOnSpecletChange != nil {
		return *speclet.RestartOnSpecletChange
	}
	return DefaultRestartOnSpecletChange
}

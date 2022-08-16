package strawberry

import "a.yandex-team.ru/yt/go/yt"

type Speclet struct {
	Active                 *bool   `yson:"active"`
	Family                 *string `yson:"family"`
	Stage                  *string `yson:"stage"`
	RestartOnSpecletChange *bool   `yson:"restart_on_speclet_change"`
	// MinSpecletRevision is a minimum speclet revision with which an operation does not require a force restart.
	// If the speclet revision of an running operation is less than that, it will be restarted despite the RestartOnSpecletChange.
	MinSpecletRevision yt.Revision `yson:"min_speclet_revision"`
	// Dummy is a field which does not affect the state of oplet.
	// It can be used to triger restart on speclet change without changing any real parameters.
	Dummy uint64 `yson:"dummy"`

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
	DefaultRestartOnSpecletChange = false
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

func NeedRestartOnSpecletChange(oldSpeclet, newSpeclet *Speclet) bool {
	// Change of most strawberrySpeclet options does not require an operation restart.
	return oldSpeclet.Dummy != newSpeclet.Dummy
}

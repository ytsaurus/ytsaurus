package strawberry

import (
	"a.yandex-team.ru/yt/go/yt"
)

// PersistentState contains a part of agent's state which should be persistent in a cypress.
// It is written to the cypress when changed and is read from the cypress when the cypress revision is changed.
// The persistence of the state is needed for fault tolerance.
type PersistentState struct {
	YTOpID yt.OperationID `yson:"yt_operation_id"`
	// OperationSpecletRevision is a revision of the speclet with which the last operation was started.
	OperationSpecletRevision yt.Revision `yson:"operation_speclet_revision"`
	IncarnationIndex         int         `yson:"incarnation_index"`

	// SpecletRevision is a revision of the last speclet seen by an agent.
	//
	// TODO(dakovalkov): a speclet revision is too noisy. It is better to use a speclet hash instead.
	SpecletRevision              yt.Revision `yson:"speclet_revision"`
	SpecletChangeRequiresRestart bool        `yson:"speclet_change_requires_restart"`
}

// InfoState contains fields which are useful for understanding the current status of the oplet,
// but they are not used by an agent itself and so they are not a part of the persistent state.
// It is written to cypress when changed and is never read.
type InfoState struct {
	Error      *string           `yson:"error"`
	YTOpState  yt.OperationState `yson:"yt_operation_state"`
	Controller struct {
		Address string `yson:"address"`
		// TODO(max42): build Revision, etc.
	} `yson:"controller"`
}

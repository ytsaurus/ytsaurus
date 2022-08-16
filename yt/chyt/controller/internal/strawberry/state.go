package strawberry

import (
	"time"

	"a.yandex-team.ru/yt/go/yt"
)

// PersistentState contains a part of agent's state which should be persistent in a cypress.
// It is written to the cypress when changed and is read from the cypress when the cypress revision is changed.
// The persistence of the state is needed for fault tolerance.
type PersistentState struct {
	YTOpID    yt.OperationID    `yson:"yt_operation_id"`
	YTOpState yt.OperationState `yson:"yt_operation_state"`

	// OperationSpecletRevision is a revision of the speclet with which the last operation was started.
	OperationSpecletRevision yt.Revision `yson:"operation_speclet_revision"`
	IncarnationIndex         int         `yson:"incarnation_index"`

	// SpecletRevision is a revision of the last speclet seen by an agent.
	//
	// TODO(dakovalkov): a speclet revision is too noisy. It is better to use a speclet hash instead.
	SpecletRevision              yt.Revision `yson:"speclet_revision"`
	SpecletChangeRequiresRestart bool        `yson:"speclet_change_requires_restart"`

	// BackoffDuration is a duration during which oplet passes will be skipped after a failed pass.
	// It is increased after every failed pass and is reset after an successful pass.
	BackoffDuration time.Duration `yson:"backoff_duration"`
	// BackoffUntil is a time point until which oplet passes will be skipped due to previously failed passes.
	BackoffUntil time.Time `yson:"backoff_until"`
}

const (
	initialBackoffDuration   = time.Second
	exponentialBackoffFactor = 1.5
	maxBackoffDuration       = 10 * time.Minute
)

// InfoState contains fields which are useful for understanding the current status of the oplet,
// but they are not used by an agent itself and so they are not a part of the persistent state.
// It is written to cypress when changed and is never read.
type InfoState struct {
	Error      *string `yson:"error,omitempty"`
	Controller struct {
		Address string `yson:"address"`
		// TODO(max42): build Revision, etc.
	} `yson:"controller"`
}

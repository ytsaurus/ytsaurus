package strawberry

import (
	"time"

	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

// PersistentState contains a part of agent's state which should be persistent in a cypress.
// It is written to the cypress when changed and is read from the cypress when the cypress revision is changed.
// The persistence of the state is needed for fault tolerance.
type PersistentState struct {
	YTOpID    yt.OperationID    `yson:"yt_operation_id"`
	YTOpState yt.OperationState `yson:"yt_operation_state"`

	IncarnationIndex int `yson:"incarnation_index"`

	// YTOpSpeclet is an unparsed speclet with which current yt operation is started.
	YTOpSpeclet yson.RawValue `yson:"yt_op_strawberry_speclet,omitempty"`
	// YTOpSpecletRevision is a revision of the speclet node with which current yt operation is started.
	YTOpSpecletRevision yt.Revision `yson:"yt_op_speclet_revision,omitempty"`
	// YTOpACL is the last set ACL of the current yt operation.
	YTOpACL []yt.ACE `yson:"yt_op_acl,omitempty"`
	// YTOpPool is the last set pool of the current yt operation.
	YTOpPool *string `yson:"yt_op_pool,omitempty"`

	// SpecletRevision is a revision of the last seen speclet node.
	SpecletRevision yt.Revision `yson:"speclet_revision"`

	// BackoffDuration is a duration during which oplet passes will be skipped after a failed pass.
	// It is increased after every failed pass and is reset after an successful pass.
	BackoffDuration time.Duration `yson:"backoff_duration"`
	// BackoffUntil is a time point until which oplet passes will be skipped due to previously failed passes.
	BackoffUntil time.Time `yson:"backoff_until"`

	// Creator is a user who created the strawberry operation.
	// Creator will automatically gain access to the strawberry operation when the access control object is created.
	Creator string `yson:"creator"`
}

const (
	initialBackoffDuration   = time.Second
	exponentialBackoffFactor = 1.5
	maxBackoffDuration       = 5 * time.Minute
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

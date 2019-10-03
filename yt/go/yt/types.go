package yt

import (
	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
)

type NodeType string

const (
	// NodeMap is cypress analog for directory.
	NodeMap NodeType = "map_node"
	// NodeLink is symbolic link.
	NodeLink NodeType = "link"
	// NodeFile is regular file. Used for artifacts and opaque blobs.
	NodeFile NodeType = "file"
	// NodeTable is table.
	NodeTable   NodeType = "table"
	NodeUser    NodeType = "user"
	NodeGroup   NodeType = "group"
	NodeAccount NodeType = "account"
	NodeSys     NodeType = "sys_node"
)

func (n NodeType) String() string {
	return string(n)
}

func (n NodeType) MarshalYSON(w *yson.Writer) error {
	w.String(string(n))
	return nil
}

func (n *NodeType) UnmarshalYSON(data []byte) error {
	var value string
	if err := yson.Unmarshal(data, &value); err != nil {
		return err
	}

	// TODO(prime@): validate
	*n = NodeType(value)
	return nil
}

var _ yson.StreamMarshaller = NodeType("")

type OperationType string

const (
	OperationMap        OperationType = "map"
	OperationReduce     OperationType = "reduce"
	OperationMapReduce  OperationType = "map_reduce"
	OperationJoinReduce OperationType = "join_reduce"
	OperationSort       OperationType = "sort"
	OperationMerge      OperationType = "merge"
	OperationErase      OperationType = "erase"
	OperationRemoteCopy OperationType = "remote_copy"
	OperationVanilla    OperationType = "vanilla"
)

func (o OperationType) MarshalYSON(w *yson.Writer) error {
	w.String(string(o))
	return nil
}

func (o *OperationType) UnmarshalYSON(data []byte) error {
	var value string
	if err := yson.Unmarshal(data, &value); err != nil {
		return err
	}

	// TODO(prime@): validate
	*o = OperationType(value)
	return nil
}

type OperationState string

const (
	StateRunning       OperationState = "running"
	StatePending       OperationState = "pending"
	StateCompleted     OperationState = "completed"
	StateFailed        OperationState = "failed"
	StateAborted       OperationState = "aborted"
	StateReviving      OperationState = "reviving"
	StateInitializing  OperationState = "initializing"
	StatePreparing     OperationState = "preparing"
	StateMaterializing OperationState = "materializing"
	StateCompleting    OperationState = "completing"
	StateAborting      OperationState = "aborting"
	StateFailing       OperationState = "failing"
)

func (o OperationState) IsFinished() bool {
	switch o {
	case StateCompleted, StateFailed, StateAborted:
		return true
	}

	return false
}

func (o OperationState) MarshalYSON(w *yson.Writer) error {
	w.String(string(o))
	return nil
}

func (o *OperationState) UnmarshalYSON(data []byte) error {
	var value string
	if err := yson.Unmarshal(data, &value); err != nil {
		return err
	}

	// TODO(prime@): validate
	*o = OperationState(value)
	return nil
}

type NodeID guid.GUID

func (id NodeID) String() string {
	return guid.GUID(id).String()
}

func (id NodeID) MarshalYSON(w *yson.Writer) error {
	return guid.GUID(id).MarshalYSON(w)
}

func (id *NodeID) UnmarshalYSON(data []byte) (err error) {
	var g guid.GUID
	err = g.UnmarshalYSON(data)
	*id = NodeID(g)
	return
}

func (id NodeID) YPath() ypath.Path {
	return ypath.Path("#" + id.String())
}

type Revision uint64

type OperationID guid.GUID

func (id OperationID) String() string {
	return guid.GUID(id).String()
}

func (id OperationID) MarshalYSON(w *yson.Writer) error {
	return guid.GUID(id).MarshalYSON(w)
}

func (id *OperationID) UnmarshalYSON(data []byte) (err error) {
	var g guid.GUID
	err = g.UnmarshalYSON(data)
	*id = OperationID(g)
	return
}

type TxID guid.GUID

func (id TxID) String() string {
	return guid.GUID(id).String()
}

func (id TxID) MarshalYSON(w *yson.Writer) error {
	return guid.GUID(id).MarshalYSON(w)
}

func (id *TxID) UnmarshalYSON(data []byte) (err error) {
	var g guid.GUID
	err = g.UnmarshalYSON(data)
	*id = TxID(g)
	return
}

type MutationID guid.GUID

func (id MutationID) String() string {
	return guid.GUID(id).String()
}

func (id MutationID) MarshalYSON(w *yson.Writer) error {
	return guid.GUID(id).MarshalYSON(w)
}

func (id *MutationID) UnmarshalYSON(data []byte) (err error) {
	var g guid.GUID
	err = g.UnmarshalYSON(data)
	*id = MutationID(g)
	return
}

type LockMode string

const (
	LockShared    LockMode = "shared"
	LockExclusive LockMode = "exclusive"
)

func (m LockMode) MarshalText() ([]byte, error) {
	return []byte(m), nil
}

func (m *LockMode) UnmarshalText(b []byte) error {
	*m = LockMode(b)
	return nil
}

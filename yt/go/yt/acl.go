package yt

import (
	"go.ytsaurus.tech/library/go/ptr"
	"golang.org/x/xerrors"
)

// TODO(dakovalkov): create a different type for Permission
type Permission = string

const (
	PermissionRead                       Permission = "read"
	PermissionWrite                      Permission = "write"
	PermissionUse                        Permission = "use"
	PermissionAdminister                 Permission = "administer"
	PermissionCreate                     Permission = "create"
	PermissionRemove                     Permission = "remove"
	PermissionMount                      Permission = "mount"
	PermissionManage                     Permission = "manage"
	PermissionModifyChildren             Permission = "modify_children"
	PermissionRegisterQueueConsumer      Permission = "register_queue_consumer"
	PermissionRegisterQueueConsumerVital Permission = "register_queue_consumer_vital"
)

type SecurityAction string

const (
	ActionAllow SecurityAction = "allow"
	ActionDeny  SecurityAction = "deny"
)

// TODO: create a different type for InheritanceMode
type InheritanceMode = string

const (
	InheritanceModeNone                     InheritanceMode = ""
	InheritanceModeObjectOnly               InheritanceMode = "object_only"
	InheritanceModeObjectAndDescendants     InheritanceMode = "object_and_descendants"
	InheritanceModeDescendantsOnly          InheritanceMode = "descendants_only"
	InheritanceModeImmediateDescendantsOnly InheritanceMode = "immediate_descendants_only"
)

type ACE struct {
	Action             SecurityAction  `yson:"action,omitempty"`
	Subjects           []string        `yson:"subjects,omitempty"`
	Permissions        []Permission    `yson:"permissions,omitempty"`
	InheritanceMode    InheritanceMode `yson:"inheritance_mode,omitempty"`
	Columns            []string        `yson:"columns,omitempty"`
	RowAccessPredicate string          `yson:"row_access_predicate,omitempty"`
	Vital              *bool           `yson:"vital,omitempty"`
	SubjectTagFilter   string          `yson:"subject_tag_filter,omitempty"`
}

func ConvertPermissionType(typ *Permission) (*int32, error) {
	if typ == nil {
		return nil, nil
	}

	var ret int32

	switch *typ {
	case PermissionRead:
		ret = 0x0001
	case PermissionWrite:
		ret = 0x0002
	case PermissionUse:
		ret = 0x0004
	case PermissionAdminister:
		ret = 0x0008
	case PermissionCreate:
		ret = 0x0100
	case PermissionRemove:
		ret = 0x0200
	case PermissionMount:
		ret = 0x0400
	case PermissionManage:
		ret = 0x0800
	case PermissionModifyChildren:
		ret = 0x1000
	case PermissionRegisterQueueConsumer, PermissionRegisterQueueConsumerVital:
		ret = 0x0010
	default:
		return nil, xerrors.Errorf("unexpected permission type %q", *typ)
	}

	return &ret, nil
}

// NormalizeCheckPermission maps PermissionRegisterQueueConsumerVital to PermissionRegisterQueueConsumer
// and sets options.Vital to true when it was unset, so HTTP and RPC proxies receive a valid EPermission
// name plus the vital flag (see check_permission driver command).
func NormalizeCheckPermission(permission Permission, options *CheckPermissionOptions) (Permission, *CheckPermissionOptions) {
	if permission != PermissionRegisterQueueConsumerVital {
		return permission, options
	}
	var out *CheckPermissionOptions
	if options == nil {
		out = &CheckPermissionOptions{}
	} else {
		cp := *options
		out = &cp
	}
	if out.Vital == nil {
		out.Vital = ptr.Bool(true)
	}
	return PermissionRegisterQueueConsumer, out
}

func MustConvertPermissionType(typ *Permission) *int32 {
	code, err := ConvertPermissionType(typ)
	if err != nil {
		panic(err)
	}

	return code
}

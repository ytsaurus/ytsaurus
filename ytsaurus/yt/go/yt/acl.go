package yt

import "golang.org/x/xerrors"

// TODO(dakovalkov): create a different type for Permission
type Permission = string

const (
	PermissionRead           Permission = "read"
	PermissionWrite          Permission = "write"
	PermissionUse            Permission = "use"
	PermissionAdminister     Permission = "administer"
	PermissionCreate         Permission = "create"
	PermissionRemove         Permission = "remove"
	PermissionMount          Permission = "mount"
	PermissionManage         Permission = "manage"
	PermissionModifyChildren Permission = "modify_children"
)

type SecurityAction string

const (
	ActionAllow SecurityAction = "allow"
	ActionDeny  SecurityAction = "deny"
)

type ACE struct {
	Action          SecurityAction `yson:"action,omitempty"`
	Subjects        []string       `yson:"subjects,omitempty"`
	Permissions     []Permission   `yson:"permissions,omitempty"`
	InheritanceMode string         `yson:"inheritance_mode,omitempty"`
	Columns         []string       `yson:"columns,omitempty"`
	Vital           *bool          `yson:"vital,omitempty"`
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
	default:
		return nil, xerrors.Errorf("unexpected permission type %q", *typ)
	}

	return &ret, nil
}

func MustConvertPermissionType(typ *Permission) *int32 {
	code, err := ConvertPermissionType(typ)
	if err != nil {
		panic(err)
	}

	return code
}

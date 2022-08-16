package strawberry

import (
	"fmt"
	"regexp"

	"golang.org/x/xerrors"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
)

var (
	noSuchOperationRE         = regexp.MustCompile("[Nn]o such operation")
	aliasAlreadyUsedRE        = regexp.MustCompile("alias is already used by an operation")
	prerequisiteCheckFailedRE = regexp.MustCompile("[Pp]rerequisite check failed")
)

func requiresRestart(state yt.OperationState) bool {
	return state == yt.StateAborted ||
		state == yt.StateAborting ||
		state == yt.StateCompleted ||
		state == yt.StateCompleting ||
		state == yt.StateFailed ||
		state == yt.StateFailing
}

func toOperationACL(acl []yt.ACE) []yt.ACE {
	if acl == nil {
		return nil
	}
	result := make([]yt.ACE, 0, len(acl))

	for _, ace := range acl {
		shouldAllowReadPermission := false
		for _, permission := range ace.Permissions {
			if permission == yt.PermissionUse {
				shouldAllowReadPermission = true
				break
			}
		}
		if shouldAllowReadPermission {
			result = append(result, yt.ACE{
				Action:      ace.Action,
				Subjects:    ace.Subjects,
				Permissions: []yt.Permission{yt.PermissionRead},
			})
		}
	}

	return result
}

func extractOpID(err error) yt.OperationID {
	var ytErr *yterrors.Error
	if ok := xerrors.As(err, &ytErr); !ok {
		panic(fmt.Errorf("cannot convert error to YT error: %v", err))
	}
	// TODO(max42): there must be a way better to do this...
	ytErr = ytErr.InnerErrors[0].InnerErrors[0]
	opID, parseErr := guid.ParseString(ytErr.Attributes["operation_id"].(string))
	if parseErr != nil {
		panic(fmt.Errorf("malformed YT operation ID in error attributes: %v", parseErr))
	}
	return yt.OperationID(opID)
}

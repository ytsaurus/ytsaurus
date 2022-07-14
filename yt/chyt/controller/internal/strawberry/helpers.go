package strawberry

import (
	"strings"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

func tokenize(path ypath.Path) []string {
	parts := strings.Split(string(path), "/")
	var j int
	for i := 0; i < len(parts); i++ {
		if len(parts[i]) > 0 {
			parts[j] = parts[i]
			j++
		}
	}
	return parts[:j]
}

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

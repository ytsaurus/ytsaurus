package strawberry

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"text/template"

	"golang.org/x/xerrors"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

var (
	noSuchOperationRE         = regexp.MustCompile("[Nn]o such operation")
	aliasAlreadyUsedRE        = regexp.MustCompile("alias is already used by an operation")
	prerequisiteCheckFailedRE = regexp.MustCompile("[Pp]rerequisite check failed")
)

const AccessControlNamespacesPath = ypath.Path("//sys/access_control_object_namespaces")

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

type FieldDiff struct {
	OldValue any `yson:"old_value" json:"old_value"`
	NewValue any `yson:"new_value" json:"new_value"`
}

func specletDiff(oldSpeclet, newSpeclet any) map[string]FieldDiff {
	if !reflect.DeepEqual(reflect.TypeOf(oldSpeclet), reflect.TypeOf(newSpeclet)) {
		return nil
	}
	diff := make(map[string]FieldDiff)
	for _, field := range reflect.VisibleFields(reflect.TypeOf(oldSpeclet)) {
		if field.Anonymous {
			continue
		}
		old := reflect.ValueOf(oldSpeclet).FieldByIndex(field.Index).Interface()
		new := reflect.ValueOf(newSpeclet).FieldByIndex(field.Index).Interface()
		if !reflect.DeepEqual(old, new) {
			diff[field.Tag.Get("yson")] = FieldDiff{old, new}
		}
	}
	return diff
}

func ExecuteTemplate(templateString string, data any) string {
	t := template.Must(template.New("strawberry").Parse(templateString))
	b := new(bytes.Buffer)
	if err := t.Execute(b, data); err != nil {
		panic(err)
	}
	return b.String()
}

func ParseSpeclet(specletYson yson.RawValue) (Speclet, error) {
	var speclet Speclet
	if specletYson == nil {
		return speclet, nil
	}
	err := yson.Unmarshal(specletYson, &speclet)
	if err != nil {
		return speclet, yterrors.Err("failed to parse strawberry speclet", err)
	}
	return speclet, nil
}

func getYSONTimePointerOrNil(t yson.Time) *yson.Time {
	if t.IsZero() {
		return nil
	} else {
		return &t
	}
}

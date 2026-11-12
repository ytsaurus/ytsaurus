package strawberry

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"reflect"
	"regexp"
	"text/template"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

var (
	unknownOperationAliasRE   = regexp.MustCompile("^Operation alias is unknown$")
	noSuchOperationRE         = regexp.MustCompile("[Nn]o such operation")
	aliasAlreadyUsedRE        = regexp.MustCompile("alias is already used by an operation")
	prerequisiteCheckFailedRE = regexp.MustCompile("[Pp]rerequisite check failed")
)

type YsonHashEncoder struct {
	*yson.Encoder
	hasher hash.Hash
}

func (encoder *YsonHashEncoder) Sum() string {
	return hex.EncodeToString(encoder.hasher.Sum(nil))
}

func NewYsonHashEncoder() YsonHashEncoder {
	hasher := sha256.New()
	return YsonHashEncoder{
		Encoder: yson.NewEncoderWriter(
			yson.NewWriterConfig(
				hasher,
				yson.WriterConfig{
					Format: yson.FormatBinary,
					Kind:   yson.StreamListFragment,
				},
			),
		),
		hasher: hasher,
	}
}

func containsAnyErrorCode(err error, brokenStateSignalErrorCodes []yterrors.ErrorCode) bool {
	for _, code := range brokenStateSignalErrorCodes {
		if yterrors.ContainsErrorCode(err, code) {
			return true
		}
	}
	return false
}

const (
	AccessControlNamespacesPath = ypath.Path("//sys/access_control_object_namespaces")
	OpAliasFamilyDelimiter      = "::"
)

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
		if field.Anonymous || !field.IsExported() {
			continue
		}
		if field.Tag.Get("requires_restart") == "false" {
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

func ApplyControllerConfigOverrides(ctlCfgYson, overridesYson yson.RawValue) (yson.RawValue, error) {
	if len(overridesYson) == 0 {
		return ctlCfgYson, nil
	}

	var ctlCfg map[string]any
	if len(ctlCfgYson) > 0 {
		if err := yson.Unmarshal(ctlCfgYson, &ctlCfg); err != nil {
			return nil, fmt.Errorf("failed to parse controller config: %w", err)
		}
	}
	if ctlCfg == nil {
		ctlCfg = make(map[string]any)
	}

	var overrides map[string]any
	if err := yson.Unmarshal(overridesYson, &overrides); err != nil {
		return nil, fmt.Errorf("failed to parse controller overrides: %w", err)
	}
	mergeConfigMaps(ctlCfg, overrides)
	return yson.Marshal(ctlCfg)
}

func mergeConfigMaps(config, overrides map[string]any) {
	for key, value := range overrides {
		configMap, configIsMap := config[key].(map[string]any)
		overridesMap, overridesIsMap := value.(map[string]any)
		if configIsMap && overridesIsMap {
			mergeConfigMaps(configMap, overridesMap)
		} else {
			config[key] = value
		}
	}
}

func AddDefaultSpecletToConfig(ctlCfgYson, defaultSpecletYson yson.RawValue) (yson.RawValue, error) {
	if len(defaultSpecletYson) == 0 {
		return ctlCfgYson, nil
	}

	var defaultSpeclet map[string]any
	if err := yson.Unmarshal(defaultSpecletYson, &defaultSpeclet); err != nil {
		return nil, fmt.Errorf("failed to parse default speclet yson: %w", err)
	}

	var ctlCfg map[string]any
	if err := yson.Unmarshal(ctlCfgYson, &ctlCfg); err != nil {
		return nil, fmt.Errorf("failed to parse controller config yson: %w", err)
	}

	ctlCfg["default_speclet"] = defaultSpeclet
	return yson.Marshal(ctlCfg)
}

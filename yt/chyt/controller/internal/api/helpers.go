package api

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"

	"golang.org/x/exp/slices"

	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

func validateStringParameter(pattern string, value string) error {
	matched, err := regexp.MatchString(pattern, value)
	if err != nil {
		return err
	}
	if !matched {
		return yterrors.Err(fmt.Sprintf("%q does not match regular expression %q", value, pattern))
	}
	return nil
}

func validateAlias(alias any) error {
	return validateStringParameter(`^[A-Za-z][\w-]*$`, alias.(string))
}

func validateOption(option any) error {
	return validateStringParameter(`^[A-Za-z][\w./-]*$`, option.(string))
}

func unexpectedTypeError(typeName string) error {
	return yterrors.Err(
		fmt.Sprintf("parameter has unexpected value type %v", typeName),
		yterrors.Attr("type", typeName))
}

func validateSpecletOptions(speclet any) error {
	_, ok := speclet.(map[string]any)
	if !ok {
		typeName := fmt.Sprintf("%T", speclet)
		return unexpectedTypeError(typeName)
	}
	return nil
}

func validateSecrets(secrets any) error {
	secretsMap, ok := secrets.(map[string]any)
	if !ok {
		typeName := fmt.Sprintf("%T", secrets)
		return unexpectedTypeError(typeName)
	}
	allowedTypes := []string{"string", "int64", "uint64", "bool", "float64"}
	for _, v := range secretsMap {
		if vType := fmt.Sprintf("%T", v); !slices.Contains(allowedTypes, vType) {
			return unexpectedTypeError(vType)
		}
	}
	return nil
}

func validateBool(value any) error {
	_, ok := value.(bool)
	if !ok {
		typeName := fmt.Sprintf("%T", value)
		return unexpectedTypeError(typeName)
	}
	return nil
}

func validateString(value any) error {
	_, ok := value.(string)
	if !ok {
		typeName := fmt.Sprintf("%T", value)
		return unexpectedTypeError(typeName)
	}
	return nil
}

func transformToStringSlice(value any) (any, error) {
	if value == nil {
		return []string(nil), nil
	}

	array, ok := value.([]any)
	if !ok {
		typeName := fmt.Sprintf("%T", value)
		return nil, unexpectedTypeError(typeName)
	}

	transformedAttributes := []string{}
	for _, element := range array {
		if _, ok = element.(string); !ok {
			typeName := fmt.Sprintf("%T", value)
			return nil, unexpectedTypeError(typeName)
		}
		transformedAttributes = append(transformedAttributes, element.(string))
	}

	return transformedAttributes, nil
}

func transformFilterValues(value any) (any, error) {
	if value == nil {
		return map[string]any(nil), nil
	}

	filterVals, ok := value.(map[string]any)
	if !ok {
		typeName := fmt.Sprintf("%T", value)
		return nil, unexpectedTypeError(typeName)
	}

	newMap := make(map[string]any, len(filterVals))
	for k, v := range filterVals {
		if k == "state" {
			if stringV, ok := v.(string); ok {
				newMap[k] = strawberry.OpletState(stringV)
			} else {
				typeName := fmt.Sprintf("%T", v)
				return nil, unexpectedTypeError(typeName)
			}
		} else if k == "health" {
			if stringV, ok := v.(string); ok {
				newMap[k] = strawberry.OpletHealth(stringV)
			} else {
				typeName := fmt.Sprintf("%T", v)
				return nil, unexpectedTypeError(typeName)
			}
		} else {
			newMap[k] = v
		}
	}
	return newMap, nil
}

func getCreateSecretNodeOptions(secrets map[string]any, txOptions *yt.TransactionOptions) *yt.CreateNodeOptions {
	return &yt.CreateNodeOptions{
		Attributes: map[string]any{
			"value": secrets,
			"acl": []yt.ACE{
				{
					Action:   yt.ActionAllow,
					Subjects: []string{"owner"},
					Permissions: []yt.Permission{
						yt.PermissionAdminister,
						yt.PermissionRead,
						yt.PermissionWrite,
						yt.PermissionRemove,
					},
				},
			},
			"inherit_acl": false,
		},
		Force:              true,
		TransactionOptions: txOptions,
	}
}

func getRandomString(length int) string {
	letters := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func parseSpecletOptionPath(key string) ([]string, error) {
	tokens, err := ypath.SplitTokens("//" + key)
	if err != nil || len(tokens) < 2 || strings.Join(tokens, "") != "//"+key {
		return nil, fmt.Errorf("invalid option path %q", key)
	}
	parts := make([]string, 0, len(tokens)-1)
	for _, token := range tokens[1:] {
		part, err := ypath.UnescapeLiteral(strings.TrimPrefix(token, "/"))
		if err != nil || part == "" {
			return nil, fmt.Errorf("invalid option path %q", key)
		}
		parts = append(parts, part)
	}
	return parts, nil
}

// Apply path-based edits to the actual final map so permission checks see the
// same values that SetNode stores. This includes disk_request/account edits.
func editSpecletOption(speclet map[string]any, key string, value any, remove bool) (string, error) {
	parts, err := parseSpecletOptionPath(key)
	if err != nil {
		return "", err
	}
	path := ypath.Path("/" + key)
	if remove {
		_, err = ypath.RemoveMapValue(speclet, path)
	} else {
		err = ypath.SetMapValue(speclet, path, value, true)
	}
	return parts[0], err
}

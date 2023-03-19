package api

import (
	"fmt"
	"reflect"
	"regexp"

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

func validateSpeclet(speclet any) error {
	_, ok := speclet.(map[string]any)
	if !ok {
		typeName := reflect.TypeOf(speclet).String()
		return unexpectedTypeError(typeName)
	}
	return nil
}

func validateBool(value any) error {
	_, ok := value.(bool)
	if !ok {
		typeName := reflect.TypeOf(value).String()
		return unexpectedTypeError(typeName)
	}
	return nil
}

func validateUntracked(untracked any) error {
	if untracked == nil {
		return nil
	}
	return validateBool(untracked)
}

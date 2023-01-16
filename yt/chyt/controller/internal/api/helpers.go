package api

import (
	"fmt"
	"reflect"
	"regexp"

	"a.yandex-team.ru/yt/go/yterrors"
)

func ValidateStringParameter(pattern string, value string) error {
	matched, err := regexp.MatchString(pattern, value)
	if err != nil {
		return err
	}
	if !matched {
		return yterrors.Err(fmt.Sprintf("%q does not match regular expression %q", value, pattern))
	}
	return nil
}

func ValidateAlias(alias any) error {
	return ValidateStringParameter(`^[A-Za-z][\w-]*$`, alias.(string))
}

func ValidateOption(option any) error {
	return ValidateStringParameter(`^[A-Za-z][\w./-]*$`, option.(string))
}

func ValidateSpeclet(speclet any) error {
	_, ok := speclet.(map[string]any)
	if !ok {
		typeName := reflect.TypeOf(speclet).String()
		return yterrors.Err(
			fmt.Sprintf("speclet has unexpected value type %v", typeName),
			yterrors.Attr("type", typeName))
	}
	return nil
}

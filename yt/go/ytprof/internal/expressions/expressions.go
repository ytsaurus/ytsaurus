package expressions

import (
	"reflect"
	"regexp"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"

	"a.yandex-team.ru/yt/go/ytprof"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

var env *cel.Env
var envErr error
var envOnce sync.Once

func regexpInternal(pattern string, value string) (bool, error) {
	if len(pattern) > 0 && pattern[0] == '-' {
		val, err := regexp.MatchString(pattern[1:], value)
		return !val, err
	}
	return regexp.MatchString(pattern, value)
}

type Expression struct {
	expr cel.Program

	metadataPatterns map[string]string
}

func NewExpression(metaquery string, metadataPatterns map[string]string) (*Expression, error) {
	envOnce.Do(DeclareMetadataEnviroment)

	if envErr != nil {
		return nil, envErr
	}

	ast, issues := env.Compile(metaquery)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	prg, err := env.Program(ast)

	return &Expression{expr: prg, metadataPatterns: metadataPatterns}, err
}

func DeclareMetadataEnviroment() {
	var metadata ytprof.Metadata
	types := metadata.Types()

	declarations := make([]*expr.Decl, 0)
	for key, value := range types {
		declarations = append(declarations, decls.NewVar(key, value))
	}

	env, envErr = cel.NewEnv(cel.Declarations(declarations...))
}

func (e *Expression) Evaluate(metadata ytprof.Metadata) (bool, error) {
	out, _, err := e.expr.Eval(metadata.Vars())
	if err != nil {
		return false, err
	}

	val, err := out.ConvertToNative(reflect.TypeOf(true))

	if err != nil {
		return false, err
	}

	if !val.(bool) {
		return false, nil
	}

	for key, pattern := range e.metadataPatterns {
		if value, ok := metadata.MapData[key]; ok {
			match, err := regexpInternal(pattern, value)
			if err != nil {
				return false, err
			}
			if !match {
				return false, nil
			}
		} else {
			return false, nil
		}
	}

	return true, err
}

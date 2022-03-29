package expressions

import (
	"reflect"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"

	"a.yandex-team.ru/yt/go/ytprof"

	expr "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

var env *cel.Env
var envErr error
var envOnce sync.Once

type Expression struct {
	expr cel.Program
}

func NewExpression(exprText string) (*Expression, error) {
	envOnce.Do(DeclareMetadataEnviroment)

	if envErr != nil {
		return nil, envErr
	}

	ast, issues := env.Compile(exprText)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	prg, err := env.Program(ast)

	return &Expression{expr: prg}, err
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

	return val.(bool), err
}

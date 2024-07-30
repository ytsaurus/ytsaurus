package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/printer"
	"go/token"
	"go/types"
	"reflect"
	"strconv"
	"strings"
	"unicode"
)

var fset = token.NewFileSet()

type optionField struct {
	fieldName string
	httpName  string
	typ       string
	omitnil   bool
	omitempty bool
}

type optionsType struct {
	name     string
	embedded []string

	fields []optionField
}

func errorf(pos token.Pos, msg string, args ...any) error {
	return fmt.Errorf("%s: %s", fset.Position(pos).String(), fmt.Sprintf(msg, args...))
}

func parseOptions(typeSpec *ast.TypeSpec) (option *optionsType, err error) {
	option = &optionsType{}

	structType, ok := typeSpec.Type.(*ast.StructType)
	if !ok {
		err = errorf(typeSpec.Pos(), "type is not a struct")
		return
	}

	option.name = typeSpec.Name.Name
	for _, field := range structType.Fields.List {
		if star, ok := field.Type.(*ast.StarExpr); ok && len(field.Names) == 0 {
			option.embedded = append(option.embedded, star.X.(*ast.Ident).Name)
		} else {
			var f optionField
			f.fieldName = field.Names[0].Name
			f.typ = types.ExprString(field.Type)

			if field.Tag == nil {
				continue
			}

			var tagValue string
			tagValue, err = strconv.Unquote(field.Tag.Value)
			if err != nil {
				err = errorf(field.Pos(), "invalid string literal %v", err)
				return
			}

			httpName, ok := reflect.StructTag(tagValue).Lookup("http")
			if !ok {
				err = errorf(field.Pos(), "field %s is missing \"http\" tag\n", field.Names[0].Name)
				return
			}

			params := strings.Split(httpName, ",")

			f.httpName = params[0]

			for _, p := range params {
				if p == "omitnil" {
					f.omitnil = true
				}
				if p == "omitempty" {
					f.omitempty = true
				}
			}

			option.fields = append(option.fields, f)
		}
	}

	return
}

type variable struct {
	name string
	typ  string
}

type method struct {
	name        string
	httpVerb    string
	httpParams  []string
	optionsName string

	params  []variable
	results []variable

	extra bool
}

type interfaceType struct {
	name string

	methods []method
}

const (
	methodTagHTTPParams = "http:params:"
	methodTagHTTPVerb   = "http:verb:"
	methodTagHTTPExtra  = "http:extra"
)

func parseClient(typeSpec *ast.TypeSpec) (c *interfaceType, err error) {
	c = &interfaceType{}

	ifaceType, ok := typeSpec.Type.(*ast.InterfaceType)
	if !ok {
		err = errorf(typeSpec.Pos(), "type is not an interface")
		return
	}

	c.name = typeSpec.Name.Name
	for _, ifaceMethod := range ifaceType.Methods.List {
		switch typ := ifaceMethod.Type.(type) {
		case *ast.Ident:
			// skip embedded interfaces

		case *ast.FuncType:
			var m method

			m.name = ifaceMethod.Names[0].Name

			if ifaceMethod.Doc == nil {
				continue
			}

			for _, comment := range ifaceMethod.Doc.List {
				commentLine := strings.Trim(strings.TrimPrefix(comment.Text, "//"), " ")
				switch {
				case strings.HasPrefix(commentLine, methodTagHTTPParams):
					paramList := "[" + strings.TrimPrefix(commentLine, methodTagHTTPParams) + "]"
					if err = json.Unmarshal([]byte(paramList), &m.httpParams); err != nil {
						err = errorf(comment.Pos(), "invalid value of %q tag: %v", methodTagHTTPParams, err)
						return
					}

				case strings.HasPrefix(commentLine, methodTagHTTPVerb):
					httpVerb := strings.TrimPrefix(commentLine, methodTagHTTPVerb)
					m.httpVerb, err = strconv.Unquote(httpVerb)
					if err != nil {
						err = errorf(comment.Pos(), "invalid value of %q tag: %v", methodTagHTTPVerb, err)
						return
					}

				case strings.HasPrefix(commentLine, methodTagHTTPExtra):
					m.extra = true

				}
			}

			if m.httpVerb == "" {
				continue
			}

			extra := 2
			if m.extra {
				extra++
			}

			if len(m.httpParams)+extra != len(typ.Params.List) {
				err = errorf(typ.Pos(), "number of parameters is inconsistent with %q annotation", methodTagHTTPParams)
				return
			}

			var getTypeName func(e ast.Expr) string
			getTypeName = func(e ast.Expr) string {
				var typeName string

				switch typ := e.(type) {
				case *ast.SelectorExpr:
					typeName = fmt.Sprintf("%s.%s", typ.X.(*ast.Ident).Name, typ.Sel.Name)
				case *ast.Ident:
					typeName = typ.Name
					if unicode.IsUpper(rune(typeName[0])) {
						typeName = "yt." + typeName
					}
				case *ast.InterfaceType:
					typeName = "any"
				case *ast.ArrayType:
					typeName = "[]" + getTypeName(typ.Elt)
				default:
					var b strings.Builder
					_ = printer.Fprint(&b, fset, e)
					typeName = b.String()
				}
				return typeName
			}

			paramToVariable := func(p *ast.Field) variable {
				name := p.Names[0].Name
				typeName := getTypeName(p.Type)
				return variable{name: name, typ: typeName}
			}

			for i, p := range typ.Params.List {
				if i == 0 || i+1 == len(typ.Params.List) {
					continue
				}

				m.params = append(m.params, paramToVariable(p))
			}

			for i, r := range typ.Results.List {
				if i+1 == len(typ.Results.List) {
					continue
				}

				m.results = append(m.results, paramToVariable(r))
			}

			opts := typ.Params.List[len(typ.Params.List)-1]
			m.optionsName = paramToVariable(opts).typ
			if !strings.HasPrefix(m.optionsName, "*") {
				err = errorf(opts.Pos(), "options can be nil; options of %q should be ptr", m.name)
				return
			}
			m.optionsName = m.optionsName[1:]

			c.methods = append(c.methods, m)
		}
	}
	return
}

type file struct {
	clients []*interfaceType
	options []*optionsType
}

func parseFile(node *ast.File) (f file, err error) {
	for _, decl := range node.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}

		if len(genDecl.Specs) != 1 {
			continue
		}

		typeSpec, ok := genDecl.Specs[0].(*ast.TypeSpec)
		if !ok {
			continue
		}

		switch typeSpec.Type.(type) {
		case *ast.InterfaceType:
			var iface *interfaceType
			iface, err = parseClient(typeSpec)
			if err != nil {
				return
			}
			f.clients = append(f.clients, iface)

		case *ast.StructType:
			if !strings.HasSuffix(typeSpec.Name.Name, "Options") {
				continue
			}

			var option *optionsType
			option, err = parseOptions(typeSpec)
			if err != nil {
				return
			}
			f.options = append(f.options, option)
		}
	}

	return
}

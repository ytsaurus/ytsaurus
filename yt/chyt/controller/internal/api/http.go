package api

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"reflect"
	"strings"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/httputil/headers"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type RequestParams struct {
	// Params contains request parameters which are set by the user.
	// E.g. in CLI "--xxx yyy" should set an "xxx" parameter with the value "yyy".
	Params map[string]any `yson:"params" json:"params"`

	// Unparsed indicates that:
	//
	// 1. All params with a "store" action are provided as YSON strings or an array of YSON strings
	//    and should be parsed to proper types,
	// 2. Params with a "store_true" action can be provided as true/false,
	// 3. A null value for a param is equivalent to a missing param.
	//
	// It can be useful in CLI, where all params' types are unknown.
	Unparsed bool `yson:"unparsed" json:"unparsed"`
}

type FormatType string

const (
	FormatJSON FormatType = "json"
	FormatYSON FormatType = "yson"

	DefaultFormat FormatType = FormatYSON
)

func handleFormatNegotiation(header string) (FormatType, error) {
	if header == "" {
		return DefaultFormat, nil
	}

	formats, err := headers.ParseAccept(header)
	if err != nil {
		return "", err
	}

	for _, format := range formats {
		switch format.Type {
		case "application/yson":
			return FormatYSON, nil
		case "application/json":
			return FormatJSON, nil
		case "application/*", "*/*":
			return DefaultFormat, nil
		}
	}

	return "", yterrors.Err("unsupported format in Accept header", yterrors.Attr("accept_header", header))
}

func getFormatFromContentTypeHeader(header string) (FormatType, error) {
	mediatype, _, err := mime.ParseMediaType(header)
	if err != nil {
		return "", err
	}

	switch mediatype {
	case "application/yson":
		return FormatYSON, nil
	case "application/json":
		return FormatJSON, nil
	}

	return "", yterrors.Err("unsupported format in Content-Type header", yterrors.Attr("content_type_header", header))
}

func Unmarshal(data []byte, v any, format FormatType) error {
	switch format {
	case FormatYSON:
		return yson.Unmarshal(data, v)
	case FormatJSON:
		return json.Unmarshal(data, v)
	}
	return yterrors.Err("cannot unmarshal, invalid format type")
}

func Marshal(v any, format FormatType) ([]byte, error) {
	switch format {
	case FormatYSON:
		return yson.Marshal(v)
	case FormatJSON:
		return json.Marshal(v)
	}
	return nil, yterrors.Err("cannot marshal, invalid format type")
}

func tryCastFloats(v any) any {
	if f, ok := v.(float64); ok && f == float64(int64(f)) {
		return int64(f)
	} else if m, ok := v.(map[string]any); ok {
		for key, value := range m {
			m[key] = tryCastFloats(value)
		}
		return m
	}
	return v
}

// HTTPAPI is a lightweight wrapper of API which handles http requests and transforms them to proper API calls.
type HTTPAPI struct {
	api         *API
	l           log.Logger
	disableAuth bool
}

func NewHTTPAPI(ytc yt.Client, config APIConfig, ctl strawberry.Controller, l log.Logger, disableAuth bool) HTTPAPI {
	return HTTPAPI{
		api:         NewAPI(ytc, config, ctl, l),
		l:           l,
		disableAuth: disableAuth,
	}
}

func (a HTTPAPI) reply(w http.ResponseWriter, status int, rsp any) {
	format, err := getFormatFromContentTypeHeader(w.Header().Get("Content-Type"))
	if err != nil {
		a.l.Error("failed to get output format", log.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	body, err := Marshal(rsp, format)
	if err != nil {
		a.l.Error("failed to marshal response", log.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(status)
	_, err = w.Write(body)
	if err != nil {
		a.l.Error("failed to write response body", log.Error(err))
	}
}

// TODO(gudqeit): old CLI clients rely on "to_print" field to print appropriate message in case of error.
// Introduce better way to return error message and remove this wrapper.
type legacyErrorWrapper struct {
	*yterrors.Error
	ToPrint string `yson:"to_print,omitempty" json:"-"`
}

func (a HTTPAPI) replyWithError(w http.ResponseWriter, err error) {
	apiError := legacyErrorWrapper{
		Error:   yterrors.FromError(err).(*yterrors.Error),
		ToPrint: err.Error(),
	}
	a.reply(w, http.StatusBadRequest, apiError)
}

func (a HTTPAPI) replyOK(w http.ResponseWriter, result any) {
	if result == nil {
		a.reply(w, http.StatusOK, struct{}{})
	} else {
		a.reply(w, http.StatusOK, map[string]any{
			"result": result,
		})
	}
}

type ParamType string

const (
	TypeString ParamType = "string"
	TypeAny    ParamType = "any"

	ActionStore     string = "store"
	ActionStoreTrue string = "store_true"

	DefaultAction string = ActionStore
)

type CmdParameter struct {
	Name        string                 `yson:"name"`
	Aliases     []string               `yson:"aliases,omitempty"`
	Type        ParamType              `yson:"type"`
	Required    bool                   `yson:"required"`
	Action      string                 `yson:"action,omitempty"`
	Description string                 `yson:"description,omitempty"`
	EnvVariable string                 `yson:"env_variable,omitempty"`
	Validator   func(any) error        `yson:"-"`
	Transformer func(any) (any, error) `yson:"-"`

	// Element* fields describe an element of the parameter if the parameter is of an array type.
	// They are used in CLI to specify an array by repetition of an element option.

	ElementName        string    `yson:"element_name,omitempty"`
	ElementType        ParamType `yson:"element_type,omitempty"`
	ElementAliases     []string  `yson:"element_aliases,omitempty"`
	ElementDescription string    `yson:"element_description,omitempty"`
}

func (c *CmdParameter) ActionOrDefault() string {
	if c.Action != "" {
		return c.Action
	}
	return DefaultAction
}

// AsExplicit returns a copy of the parameter with empty EnvVariable field,
// so this parameter should be set explicitly in CLI.
func (c CmdParameter) AsExplicit() CmdParameter {
	c.EnvVariable = ""
	return c
}

type HandlerFunc func(api HTTPAPI, w http.ResponseWriter, r *http.Request, params map[string]any)

type CmdDescriptor struct {
	Name        string         `yson:"name"`
	Parameters  []CmdParameter `yson:"parameters"`
	Description string         `yson:"description,omitempty"`
	Handler     HandlerFunc    `yson:"-"`
}

func (a HTTPAPI) parseAndValidateRequestParams(w http.ResponseWriter, r *http.Request, cmd CmdDescriptor) map[string]any {
	outputFormat, err := handleFormatNegotiation(r.Header.Get("Accept"))
	if err != nil {
		a.replyWithError(w, err)
		return nil
	}

	// We need to set this header immediately because all reply functions use it.
	w.Header()["Content-Type"] = []string{fmt.Sprintf("application/%v", outputFormat)}

	inputFormat, err := getFormatFromContentTypeHeader(r.Header.Get("Content-Type"))
	if err != nil {
		a.replyWithError(w, err)
		return nil
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.replyWithError(w, yterrors.Err("error reading request body", err))
		return nil
	}

	var request RequestParams
	if err = Unmarshal(body, &request, inputFormat); err != nil {
		a.replyWithError(w, yterrors.Err("error parsing request body", err))
		return nil
	}

	if inputFormat == FormatJSON {
		// To unmarshal JSON into an interface value, Unmarshal stores number in float64 value.
		// https://pkg.go.dev/encoding/json#Unmarshal
		// That's why we try to cast some of the numbers to int64.
		request.Params = tryCastFloats(request.Params).(map[string]any)
	}

	params := request.Params

	// Remove nil unparsed params.
	if request.Unparsed {
		for key, value := range params {
			if value == nil {
				delete(params, key)
			}
		}
	}

	// Check that all required parameters are present.
	for _, param := range cmd.Parameters {
		if _, ok := params[param.Name]; param.Required && !ok {
			a.replyWithError(w, yterrors.Err("missing required parameter", yterrors.Attr("param_name", param.Name)))
			return nil
		}
	}

	// Validate that all present parameters are supported in the command.
	supportedParams := make(map[string]bool)
	for _, param := range cmd.Parameters {
		supportedParams[param.Name] = true
	}
	for name := range params {
		if !supportedParams[name] {
			a.replyWithError(w, yterrors.Err("unexpected parameter", yterrors.Attr("param_name", name)))
			return nil
		}
	}

	// Cast params to proper type.
	if request.Unparsed {
		if inputFormat == FormatJSON {
			a.replyWithError(w, yterrors.Err("decoding unparsed params in JSON format is unsupported"))
			return nil
		}
		for _, param := range cmd.Parameters {
			if value, ok := params[param.Name]; param.ActionOrDefault() == ActionStore && ok {
				if unparsedValue, ok := value.(string); ok {
					// Try to parse anything except the TypeString as a yson-string.
					if param.Type != TypeString {
						var parsedValue any
						if err = Unmarshal([]byte(unparsedValue), &parsedValue, inputFormat); err != nil {
							a.replyWithError(w, err)
							return nil
						}
						params[param.Name] = parsedValue
					}

				} else if array, ok := value.([]any); ok {
					for i, element := range array {
						unparsedElement, ok := element.(string)
						if !ok {
							a.replyWithError(w, yterrors.Err("unparsed parameter element has unexpected type",
								yterrors.Attr("param_name", param.ElementName),
								yterrors.Attr("param_type", reflect.TypeOf(element).String())))
							return nil
						}
						if param.ElementType != TypeString {
							var parsedElement any
							if err := Unmarshal([]byte(unparsedElement), &parsedElement, inputFormat); err != nil {
								a.replyWithError(w, err)
								return nil
							}
							array[i] = parsedElement
						}
					}

				} else {
					a.replyWithError(w, yterrors.Err("unparsed parameter has unexpected type",
						yterrors.Attr("param_name", param.Name),
						yterrors.Attr("param_type", reflect.TypeOf(value).String())))
					return nil
				}
			}
		}
	}

	// COMPAT(dakovalkov): We allow to specify alias with leading * for backward compatibility.
	if value, ok := params["alias"]; ok {
		alias := value.(string)
		if strings.HasPrefix(alias, "*") {
			params["alias"] = alias[1:]
		}
	}

	// Validate params' types and values.
	for _, param := range cmd.Parameters {
		if value, ok := params[param.Name]; ok {
			switch param.Type {
			case TypeString:
				_, ok = value.(string)
			}

			if !ok {
				a.replyWithError(w, yterrors.Err(fmt.Sprintf("parameter %v has unexpected type: expected %v, got %v",
					param.Name,
					param.Type,
					reflect.TypeOf(value).String()),
					yterrors.Attr("param_name", param.Name),
					yterrors.Attr("expected_type", param.Type),
					yterrors.Attr("actual_type", reflect.TypeOf(value).String())))
				return nil
			}

			if param.Validator != nil {
				if err := param.Validator(value); err != nil {
					a.replyWithError(w, yterrors.Err(fmt.Sprintf("failed to validate parameter %v", param.Name),
						err,
						yterrors.Attr("param_name", param.Name),
						yterrors.Attr("param_value", value)))
					return nil
				}
			}

			if param.Transformer != nil {
				transformedValue, err := param.Transformer(value)
				if err != nil {
					a.replyWithError(w, yterrors.Err(fmt.Sprintf("failed to transform parameter %v", param.Name),
						err,
						yterrors.Attr("param_name", param.Name),
						yterrors.Attr("param_value", value)))
					return nil
				}
				params[param.Name] = transformedValue
			}
		}
	}

	return params
}

func HandlePing(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func HandleDescribe(w http.ResponseWriter, r *http.Request, clusters []string) {
	body, err := yson.Marshal(map[string]any{
		"clusters": clusters,
		"commands": AllCommands,
	})
	if err != nil {
		panic(err)
	}
	w.Header()["Content-Type"] = []string{"application/yson"}
	_, _ = w.Write(body)
}

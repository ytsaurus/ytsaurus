package api

import (
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

type RequestParams struct {
	// Params contains request parameters which are set by the user.
	// E.g. in CLI "--xxx yyy" should set an "xxx" parameter with the value "yyy".
	Params map[string]any `yson:"params"`
	// Unparsed indicates that all params with action "store" are provided as strings and should be parsed to proper types.
	// It can be useful in CLI, where all arguments are strings and params' types are unknown.
	Unparsed bool `yson:"unparsed"`
}

// HTTPAPI is a lightweight wrapper of API which handles http requests and transforms them to proper API calls.
type HTTPAPI struct {
	api *API
	l   log.Logger
}

func NewHTTPAPI(ytc yt.Client, config APIConfig, ctl strawberry.Controller, l log.Logger) HTTPAPI {
	return HTTPAPI{
		api: NewAPI(ytc, config, ctl, l),
		l:   l,
	}
}

func (a HTTPAPI) reply(w http.ResponseWriter, status int, rsp any) {
	body, err := yson.Marshal(rsp)
	if err != nil {
		a.l.Error("failed to marshal response", log.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header()["Content-Type"] = []string{"application/yson"}
	w.WriteHeader(status)
	_, err = w.Write(body)
	if err != nil {
		a.l.Error("failed to write response body", log.Error(err))
	}
}

func (a HTTPAPI) replyWithError(w http.ResponseWriter, err error) {
	a.reply(w, http.StatusBadRequest, map[string]any{
		"to_print": err.Error(),
		"error":    yterrors.FromError(err),
	})
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

	DefaultAction   string = "store"
	StoreAction     string = "store"
	StoreTrueAction string = "store_true"
)

type CmdParameter struct {
	Name        string          `yson:"name"`
	Aliases     []string        `yson:"aliases,omitempty"`
	Type        ParamType       `yson:"type"`
	Required    bool            `yson:"required"`
	Action      string          `yson:"action,omitempty"`
	Description string          `yson:"description,omitempty"`
	EnvVariable string          `yson:"env_variable,omitempty"`
	Validator   func(any) error `yson:"-"`
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

type CmdDescriptor struct {
	Name        string         `yson:"name"`
	Parameters  []CmdParameter `yson:"parameters"`
	Description string         `yson:"description,omitempty"`
}

func (a HTTPAPI) parseAndValidateRequestParams(w http.ResponseWriter, r *http.Request, cmd CmdDescriptor) map[string]any {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.replyWithError(w, yterrors.Err("error reading request body", err))
		return nil
	}

	var request RequestParams
	err = yson.Unmarshal(body, &request)
	if err != nil {
		a.replyWithError(w, yterrors.Err("error parsing request body", err))
		return nil
	}

	params := request.Params

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
		for _, param := range cmd.Parameters {
			if value, ok := params[param.Name]; param.ActionOrDefault() == StoreAction && ok {
				unparsedValue, ok := value.(string)
				if !ok {
					a.replyWithError(w, yterrors.Err("unparsed parameter has unexpected type",
						yterrors.Attr("param_name", param.Name),
						yterrors.Attr("param_type", reflect.TypeOf(value).String())))
					return nil
				}
				// Try to parse anything except the TypeString as a yson-string.
				if param.Type != TypeString {
					var parsedValue any
					err := yson.Unmarshal([]byte(unparsedValue), &parsedValue)
					if err != nil {
						a.replyWithError(w, err)
						return nil
					}
					params[param.Name] = parsedValue
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
		"commands": []CmdDescriptor{
			ListCmdDescriptor,
			CreateCmdDescriptor,
			RemoveCmdDescriptor,
			ExistsCmdDescriptor,
			StatusCmdDescriptor,
			GetOptionCmdDescriptor,
			SetOptionCmdDescriptor,
			RemoveOptionCmdDescriptor,
			GetSpecletCmdDescriptor,
			SetSpecletCmdDescriptor,
			StartCmdDescriptor,
			StopCmdDescriptor,
		}})
	if err != nil {
		panic(err)
	}
	w.Header()["Content-Type"] = []string{"application/yson"}
	_, _ = w.Write(body)
}

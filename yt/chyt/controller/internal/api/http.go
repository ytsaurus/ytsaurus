package api

import (
	"io"
	"net/http"
	"reflect"

	"a.yandex-team.ru/library/go/core/log"

	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yterrors"
)

type RequestParams struct {
	// Params contains request parameters which are set by the user.
	// E.g. in CLI "--xxx yyy" should set an "xxx" parameter with the value "yyy".
	Params map[string]any `yson:"params"`
	// ImplicitParams contains params which are not set directly by the user.
	// Implicit parameters do not cause "unknown parameter" errors if the command does not support such a parameter.
	// If there are both explicit and implicit parameters with the same name, the explicit one is prefered.
	// E.g. these parameters can be set via environment variables (YT_STRAWBERRY_ALIAS).
	ImplicitParams map[string]any `yson:"implicit_params"`
	// Args contains a list of parameters without names. The names will be derived from the command syntax.
	// If the parameter is present in both Args and Params, the error will be thrown.
	// E.g. in CLI "... set xxx yyy" should set Args to ["xxx";"yyy"].
	Args []any `yson:"args"`
	// Unparsed indicates that all params are provided as strings and should be parsed to proper types.
	// It can be useful in CLI, where all arguments are strings and params' types are unknown.
	Unparsed bool `yson:"unparsed"`
}

// HTTPAPI is a lightweight wrapper of API which handles http requests and transforms them to proper API calls.
type HTTPAPI struct {
	api *API
	l   log.Logger
}

func NewHTTPAPI(ytc yt.Client, config APIConfig, l log.Logger) HTTPAPI {
	return HTTPAPI{
		api: NewAPI(ytc, config, l),
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
	a.reply(w, http.StatusBadRequest, yterrors.FromError(err))
}

func (a HTTPAPI) replyOK(w http.ResponseWriter, rsp any) {
	a.reply(w, http.StatusOK, rsp)
}

type ParamType string

const (
	TypeString ParamType = "string"
	TypeAny    ParamType = "any"
)

type CmdDescriptor struct {
	Args           []string
	RequiredParams []string
	OptionalParams []string
	ParamTypes     map[string]ParamType
}

var defaultParamTypes = map[string]ParamType{
	"alias": TypeString,
	"key":   TypeString,
	"value": TypeAny,
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

	if len(request.Args) > len(cmd.Args) {
		a.replyWithError(w, yterrors.Err("too many arguments",
			yterrors.Attr("req_arg_count", len(request.Args)),
			yterrors.Attr("cmd_arg_count", len(cmd.Args))))
		return nil
	}

	params := request.Params

	// Converting args to params.
	for index, value := range request.Args {
		name := cmd.Args[index]
		if _, ok := params[name]; ok {
			a.replyWithError(w, yterrors.Err("parameter is present in both arguments and parameters",
				yterrors.Attr("param_name", name)))
			return nil
		}
		params[name] = value
	}

	// Check that all required parameters are present.
	for _, name := range cmd.RequiredParams {
		if _, ok := params[name]; !ok {
			if value, ok := request.ImplicitParams[name]; ok {
				params[name] = value
			} else {
				a.replyWithError(w, yterrors.Err("missing required parameter", yterrors.Attr("param_name", name)))
				return nil
			}
		}
	}

	// Validate that all present parameters are supported in the command.
	supportedParams := make(map[string]bool)

	for _, name := range cmd.Args {
		supportedParams[name] = true
	}
	for _, name := range cmd.RequiredParams {
		supportedParams[name] = true
	}
	for _, name := range cmd.OptionalParams {
		supportedParams[name] = true
	}

	for name := range params {
		if !supportedParams[name] {
			a.replyWithError(w, yterrors.Err("unexpected parameter", yterrors.Attr("param_name", name)))
			return nil
		}
	}

	// Cast params to proper type.
	if request.Unparsed {
		for name, value := range params {
			unparsedValue, ok := value.(string)
			if !ok {
				a.replyWithError(w, yterrors.Err("unparsed parameter has unexpected type",
					yterrors.Attr("param_name", name),
					yterrors.Attr("param_type", reflect.TypeOf(value).String())))
				return nil
			}
			// Try to parse anything expect the TypeString as a yson-string.
			if cmd.ParamTypes[name] != TypeString {
				var parsedValue any
				err := yson.Unmarshal([]byte(unparsedValue), &parsedValue)
				if err != nil {
					a.replyWithError(w, err)
					return nil
				}
				params[name] = parsedValue
			}
		}
	}

	// Validate params' types.
	if cmd.ParamTypes != nil {
		for name, value := range params {
			var ok = true

			switch cmd.ParamTypes[name] {
			case TypeString:
				_, ok = value.(string)
			}

			if !ok {
				a.replyWithError(w, yterrors.Err("invalid parameter type",
					yterrors.Attr("param_name", name),
					yterrors.Attr("expected_type", cmd.ParamTypes[name]),
					yterrors.Attr("actual_type", reflect.TypeOf(value).String())))
			}
		}
	}

	return params
}

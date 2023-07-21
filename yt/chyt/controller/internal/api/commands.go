package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/auth"
	"go.ytsaurus.tech/yt/chyt/controller/internal/httpserver"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

var AliasParameter = CmdParameter{
	Name:        "alias",
	Aliases:     []string{"a"},
	Type:        TypeString,
	Required:    true,
	Description: "alias for the operation",
	EnvVariable: "ALIAS",
	Validator:   validateAlias,
}

var ListCmdDescriptor = CmdDescriptor{
	Name:        "list",
	Parameters:  []CmdParameter{},
	Description: "list all strawberry operations on the cluster",
}

func (a HTTPAPI) HandleList(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, ListCmdDescriptor)
	if params == nil {
		return
	}

	aliases, err := a.api.List(r.Context())
	if err != nil {
		a.replyWithError(w, err)
		return
	}

	a.replyOK(w, aliases)
}

var CreateCmdDescriptor = CmdDescriptor{
	Name:        "create",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit()},
	Description: "create a new strawberry operation",
}

func (a HTTPAPI) HandleCreate(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, CreateCmdDescriptor)
	if params == nil {
		return
	}

	alias := params["alias"].(string)

	err := a.api.Create(r.Context(), alias)
	if err != nil {
		a.replyWithError(w, err)
		return
	}

	a.replyOK(w, nil)
}

var RemoveCmdDescriptor = CmdDescriptor{
	Name:        "remove",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit()},
	Description: "remove the strawberry operation",
}

func (a HTTPAPI) HandleRemove(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, RemoveCmdDescriptor)
	if params == nil {
		return
	}

	alias := params["alias"].(string)

	err := a.api.Remove(r.Context(), alias)
	if err != nil {
		a.replyWithError(w, err)
		return
	}

	a.replyOK(w, nil)
}

var ExistsCmdDescriptor = CmdDescriptor{
	Name:        "exists",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit()},
	Description: "check the strawberry operation existence",
}

func (a HTTPAPI) HandleExists(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, ExistsCmdDescriptor)
	if params == nil {
		return
	}

	alias := params["alias"].(string)

	ok, err := a.api.Exists(r.Context(), alias)
	if err != nil {
		a.replyWithError(w, err)
		return
	}

	a.replyOK(w, ok)
}

var StatusCmdDescriptor = CmdDescriptor{
	Name:        "status",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit()},
	Description: "show strawberry operation status",
}

func (a HTTPAPI) HandleStatus(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, StatusCmdDescriptor)
	if params == nil {
		return
	}

	alias := params["alias"].(string)

	status, err := a.api.Status(r.Context(), alias)
	if err != nil {
		a.replyWithError(w, err)
		return
	}

	a.replyOK(w, status)
}

var KeyParameter = CmdParameter{
	Name:        "key",
	Type:        TypeString,
	Required:    true,
	Description: "speclet option name",
	Validator:   validateOption,
}

var ValueParameter = CmdParameter{
	Name:        "value",
	Type:        TypeAny,
	Required:    true,
	Description: "speclet option value",
}

var GetOptionCmdDescriptor = CmdDescriptor{
	Name:        "get_option",
	Parameters:  []CmdParameter{AliasParameter, KeyParameter},
	Description: "get speclet option",
}

func (a HTTPAPI) HandleGetOption(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, GetOptionCmdDescriptor)
	if params == nil {
		return
	}
	alias := params["alias"].(string)
	key := params["key"].(string)
	value, err := a.api.GetOption(r.Context(), alias, key)
	if err != nil {
		a.replyWithError(w, err)
		return
	}
	a.replyOK(w, value)
}

var SetOptionCmdDescriptor = CmdDescriptor{
	Name:        "set_option",
	Parameters:  []CmdParameter{AliasParameter, KeyParameter, ValueParameter},
	Description: "set speclet option",
}

func (a HTTPAPI) HandleSetOption(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, SetOptionCmdDescriptor)
	if params == nil {
		return
	}

	alias := params["alias"].(string)
	key := params["key"].(string)
	value := params["value"]

	err := a.api.SetOption(r.Context(), alias, key, value)
	if err != nil {
		a.replyWithError(w, err)
		return
	}

	a.replyOK(w, nil)
}

var RemoveOptionCmdDescriptor = CmdDescriptor{
	Name:        "remove_option",
	Parameters:  []CmdParameter{AliasParameter, KeyParameter},
	Description: "remove speclet option",
}

func (a HTTPAPI) HandleRemoveOption(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, RemoveOptionCmdDescriptor)
	if params == nil {
		return
	}

	alias := params["alias"].(string)
	key := params["key"].(string)

	err := a.api.RemoveOption(r.Context(), alias, key)
	if err != nil {
		a.replyWithError(w, err)
		return
	}

	a.replyOK(w, nil)
}

var GetSpecletCmdDescriptor = CmdDescriptor{
	Name:        "get_speclet",
	Parameters:  []CmdParameter{AliasParameter},
	Description: "get strawberry operation speclet",
}

func (a HTTPAPI) HandleGetSpeclet(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, GetSpecletCmdDescriptor)
	if params == nil {
		return
	}
	alias := params["alias"].(string)
	speclet, err := a.api.GetSpeclet(r.Context(), alias)
	if err != nil {
		a.replyWithError(w, err)
		return
	}
	a.replyOK(w, speclet)
}

var SpecletParameter = CmdParameter{
	Name:        "speclet",
	Type:        TypeAny,
	Required:    true,
	Description: "speclet in yson format",
	Validator:   validateSpecletOptions,
}

var SetSpecletCmdDescriptor = CmdDescriptor{
	Name:        "set_speclet",
	Parameters:  []CmdParameter{AliasParameter, SpecletParameter},
	Description: "set strawberry operation speclet",
}

func (a HTTPAPI) HandleSetSpeclet(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, SetSpecletCmdDescriptor)
	if params == nil {
		return
	}
	alias := params["alias"].(string)
	speclet := params["speclet"].(map[string]any)
	if err := a.api.SetSpeclet(r.Context(), alias, speclet); err != nil {
		a.replyWithError(w, err)
		return
	}
	a.replyOK(w, nil)
}

var OptionsParameter = CmdParameter{
	Name:        "options",
	Type:        TypeAny,
	Required:    true,
	Description: "speclet options in yson format",
	Validator:   validateSpecletOptions,
}

var SetOptionsCmdDescriptor = CmdDescriptor{
	Name:        "set_options",
	Parameters:  []CmdParameter{AliasParameter, OptionsParameter},
	Description: "set multiple speclet options",
}

func (a HTTPAPI) HandleSetOptions(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, SetOptionsCmdDescriptor)
	if params == nil {
		return
	}
	alias := params["alias"].(string)
	options := params["options"].(map[string]any)
	if err := a.api.SetOptions(r.Context(), alias, options); err != nil {
		a.replyWithError(w, err)
		return
	}
	a.replyOK(w, nil)
}

var UntrackedParameter = CmdParameter{
	Name:   "untracked",
	Action: StoreTrueAction,
	Description: "start operation via user credentials; " +
		"operation will not be tracked by the controller; " +
		"highly unrecommended for production operations",
	Validator: validateUntracked,
}

var StartCmdDescriptor = CmdDescriptor{
	Name:        "start",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit(), UntrackedParameter},
	Description: "start strawberry operation",
}

func (a HTTPAPI) HandleStart(w http.ResponseWriter, r *http.Request) {
	userToken, err := auth.GetTokenFromHeader(r)
	if err != nil && !a.disableAuth {
		a.replyWithError(w, err)
		return
	}
	userClient, err := ythttp.NewClient(&yt.Config{
		Token:  userToken,
		Proxy:  a.api.cfg.AgentInfo.Proxy,
		Logger: a.l.Structured(),
	})
	if err != nil {
		a.replyWithError(w, err)
		return
	}
	params := a.parseAndValidateRequestParams(w, r, StartCmdDescriptor)
	if params == nil {
		return
	}
	alias := params["alias"].(string)
	untracked := false
	if params["untracked"] != nil {
		untracked = params["untracked"].(bool)
	}
	if err := a.api.Start(r.Context(), alias, untracked, userClient); err != nil {
		a.replyWithError(w, err)
		return
	}
	status, err := a.api.Status(r.Context(), alias)
	if err != nil {
		a.replyWithError(w, err)
		return
	}
	a.replyOK(w, status)
}

var StopCmdDescriptor = CmdDescriptor{
	Name:        "stop",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit()},
	Description: "stop strawberry operation",
}

func (a HTTPAPI) HandleStop(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, StopCmdDescriptor)
	if params == nil {
		return
	}
	alias := params["alias"].(string)
	if err := a.api.Stop(r.Context(), alias); err != nil {
		a.replyWithError(w, err)
		return
	}
	a.replyOK(w, nil)
}

var DescribeOptionsCmdDescriptor = CmdDescriptor{
	Name:       "describe_options",
	Parameters: []CmdParameter{AliasParameter.AsExplicit()},
}

func (a HTTPAPI) HandleDescribeOptions(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, DescribeOptionsCmdDescriptor)
	if params == nil {
		return
	}

	alias := params["alias"].(string)

	options, err := a.api.DescribeOptions(r.Context(), alias)
	if err != nil {
		a.replyWithError(w, err)
		return
	}

	a.replyOK(w, options)
}

func RegisterHTTPAPI(cfg HTTPAPIConfig, l log.Logger) chi.Router {
	var clusters []string
	for _, clusterInfo := range cfg.ClusterInfos {
		clusters = append(clusters, clusterInfo.Proxy)
	}

	r := chi.NewRouter()
	r.Get("/ping", HandlePing)
	r.Get("/describe", func(w http.ResponseWriter, r *http.Request) {
		HandleDescribe(w, r, clusters)
	})

	for _, clusterInfo := range cfg.ClusterInfos {
		ytc, err := ythttp.NewClient(&yt.Config{
			Token:  cfg.Token,
			Proxy:  clusterInfo.Proxy,
			Logger: l.Structured(),
		})
		if err != nil {
			l.Fatal("failed to create yt client", log.Error(err), log.String("cluster", clusterInfo.Proxy))
		}

		apiCfg := cfg.BaseAPIConfig
		apiCfg.AgentInfo = clusterInfo

		ctl := apiCfg.ControllerFactory.Factory(l, ytc, clusterInfo.StrawberryRoot, clusterInfo.Proxy, apiCfg.ControllerConfig)

		api := NewHTTPAPI(ytc, apiCfg, ctl, l, cfg.DisableAuth)

		r.Route("/"+clusterInfo.Proxy, func(r chi.Router) {
			// TODO(dakovalkov): Enable CORS when cookie authentication is supported.
			// r.Use(ythttputil.CORS())
			r.Use(auth.Auth(clusterInfo.Proxy, cfg.DisableAuth, l.Structured()))
			r.Post("/"+ListCmdDescriptor.Name, api.HandleList)
			r.Post("/"+CreateCmdDescriptor.Name, api.HandleCreate)
			r.Post("/"+RemoveCmdDescriptor.Name, api.HandleRemove)
			r.Post("/"+ExistsCmdDescriptor.Name, api.HandleExists)
			r.Post("/"+StatusCmdDescriptor.Name, api.HandleStatus)
			r.Post("/"+GetOptionCmdDescriptor.Name, api.HandleGetOption)
			r.Post("/"+SetOptionCmdDescriptor.Name, api.HandleSetOption)
			r.Post("/"+RemoveOptionCmdDescriptor.Name, api.HandleRemoveOption)
			r.Post("/"+GetSpecletCmdDescriptor.Name, api.HandleGetSpeclet)
			r.Post("/"+SetSpecletCmdDescriptor.Name, api.HandleSetSpeclet)
			r.Post("/"+SetOptionsCmdDescriptor.Name, api.HandleSetOptions)
			r.Post("/"+StartCmdDescriptor.Name, api.HandleStart)
			r.Post("/"+StopCmdDescriptor.Name, api.HandleStop)
			r.Post("/"+DescribeOptionsCmdDescriptor.Name, api.HandleDescribeOptions)
		})
	}
	return r
}

func NewServer(c HTTPAPIConfig, l log.Logger) *httpserver.HTTPServer {
	handler := RegisterHTTPAPI(c, l)
	return httpserver.New(c.Endpoint, handler)
}

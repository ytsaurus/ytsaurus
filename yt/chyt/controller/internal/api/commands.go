package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/hostrouter"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/chyt/controller/internal/auth"
	"go.ytsaurus.tech/yt/chyt/controller/internal/httpserver"
	"go.ytsaurus.tech/yt/chyt/controller/internal/strawberry"
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

var AttributesParameter = CmdParameter{
	Name:        "attributes",
	Type:        TypeAny,
	Description: "strawberry operation attributes to add into response in yson list format",
	Transformer: transformToStringSlice,

	ElementName:        "attribute",
	ElementType:        TypeString,
	ElementDescription: "strawberry operation attribute to add into response",
}

var ListCmdDescriptor = CmdDescriptor{
	Name:        "list",
	Parameters:  []CmdParameter{AttributesParameter},
	Description: "list all strawberry operations on the cluster",
	Handler:     HTTPAPI.HandleList,
}

func (a HTTPAPI) HandleList(w http.ResponseWriter, r *http.Request, params map[string]any) {
	var attributes []string
	if value, ok := params["attributes"]; ok {
		attributes = value.([]string)
	}
	aliases, err := a.API.List(r.Context(), attributes)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}

	a.ReplyOK(w, aliases)
}

var SpecletOptionsParameter = CmdParameter{
	Name:        "speclet_options",
	Type:        TypeAny,
	Description: "speclet options in yson format",
	Validator:   validateSpecletOptions,
}

var CreateCmdDescriptor = CmdDescriptor{
	Name: "create",
	Parameters: []CmdParameter{
		AliasParameter.AsExplicit(),
		SpecletOptionsParameter,
	},
	Description: "create a new strawberry operation",
	Handler:     HTTPAPI.HandleCreate,
}

func (a HTTPAPI) HandleCreate(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)
	var specletOptions map[string]any
	if value, ok := params["speclet_options"]; ok {
		specletOptions = value.(map[string]any)
	}

	err := a.API.Create(r.Context(), alias, specletOptions)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}

	a.ReplyOK(w, nil)
}

var RemoveCmdDescriptor = CmdDescriptor{
	Name:        "remove",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit()},
	Description: "remove the strawberry operation",
	Handler:     HTTPAPI.HandleRemove,
}

func (a HTTPAPI) HandleRemove(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)

	err := a.API.Remove(r.Context(), alias)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}

	a.ReplyOK(w, nil)
}

var ExistsCmdDescriptor = CmdDescriptor{
	Name:        "exists",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit()},
	Description: "check the strawberry operation existence",
	Handler:     HTTPAPI.HandleExists,
}

func (a HTTPAPI) HandleExists(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)

	ok, err := a.API.Exists(r.Context(), alias)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}

	a.ReplyOK(w, ok)
}

// COMPAT(gudqeit)
var StatusCmdDescriptor = CmdDescriptor{
	Name:        "status",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit()},
	Description: "show strawberry operation status",
	Handler:     HTTPAPI.HandleGetBriefInfo,
}

var GetBriefInfoCmdDescriptor = CmdDescriptor{
	Name:        "get_brief_info",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit()},
	Description: "show strawberry operation brief info",
	Handler:     HTTPAPI.HandleGetBriefInfo,
}

func (a HTTPAPI) HandleGetBriefInfo(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)

	briefInfo, err := a.API.GetBriefInfo(r.Context(), alias)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}

	a.ReplyOK(w, briefInfo)
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
	Handler:     HTTPAPI.HandleGetOption,
}

func (a HTTPAPI) HandleGetOption(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)
	key := params["key"].(string)
	value, err := a.API.GetOption(r.Context(), alias, key)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}
	a.ReplyOK(w, value)
}

var SetOptionCmdDescriptor = CmdDescriptor{
	Name:        "set_option",
	Parameters:  []CmdParameter{AliasParameter, KeyParameter, ValueParameter},
	Description: "set speclet option",
	Handler:     HTTPAPI.HandleSetOption,
}

func (a HTTPAPI) HandleSetOption(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)
	key := params["key"].(string)
	value := params["value"]

	err := a.API.SetOption(r.Context(), alias, key, value)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}

	a.ReplyOK(w, nil)
}

var RemoveOptionCmdDescriptor = CmdDescriptor{
	Name:        "remove_option",
	Parameters:  []CmdParameter{AliasParameter, KeyParameter},
	Description: "remove speclet option",
	Handler:     HTTPAPI.HandleRemoveOption,
}

func (a HTTPAPI) HandleRemoveOption(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)
	key := params["key"].(string)

	err := a.API.RemoveOption(r.Context(), alias, key)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}

	a.ReplyOK(w, nil)
}

var GetSpecletCmdDescriptor = CmdDescriptor{
	Name:        "get_speclet",
	Parameters:  []CmdParameter{AliasParameter},
	Description: "get strawberry operation speclet",
	Handler:     HTTPAPI.HandleGetSpeclet,
}

func (a HTTPAPI) HandleGetSpeclet(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)
	speclet, err := a.API.GetSpeclet(r.Context(), alias)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}
	a.ReplyOK(w, speclet)
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
	Handler:     HTTPAPI.HandleSetSpeclet,
}

func (a HTTPAPI) HandleSetSpeclet(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)
	speclet := params["speclet"].(map[string]any)
	if err := a.API.SetSpeclet(r.Context(), alias, speclet); err != nil {
		a.ReplyWithError(w, err)
		return
	}
	a.ReplyOK(w, nil)
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
	Handler:     HTTPAPI.HandleSetOptions,
}

func (a HTTPAPI) HandleSetOptions(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)
	options := params["options"].(map[string]any)
	if err := a.API.EditOptions(r.Context(), alias, options, nil); err != nil {
		a.ReplyWithError(w, err)
		return
	}
	a.ReplyOK(w, nil)
}

var OptionsToSetParameter = CmdParameter{
	Name:        "options_to_set",
	Type:        TypeAny,
	Required:    true,
	Description: "speclet options to set in yson format",
	Validator:   validateSpecletOptions,
}

var OptionsToRemoveParameter = CmdParameter{
	Name:        "options_to_remove",
	Type:        TypeAny,
	Required:    true,
	Description: "speclet options to remove in yson format",
	Transformer: transformToStringSlice,
}

var EditOptionsCmdDescriptor = CmdDescriptor{
	Name:        "edit_options",
	Parameters:  []CmdParameter{AliasParameter, OptionsToSetParameter, OptionsToRemoveParameter},
	Description: "edit multiple speclet options",
	Handler:     HTTPAPI.HandleEditOptions,
}

func (a HTTPAPI) HandleEditOptions(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)
	optionsToSet := params["options_to_set"].(map[string]any)
	optionsToRemove := params["options_to_remove"].([]string)
	if err := a.API.EditOptions(r.Context(), alias, optionsToSet, optionsToRemove); err != nil {
		a.ReplyWithError(w, err)
		return
	}
	a.ReplyOK(w, nil)
}

var UntrackedParameter = CmdParameter{
	Name:   "untracked",
	Action: ActionStoreTrue,
	Description: "start operation via user credentials; " +
		"operation will not be tracked by the controller; " +
		"highly unrecommended for production operations",
	Validator: validateBool,
}

var StartCmdDescriptor = CmdDescriptor{
	Name:        "start",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit(), UntrackedParameter},
	Description: "start strawberry operation",
	Handler:     HTTPAPI.HandleStart,
}

func (a HTTPAPI) HandleStart(w http.ResponseWriter, r *http.Request, params map[string]any) {
	credentials, err := auth.GetCredentials(r)
	if err != nil && !a.disableAuth {
		a.ReplyWithError(w, err)
		return
	}
	userClient, err := ythttp.NewClient(&yt.Config{
		Credentials: credentials,
		Proxy:       a.API.cfg.AgentInfo.Proxy,
		Logger:      a.L.Structured(),
	})
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}
	alias := params["alias"].(string)
	untracked := false
	if value, ok := params["untracked"]; ok {
		untracked = value.(bool)
	}
	if err := a.API.Start(r.Context(), alias, untracked, userClient); err != nil {
		a.ReplyWithError(w, err)
		return
	}
	briefInfo, err := a.API.GetBriefInfo(r.Context(), alias)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}
	a.ReplyOK(w, briefInfo)
}

var StopCmdDescriptor = CmdDescriptor{
	Name:        "stop",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit()},
	Description: "stop strawberry operation",
	Handler:     HTTPAPI.HandleStop,
}

func (a HTTPAPI) HandleStop(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)
	if err := a.API.Stop(r.Context(), alias); err != nil {
		a.ReplyWithError(w, err)
		return
	}
	a.ReplyOK(w, nil)
}

var DescribeOptionsCmdDescriptor = CmdDescriptor{
	Name:        "describe_options",
	Parameters:  []CmdParameter{AliasParameter.AsExplicit()},
	Description: "get available speclet options",
	Handler:     HTTPAPI.HandleDescribeOptions,
}

func (a HTTPAPI) HandleDescribeOptions(w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)

	options, err := a.API.DescribeOptions(r.Context(), alias)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}

	a.ReplyOK(w, options)
}

var AllCommands = []CmdDescriptor{
	ListCmdDescriptor,
	CreateCmdDescriptor,
	RemoveCmdDescriptor,
	ExistsCmdDescriptor,
	StatusCmdDescriptor,
	GetBriefInfoCmdDescriptor,
	GetOptionCmdDescriptor,
	SetOptionCmdDescriptor,
	RemoveOptionCmdDescriptor,
	GetSpecletCmdDescriptor,
	SetSpecletCmdDescriptor,
	SetOptionsCmdDescriptor,
	EditOptionsCmdDescriptor,
	StartCmdDescriptor,
	StopCmdDescriptor,
	DescribeOptionsCmdDescriptor,
}

func ControllerRouter(cfg HTTPAPIConfig, family string, cf strawberry.ControllerFactory, l log.Logger) chi.Router {
	var clusters []string
	for _, clusterInfo := range cfg.ClusterInfos {
		if clusterInfo.Family == family {
			clusters = append(clusters, clusterInfo.Proxy)
			clusters = append(clusters, cfg.LocationAliases[clusterInfo.Proxy]...)
		}
	}

	var commands []CmdDescriptor
	commands = append(commands, AllCommands...)
	if cf.ExtraCommands != nil {
		extraCommands := cf.ExtraCommands.([]CmdDescriptor)
		commands = append(commands, extraCommands...)
	}

	r := chi.NewRouter()
	r.Get("/ping", HandlePing)
	r.Get("/describe", func(w http.ResponseWriter, r *http.Request) {
		HandleDescribe(w, r, commands, clusters)
	})

	for _, clusterInfo := range cfg.ClusterInfos {
		if clusterInfo.Family != family {
			continue
		}
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

		ctl := cf.Ctor(l, ytc, clusterInfo.StrawberryRoot, clusterInfo.Proxy, cf.Config)

		api := NewHTTPAPI(ytc, apiCfg, ctl, l, cfg.DisableAuth)

		locationRouter := r.Route("/"+clusterInfo.Proxy, func(r chi.Router) {
			// TODO(dakovalkov): Enable CORS when cookie authentication is supported.
			// r.Use(ythttputil.CORS())
			r.Use(auth.Auth(clusterInfo.Proxy, cfg.DisableAuth, l.Structured()))

			for _, cmdVar := range commands {
				// NB: variable is not copied into closure by default.
				cmd := cmdVar
				l.Info("Registering command", log.String("command", cmd.Name))
				r.Post("/"+cmd.Name, func(w http.ResponseWriter, r *http.Request) {
					api.L.Info("command started", log.String("cmd", cmd.Name))
					params := api.parseAndValidateRequestParams(w, r, cmd)
					if params == nil {
						return
					}
					cmd.Handler(api, w, r, params)
					api.L.Info("command finished", log.String("cmd", cmd.Name))
				})
			}
		})

		for _, alias := range cfg.LocationAliases[clusterInfo.Proxy] {
			r.Mount("/"+alias, locationRouter)
		}
	}
	return r
}

func NewServer(c HTTPAPIConfig, l log.Logger) *httpserver.HTTPServer {
	hr := hostrouter.New()
	routers := map[string]chi.Router{}
	for family, cf := range c.BaseAPIConfig.ControllerFactories {
		routers[family] = ControllerRouter(c, family, cf, log.With(l, log.String("family", family)))
	}
	for host, family := range c.BaseAPIConfig.ControllerMappingsOrDefault() {
		hr.Map(host, routers[family])
	}
	return httpserver.New(c.Endpoint, hr)
}

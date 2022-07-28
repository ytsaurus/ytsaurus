package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/yandex/blackbox"
	"a.yandex-team.ru/library/go/yandex/blackbox/httpbb"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
	"a.yandex-team.ru/yt/internal/go/ythttputil"
)

var AliasParameter = CmdParameter{
	Name:        "alias",
	Aliases:     []string{"a"},
	Type:        TypeString,
	Required:    true,
	Description: "alias for the operation",
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

	a.replyOK(w, struct{}{})
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

	a.replyOK(w, struct{}{})
}

var KeyParameter = CmdParameter{
	Name:        "key",
	Type:        TypeString,
	Required:    true,
	Description: "speclet option name",
}

var ValueParameter = CmdParameter{
	Name:        "value",
	Type:        TypeAny,
	Required:    true,
	Description: "speclet option value",
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

	a.replyOK(w, struct{}{})
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

	a.replyOK(w, struct{}{})
}

func RegisterHTTPAPI(r chi.Router, c HTTPAPIConfig, l log.Logger) {
	var bb blackbox.Client

	if !c.DisableAuth {
		var err error
		bb, err = httpbb.NewIntranet(
			httpbb.WithLogger(l.Structured()),
		)
		if err != nil {
			l.Fatal("failed to create blackbox client", log.Error(err))
		}
	}

	r.Get("/ping", HandlePing)
	r.Get("/describe", func(w http.ResponseWriter, r *http.Request) {
		HandleDescribe(w, r, c.Clusters)
	})

	for _, cluster := range c.Clusters {
		ytc, err := ythttp.NewClient(&yt.Config{
			Token:  c.Token,
			Proxy:  cluster,
			Logger: l.Structured(),
		})
		if err != nil {
			l.Fatal("failed to create yt client", log.Error(err), log.String("cluster", cluster))
		}

		api := NewHTTPAPI(ytc, c.APIConfig, l)

		r.Route("/"+cluster, func(r chi.Router) {
			r.Use(ythttputil.CORS())
			r.Use(ythttputil.Auth(bb, l.Structured()))
			r.Post("/"+CreateCmdDescriptor.Name, api.HandleCreate)
			r.Post("/"+RemoveCmdDescriptor.Name, api.HandleRemove)
			r.Post("/"+SetOptionCmdDescriptor.Name, api.HandleSetOption)
			r.Post("/"+RemoveOptionCmdDescriptor.Name, api.HandleRemoveOption)
		})
	}
}

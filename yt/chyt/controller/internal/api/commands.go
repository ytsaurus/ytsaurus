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

var CreateCmdDescriptor = CmdDescriptor{
	Args:           []string{"alias"},
	RequiredParams: []string{"alias"},
	ParamTypes:     defaultParamTypes,
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
	}

	a.replyOK(w, struct{}{})
}

var DeleteCmdDescriptor = CmdDescriptor{
	Args:           []string{"alias"},
	RequiredParams: []string{"alias"},
	ParamTypes:     defaultParamTypes,
}

func (a HTTPAPI) HandleDelete(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, DeleteCmdDescriptor)
	if params == nil {
		return
	}

	alias := params["alias"].(string)

	err := a.api.Delete(r.Context(), alias)
	if err != nil {
		a.replyWithError(w, err)
	}

	a.replyOK(w, struct{}{})
}

var SetCmdDescriptor = CmdDescriptor{
	Args:           []string{"key", "value", "alias"},
	RequiredParams: []string{"key", "value", "alias"},
	ParamTypes:     defaultParamTypes,
}

func (a HTTPAPI) HandleSet(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, SetCmdDescriptor)
	if params == nil {
		return
	}

	alias := params["alias"].(string)
	key := params["key"].(string)
	value := params["value"]

	err := a.api.SetOption(r.Context(), alias, key, value)
	if err != nil {
		a.replyWithError(w, err)
	}

	a.replyOK(w, struct{}{})
}

var RemoveCmdDescriptor = CmdDescriptor{
	Args:           []string{"key", "alias"},
	RequiredParams: []string{"key", "alias"},
	ParamTypes:     defaultParamTypes,
}

func (a HTTPAPI) HandleRemove(w http.ResponseWriter, r *http.Request) {
	params := a.parseAndValidateRequestParams(w, r, RemoveCmdDescriptor)
	if params == nil {
		return
	}

	alias := params["alias"].(string)
	key := params["key"].(string)

	err := a.api.RemoveOption(r.Context(), alias, key)
	if err != nil {
		a.replyWithError(w, err)
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

	r.Use(ythttputil.CORS())
	r.Use(ythttputil.Auth(bb, l.Structured()))

	for _, cluster := range c.Clusters {
		ytc, err := ythttp.NewClient(&yt.Config{
			Proxy:  cluster,
			Logger: l.Structured(),
		})
		if err != nil {
			l.Fatal("failed to create yt client", log.Error(err), log.String("cluster", cluster))
		}

		api := NewHTTPAPI(ytc, c.APIConfig, l)

		r.Route("/"+cluster, func(r chi.Router) {
			r.Post("/create", api.HandleCreate)
			r.Post("/delete", api.HandleDelete)
			r.Post("/set", api.HandleSet)
			r.Post("/remove", api.HandleRemove)
		})
	}
}

package dq

import (
	"net/http"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
)

var ListInstancesCmdDescriptor = api.CmdDescriptor{
	Name:        "list_instances",
	Parameters:  []api.CmdParameter{api.AliasParameter.AsExplicit()},
	Description: "list active instances from Discovery",
	Handler:     HandleListInstances,
}

func HandleListInstances(a api.HTTPAPI, w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)

	instances, err := ListInstances(a.API, r.Context(), alias)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}

	a.ReplyOK(w, instances)
}

var AllCommands = []api.CmdDescriptor{
	ListInstancesCmdDescriptor,
}

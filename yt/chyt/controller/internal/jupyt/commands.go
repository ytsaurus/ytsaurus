package jupyt

import (
	"net/http"

	"go.ytsaurus.tech/yt/chyt/controller/internal/api"
)

var GetEndpointCmdDescriptor = api.CmdDescriptor{
	Name:        "get_endpoint",
	Parameters:  []api.CmdParameter{api.AliasParameter.AsExplicit()},
	Description: "get endpoint of JupYT strawberry operation",
	Handler:     HandleGetEndpoint,
}

func HandleGetEndpoint(a api.HTTPAPI, w http.ResponseWriter, r *http.Request, params map[string]any) {
	alias := params["alias"].(string)

	result, err := GetEndpoint(a.API, r.Context(), alias)
	if err != nil {
		a.ReplyWithError(w, err)
		return
	}

	a.ReplyOK(w, result)
}

var AllCommands = []api.CmdDescriptor{
	GetEndpointCmdDescriptor,
}

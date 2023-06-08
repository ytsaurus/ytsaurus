package strawberry

import (
	"time"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func ToYsonURL(value interface{}) interface{} {
	return &yson.ValueWithAttrs{
		Attrs: map[string]interface{}{"_type_tag": "url"},
		Value: value,
	}
}

func navigationStringURL(clusterURL string, path ypath.Path) string {
	return clusterURL + "/navigation?path=" + path.String()
}

func navigationYsonURL(clusterURL string, path ypath.Path) any {
	return ToYsonURL(navigationStringURL(clusterURL, path))
}

func operationStringURL(clusterURL string, opID yt.OperationID) string {
	return clusterURL + "/operations/" + opID.String()
}

func operationYsonURL(clusterURL string, opID yt.OperationID) any {
	return ToYsonURL(operationStringURL(clusterURL, opID))
}

func (oplet *Oplet) OpAnnotations() map[string]any {
	return map[string]any{
		"strawberry_family":              oplet.c.Family(),
		"strawberry_stage":               oplet.agentInfo.Stage,
		"strawberry_operation_namespace": oplet.agentInfo.OperationNamespace,
		"strawberry_node":                oplet.cypressNode,
		"strawberry_controller": map[string]any{
			"address": oplet.agentInfo.Hostname,
			// TODO(max42): build Revision, etc.
		},
		"strawberry_incarnation":           oplet.NextIncarnationIndex(),
		"strawberry_previous_operation_id": oplet.persistentState.YTOpID,
	}
}

func (oplet *Oplet) OpDescription() map[string]any {
	desc := map[string]any{
		"strawberry_node":        navigationYsonURL(oplet.agentInfo.ClusterURL, oplet.cypressNode),
		"strawberry_incarnation": oplet.NextIncarnationIndex(),
	}
	if oplet.persistentState.YTOpID != yt.NullOperationID {
		desc["strawberry_previous_operation"] = operationYsonURL(oplet.agentInfo.ClusterURL, oplet.persistentState.YTOpID)
	}
	return desc
}

func (oplet *Oplet) CypAnnotation() string {
	data := struct {
		Alias             string
		PersistentState   PersistentState
		InfoState         InfoState
		StrawberrySpeclet Speclet
		Now               time.Time
		OperationURL      string
	}{
		oplet.alias,
		oplet.persistentState,
		oplet.infoState,
		oplet.strawberrySpeclet,
		time.Now(),
		operationStringURL(oplet.agentInfo.ClusterURL, oplet.persistentState.YTOpID),
	}

	templateString := `
## Strawberry operation {{.Alias}}
**Active**: {{.StrawberrySpeclet.ActiveOrDefault}}
**Pool**: {{.StrawberrySpeclet.Pool}}

**Current operation state**: {{.PersistentState.YTOpState}}
**Current operation id**: [{{.PersistentState.YTOpID}}]({{.OperationURL}})
**Current operation incarnation**: {{.PersistentState.IncarnationIndex}}
**Current operation speclet revision**: {{.PersistentState.YTOpSpecletRevision}}

**Cypress speclet revision**: {{.PersistentState.SpecletRevision}}
**Restart on speclet change**: {{.StrawberrySpeclet.RestartOnSpecletChangeOrDefault}}

**Last updated time**: {{.Now}}

{{if .PersistentState.BackoffUntil}}**Backoff until**: {{.PersistentState.BackoffUntil}}
{{end}}{{if .InfoState.Error}}**Error**: {{.InfoState.Error}}
{{end}}`

	return ExecuteTemplate(templateString, &data)
}

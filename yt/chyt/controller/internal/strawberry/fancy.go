package strawberry

import (
	"bytes"
	"text/template"
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

func navigationURL(cluster string, path ypath.Path) interface{} {
	return ToYsonURL("https://yt.yandex-team.ru/" + cluster + "/navigation?path=" + path.String())
}

func operationURL(cluster string, opID yt.OperationID) interface{} {
	return ToYsonURL("https://yt.yandex-team.ru/" + cluster + "/operations/" + opID.String())
}

func (oplet *Oplet) OpAnnotations() map[string]interface{} {
	return map[string]interface{}{
		"strawberry_family":              oplet.c.Family(),
		"strawberry_stage":               oplet.agentInfo.Stage,
		"strawberry_operation_namespace": oplet.agentInfo.OperationNamespace,
		"strawberry_node":                oplet.cypressNode,
		"strawberry_controller": map[string]interface{}{
			"address": oplet.agentInfo.Hostname,
			// TODO(max42): build Revision, etc.
		},
		"strawberry_incarnation":           oplet.NextIncarnationIndex(),
		"strawberry_previous_operation_id": oplet.persistentState.YTOpID,
	}
}

func (oplet *Oplet) OpDescription() map[string]interface{} {
	desc := map[string]interface{}{
		"strawberry_node":        navigationURL(oplet.agentInfo.Proxy, oplet.cypressNode),
		"strawberry_incarnation": oplet.NextIncarnationIndex(),
	}
	if oplet.persistentState.YTOpID != yt.NullOperationID {
		desc["strawberry_previous_operation"] = operationURL(oplet.agentInfo.Proxy, oplet.persistentState.YTOpID)
	}
	return desc
}

func (oplet *Oplet) CypAnnotation() string {
	data := struct {
		Alias             string
		AgentInfo         AgentInfo
		PersistentState   PersistentState
		InfoState         InfoState
		StrawberrySpeclet Speclet
		Now               time.Time
	}{
		oplet.alias,
		oplet.agentInfo,
		oplet.persistentState,
		oplet.infoState,
		oplet.strawberrySpeclet,
		time.Now(),
	}

	t := template.Must(template.New("cypAnnotation").Parse(`
## Strawberry operation {{.Alias}}
**Active**: {{.StrawberrySpeclet.ActiveOrDefault}}
**Pool**: {{.StrawberrySpeclet.Pool}}

**Current operation state**: {{.PersistentState.YTOpState}}
**Current operation id**: [{{.PersistentState.YTOpID}}](https://yt.yandex-team.ru/{{.AgentInfo.Proxy}}/operations/{{.PersistentState.YTOpID}})
**Current operation incarnation**: {{.PersistentState.IncarnationIndex}}
**Current operation speclet revision**: {{.PersistentState.YTOpSpecletRevision}}

**Cypress speclet revision**: {{.PersistentState.SpecletRevision}}
**Restart on speclet change**: {{.StrawberrySpeclet.RestartOnSpecletChangeOrDefault}}

**Last updated time**: {{.Now}}

{{if .PersistentState.BackoffUntil}}**Backoff until**: {{.PersistentState.BackoffUntil}}
{{end}}{{if .InfoState.Error}}**Error**: {{.InfoState.Error}}
{{end}}`))

	b := new(bytes.Buffer)
	if err := t.Execute(b, &data); err != nil {
		panic(err)
	}

	return b.String()
}

package strawberry

import (
	"bytes"
	"text/template"
	"time"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
)

func ToYsonURL(value interface{}) interface{} {
	return yson.ValueWithAttrs{
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

func opAnnotations(a *Agent, oplet *Oplet) map[string]interface{} {
	return map[string]interface{}{
		"strawberry_family":              oplet.c.Family(),
		"strawberry_stage":               a.config.Stage,
		"strawberry_operation_namespace": a.OperationNamespace(),
		"strawberry_node":                a.opletNode(oplet),
		"strawberry_controller": map[string]interface{}{
			"address": a.hostname,
			// TODO(max42): build Revision, etc.
		},
		"strawberry_incarnation":           oplet.NextIncarnationIndex(),
		"strawberry_previous_operation_id": oplet.persistentState.YTOpID,
	}
}

func opDescription(a *Agent, oplet *Oplet) map[string]interface{} {
	desc := map[string]interface{}{
		"strawberry_node":        navigationURL(a.proxy, a.opletNode(oplet)),
		"strawberry_incarnation": oplet.NextIncarnationIndex(),
	}
	if oplet.persistentState.YTOpID != yt.OperationID(guid.FromParts(0, 0, 0, 0)) {
		desc["strawberry_previous_operation"] = operationURL(a.proxy, oplet.persistentState.YTOpID)
	}
	return desc
}

func cypAnnotation(a *Agent, oplet *Oplet) string {
	data := struct {
		Proxy             string
		Alias             string
		PersistentState   PersistentState
		InfoState         InfoState
		StrawberrySpeclet Speclet
		Now               time.Time
	}{
		a.proxy,
		oplet.alias,
		oplet.persistentState,
		oplet.infoState,
		oplet.strawberrySpeclet,
		time.Now(),
	}

	t := template.Must(template.New("cypAnnotation").Parse(`
## Strawberry operation {{.Alias}}
**Active**: {{.StrawberrySpeclet.ActiveOrDefault}}
**Pool**: {{.StrawberrySpeclet.Pool}}

**Current operation state**: {{.InfoState.YTOpState}}
**Current operation id**: [{{.PersistentState.YTOpID}}](https://yt.yandex-team.ru/{{.Proxy}}/operations/{{.PersistentState.YTOpID}})
**Current incarnation**: {{.PersistentState.IncarnationIndex}}
**Curent operation speclet revision**: {{.PersistentState.OperationSpecletRevision}}

**Cypress speclet revision**: {{.PersistentState.SpecletRevision}}
**Speclet change requires restart**: {{.PersistentState.SpecletChangeRequiresRestart}}
**Restart on speclet change**: {{.StrawberrySpeclet.RestartOnSpecletChangeOrDefault}}

**Last updated time**: {{.Now}}

{{if .InfoState.Error}}**Error**: {{.InfoState.Error}}{{end}}
`))

	b := new(bytes.Buffer)
	if err := t.Execute(b, &data); err != nil {
		panic(err)
	}

	return b.String()
}

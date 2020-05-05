package ytrecipe

import (
	"bytes"
	"io/ioutil"
	"text/template"

	"a.yandex-team.ru/library/go/test/yatest"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

type readmeArgs struct {
	Cluster       string
	OpID          yt.OperationID
	OpURL         string
	CypressOutDir ypath.Path
	CypressURL    string
	Binaries      map[MD5]*File
}

var readmeTmpl = template.Must(template.New("").Parse(`
# ytrecipe debugging
{{$cluster := .Cluster}}
This test was running in operation {{.OpID}} on cluster {{$cluster}}.
Operation link: {{.OpURL}}

Full test output is available in {{.CypressOutDir}}.
Cypress link: {{.CypressURL}}

Use following command to download all logs and core dumps:

ytrecipe download --cluster {{$cluster}} {{.CypressOutDir}}

Test was using following binaries:

{{range .Binaries}}
{{.LocalPath}}
yt --proxy {{$cluster}} download {{.CypressPath}}
{{end}}
`[1:]))

func (r *Runner) writeReadme(opID yt.OperationID, job *Job, outDir ypath.Path) error {
	args := readmeArgs{
		Cluster:       r.Config.Cluster,
		Binaries:      job.FS.Files,
		OpID:          opID,
		CypressOutDir: outDir,
		CypressURL:    yt.WebUITableURL(r.Config.Cluster, outDir),
		OpURL:         yt.WebUIOperationURL(r.Config.Cluster, opID),
	}

	var buf bytes.Buffer
	if err := readmeTmpl.Execute(&buf, args); err != nil {
		return err
	}

	return ioutil.WriteFile(yatest.OutputPath("README.md"), buf.Bytes(), 0666)
}

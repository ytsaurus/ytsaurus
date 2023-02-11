package ytexec

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"text/template"
	"time"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/job"
	"a.yandex-team.ru/yt/go/ytrecipe/internal/jobfs"
)

type readmeArgs struct {
	Cluster       string
	OpID          yt.OperationID
	OpURL         string
	CypressOutDir ypath.Path
	CypressURL    string
	Files         map[jobfs.MD5]*jobfs.File
	WorkDir       string
}

var funcMap = template.FuncMap{
	"filepathBase": filepath.Base,
}

var readmeTmpl = template.Must(template.New("").Funcs(funcMap).Parse(`
# ytrecipe debugging
{{$cluster := .Cluster}}
This test was running in operation {{.OpID}} on cluster {{$cluster}}.
Operation link: {{.OpURL}}

Full test output is available in {{.CypressOutDir}}.
Cypress link: {{.CypressURL}}

Use following command to download all logs and core dumps:

ytrecipe-tool download --proxy {{$cluster}} --path {{.CypressOutDir}} --update-me-to-v3

Download porto layer to open core dump:
mkdir -p /tmp/testlayer
yt --proxy hahn download //porto_layers/base/xenial/porto_layer_search_ubuntu_xenial_app_lastest.tar.gz > porto_layer.tgz
tar xf porto_layer.tgz
Run in GDB: set sysroot /tmp/testlayer

Install ytrecipe-tool from source:

ya make -r yt/go/ytrecipe/cmd/ytrecipe-tool/ --install $GOPATH/bin

Test was using following files:

{{range .Files}} {{$cypressPath := .CypressPath}} {{range .LocalPath}}
{{.Path}}
yt --proxy {{$cluster}} download {{$cypressPath}} > {{filepathBase .Path}}
{{end}} {{end}}
`[1:]))

var downloadTmpl = template.Must(template.New("").Parse(`
#!/bin/sh
{{$cluster := .Cluster}}
ytrecipe-tool download --proxy {{$cluster}} --path {{.CypressOutDir}} --output $(dirname "$0") --skip-ya-output --update-me-to-v3
`[1:]))

func (e *Exec) writePrepare(job *job.Job, outDir ypath.Path) error {
	operationURL := yt.WebUIOperationURL(e.config.Operation.Cluster, e.op.ID())
	_, _ = fmt.Fprintf(os.Stderr, "Operation started %s\n", operationURL)

	args := readmeArgs{
		Cluster:       e.config.Operation.Cluster,
		Files:         job.FS.Files,
		OpID:          e.op.ID(),
		CypressOutDir: outDir,
		CypressURL:    yt.WebUITableURL(e.config.Operation.Cluster, outDir),
		OpURL:         operationURL,
		// WorkDir:       job.Env.WorkPath,
	}

	var buf bytes.Buffer
	if err := readmeTmpl.Execute(&buf, args); err != nil {
		return err
	}

	err := os.WriteFile(e.config.Exec.ReadmeFile, buf.Bytes(), 0666)
	if err != nil {
		return err
	}

	buf.Reset()
	if err := downloadTmpl.Execute(&buf, args); err != nil {
		return err
	}

	err = os.WriteFile(e.config.Exec.DownloadScript, buf.Bytes(), 0777)
	if err != nil {
		return err
	}

	prepared := PreparedFile{
		OperationID:  args.OpID.String(),
		OperationURL: args.OpURL,
	}

	preparedJS, err := json.MarshalIndent(prepared, "", "    ")
	if err != nil {
		return err
	}

	return os.WriteFile(e.config.Exec.PreparedFile, preparedJS, 0666)
}

func (e *Exec) writeResult(exitRow *jobfs.ExitRow) error {
	result := ResultFile{
		ExitCode:   exitRow.ExitCode,
		ExitSignal: int(exitRow.KilledBySignal),
		IsOOM:      exitRow.IsOOM,

		Statistics: Statistics{
			UploadTime:     e.scheduledAt.Sub(e.execedAt),
			SchedulingTime: exitRow.StartedAt.Sub(e.scheduledAt),
			UnpackingTime:  exitRow.UnpackedAt.Sub(exitRow.StartedAt),
			ExecutionTime:  exitRow.FinishedAt.Sub(exitRow.UnpackedAt),
			DownloadTime:   time.Since(exitRow.FinishedAt),
		},
	}

	resultJS, err := json.MarshalIndent(result, "", "    ")
	if err != nil {
		return err
	}

	return os.WriteFile(e.config.Exec.ResultFile, resultJS, 0666)
}

// package spec defines specification of YT operation.
//
// See https://wiki.yandex-team.ru/yt/userdoc/operations/
package spec

import (
	"github.com/mitchellh/copystructure"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

type Spec struct {
	Type yt.OperationType `yson:"-"`

	Title       string                 `yson:"title,omitempty"`
	StartedBy   map[string]interface{} `yson:"started_by,omitempty"`
	Description map[string]interface{} `yson:"description,omitempty"`

	Pool string `yson:"pool,omitempty"`

	InputTablePaths  []ypath.YPath `yson:"input_table_paths,omitempty"`
	OutputTablePaths []ypath.YPath `yson:"output_table_paths,omitempty"`

	Mapper *UserScript            `yson:"mapper,omitempty"`
	Tasks  map[string]*UserScript `yson:"tasks,omitempty"`
}

type File struct {
	FileName   string      `yson:"file_name,attr,omitempty"`
	Format     interface{} `yson:"format,attr,omitempty"`
	Executable bool        `yson:"executable,attr,omitempty"`

	CypressPath ypath.Path `yson:",value"`
}

type UserScript struct {
	Command     string            `yson:"command"`
	Format      interface{}       `yson:"format,omitempty"`
	Environment map[string]string `yson:"environment,omitempty"`
	FilePaths   []File            `yson:"file_paths,omitempty"`
}

func (s *Spec) Clone() *Spec {
	return copystructure.Must(copystructure.Copy(s)).(*Spec)
}

func (s *Spec) VisitUserScripts(cb func(*UserScript)) {
	if s.Mapper != nil {
		cb(s.Mapper)
	}

	for _, t := range s.Tasks {
		cb(t)
	}
}

func (s *Spec) PatchUserBinary(path ypath.Path) {
	s.VisitUserScripts(func(u *UserScript) {
		u.FilePaths = append(u.FilePaths, File{
			FileName:    "go-binary",
			CypressPath: path,
			Executable:  true,
		})
	})
}

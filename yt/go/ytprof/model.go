package ytprof

import (
	"context"

	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"

	"github.com/google/cel-go/checker/decls"

	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

const (
	TableMetadata = "metadata"
	TableData     = "data"
)

var (
	SchemaMetadata = schema.MustInfer(&ProfileMetadata{})
	SchemaData     = schema.MustInfer(&ProfileData{})

	Schema = map[string]schema.Schema{
		TableMetadata: SchemaMetadata,
		TableData:     SchemaData,
	}
)

type ProfID struct {
	ProfIDHigh uint64 `yson:",key"`
	ProfIDLow  uint64 `yson:",key"`
}

type Metadata struct {
	ProfileType string `yson:",omitempty"`
}

func (m *Metadata) Vars() map[string]interface{} {
	return map[string]interface{}{
		"ProfileType": m.ProfileType,
	}
}

func (m *Metadata) Types() map[string]*exprpb.Type {
	return map[string]*exprpb.Type{
		"ProfileType": decls.String,
	}
}

type ProfileMetadata struct {
	Timestamp  schema.Timestamp `yson:",key"`
	ProfIDHigh uint64           `yson:",key"`
	ProfIDLow  uint64           `yson:",key"`

	Metadata Metadata `yson:",omitempty"`
}

type ProfileData struct {
	ProfIDHigh uint64 `yson:",key"`
	ProfIDLow  uint64 `yson:",key"`

	Data []byte `yson:",omitempty"`
}

func (s *ProfileMetadata) ProfID() ProfID {
	return ProfID{
		ProfIDHigh: s.ProfIDHigh,
		ProfIDLow:  s.ProfIDLow,
	}
}

func (s *ProfileData) ProfID() ProfID {
	return ProfID{
		ProfIDHigh: s.ProfIDHigh,
		ProfIDLow:  s.ProfIDLow,
	}
}

func MigrateTables(yc yt.Client, root ypath.Path) error {
	tables := map[ypath.Path]migrate.Table{}

	for name, tableSchema := range Schema {
		tables[root.Child(name)] = migrate.Table{
			Schema: tableSchema,
		}
	}

	alter := migrate.OnConflictTryAlter(context.Background(), yc)
	return migrate.EnsureTables(context.Background(), yc, tables, alter)
}

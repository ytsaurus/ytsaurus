package ytprof

import (
	"context"

	"a.yandex-team.ru/yt/go/migrate"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
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

type ProfileMetadata struct {
	Timestamp  int64  `yson:",key"`
	ProfIDHigh uint64 `yson:",key"`
	ProfIDLow  uint64 `yson:",key"`

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

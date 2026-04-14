package consts

import (
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/schema"
)

var ParsedTableSchema = schema.Schema{
	Strict: ptr.T(false),
	Columns: []schema.Column{
		{Name: "cluster", Type: schema.TypeString, SortOrder: schema.SortAscending},
		{Name: "path", Type: schema.TypeString, SortOrder: schema.SortAscending},
		{Name: "instant", Type: schema.TypeString, SortOrder: schema.SortAscending},
		{Name: "user", Type: schema.TypeString, Required: true},
		{Name: "method", Type: schema.TypeString, Required: true},
		{Name: "type", Type: schema.TypeString, Required: false},
		{Name: "id", Type: schema.TypeString, Required: false},
		{Name: "mutation_id", Type: schema.TypeString, Required: false},
		{Name: "revision_type", Type: schema.TypeString, Required: false},
		{Name: "revision", Type: schema.TypeString, Required: false},
		{Name: "level", Type: schema.TypeString, Required: false},
		{Name: "category", Type: schema.TypeString, Required: false},
		{Name: "original_path", Type: schema.TypeString, Required: false},
		{Name: "destination_path", Type: schema.TypeString, Required: false},
		{Name: "original_destination_path", Type: schema.TypeString, Required: false},
		{Name: "transaction_info", Type: schema.TypeAny, Required: false},
		{Name: "iso_eventtime", Type: schema.TypeString, Required: false},
	},
}

var InstantsTableSchema = schema.Schema{
	Strict: ptr.T(false),
	Columns: []schema.Column{
		{Name: "max_instant", Type: schema.TypeString},
		{Name: "min_instant", Type: schema.TypeString},
	},
}

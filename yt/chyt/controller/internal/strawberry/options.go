package strawberry

type OptionType string

const (
	TypeBool      OptionType = "bool"
	TypeString    OptionType = "string"
	TypeInt64     OptionType = "int64"
	TypeYson      OptionType = "yson"
	TypePath      OptionType = "path"
	TypePool      OptionType = "pool"
	TypeByteCount OptionType = "byte_count"
)

type OptionDescriptor struct {
	Title        string     `yson:"title" json:"title"`
	Name         string     `yson:"name" json:"name"`
	Type         OptionType `yson:"type" json:"type"`
	CurrentValue any        `yson:"current_value,omitempty" json:"current_value,omitempty"`
	DefaultValue any        `yson:"default_value,omitempty" json:"default_value,omitempty"`
	MinValue     any        `yson:"min_value,omitempty" json:"min_value,omitempty"`
	MaxValue     any        `yson:"max_value,omitempty" json:"max_value,omitempty"`
	Choices      []any      `yson:"choices,omitempty" json:"choices,omitempty"`
	Description  string     `yson:"description,omitempty" json:"description,omitempty"`
}

type OptionGroupDescriptor struct {
	Title   string             `yson:"title" json:"title"`
	Options []OptionDescriptor `yson:"options" json:"options"`

	// Hidden indicates that the option group consists of non-important or rarely used options
	// and these options should be hidden in UI if possible (e.g. under a cut element).
	Hidden bool `yson:"hidden" json:"hidden"`
}

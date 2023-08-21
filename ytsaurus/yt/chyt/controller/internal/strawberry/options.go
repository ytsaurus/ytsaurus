package strawberry

type OptionType string

const (
	TypeBool   OptionType = "bool"
	TypeString OptionType = "string"
	TypeUInt64 OptionType = "uint64"
	TypeYson   OptionType = "yson"
)

type OptionDescriptor struct {
	Name         string     `yson:"name"`
	Type         OptionType `yson:"type"`
	CurrentValue any        `yson:"current_value,omitempty"`
	DefaultValue any        `yson:"default_value,omitempty"`
	Choises      []any      `yson:"choises,omitempty"`
	Description  string     `yson:"description,omitempty"`
}

type OptionGroupDescriptor struct {
	Title   string             `yson:"title"`
	Options []OptionDescriptor `yson:"options"`

	// Hidden indicates that the option group consists of non-important or rarely used options
	// and these options should be hidden in UI if possible (e.g. under a cut element).
	Hidden bool `yson:"hidden"`
}

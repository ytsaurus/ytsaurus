package yt

type ACE struct {
	Action          SecurityAction `yson:"action,omitempty"`
	Subjects        []string       `yson:"subjects,omitempty"`
	Permissions     []Permission   `yson:"permissions,omitempty"`
	InheritanceMode string         `yson:"inheritance_mode,omitempty"`
	Columns         []string       `yson:"columns,omitempty"`
}

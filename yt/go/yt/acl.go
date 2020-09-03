package yt

type ACE struct {
	Action          string   `yson:"action,omitempty"`
	Subjects        []string `yson:"subjects,omitempty"`
	Permissions     []string `yson:"permissions,omitempty"`
	InheritanceMode string   `yson:"inheritance_mode,omitempty"`
	Columns         []string `yson:"columns,omitempty"`
}

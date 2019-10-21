package yt

type ACE struct {
	Action          string   `yson:"action,omitempty"`
	Subjects        []string `yson:"subjects"`
	Permissions     []string `yson:"permissions"`
	InheritanceMode string   `yson:"inheritance_mode,omitempty"`
	Columns         []string `yson:"columns,omitempty"`
}

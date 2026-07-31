package flow

import "slices"

// OutputGroup contains output attributed to a set of inputs.
type OutputGroup struct {
	ParentIDs  []string
	Messages   []Message
	Distribute []bool
	Timers     []TimerRequest
}

// DistributeAt reports whether the i-th message is published downstream.
func (r OutputGroup) DistributeAt(i int) bool {
	if len(r.Distribute) == 0 {
		return true
	}
	return r.Distribute[i]
}

// IsEmpty reports whether the group has no output.
func (r OutputGroup) IsEmpty() bool {
	return len(r.Messages) == 0 && len(r.Timers) == 0
}

// OutputCollector collects output for one group.
type OutputCollector interface {
	// AddMessage collects a message and publishes it downstream.
	AddMessage(msg Message)

	// AddUndistributedMessage collects a source message without publishing it downstream.
	AddUndistributedMessage(msg Message)

	// AddTimer asks the worker to set a timer on the key being handled.
	AddTimer(timer TimerRequest)

	// WithParentIDs returns a collector for a separate lineage group.
	WithParentIDs(parentIDs ...string) OutputCollector
}

type rootCollector struct {
	groups []OutputGroup
}

func newRootCollector() *rootCollector {
	return &rootCollector{}
}

func (c *rootCollector) WithParentIDs(parentIDs ...string) OutputCollector {
	c.groups = append(c.groups, OutputGroup{ParentIDs: slices.Clone(parentIDs)})
	return &groupCollector{root: c, group: len(c.groups) - 1}
}

func (c *rootCollector) CollectResults() []OutputGroup {
	collected := make([]OutputGroup, 0, len(c.groups))
	for _, group := range c.groups {
		if !group.IsEmpty() {
			collected = append(collected, group)
		}
	}
	return collected
}

type groupCollector struct {
	root  *rootCollector
	group int
}

func (c *groupCollector) AddMessage(msg Message) {
	c.addMessage(msg, true)
}

func (c *groupCollector) AddUndistributedMessage(msg Message) {
	c.addMessage(msg, false)
}

func (c *groupCollector) addMessage(msg Message, distribute bool) {
	group := &c.root.groups[c.group]
	group.Messages = append(group.Messages, msg)

	// An empty flag list means distribute all.
	if len(group.Distribute) == 0 {
		if distribute {
			return
		}
		group.Distribute = make([]bool, len(group.Messages)-1, len(group.Messages))
		for i := range group.Distribute {
			group.Distribute[i] = true
		}
	}
	group.Distribute = append(group.Distribute, distribute)
}

func (c *groupCollector) AddTimer(timer TimerRequest) {
	group := &c.root.groups[c.group]
	group.Timers = append(group.Timers, timer)
}

func (c *groupCollector) WithParentIDs(parentIDs ...string) OutputCollector {
	return c.root.WithParentIDs(parentIDs...)
}

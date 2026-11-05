package flow

import (
	"errors"
	"slices"
)

// MessageIDSuffixMode selects how a Swift computation identifies sibling output messages.
type MessageIDSuffixMode uint8

const (
	MessageIDSuffixSequenceNumber MessageIDSuffixMode = iota
	MessageIDSuffixPayloadHash
	MessageIDSuffixUserDefined
)

// MessageIDSuffix selects how a Swift computation identifies sibling output messages.
type MessageIDSuffix struct {
	mode  MessageIDSuffixMode
	value string
}

// SequenceNumberMessageIDSuffix returns the default sequence-number selector.
func SequenceNumberMessageIDSuffix() MessageIDSuffix {
	return MessageIDSuffix{}
}

// PayloadHashMessageIDSuffix returns a payload-hash selector.
func PayloadHashMessageIDSuffix() MessageIDSuffix {
	return MessageIDSuffix{mode: MessageIDSuffixPayloadHash}
}

// UserDefinedMessageIDSuffix returns a user-defined selector.
func UserDefinedMessageIDSuffix(value string) (MessageIDSuffix, error) {
	if value == "" {
		return MessageIDSuffix{}, errors.New("user-defined output message ID suffix must not be empty")
	}
	return MessageIDSuffix{mode: MessageIDSuffixUserDefined, value: value}, nil
}

// MessageDistribution controls whether an output message is published downstream.
type MessageDistribution uint8

const (
	// DistributeDefault publishes the message downstream.
	DistributeDefault MessageDistribution = iota
	// DistributeMessage publishes the message downstream.
	DistributeMessage
	// DoNotDistributeMessage keeps a source message only for watermark generation.
	DoNotDistributeMessage
)

// AddMessageOptions controls output distribution and message ID generation.
type AddMessageOptions struct {
	Distribute      MessageDistribution
	MessageIDSuffix MessageIDSuffix
}

// OutputGroup contains output attributed to a set of inputs.
type OutputGroup struct {
	ParentIDs         []string
	Messages          []Message
	Distribute        []bool
	MessageIDSuffixes []MessageIDSuffix
	Timers            []TimerRequest
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
	AddMessage(msg Message, options ...AddMessageOptions)

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

func (c *groupCollector) AddMessage(msg Message, options ...AddMessageOptions) {
	if len(options) > 1 {
		panic("flow: AddMessage accepts at most one options argument")
	}
	var opts AddMessageOptions
	if len(options) == 1 {
		opts = options[0]
	}
	c.addMessage(msg, opts)
}

func (c *groupCollector) AddUndistributedMessage(msg Message) {
	c.addMessage(msg, AddMessageOptions{Distribute: DoNotDistributeMessage})
}

func (c *groupCollector) addMessage(msg Message, options AddMessageOptions) {
	group := &c.root.groups[c.group]
	group.Messages = append(group.Messages, msg)
	distribute := options.Distribute != DoNotDistributeMessage

	// An empty flag list means distribute all.
	if len(group.Distribute) == 0 {
		if !distribute {
			group.Distribute = make([]bool, len(group.Messages)-1, len(group.Messages))
			for i := range group.Distribute {
				group.Distribute[i] = true
			}
		}
	}
	if group.Distribute != nil {
		group.Distribute = append(group.Distribute, distribute)
	}

	// An empty selector list means sequence numbers for all messages.
	if len(group.MessageIDSuffixes) == 0 {
		if options.MessageIDSuffix.mode != MessageIDSuffixSequenceNumber {
			group.MessageIDSuffixes = make([]MessageIDSuffix, len(group.Messages)-1, len(group.Messages))
		}
	}
	if group.MessageIDSuffixes != nil {
		group.MessageIDSuffixes = append(group.MessageIDSuffixes, options.MessageIDSuffix)
	}
}

func (c *groupCollector) AddTimer(timer TimerRequest) {
	group := &c.root.groups[c.group]
	group.Timers = append(group.Timers, timer)
}

func (c *groupCollector) WithParentIDs(parentIDs ...string) OutputCollector {
	return c.root.WithParentIDs(parentIDs...)
}

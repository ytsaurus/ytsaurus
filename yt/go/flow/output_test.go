package flow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func testMessage(id string) Message {
	return Message{Meta: Meta{ID: id, StreamID: "out"}}
}

func messageIDs(messages []Message) []string {
	return inputIDs(messages, func(m Message) string { return m.ID })
}

func TestRootCollectorKeepsGroupsInOrderOpened(t *testing.T) {
	root := newRootCollector()

	first := root.WithParentIDs("m1")
	second := root.WithParentIDs("m2", "m3")
	second.AddMessage(testMessage("b"))
	first.AddMessage(testMessage("a"))

	groups := root.CollectResults()
	require.Len(t, groups, 2)
	require.Equal(t, []string{"m1"}, groups[0].ParentIDs)
	require.Equal(t, []string{"a"}, messageIDs(groups[0].Messages))
	require.Equal(t, []string{"m2", "m3"}, groups[1].ParentIDs)
	require.Equal(t, []string{"b"}, messageIDs(groups[1].Messages))
}

func TestRootCollectorOwnsParentIDs(t *testing.T) {
	root := newRootCollector()
	parentIDs := []string{"m1"}
	root.WithParentIDs(parentIDs...).AddMessage(testMessage("a"))
	parentIDs[0] = "changed"

	require.Equal(t, []string{"m1"}, root.CollectResults()[0].ParentIDs)
}

func TestRootCollectorDropsGroupsWithoutOutput(t *testing.T) {
	root := newRootCollector()

	root.WithParentIDs("silent")
	root.WithParentIDs("emits-message").AddMessage(testMessage("a"))
	root.WithParentIDs("also-silent")
	root.WithParentIDs("emits-timer").AddTimer(TimerRequest{TriggerTimestamp: 100})

	groups := root.CollectResults()
	require.Len(t, groups, 2)
	require.Equal(t, []string{"emits-message"}, groups[0].ParentIDs)
	require.Equal(t, []string{"emits-timer"}, groups[1].ParentIDs)
	require.Len(t, groups[1].Timers, 1)
}

func TestRootCollectorCollectsNothingWhenIdle(t *testing.T) {
	require.Empty(t, newRootCollector().CollectResults())
}

func TestOutputCollectorLeavesDistributeUnsetWhenAllDistributed(t *testing.T) {
	root := newRootCollector()

	out := root.WithParentIDs("m1")
	out.AddMessage(testMessage("a"))
	out.AddMessage(testMessage("b"))

	group := root.CollectResults()[0]
	require.Empty(t, group.Distribute)
	require.True(t, group.DistributeAt(0))
	require.True(t, group.DistributeAt(1))
}

func TestOutputCollectorAlignsDistributeWithMessages(t *testing.T) {
	root := newRootCollector()

	out := root.WithParentIDs("m1")
	out.AddMessage(testMessage("a"))
	out.AddMessage(testMessage("b"))
	out.AddUndistributedMessage(testMessage("c"))
	out.AddMessage(testMessage("d"))

	group := root.CollectResults()[0]
	require.Equal(t, []string{"a", "b", "c", "d"}, messageIDs(group.Messages))
	require.Equal(t, []bool{true, true, false, true}, group.Distribute)
	require.False(t, group.DistributeAt(2))
	require.True(t, group.DistributeAt(3))
}

func TestOutputCollectorAlignsDistributeWhenFirstMessageOptsOut(t *testing.T) {
	root := newRootCollector()

	out := root.WithParentIDs("m1")
	out.AddUndistributedMessage(testMessage("a"))
	out.AddMessage(testMessage("b"))

	group := root.CollectResults()[0]
	require.Equal(t, []bool{false, true}, group.Distribute)
}

func TestOutputCollectorCollectsTimers(t *testing.T) {
	root := newRootCollector()

	out := root.WithParentIDs("m1")
	out.AddTimer(TimerRequest{TriggerTimestamp: 100})
	out.AddTimer(TimerRequest{TriggerTimestamp: 200, EventTimestamp: 50, StreamID: "late"})

	group := root.CollectResults()[0]
	require.Equal(t, []TimerRequest{
		{TriggerTimestamp: 100},
		{TriggerTimestamp: 200, EventTimestamp: 50, StreamID: "late"},
	}, group.Timers)
}

func TestOutputCollectorWithParentIDsOpensSiblingGroup(t *testing.T) {
	root := newRootCollector()

	out := root.WithParentIDs("m1")
	out.AddMessage(testMessage("a"))
	redirected := out.WithParentIDs("m2")
	redirected.AddMessage(testMessage("b"))
	out.AddMessage(testMessage("c"))

	groups := root.CollectResults()
	require.Len(t, groups, 2)
	require.Equal(t, []string{"m1"}, groups[0].ParentIDs)
	require.Equal(t, []string{"a", "c"}, messageIDs(groups[0].Messages))
	require.Equal(t, []string{"m2"}, groups[1].ParentIDs)
	require.Equal(t, []string{"b"}, messageIDs(groups[1].Messages))
}

func TestTransformResultIsEmpty(t *testing.T) {
	require.True(t, OutputGroup{ParentIDs: []string{"m1"}}.IsEmpty())
	require.False(t, OutputGroup{Messages: []Message{testMessage("a")}}.IsEmpty())
	require.False(t, OutputGroup{Timers: []TimerRequest{{TriggerTimestamp: 1}}}.IsEmpty())
}

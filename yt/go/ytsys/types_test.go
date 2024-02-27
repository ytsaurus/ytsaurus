package ytsys

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestYTBool_UnmarshalYSON(t *testing.T) {
	type Result struct {
		b   YTBool
		err bool
	}

	for _, tc := range []struct {
		name     string
		data     []byte
		expected *Result
	}{
		{
			name:     "%true",
			data:     []byte(`%true`),
			expected: &Result{b: true},
		},
		{
			name:     "%false",
			data:     []byte(`%false`),
			expected: &Result{b: false},
		},
		{
			name:     "true",
			data:     []byte(`true`),
			expected: &Result{b: true},
		},
		{
			name:     "false",
			data:     []byte(`false`),
			expected: &Result{b: false},
		},
		{
			name:     "unexpected literal",
			data:     []byte(`True`),
			expected: &Result{err: true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var b YTBool
			err := yson.Unmarshal(tc.data, &b)
			if tc.expected.err {
				require.Error(t, err)
			} else {
				require.Equal(t, tc.expected.b, b)
			}
		})
	}
}

func TestYTFloat64_UnmarshalYSON(t *testing.T) {
	type Result struct {
		f64 YTFloat64
		err bool
	}

	for _, tc := range []struct {
		name     string
		data     []byte
		expected *Result
	}{
		{
			name:     "42",
			data:     []byte(`42`),
			expected: &Result{f64: 42},
		},
		{
			name:     "42.2",
			data:     []byte(`42.2`),
			expected: &Result{f64: 42.2},
		},
		{
			name:     "#",
			data:     []byte(`#`),
			expected: &Result{f64: 0},
		},
		{
			name:     "not-float",
			data:     []byte(`abcdef`),
			expected: &Result{err: true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var f64 YTFloat64
			err := yson.Unmarshal(tc.data, &f64)
			if tc.expected.err {
				require.Error(t, err)
			} else {
				require.Equal(t, tc.expected.f64, f64)
			}
		})
	}
}

func TestNodeMap_GetFreeTabletCommonNodeCount(t *testing.T) {
	for _, tc := range []struct {
		name     string
		nodes    NodeMap
		expected int
	}{
		{
			name: "no-tag",
			nodes: NodeMap{
				Addr{FQDN: "n", Port: "9012"}: &Node{State: NodeStateOnline},
			},
			expected: 0,
		},
		{
			name: "all-free",
			nodes: NodeMap{
				Addr{FQDN: "free1", Port: "9012"}: &Node{State: NodeStateOnline, Tags: []string{TabletCommonTag}},
				Addr{FQDN: "free2", Port: "9012"}: &Node{State: NodeStateOnline, Tags: []string{TabletCommonTag}},
			},
			expected: 2,
		},
		{
			name: "banned",
			nodes: NodeMap{
				Addr{FQDN: "free", Port: "9012"}: &Node{State: NodeStateOnline, Tags: []string{TabletCommonTag}},
				Addr{FQDN: "n", Port: "9012"}: &Node{
					State: NodeStateOnline, Banned: true, Tags: []string{TabletCommonTag},
				},
			},
			expected: 1,
		},
		{
			name: "decommissioned",
			nodes: NodeMap{
				Addr{FQDN: "free", Port: "9012"}: &Node{State: NodeStateOnline, Tags: []string{TabletCommonTag}},
				Addr{FQDN: "n", Port: "9012"}: &Node{
					State: NodeStateOnline, Decommissioned: true, Tags: []string{TabletCommonTag},
				},
			},
			expected: 1,
		},
		{
			name: "offline",
			nodes: NodeMap{
				Addr{FQDN: "free", Port: "9012"}: &Node{State: NodeStateOnline, Tags: []string{TabletCommonTag}},
				Addr{FQDN: "n", Port: "9012"}:    &Node{State: NodeStateOffline, Tags: []string{TabletCommonTag}},
			},
			expected: 1,
		},
		{
			name: "disable_tablet_cells",
			nodes: NodeMap{
				Addr{FQDN: "free", Port: "9012"}: &Node{State: NodeStateOnline, Tags: []string{TabletCommonTag}},
				Addr{FQDN: "n", Port: "9012"}: &Node{
					State: NodeStateOffline, DisableTabletCells: true, Tags: []string{TabletCommonTag},
				},
			},
			expected: 1,
		},
		{
			name: "fully-decommissioned",
			nodes: NodeMap{
				Addr{FQDN: "free", Port: "9012"}: &Node{State: NodeStateOnline, Tags: []string{TabletCommonTag}},
				Addr{FQDN: "n", Port: "9012"}: &Node{
					State:              NodeStateOffline,
					DisableTabletCells: true,
					Decommissioned:     true,
					Banned:             true,
					Tags:               []string{TabletCommonTag},
				},
			},
			expected: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.nodes.GetFreeTabletCommonNodeCount())
		})
	}
}

func TestNodeMap_SetSlotState(t *testing.T) {
	nodes := []struct {
		Node            *Node
		nodeUnavailable bool
	}{
		{
			Node: &Node{
				Addr:  &Addr{FQDN: "free", Port: "9012"},
				State: NodeStateOnline,
			},
			nodeUnavailable: false,
		},
		{
			Node: &Node{
				Addr:  &Addr{FQDN: "offline", Port: "9012"},
				State: NodeStateOffline,
			},
			nodeUnavailable: true,
		},
		{
			Node: &Node{
				Addr:               &Addr{FQDN: "disable-tablet-cells", Port: "9012"},
				State:              NodeStateOnline,
				DisableTabletCells: true,
			},
			nodeUnavailable: true,
		},
		{
			Node: &Node{
				Addr:           &Addr{FQDN: "decommissioned", Port: "9012"},
				State:          NodeStateOnline,
				Decommissioned: true,
			},
			nodeUnavailable: true,
		},
		{
			Node: &Node{
				Addr:   &Addr{FQDN: "banned", Port: "9012"},
				State:  NodeStateOnline,
				Banned: true,
			},
			nodeUnavailable: true,
		},
	}

	// Build node map.
	m := make(NodeMap)
	for _, n := range nodes {
		n.Node.TabletSlots = []*TabletSlot{{State: TabletSlotStateNone}, {State: TabletSlotStateNone}}
		m[*n.Node.Addr] = n.Node
	}
	m.SetSlotState()

	for _, n := range nodes {
		require.NotEmpty(t, n.Node.TabletSlots)
		for _, s := range n.Node.TabletSlots {
			require.Equal(t, n.nodeUnavailable, s.nodeUnavailable)
		}
	}
}

func TestTabletCellBundle_GetFreeSlots(t *testing.T) {
	for _, tc := range []struct {
		name     string
		bundle   *TabletCellBundle
		expected int
	}{
		{
			name:     "no-slots",
			bundle:   &TabletCellBundle{},
			expected: 0,
		},
		{
			name: "all-free",
			bundle: &TabletCellBundle{
				Slots: map[Addr][]*TabletSlot{
					Addr{FQDN: "free1", Port: "9012"}: {{State: TabletSlotStateNone}, {State: TabletSlotStateNone}},
					Addr{FQDN: "free2", Port: "9012"}: {{State: TabletSlotStateNone}},
				},
			},
			expected: 3,
		},
		{
			name: "leading",
			bundle: &TabletCellBundle{
				Slots: map[Addr][]*TabletSlot{
					Addr{FQDN: "free1", Port: "9012"}: {{State: TabletSlotStateNone}, {State: TabletSlotStateLeading}},
					Addr{FQDN: "free2", Port: "9012"}: {{State: TabletSlotStateLeading}},
				},
			},
			expected: 1,
		},
		{
			name: "node-unavailable",
			bundle: &TabletCellBundle{
				Slots: map[Addr][]*TabletSlot{
					Addr{FQDN: "free1", Port: "9012"}: {{State: TabletSlotStateNone, nodeUnavailable: true}},
					Addr{FQDN: "free2", Port: "9012"}: {{State: TabletSlotStateLeading, nodeUnavailable: true}},
				},
			},
			expected: 0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.bundle.GetFreeSlots())
		})
	}
}

func TestNode_GetFreeSlots(t *testing.T) {
	for _, tc := range []struct {
		name     string
		node     *Node
		expected int
	}{
		{
			name:     "no-slots",
			node:     &Node{},
			expected: 0,
		},
		{
			name: "all-free",
			node: &Node{
				TabletSlots: []*TabletSlot{
					{State: TabletSlotStateNone},
					{State: TabletSlotStateNone},
					{State: TabletSlotStateNone},
				},
			},
			expected: 3,
		},
		{
			name: "leading",
			node: &Node{
				TabletSlots: []*TabletSlot{
					{State: TabletSlotStateNone},
					{State: TabletSlotStateLeading},
					{State: TabletSlotStateLeading},
				},
			},
			expected: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.node.GetFreeSlots())
		})
	}
}

func TestExtractCellTag(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cellID   string
		expected string
	}{
		{
			name:     "zero",
			cellID:   "0-0-0-0",
			expected: "0",
		},
		{
			name:     "simple",
			cellID:   "d08de3f8-d91c4dc4-3e960259-155acef4",
			expected: "16022",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, err := guid.ParseString(tc.cellID)
			require.NoError(t, err)
			require.Equal(t, tc.expected, ExtractCellTag(yt.NodeID(g)))
		})
	}
}

func TestNewMaintenanceRequest(t *testing.T) {
	req := NewMaintenanceRequest("gocms", "robot-yt-cms", "kernel reboot", map[string]interface{}{
		"walle_task_id": "production-6934348",
	})
	require.Equal(t, "gocms", req.ID)
	require.False(t, time.Time(req.CreationTime).IsZero())
	require.Equal(t, "robot-yt-cms", req.Issuer)
	require.Equal(t, "kernel reboot", req.Comment)
	require.Equal(t, "production-6934348", req.Extra.(map[string]interface{})["walle_task_id"])
}

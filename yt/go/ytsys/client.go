package ytsys

import (
	"context"
	"fmt"
	"os"
	"regexp"

	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/guid"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
	"go.ytsaurus.tech/yt/go/ytlog"
)

const (
	PrimaryMastersPath     = ypath.Path("//sys/primary_masters")
	SecondaryMastersPath   = ypath.Path("//sys/secondary_masters")
	TimestampProvidersPath = ypath.Path("//sys/timestamp_providers")
	ClusterNodesPath       = ypath.Path("//sys/cluster_nodes")
	HTTPProxiesPath        = ypath.Path("//sys/http_proxies")
	// COMPAT(babenko): drop once binaries in tests are updated
	LegacyHTTPProxiesPath = ypath.Path("//sys/proxies")
	RPCProxiesPath        = ypath.Path("//sys/rpc_proxies")
	SchedulersPath        = ypath.Path("//sys/scheduler/instances")
	ControllerAgentsPath  = ypath.Path("//sys/controller_agents/instances")

	PoolTreesPath         = ypath.Path("//sys/pool_trees")
	TabletCellsPath       = ypath.Path("//sys/tablet_cells")
	TabletCellBundlesPath = ypath.Path("//sys/tablet_cell_bundles")

	ChunksCountPath = ypath.Path("//sys/chunks/@count")
	LVCCountPath    = ypath.Path("//sys/lost_vital_chunks/@count")
	DMCCountPath    = ypath.Path("//sys/data_missing_chunks/@count")
	PMCCountPath    = ypath.Path("//sys/parity_missing_chunks/@count")
	URCCountPath    = ypath.Path("//sys/underreplicated_chunks/@count")
	QMCCountPath    = ypath.Path("//sys/quorum_missing_chunks/@count")

	ChunkRequisitionUpdateEnabledPath = ypath.Path("//sys/@chunk_requisition_update_enabled")
	ChunkRefreshEnabledPath           = ypath.Path("//sys/@chunk_refresh_enabled")
	ChunkReplicatorEnabledPath        = ypath.Path("//sys/@chunk_replicator_enabled")

	DefaultRPCProxyRole  = "default"
	DefaultHTTPProxyRole = "default"

	bannedAttr     = "banned"
	banMessageAttr = "ban_message"

	maintenanceAttr            = "maintenance"
	maintenanceMessageAttr     = "maintenance_message"
	maintenanceRequestsAttr    = "maintenance_requests"
	cmsMaintenanceRequestsAttr = "cms_maintenance_requests"

	userTagsAttr = "user_tags"

	disableSchedulerJobsAttr = "disable_scheduler_jobs"
	disableWriteSessionsAttr = "disable_write_sessions"
	decommissionedAttr       = "decommissioned"
	decommissionMessageAttr  = "decommission_message"
	enableBundleBalancerAttr = "enable_bundle_balancer"

	strongGuaranteeResourcesAttr = "strong_guarantee_resources"

	cmsLimitsAttr = "cms_limits"

	defaultNodeAnnotationsAttr = "annotations"
	testNodeAnnotationsAttr    = "integration_test_node_annotations"

	listResultMaxSize = 100000
)

// nodeAnnotationsAttr stores the name of an attribute used to acquire component info of a node
// e.g. physical host.
//
// "annotations" is a default value used in production.
// Integration tests can set this variable to "integration_test_node_annotations"
// by using USE_TEST_NODE_ANNOTATIONS_ATTR env variable.
//
// Masters, schedulers, controller agents and proxies have user attribute "annotations".
// Nodes, however, have system attribute annotations that can not be changed after initialization.
var nodeAnnotationsAttr = defaultNodeAnnotationsAttr

func init() {
	attr := os.Getenv("USE_TEST_NODE_ANNOTATIONS_ATTR")
	if attr != "" {
		nodeAnnotationsAttr = testNodeAnnotationsAttr
	}
}

type PrimaryMasterMap map[Addr]*PrimaryMaster
type SecondaryMasterMap map[Addr]*SecondaryMaster
type TimestampProviderMap map[Addr]*TimestampProvider
type HTTPProxyMap map[Addr]*HTTPProxy
type RPCProxyMap map[Addr]*RPCProxy
type SchedulerMap map[Addr]*Scheduler
type ControllerAgentMap map[Addr]*ControllerAgent

type PoolTrees map[string]*PoolTree
type NodePoolTrees map[Addr]string
type TabletCells map[yt.NodeID]*TabletCell
type TabletCellBundles map[string]*TabletCellBundle

// Client provides functions for cluster state discovery.
type Client struct {
	yc yt.Client
	l  log.Structured
}

// NewClient creates new client.
func NewClient(yc yt.Client, l log.Structured) *Client {
	if l == nil {
		l = ytlog.Must()
	}
	return &Client{
		yc: yc,
		l:  l,
	}
}

// GetPrimaryMasters loads primary masters from cypress.
func (c *Client) GetPrimaryMasters(ctx context.Context) (PrimaryMasterMap, error) {
	options := &yt.ListNodeOptions{
		Attributes: []string{
			"annotations", "id", "path",
			maintenanceAttr, maintenanceMessageAttr, maintenanceRequestsAttr,
			cmsMaintenanceRequestsAttr,
		},
		MaxSize: ptr.Int64(listResultMaxSize),
		TransactionOptions: &yt.TransactionOptions{
			SuppressTransactionCoordinatorSync: true,
			SuppressUpstreamSync:               true,
		},
	}

	var masters []*PrimaryMaster
	if err := c.yc.ListNode(ctx, PrimaryMastersPath, &masters, options); err != nil {
		return nil, err
	}

	grouped := make(PrimaryMasterMap)
	for _, m := range masters {
		if m.Annotations == nil {
			return nil, yterrors.Err("missing annotations attribute",
				yterrors.Attr("addr", m.Addr.String()))
		}
		grouped[*m.Addr] = m

		s, err := c.GetHydraState(ctx, m.Path)
		if err != nil {
			c.l.Warn("error retrieving hydra state", log.Error(err))
		} else {
			m.HydraState = s
		}
	}

	conf, err := c.GetMasterConfig(ctx)
	if err != nil {
		return nil, err
	}

	for _, m := range masters {
		m.CellID = conf.PrimaryCell.CellID
	}

	return grouped, nil
}

// GetSecondaryMasters loads secondary masters from cypress.
func (c *Client) GetSecondaryMasters(ctx context.Context) (SecondaryMasterMap, error) {
	var cellTags []string
	if err := c.yc.ListNode(ctx, SecondaryMastersPath, &cellTags, &yt.ListNodeOptions{
		MaxSize: ptr.Int64(listResultMaxSize),
	}); err != nil {
		return nil, err
	}

	var masters []*SecondaryMaster
	for _, cellTag := range cellTags {
		options := &yt.ListNodeOptions{
			Attributes: []string{
				"annotations", "id", "path",
				maintenanceAttr, maintenanceMessageAttr, maintenanceRequestsAttr,
				cmsMaintenanceRequestsAttr,
			},
			MaxSize: ptr.Int64(listResultMaxSize),
		}
		var out []*SecondaryMaster
		if err := c.yc.ListNode(ctx, SecondaryMastersPath.Child(cellTag), &out, options); err != nil {
			return nil, err
		}

		for _, m := range out {
			m.CellTag = cellTag
		}

		masters = append(masters, out...)
	}

	grouped := make(SecondaryMasterMap)
	for _, m := range masters {
		if m.Annotations == nil {
			return nil, yterrors.Err("missing annotations attribute",
				yterrors.Attr("addr", m.Addr.String()))
		}
		grouped[*m.Addr] = m

		s, err := c.GetHydraState(ctx, m.Path)
		if err != nil {
			c.l.Warn("error retrieving hydra state", log.Error(err))
		} else {
			m.HydraState = s
		}
	}

	conf, err := c.GetMasterConfig(ctx)
	if err != nil {
		return nil, err
	}

	for _, m := range masters {
		if masterConf, ok := conf.SecondaryCellsByTag[m.CellTag]; ok {
			m.CellID = masterConf.CellID
		}
	}

	return grouped, nil
}

func (c *Client) GetTimestampProviders(ctx context.Context) (TimestampProviderMap, error) {
	options := &yt.ListNodeOptions{
		Attributes: []string{
			"annotations", "id", "path",
			maintenanceAttr, maintenanceMessageAttr, maintenanceRequestsAttr,
			cmsMaintenanceRequestsAttr,
		},
		MaxSize: ptr.Int64(listResultMaxSize),
		TransactionOptions: &yt.TransactionOptions{
			SuppressTransactionCoordinatorSync: true,
			SuppressUpstreamSync:               true,
		},
	}

	var providers []*TimestampProvider
	if err := c.yc.ListNode(ctx, TimestampProvidersPath, &providers, options); err != nil {
		if yterrors.ContainsErrorCode(err, yterrors.CodeResolveError) {
			c.l.Warn("error listing timestamp providers", log.Error(err))
			return nil, nil
		}
		return nil, err
	}

	m := make(TimestampProviderMap)
	for _, p := range providers {
		if p.Annotations == nil {
			return nil, yterrors.Err("missing annotations attribute",
				yterrors.Attr("addr", p.Addr.String()))
		}
		m[*p.Addr] = p

		s, err := c.GetHydraState(ctx, p.Path)
		if err != nil {
			c.l.Warn("error retrieving hydra state", log.Error(err))
		} else {
			p.HydraState = s
		}
	}

	conf, err := c.GetTimestampProviderConfig(ctx)
	if err != nil {
		return nil, err
	}

	for _, p := range providers {
		if conf.ClockCell != nil {
			p.CellID = conf.ClockCell.CellID
		} else {
			p.CellID = conf.PrimaryCell.CellID
		}
	}

	return m, nil
}

func (c *Client) GetHydraState(ctx context.Context, path ypath.Path) (*HydraState, error) {
	p := path.Child("orchid/monitoring/hydra")

	var s *HydraState
	if err := c.yc.GetNode(ctx, p, &s, nil); err != nil {
		return nil, err
	}

	return s, nil
}

func (c *Client) GetDiskIDsMismatched(ctx context.Context, node *Node) (*bool, error) {
	path := node.GetCypressPath().Child("orchid/disk_monitoring/disk_ids_mismatched")

	var diskIDsMismatched *bool
	if err := c.yc.GetNode(ctx, path, &diskIDsMismatched, nil); err != nil {
		return nil, err
	}

	return diskIDsMismatched, nil
}

func (c *Client) GetNodeStartTime(ctx context.Context, node *Node) (*yson.Time, error) {
	path := node.GetCypressPath().Child("orchid/service/start_time")

	var startTime *yson.Time
	if err := c.yc.GetNode(ctx, path, &startTime, nil); err != nil {
		return nil, err
	}

	return startTime, nil
}

func (c *Client) GetMasterConfig(ctx context.Context) (*MasterConfig, error) {
	options := &yt.ListNodeOptions{
		Attributes: []string{"path"},
		MaxSize:    ptr.Int64(listResultMaxSize),
	}

	var masters []*PrimaryMaster
	if err := c.yc.ListNode(ctx, PrimaryMastersPath, &masters, options); err != nil {
		return nil, err
	}

	var lastError error
	for _, m := range masters {
		conf, err := c.getMasterConfig(ctx, m.Path)
		if err != nil {
			c.l.Warn("error retrieving master config", log.Error(err))
			lastError = err
		} else {
			return conf, nil
		}
	}

	return nil, lastError
}

func (c *Client) getMasterConfig(ctx context.Context, path ypath.Path) (*MasterConfig, error) {
	p := path.Child("orchid/config")

	var conf MasterConfig
	if err := c.yc.GetNode(ctx, p, &conf, nil); err != nil {
		return nil, err
	}

	conf.SecondaryCellsByTag = make(map[string]*MasterCellConfig, len(conf.SecondaryCells))
	for _, cell := range conf.SecondaryCells {
		cellTag := ExtractCellTag(cell.CellID)
		conf.SecondaryCellsByTag[cellTag] = cell
	}

	return &conf, nil
}

func (c *Client) GetTimestampProviderConfig(ctx context.Context) (*TimestampProviderConfig, error) {
	options := &yt.ListNodeOptions{
		Attributes: []string{"path"},
		MaxSize:    ptr.Int64(listResultMaxSize),
	}

	var providers []*TimestampProvider
	if err := c.yc.ListNode(ctx, TimestampProvidersPath, &providers, options); err != nil {
		return nil, err
	}

	var lastError error
	for _, p := range providers {
		conf, err := c.getTimestampProviderConfig(ctx, p.Path)
		if err != nil {
			c.l.Warn("error retrieving timestamp provider config", log.Error(err))
			lastError = err
		} else {
			return conf, nil
		}
	}

	return nil, lastError
}

func (c *Client) getTimestampProviderConfig(ctx context.Context, path ypath.Path) (*TimestampProviderConfig, error) {
	p := path.Child("orchid/config")

	var conf TimestampProviderConfig
	if err := c.yc.GetNode(ctx, p, &conf, nil); err != nil {
		return nil, err
	}

	return &conf, nil
}

// GetNodes loads nodes querying multiple attributes.
//
// Consider using ListNodes instead.
func (c *Client) GetNodes(ctx context.Context) (NodeMap, error) {
	nodes, err := c.ListNodes(ctx,
		[]string{
			"id", nodeAnnotationsAttr, "rack",
			bannedAttr, banMessageAttr, decommissionedAttr, decommissionMessageAttr,
			maintenanceAttr, maintenanceMessageAttr, maintenanceRequestsAttr,
			cmsMaintenanceRequestsAttr,
			"disable_scheduler_jobs", "disable_write_sessions",
			"state", "last_seen_time", "version", "statistics",
			"tags", "flavors", "tablet_slots", "resource_limits", "resource_limits_overrides",
			"resource_usage",
		},
	)
	if err != nil {
		return nil, err
	}

	grouped := make(NodeMap)
	for _, n := range nodes {
		grouped[*n.Addr] = n
		if n.Statistics == nil {
			n.Statistics = &NodeStatistics{}
		}
		if n.ResourceUsage == nil {
			n.ResourceUsage = &NodeResourceUsage{}
		}
	}

	return grouped, nil
}

// ListNodes lists cluster nodes querying only given set of attributes.
func (c *Client) ListNodes(ctx context.Context, attrs []string) ([]*Node, error) {
	options := &yt.ListNodeOptions{
		Attributes: attrs,
		MaxSize:    ptr.Int64(listResultMaxSize),
		TransactionOptions: &yt.TransactionOptions{
			SuppressTransactionCoordinatorSync: true,
			SuppressUpstreamSync:               true,
		},
	}

	var nodes []*Node
	if err := c.yc.ListNode(ctx, ClusterNodesPath, &nodes, options); err != nil {
		return nil, err
	}

	return nodes, nil
}

// GetHTTPProxies loads http proxies from cypress.
func (c *Client) GetHTTPProxies(ctx context.Context) (HTTPProxyMap, error) {
	options := &yt.ListNodeOptions{
		Attributes: []string{
			"id", "path", "annotations", "role", bannedAttr, banMessageAttr, "liveness",
		},
		MaxSize: ptr.Int64(listResultMaxSize),
		TransactionOptions: &yt.TransactionOptions{
			SuppressTransactionCoordinatorSync: true,
			SuppressUpstreamSync:               true,
		},
	}

	var proxies []*HTTPProxy
	// COMPAT(babenko): drop //sys/proxies once YT binaries are updated
	if err := c.yc.ListNode(ctx, HTTPProxiesPath, &proxies, options); err != nil {
		if compatErr := c.yc.ListNode(ctx, LegacyHTTPProxiesPath, &proxies, options); compatErr != nil {
			return nil, err
		}
	}

	grouped := make(HTTPProxyMap)
	for _, p := range proxies {
		grouped[*p.Addr] = p
		if p.Role == "" {
			p.Role = DefaultHTTPProxyRole
		}
	}

	return grouped, nil
}

// GetRPCProxies loads rpc proxies from cypress.
func (c *Client) GetRPCProxies(ctx context.Context) (RPCProxyMap, error) {
	options := &yt.GetNodeOptions{
		Attributes: []string{
			"id", "path", "annotations", "role",
			bannedAttr, banMessageAttr,
			maintenanceAttr, maintenanceMessageAttr, maintenanceRequestsAttr,
			cmsMaintenanceRequestsAttr,
		},
		MaxSize: ptr.Int64(listResultMaxSize),
		TransactionOptions: &yt.TransactionOptions{
			SuppressTransactionCoordinatorSync: true,
			SuppressUpstreamSync:               true,
		},
	}

	proxies := make(RPCProxyMap)
	if err := c.yc.GetNode(ctx, RPCProxiesPath, &proxies, options); err != nil {
		return nil, err
	}

	for addr, p := range proxies {
		addrCopy := addr
		p.Addr = &addrCopy
		if p.Role == "" {
			p.Role = DefaultRPCProxyRole
		}
	}

	return proxies, nil
}

// GetSchedulers loads schedulers from cypress.
func (c *Client) GetSchedulers(ctx context.Context) (SchedulerMap, error) {
	options := &yt.ListNodeOptions{
		Attributes: []string{
			"id", "path", "annotations",
			maintenanceAttr, maintenanceMessageAttr, maintenanceRequestsAttr,
			cmsMaintenanceRequestsAttr,
		},
		MaxSize: ptr.Int64(listResultMaxSize),
		TransactionOptions: &yt.TransactionOptions{
			SuppressTransactionCoordinatorSync: true,
			SuppressUpstreamSync:               true,
		},
	}

	var schedulers []*Scheduler
	if err := c.yc.ListNode(ctx, SchedulersPath, &schedulers, options); err != nil {
		return nil, err
	}

	grouped := make(SchedulerMap)
	for _, s := range schedulers {
		grouped[*s.Addr] = s

		p := SchedulersPath.
			Child(s.Addr.String()).
			Child("orchid/scheduler/service/connected")

		connected := false
		if err := c.yc.GetNode(ctx, p, &connected, nil); err != nil {
			c.l.Info("error checking if scheduler is connected",
				log.String("addr", s.Addr.String()), log.Error(err))
			s.ConnectionRequestError = err
			continue
		}

		s.Connected = connected
	}

	return grouped, nil
}

// GetControllerAgents loads controller agents from cypress.
func (c *Client) GetControllerAgents(ctx context.Context) (ControllerAgentMap, error) {
	options := &yt.ListNodeOptions{
		Attributes: []string{
			"id", "path", "annotations",
			maintenanceAttr, maintenanceMessageAttr, maintenanceRequestsAttr,
			cmsMaintenanceRequestsAttr,
		},
		MaxSize: ptr.Int64(listResultMaxSize),
		TransactionOptions: &yt.TransactionOptions{
			SuppressTransactionCoordinatorSync: true,
			SuppressUpstreamSync:               true,
		},
	}

	var agents []*ControllerAgent
	if err := c.yc.ListNode(ctx, ControllerAgentsPath, &agents, options); err != nil {
		return nil, err
	}

	grouped := make(ControllerAgentMap)
	for _, a := range agents {
		grouped[*a.Addr] = a

		p := ControllerAgentsPath.
			Child(a.Addr.String()).
			Child("orchid/controller_agent/service/connected")

		connected := false
		if err := c.yc.GetNode(ctx, p, &connected, nil); err != nil {
			c.l.Info("error checking if controller agent is connected",
				log.String("addr", a.Addr.String()), log.Error(err))
			a.ConnectionRequestError = err
			continue
		}

		a.Connected = connected
	}

	return grouped, nil
}

func (c *Client) DisableChunkLocations(ctx context.Context, addr *Addr, uuids []guid.GUID) ([]guid.GUID, error) {
	res, err := c.yc.DisableChunkLocations(ctx, addr.String(), uuids, nil)
	if err != nil {
		return nil, err
	}
	return res.LocationUUIDs, nil
}

func (c *Client) DestroyChunkLocations(ctx context.Context, addr *Addr, recoverUnlinkedDisks bool, uuids []guid.GUID) ([]guid.GUID, error) {
	res, err := c.yc.DestroyChunkLocations(ctx, addr.String(), recoverUnlinkedDisks, uuids, nil)
	if err != nil {
		return nil, err
	}
	return res.LocationUUIDs, nil
}

func (c *Client) ResurrectChunkLocations(ctx context.Context, addr *Addr, uuids []guid.GUID) ([]guid.GUID, error) {
	res, err := c.yc.ResurrectChunkLocations(ctx, addr.String(), uuids, nil)
	if err != nil {
		return nil, err
	}
	return res.LocationUUIDs, nil
}

func (c *Client) RequestRestart(ctx context.Context, addr *Addr) error {
	return c.yc.RequestRestart(ctx, addr.String(), nil)
}

func (c *Client) GetPoolTrees(ctx context.Context) (PoolTrees, error) {
	var poolTrees []string
	if err := c.yc.ListNode(ctx, PoolTreesPath, &poolTrees, &yt.ListNodeOptions{
		MaxSize: ptr.Int64(listResultMaxSize),
	}); err != nil {
		return nil, err
	}

	ret := make(PoolTrees)
	for _, tree := range poolTrees {
		p := PoolTreesPath.Child(tree)

		options := &yt.ListNodeOptions{
			Attributes: []string{"min_share_resources"},
			MaxSize:    ptr.Int64(listResultMaxSize),
		}

		var nodes []*PoolTreeNode
		if err := c.yc.ListNode(ctx, p, &nodes, options); err != nil {
			return nil, err
		}

		t := NewPoolTree(tree)
		for _, n := range nodes {
			t.Nodes[n.Name] = n
			if n.Resources == nil {
				n.Resources = &PoolTreeResourceLimits{}
			}
		}
		ret[tree] = t

		// Get available resources.
		p = ypath.Root.Child("sys/scheduler").
			Child("orchid/scheduler/scheduling_info_per_pool_tree").
			Child(tree).
			Child("resource_limits")

		var resources *PoolTreeAvailableResources
		if err := c.yc.GetNode(ctx, p, &resources, nil); err != nil {
			return nil, err
		}
		ret[tree].AvailableResources = resources

		// Get reserves.
		p = ypath.Root.Child("sys/scheduler").
			Child("orchid/scheduler/scheduling_info_per_pool_tree").
			Child(tree).
			Child("fair_share_info/resource_distribution_info/undistributed_resources")

		var reserves *PoolTreeResourceReserves
		if err := c.yc.GetNode(ctx, p, &reserves, nil); err != nil {
			c.l.Info("error retrieving resource reserves", log.String("tree", tree), log.Error(err))
		} else {
			ret[tree].ResourceReserves = reserves
		}
	}

	return ret, nil
}

// GetNodePoolTrees returns addr->tree map for all nodes that belong to some pool tree.
func (c *Client) GetNodePoolTrees(ctx context.Context) (NodePoolTrees, error) {
	var poolTrees []string
	if err := c.yc.ListNode(ctx, PoolTreesPath, &poolTrees, &yt.ListNodeOptions{
		MaxSize: ptr.Int64(listResultMaxSize),
	}); err != nil {
		return nil, err
	}

	m := make(NodePoolTrees)
	for _, tree := range poolTrees {
		p := ypath.Root.Child("sys/scheduler").
			Child("orchid/scheduler/scheduling_info_per_pool_tree").
			Child(tree).
			Child("node_addresses")

		var addrs []Addr
		if err := c.yc.GetNode(ctx, p, &addrs, nil); err != nil {
			return nil, err
		}

		for _, addr := range addrs {
			m[addr] = tree
		}
	}

	return m, nil
}

func (c *Client) GetStrongGuaranteeResources(
	ctx context.Context,
	path ypath.Path,
) (*StrongGuaranteeResources, error) {
	p := path.Attr(strongGuaranteeResourcesAttr)

	var sg *StrongGuaranteeResources
	if err := c.yc.GetNode(ctx, p, &sg, nil); err != nil {
		return nil, err
	}

	return sg, nil
}

func (c *Client) SetStrongGuaranteeCPU(
	ctx context.Context,
	path ypath.Path,
	guarantee float64,
) error {
	p := path.Attr(strongGuaranteeResourcesAttr).Child("cpu")

	if err := c.yc.SetNode(ctx, p, guarantee, nil); err != nil {
		return err
	}

	return nil
}

func (c *Client) GetReservePoolCMSLimits(ctx context.Context, path ypath.Path) (*ReservePoolCMSLimits, error) {
	p := path.Attr(cmsLimitsAttr)

	var l *ReservePoolCMSLimits
	if err := c.yc.GetNode(ctx, p, &l, nil); err != nil {
		return nil, err
	}

	return l, nil
}

// GetTabletCells loads tablet cells from cypress.
func (c *Client) GetTabletCells(ctx context.Context) (TabletCells, error) {
	options := &yt.ListNodeOptions{
		Attributes: []string{"tablet_cell_bundle"},
		MaxSize:    ptr.Int64(listResultMaxSize),
	}

	var cells []*TabletCell
	if err := c.yc.ListNode(ctx, TabletCellsPath, &cells, options); err != nil {
		return nil, err
	}

	ret := make(TabletCells)
	for _, cell := range cells {
		ret[cell.ID] = cell
	}

	return ret, nil
}

// GetTabletCells loads tablet cell bundles from cypress.
func (c *Client) GetTabletCellBundles(ctx context.Context) (TabletCellBundles, error) {
	options := &yt.ListNodeOptions{
		Attributes: []string{"enable_bundle_balancer", "nodes", "production"},
		MaxSize:    ptr.Int64(listResultMaxSize),
	}

	var bundles []*TabletCellBundle
	if err := c.yc.ListNode(ctx, TabletCellBundlesPath, &bundles, options); err != nil {
		return nil, err
	}

	ret := make(TabletCellBundles)
	for _, b := range bundles {
		if b.BalancerEnabled == nil {
			t := YTBool(true)
			b.BalancerEnabled = &t
		}
		if b.Production == nil {
			t := YTBool(true)
			b.Production = &t
		}
		ret[b.Name] = b
	}

	return ret, nil
}

// getNodeChunksDefaultLimit is a default individual chunk request limit
// used in GetNodeChunks function.
const getNodeChunksDefaultLimit = 100

type GetNodeChunksOption struct {
	// All flags makes GetNodeChunks function query all individual chunks.
	// By default only a limited number of chunks queried.
	All bool
	// Limits limits number of individual chunk queries.
	Limit int
}

// GetNodeChunks returns chunks stored on given node.
//
// This function makes individual requests for to a limited number of chunk states.
// Use options to override this behaviour.
func (c *Client) GetNodeChunks(ctx context.Context, addr *Addr, opts *GetNodeChunksOption) ([]*Chunk, error) {
	var version string

	err := c.yc.GetNode(ctx, ClusterNodesPath.Child(addr.String()).Attr("version"), &version, nil)
	if err != nil {
		return nil, err
	}

	p := ClusterNodesPath.Child(addr.String()).Child("orchid/data_node/stored_chunks")
	if version < "23.2.12362008-stable-ya~0e283816d7200f01" {
		p = ClusterNodesPath.Child(addr.String()).Child("orchid/stored_chunks")
	}

	var chunkIDs []yt.NodeID
	if err := c.yc.ListNode(ctx, p, &chunkIDs, &yt.ListNodeOptions{
		MaxSize: ptr.Int64(listResultMaxSize),
	}); err != nil {
		return nil, err
	}

	if opts == nil {
		opts = &GetNodeChunksOption{
			Limit: getNodeChunksDefaultLimit,
		}
	}

	chunks := make([]*Chunk, 0, len(chunkIDs))
	for i, id := range chunkIDs {
		if !opts.All && i >= opts.Limit {
			break
		}
		chunk, err := c.getChunk(ctx, id)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// getChunk queries single chunk.
func (c *Client) getChunk(ctx context.Context, id yt.NodeID) (*Chunk, error) {
	options := &yt.GetNodeOptions{
		Attributes: []string{"id", "confirmed"},
	}

	p := ypath.Path(fmt.Sprintf("#%s", id.String()))

	chunk := &Chunk{}
	if err := c.yc.GetNode(ctx, p, chunk, options); err != nil {
		return nil, err
	}

	return chunk, nil
}

// GetDisableBundleBalancer returns corresponding attribute.
func (c *Client) GetDisableBundleBalancer(ctx context.Context) (bool, error) {
	path := ypath.Root.Child("sys").Attr("disable_bundle_balancer")
	var out bool
	err := c.yc.GetNode(ctx, path, &out, nil)
	if yterrors.ContainsErrorCode(err, yterrors.CodeResolveError) {
		return false, nil
	}
	return out, err
}

// GetLVC returns number of lost vital chunks.
func (c *Client) GetChunkCount(ctx context.Context) (int64, error) {
	return c.getInt64Attr(ctx, ChunksCountPath)
}

// GetLVC returns number of lost vital chunks.
func (c *Client) GetLVC(ctx context.Context) (int64, error) {
	return c.getInt64Attr(ctx, LVCCountPath)
}

// GetDMC returns number of data missing chunks.
func (c *Client) GetDMC(ctx context.Context) (int64, error) {
	return c.getInt64Attr(ctx, DMCCountPath)
}

// GetPMC returns number of parity missing chunks.
func (c *Client) GetPMC(ctx context.Context) (int64, error) {
	return c.getInt64Attr(ctx, PMCCountPath)
}

// GetURC returns number of underreplicated chunks.
func (c *Client) GetURC(ctx context.Context) (int64, error) {
	return c.getInt64Attr(ctx, URCCountPath)
}

// GetQMC returns number of quorum missing chunks.
func (c *Client) GetQMC(ctx context.Context) (int64, error) {
	return c.getInt64Attr(ctx, QMCCountPath)
}

// GetChunkRequisitionUpdateEnabled returns corresponding flag.
func (c *Client) GetChunkRequisitionUpdateEnabled(ctx context.Context) (bool, error) {
	return c.getBoolAttr(ctx, ChunkRequisitionUpdateEnabledPath)
}

// GetChunkRefreshEnabled returns corresponding flag.
func (c *Client) GetChunkRefreshEnabled(ctx context.Context) (bool, error) {
	return c.getBoolAttr(ctx, ChunkRefreshEnabledPath)
}

// GetChunkReplicatorEnabled returns corresponding flag.
func (c *Client) GetChunkReplicatorEnabled(ctx context.Context) (bool, error) {
	return c.getBoolAttr(ctx, ChunkReplicatorEnabledPath)
}

// getInt64Attr returns int64 value of an attribute specified by path.
func (c *Client) getInt64Attr(ctx context.Context, path ypath.Path) (int64, error) {
	var out int64
	if err := c.yc.GetNode(ctx, path, &out, nil); err != nil {
		return out, err
	}
	return out, nil
}

// getBoolAttr returns bool value of an attribute specified by path.
func (c *Client) getBoolAttr(ctx context.Context, path ypath.Path) (bool, error) {
	var out bool
	if err := c.yc.GetNode(ctx, path, &out, nil); err != nil {
		return out, err
	}
	return out, nil
}

// GetIntegrityIndicators loads chunk integrity indicators.
func (c *Client) GetIntegrityIndicators(ctx context.Context) (*ChunkIntegrity, error) {
	return LoadIntegrityIndicators(ctx, c)
}

func (c *Client) Ban(ctx context.Context, component Component, msg string) error {
	p := component.GetCypressPath()

	tx, err := c.yc.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Abort() }()

	err = tx.SetNode(ctx, p.Attr(bannedAttr), true, nil)
	if err != nil {
		return err
	}

	err = tx.SetNode(ctx, p.Attr(banMessageAttr), msg, nil)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (c *Client) Unban(ctx context.Context, component Component) error {
	p := component.GetCypressPath()

	tx, err := c.yc.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Abort() }()

	err = tx.SetNode(ctx, p.Attr(bannedAttr), false, nil)
	if err != nil {
		return err
	}

	err = tx.SetNode(ctx, p.Attr(banMessageAttr), "", nil)
	if err != nil {
		return err
	}

	return tx.Commit()
}

var (
	maintenanceRequestsCannotBeSetRE = regexp.MustCompile(`Builtin attribute "maintenance_requests" cannot be set`)
)

// SetMaintenance sets @maintenance=%true and upserts request to @maintenances_requests.
func (c *Client) SetMaintenance(ctx context.Context, component Component, req *MaintenanceRequest) error {
	p := component.GetCypressPath()

	tx, err := c.yc.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Abort() }()

	err = tx.SetNode(ctx, p.Attr(maintenanceAttr), true, nil)
	if err != nil {
		return err
	}

	err = tx.SetNode(ctx, p.Attr(maintenanceMessageAttr), req.Comment, nil)
	if err != nil {
		return err
	}

	addMaintenanceReq := func(yc yt.CypressClient, attr string) error {
		reqs := make(MaintenanceRequestMap)
		err = yc.GetNode(ctx, p.Attr(attr), &reqs, nil)
		if err != nil && !yterrors.ContainsResolveError(err) {
			return err
		}

		reqs[req.ID] = req
		err = yc.SetNode(ctx, p.Attr(attr), reqs, nil)
		if err != nil {
			return err
		}

		return nil
	}

	if err := addMaintenanceReq(tx, cmsMaintenanceRequestsAttr); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// After https://st.yandex-team.ru/YT-17461 @maintenance_requests becomes system attribute.
	// Here we make our best effort to set legacy attribute.
	// TODO(@verytable): Remove when BC is migrated to @cms_maintenance_requests.
	if err := addMaintenanceReq(c.yc, maintenanceRequestsAttr); err != nil {
		if yterrors.ContainsMessageRE(err, maintenanceRequestsCannotBeSetRE) {
			return nil
		}
		return err
	}

	return nil
}

// UnsetMaintenance sets @maintenance=%false and removes request from @maintenances_requests.
func (c *Client) UnsetMaintenance(ctx context.Context, component Component, requestID string) error {
	p := component.GetCypressPath()

	tx, err := c.yc.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Abort() }()

	err = tx.SetNode(ctx, p.Attr(maintenanceAttr), false, nil)
	if err != nil {
		return err
	}

	err = tx.SetNode(ctx, p.Attr(maintenanceMessageAttr), "", nil)
	if err != nil {
		return err
	}

	removeMaintenanceReq := func(yc yt.CypressClient, attr string) error {
		reqs := make(MaintenanceRequestMap)
		err = yc.GetNode(ctx, p.Attr(attr), &reqs, nil)
		if err != nil && !yterrors.ContainsResolveError(err) {
			return err
		}

		delete(reqs, requestID) // do nothing if no request found
		err = yc.SetNode(ctx, p.Attr(attr), reqs, nil)
		if err != nil {
			return err
		}

		return nil
	}

	if err := removeMaintenanceReq(tx, cmsMaintenanceRequestsAttr); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	// After https://st.yandex-team.ru/YT-17461 @maintenance_requests becomes system attribute.
	// Here we make our best effort to set legacy attribute.
	// TODO(@verytable): Remove when BC is migrated to @cms_maintenance_requests.
	if err := removeMaintenanceReq(c.yc, maintenanceRequestsAttr); err != nil {
		if yterrors.ContainsMessageRE(err, maintenanceRequestsCannotBeSetRE) {
			return nil
		}
		return err
	}

	return nil
}

func (c *Client) HasMaintenanceAttr(ctx context.Context, component Component) (YTBool, error) {
	p := component.GetCypressPath()

	var inMaintenance YTBool
	if err := c.yc.GetNode(ctx, p.Attr(maintenanceAttr), &inMaintenance, nil); err != nil {
		if yterrors.ContainsErrorCode(err, yterrors.CodeResolveError) {
			return false, nil
		}
		return false, err
	}

	return inMaintenance, nil
}

func (c *Client) SetTag(ctx context.Context, p ypath.Path, tag string) error {
	return c.yc.SetNode(ctx, p.Attr(userTagsAttr).ListBegin(), tag, nil)
}

func (c *Client) RemoveTag(ctx context.Context, p ypath.Path, tag string) error {
	tx, err := c.yc.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Abort() }()

	var tags []string
	if err := tx.GetNode(ctx, p.Attr(userTagsAttr), &tags, nil); err != nil {
		return err
	}

	var filteredTags []string
	for _, t := range tags {
		if t != tag {
			filteredTags = append(filteredTags, t)
		}
	}

	if err := tx.SetNode(ctx, p.Attr(userTagsAttr), filteredTags, nil); err != nil {
		return err
	}

	return tx.Commit()
}

func (c *Client) PathExists(ctx context.Context, p ypath.Path) (bool, error) {
	return c.yc.NodeExists(ctx, p, nil)
}

func (c *Client) DisableSchedulerJobs(ctx context.Context, addr *Addr) error {
	path := ClusterNodesPath.Child(addr.String()).Attr(disableSchedulerJobsAttr)
	return c.yc.SetNode(ctx, path, true, nil)
}

func (c *Client) EnableSchedulerJobs(ctx context.Context, addr *Addr) error {
	path := ClusterNodesPath.Child(addr.String()).Attr(disableSchedulerJobsAttr)
	return c.yc.SetNode(ctx, path, false, nil)
}

func (c *Client) DisableWriteSessions(ctx context.Context, addr *Addr) error {
	path := ClusterNodesPath.Child(addr.String()).Attr(disableWriteSessionsAttr)
	return c.yc.SetNode(ctx, path, true, nil)
}

func (c *Client) EnableWriteSessions(ctx context.Context, addr *Addr) error {
	path := ClusterNodesPath.Child(addr.String()).Attr(disableWriteSessionsAttr)
	return c.yc.SetNode(ctx, path, false, nil)
}

func (c *Client) AddMaintenance(
	ctx context.Context,
	component yt.MaintenanceComponent,
	addr *Addr,
	maintenanceType yt.MaintenanceType,
	comment string,
	opts *yt.AddMaintenanceOptions,
) (*yt.MaintenanceID, error) {
	res, err := c.yc.AddMaintenance(ctx, component, addr.String(), maintenanceType, comment, opts)
	if err != nil {
		return nil, err
	}
	return &res.ID, nil
}

func (c *Client) RemoveMaintenance(
	ctx context.Context,
	component yt.MaintenanceComponent,
	addr *Addr,
	opts *yt.RemoveMaintenanceOptions,
) (*yt.RemoveMaintenanceResponse, error) {
	return c.yc.RemoveMaintenance(ctx, component, addr.String(), opts)
}

func (c *Client) MarkNodeDecommissioned(ctx context.Context, addr *Addr, msg string) error {
	p := ClusterNodesPath.Child(addr.String())

	tx, err := c.yc.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Abort() }()

	err = tx.SetNode(ctx, p.Attr(decommissionedAttr), true, nil)
	if err != nil {
		return err
	}

	err = tx.SetNode(ctx, p.Attr(decommissionMessageAttr), msg, nil)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (c *Client) UnmarkNodeDecommissioned(ctx context.Context, addr *Addr) error {
	p := ClusterNodesPath.Child(addr.String())

	tx, err := c.yc.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Abort() }()

	err = tx.SetNode(ctx, p.Attr(decommissionedAttr), false, nil)
	if err != nil {
		return err
	}

	err = tx.SetNode(ctx, p.Attr(decommissionMessageAttr), "", nil)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (c *Client) OverrideRemovalSlots(ctx context.Context, addr *Addr, slots int) error {
	path := ClusterNodesPath.Child(addr.String()).
		Attr("resource_limits_overrides").
		Child("removal_slots")
	return c.yc.SetNode(ctx, path, slots, nil)
}

func (c *Client) DropRemovalSlotsOverride(ctx context.Context, addr *Addr) error {
	path := ClusterNodesPath.Child(addr.String()).
		Attr("resource_limits_overrides").
		Child("removal_slots")
	err := c.yc.RemoveNode(ctx, path, nil)
	if yterrors.ContainsErrorCode(err, yterrors.CodeResolveError) {
		return nil
	}
	return err
}

func (c *Client) DisableBundleBalancer(ctx context.Context, addr *Addr) error {
	path := ClusterNodesPath.Child(addr.String()).Attr(enableBundleBalancerAttr)
	return c.yc.SetNode(ctx, path, false, nil)
}

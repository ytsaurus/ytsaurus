package ytsys

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"time"

	"go.ytsaurus.tech/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yterrors"
)

const (
	// TabletCommonTag is a node tag used in automatic bundle distribution.
	TabletCommonTag = "tablet_common"
	// GPUTag is a node tag present on all gpu hosts.
	GPUTag = "gpu"
	// GPUProdTag is a node tag used in node tag filters of gpu trees.
	GPUProdTag = "gpu_prod"
)

const (
	FlavorData   = "data"
	FlavorExec   = "exec"
	FlavorTablet = "tablet"
)

// ClusterRole type stores all supported cluster roles.
type ClusterRole string

const (
	RoleNode              ClusterRole = "node"
	RolePrimaryMaster     ClusterRole = "primary_master"
	RoleSecondaryMaster   ClusterRole = "secondary_master"
	RoleTimestampProvider ClusterRole = "timestamp_provider"
	RoleHTTPProxy         ClusterRole = "http_proxy"
	RoleRPCProxy          ClusterRole = "rpc_proxy"
	RoleScheduler         ClusterRole = "scheduler"
	RoleControllerAgent   ClusterRole = "controller_agent"
)

// PhysicalHost is a string to hold physical host address.
type PhysicalHost = string

// YTProxyRole type to hold values of proxy's @role attribute.
type YTProxyRole string

type YTProxy interface {
	GetAddr() *Addr
	ProxyRole() YTProxyRole
	IsBanned() bool
}

// Component is a cluster component.
type Component interface {
	GetPhysicalHost() PhysicalHost
	GetAddr() *Addr
	GetCypressPath() ypath.Path
	GetRole() ClusterRole
}

// Addr is a yson (de)serializable network address.
type Addr struct {
	FQDN string
	Port string
}

func (a *Addr) String() string {
	return net.JoinHostPort(a.FQDN, a.Port)
}

func (a Addr) MarshalText() (text []byte, err error) {
	return []byte(net.JoinHostPort(a.FQDN, a.Port)), nil
}

func (a *Addr) UnmarshalText(text []byte) error {
	hostport := string(text)
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return err
	}
	a.FQDN = host
	a.Port = port
	return nil
}

func (a Addr) MarshalYSON() ([]byte, error) {
	return yson.Marshal(net.JoinHostPort(a.FQDN, a.Port))
}

func (a *Addr) UnmarshalYSON(data []byte) error {
	var hostport string
	if err := yson.Unmarshal(data, &hostport); err != nil {
		return err
	}
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return err
	}
	a.FQDN = host
	a.Port = port
	return nil
}

type MasterCellConfig struct {
	CellID yt.NodeID `yson:"cell_id"`
}

type MasterConfig struct {
	PrimaryCell    *MasterCellConfig   `yson:"primary_master"`
	SecondaryCells []*MasterCellConfig `yson:"secondary_masters"`

	SecondaryCellsByTag map[string]*MasterCellConfig `yson:"-"`
}

type TimestampProviderConfig struct {
	PrimaryCell *MasterCellConfig `yson:"primary_master"`
	ClockCell   *MasterCellConfig `yson:"clock_cell"`
}

func ExtractCellTag(cellID yt.NodeID) string {
	c := binary.LittleEndian.Uint16(cellID[6:8])
	return fmt.Sprintf("%v", c)
}

// PrimaryMaster contains a state of a single master node.
type PrimaryMaster struct {
	Addr *Addr `yson:",value"`

	ID           yt.NodeID  `yson:"id,attr"`
	Path         ypath.Path `yson:"path,attr"`
	*Annotations `yson:"annotations,attr"`

	HydraState *HydraState `yson:"-"`

	CellID yt.NodeID `yson:"-"`

	InMaintenance          YTBool                `yson:"maintenance,attr"`
	MaintenanceMessage     string                `yson:"maintenance_message,attr"`
	MaintenanceRequests    MaintenanceRequestMap `yson:"maintenance_requests,attr"`
	CMSMaintenanceRequests MaintenanceRequestMap `yson:"cms_maintenance_requests,attr"`
}

func (m *PrimaryMaster) GetAddr() *Addr {
	return m.Addr
}

func (m *PrimaryMaster) GetCypressPath() ypath.Path {
	return m.Path
}

func (m *PrimaryMaster) GetRole() ClusterRole {
	return RolePrimaryMaster
}

func (m *PrimaryMaster) HasMaintenanceAttr() bool {
	return bool(m.InMaintenance)
}

// SecondaryMaster contains a state of a single secondary master node.
type SecondaryMaster struct {
	Addr *Addr `yson:",value"`

	ID           yt.NodeID  `yson:"id,attr"`
	Path         ypath.Path `yson:"path,attr"`
	*Annotations `yson:"annotations,attr"`

	HydraState *HydraState `yson:"-"`

	CellTag string    `yson:"-"`
	CellID  yt.NodeID `yson:"-"`

	InMaintenance          YTBool                `yson:"maintenance,attr"`
	MaintenanceMessage     string                `yson:"maintenance_message,attr"`
	MaintenanceRequests    MaintenanceRequestMap `yson:"maintenance_requests,attr"`
	CMSMaintenanceRequests MaintenanceRequestMap `yson:"cms_maintenance_requests,attr"`
}

func (m *SecondaryMaster) GetAddr() *Addr {
	return m.Addr
}

func (m *SecondaryMaster) GetCypressPath() ypath.Path {
	return m.Path
}

func (m *SecondaryMaster) GetRole() ClusterRole {
	return RoleSecondaryMaster
}

func (m *SecondaryMaster) HasMaintenanceAttr() bool {
	return bool(m.InMaintenance)
}

// TimestampProvider contains a state of a single timestamp provider.
type TimestampProvider struct {
	Addr *Addr `yson:",value"`

	ID           yt.NodeID  `yson:"id,attr"`
	Path         ypath.Path `yson:"path,attr"`
	*Annotations `yson:"annotations,attr"`

	HydraState *HydraState `yson:"-"`

	CellID yt.NodeID `yson:"-"`

	InMaintenance          YTBool                `yson:"maintenance,attr"`
	MaintenanceMessage     string                `yson:"maintenance_message,attr"`
	MaintenanceRequests    MaintenanceRequestMap `yson:"maintenance_requests,attr"`
	CMSMaintenanceRequests MaintenanceRequestMap `yson:"cms_maintenance_requests,attr"`
}

func (m *TimestampProvider) GetAddr() *Addr {
	return m.Addr
}

func (m *TimestampProvider) GetCypressPath() ypath.Path {
	return m.Path
}

func (m *TimestampProvider) GetRole() ClusterRole {
	return RoleTimestampProvider
}

func (m *TimestampProvider) HasMaintenanceAttr() bool {
	return bool(m.InMaintenance)
}

const (
	NodeStateOnline  = "online"
	NodeStateOffline = "offline"
)

// Node contains a state of a single node.
type Node struct {
	Addr *Addr `yson:",value"`

	Host         string    `yson:"host,attr"`
	ID           yt.NodeID `yson:"id,attr"`
	*Annotations `yson:"annotations,attr"`
	Rack         string `yson:"rack,attr"`

	Banned               YTBool    `yson:"banned,attr"`
	BanMessage           string    `yson:"ban_message,attr"`
	Decommissioned       YTBool    `yson:"decommissioned,attr"`
	DecommissionMessage  string    `yson:"decommission_message,attr"`
	DisableSchedulerJobs YTBool    `yson:"disable_scheduler_jobs,attr"`
	DisableWriteSessions YTBool    `yson:"disable_write_sessions,attr"`
	DisableTabletCells   YTBool    `yson:"disable_tablet_cells,attr"`
	State                string    `yson:"state,attr"`
	LastSeenTime         yson.Time `yson:"last_seen_time,attr"`

	Version string `yson:"version,attr"`

	Statistics *NodeStatistics `yson:"statistics,attr"`

	Tags    []string `yson:"tags,attr"`
	Flavors []string `yson:"flavors,attr"`

	TabletSlots []*TabletSlot `yson:"tablet_slots,attr"`

	ResourceLimits          *NodeResourceLimits `yson:"resource_limits,attr"`
	ResourceLimitsOverrides *NodeResourceLimits `yson:"resource_limits_overrides,attr"`
	ResourceUsage           *NodeResourceUsage  `yson:"resource_usage,attr"`

	Alerts []yterrors.Error `yson:"alerts,attr"`

	InMaintenance          YTBool                      `yson:"maintenance,attr"`
	MaintenanceMessage     string                      `yson:"maintenance_message,attr"`
	MaintenanceRequests    SystemMaintenanceRequestMap `yson:"maintenance_requests,attr"`
	CMSMaintenanceRequests MaintenanceRequestMap       `yson:"cms_maintenance_requests,attr"`
}

func (n *Node) UnmarshalYSON(data []byte) error {
	type plain Node
	if err := yson.Unmarshal(data, (*plain)(n)); err != nil {
		return err
	}

	if nodeAnnotationsAttr == defaultNodeAnnotationsAttr {
		return nil
	}

	aux := struct {
		Addr *Addr `yson:",value"`

		*Annotations `yson:"integration_test_node_annotations,attr"`
	}{}

	if err := yson.Unmarshal(data, &aux); err != nil {
		return err
	}

	n.Annotations = aux.Annotations
	return nil
}

func (n *Node) GetAddr() *Addr {
	return n.Addr
}

func (n *Node) GetCypressPath() ypath.Path {
	return ClusterNodesPath.Child(n.Addr.String())
}

func (n *Node) GetRole() ClusterRole {
	return RoleNode
}

func (n *Node) HasMaintenanceAttr() bool {
	return bool(n.InMaintenance)
}

// HasTag check that node has given tag.
func (n *Node) HasTag(t string) bool {
	for _, tag := range n.Tags {
		if tag == t {
			return true
		}
	}
	return false
}

// HasAnyTag check that node has at least one of given tags.
func (n *Node) HasAnyTag(tags []string) bool {
	for _, tag := range tags {
		if n.HasTag(tag) {
			return true
		}
	}
	return false
}

// HasFlavor check that node has given flavor.
func (n *Node) HasFlavor(f string) bool {
	for _, flavor := range n.Flavors {
		if flavor == f {
			return true
		}
	}
	return false
}

// AttachedToBundle checks if node has tablet_common/<BUNDLE> tag.
func (n *Node) AttachedToBundle() bool {
	for _, tag := range n.Tags {
		if tag != TabletCommonTag && strings.HasPrefix(tag, TabletCommonTag+"/") {
			return true
		}
	}
	return false
}

func (n *Node) GetFreeSlots() int {
	cnt := 0
	for _, s := range n.TabletSlots {
		if s.State == TabletSlotStateNone {
			cnt++
		}
	}
	return cnt
}

type NodeMap map[Addr]*Node

// GetFreeTabletCommonNodeCount returns number of alive free nodes with 'tablet_common' tag.
func (m NodeMap) GetFreeTabletCommonNodeCount() int {
	tabletCommonNodeCount := 0
	for _, n := range m {
		nodeAlive := !n.Banned && n.State != NodeStateOffline && !n.Decommissioned && !n.DisableTabletCells
		if bool(nodeAlive) && n.HasTag(TabletCommonTag) && !n.AttachedToBundle() {
			tabletCommonNodeCount++
		}
	}
	return tabletCommonNodeCount
}

// SetSlotState iterates over nodes and sets node availability flags for each slot.
func (m NodeMap) SetSlotState() {
	for _, n := range m {
		nodeUnavailable := n.Banned || n.State == NodeStateOffline || n.Decommissioned || n.DisableTabletCells
		for _, s := range n.TabletSlots {
			s.nodeUnavailable = bool(nodeUnavailable)
		}
	}
}

// HTTPProxy contains a state of a single http proxy.
type HTTPProxy struct {
	Addr *Addr `yson:",value"`

	ID           yt.NodeID  `yson:"id,attr"`
	Path         ypath.Path `yson:"path,attr"`
	*Annotations `yson:"annotations,attr"`

	Role       YTProxyRole `yson:"role,attr"`
	Banned     YTBool      `yson:"banned,attr"`
	BanMessage string      `yson:"ban_message,attr"`

	Liveness *HTTPProxyLiveness `yson:"liveness,attr"`
}

func (p *HTTPProxy) GetAddr() *Addr {
	return p.Addr
}

func (p *HTTPProxy) GetCypressPath() ypath.Path {
	return p.Path
}

func (p *HTTPProxy) GetRole() ClusterRole {
	return RoleHTTPProxy
}

func (p *HTTPProxy) ProxyRole() YTProxyRole {
	return p.Role
}

func (p *HTTPProxy) IsBanned() bool {
	return bool(p.Banned)
}

// RPCProxy contains a state of a single rpc proxy.
type RPCProxy struct {
	Addr *Addr `yson:"-"`

	ID           yt.NodeID  `yson:"id,attr"`
	Path         ypath.Path `yson:"path,attr"`
	*Annotations `yson:"annotations,attr"`

	Role       YTProxyRole `yson:"role,attr"`
	Banned     YTBool      `yson:"banned,attr"`
	BanMessage string      `yson:"ban_message,attr"`

	InMaintenance          YTBool                `yson:"maintenance,attr"`
	MaintenanceMessage     string                `yson:"maintenance_message,attr"`
	MaintenanceRequests    MaintenanceRequestMap `yson:"maintenance_requests,attr"`
	CMSMaintenanceRequests MaintenanceRequestMap `yson:"cms_maintenance_requests,attr"`

	Alive *map[string]interface{} `yson:"alive"`
}

func (p *RPCProxy) GetAddr() *Addr {
	return p.Addr
}

func (p *RPCProxy) GetCypressPath() ypath.Path {
	return p.Path
}

func (p *RPCProxy) GetRole() ClusterRole {
	return RoleRPCProxy
}

func (p *RPCProxy) ProxyRole() YTProxyRole {
	return p.Role
}

func (p *RPCProxy) IsBanned() bool {
	return bool(p.Banned)
}

func (p *RPCProxy) HasMaintenanceAttr() bool {
	return bool(p.InMaintenance)
}

// Scheduler contains a state of a single scheduler.
type Scheduler struct {
	Addr *Addr `yson:",value"`

	ID           yt.NodeID  `yson:"id,attr"`
	Path         ypath.Path `yson:"path,attr"`
	*Annotations `yson:"annotations,attr"`

	InMaintenance          YTBool                `yson:"maintenance,attr"`
	MaintenanceMessage     string                `yson:"maintenance_message,attr"`
	MaintenanceRequests    MaintenanceRequestMap `yson:"maintenance_requests,attr"`
	CMSMaintenanceRequests MaintenanceRequestMap `yson:"cms_maintenance_requests,attr"`

	// Connected stores the value of the corresponding orchid query.
	Connected bool `yson:"-"`
	// ConnectionRequestError stores an error of the orchid query
	// that checks if scheduler is connected.
	ConnectionRequestError error `yson:"-"`
}

func (s *Scheduler) GetAddr() *Addr {
	return s.Addr
}

func (s *Scheduler) GetCypressPath() ypath.Path {
	return s.Path
}

func (s *Scheduler) GetRole() ClusterRole {
	return RoleScheduler
}

// ControllerAgent contains a state of a single controller agent.
type ControllerAgent struct {
	Addr *Addr `yson:",value"`

	ID           yt.NodeID  `yson:"id,attr"`
	Path         ypath.Path `yson:"path,attr"`
	*Annotations `yson:"annotations,attr"`

	InMaintenance          YTBool                `yson:"maintenance,attr"`
	MaintenanceMessage     string                `yson:"maintenance_message,attr"`
	MaintenanceRequests    MaintenanceRequestMap `yson:"maintenance_requests,attr"`
	CMSMaintenanceRequests MaintenanceRequestMap `yson:"cms_maintenance_requests,attr"`

	// Connected stores the value of the corresponding orchid query.
	Connected bool `yson:"-"`
	// ConnectionRequestError stores an error of the orchid query
	// that checks if controller agent is connected.
	ConnectionRequestError error `yson:"-"`
}

func (a *ControllerAgent) GetAddr() *Addr {
	return a.Addr
}

func (a *ControllerAgent) GetCypressPath() ypath.Path {
	return a.Path
}

func (a *ControllerAgent) GetRole() ClusterRole {
	return RoleControllerAgent
}

type Annotations struct {
	PhysicalHost PhysicalHost `yson:"physical_host"`
}

func (a *Annotations) GetPhysicalHost() PhysicalHost {
	return a.PhysicalHost
}

// PeerState is a string that holds all available hydra states.
type PeerState string

const (
	PeerStateLeading   PeerState = "leading"
	PeerStateFollowing PeerState = "following"
)

type HydraState struct {
	Active       YTBool    `yson:"active"`
	ReadOnly     YTBool    `yson:"read_only"`
	State        PeerState `yson:"state"`
	ActiveLeader YTBool    `yson:"active_leader"`
}

// NodeStatistics stores data of the node's statistics attribute.
type NodeStatistics struct {
	TotalStoredChunkCount int64 `yson:"total_stored_chunk_count"`
	TotalSessionCount     int64 `yson:"total_session_count"`
}

type HTTPProxyLiveness struct {
	UpdatedAt yson.Time `yson:"updated_at"`
}

// YTBool is a bool alias that can be deserialized from yson "true" and "false" strings.
type YTBool bool

func (b *YTBool) UnmarshalYSON(data []byte) error {
	var banned bool
	if err := yson.Unmarshal(data, &banned); err == nil {
		*b = YTBool(banned)
		return nil
	}

	var s string
	if err := yson.Unmarshal(data, &s); err != nil {
		return xerrors.Errorf("cannot unmarshal %s into Banned flag: %w", data, err)
	}

	switch s {
	case "true":
		*b = true
	case "false":
		*b = false
	default:
		return xerrors.Errorf("unexpected bool literal %q", s)
	}

	return nil
}

// YTFloat64 is a float64 alias that can be deserialized from # as 0.
type YTFloat64 float64

func (f *YTFloat64) UnmarshalYSON(data []byte) error {
	var f64 float64
	err := yson.Unmarshal(data, &f64)
	if err == nil {
		*f = YTFloat64(f64)
		return nil
	}

	if string(data) == "#" {
		*f = 0
		return nil
	}

	return err
}

type NodeResourceLimits struct {
	CPU          float64 `yson:"cpu"`
	GPU          float64 `yson:"gpu"`
	UserSlots    int     `yson:"user_slots"`
	RemovalSlots int     `yson:"removal_slots"`
}

type NodeResourceUsage struct {
	UserSlots int64 `yson:"user_slots"`
}

type PoolTree struct {
	Name  string                   `yson:"-"`
	Nodes map[string]*PoolTreeNode `yson:"-"`

	AvailableResources *PoolTreeAvailableResources `yson:"-"`
	ResourceReserves   *PoolTreeResourceReserves   `yson:"-"`
}

func NewPoolTree(name string) *PoolTree {
	return &PoolTree{
		Name:  name,
		Nodes: make(map[string]*PoolTreeNode),
	}
}

func (t *PoolTree) SumNodeCPUResources() float64 {
	var s float64
	for _, n := range t.Nodes {
		if n.Resources != nil {
			s += float64(n.Resources.CPU)
		}
	}
	return s
}

func (t *PoolTree) SumNodeGPUResources() float64 {
	var s float64
	for _, n := range t.Nodes {
		if n.Resources != nil {
			s += float64(n.Resources.GPU)
		}
	}
	return s
}

// IsGPUPoolTree checks if pool tree's main resource is GPU
// by searching for "gpu_" prefix.
func IsGPUPoolTree(t *PoolTree) bool {
	return strings.HasPrefix(t.Name, "gpu_")
}

type PoolTreeNode struct {
	Name      string                  `yson:",value"`
	Resources *PoolTreeResourceLimits `yson:"min_share_resources,attr"`
}

type PoolTreeResourceLimits struct {
	CPU YTFloat64 `yson:"cpu"`
	GPU YTFloat64 `yson:"gpu"`
}

type PoolTreeAvailableResources struct {
	CPU float64 `yson:"cpu"`
	GPU float64 `yson:"gpu"`
}

type PoolTreeResourceReserves struct {
	CPU float64 `yson:"cpu"`
	GPU float64 `yson:"gpu"`
}

type StrongGuaranteeResources struct {
	CPU    float64 `yson:"cpu"`
	Memory int     `yson:"memory"`
}

type ReservePoolCMSLimits struct {
	CPU    float64 `yson:"cpu"`
	Memory int     `yson:"memory"`
}

// TabletSlotState is a string to hold tablet slot state.
type TabletSlotState string

const (
	TabletSlotStateNone    TabletSlotState = "none"
	TabletSlotStateLeading TabletSlotState = "leading"
)

type TabletSlot struct {
	State  TabletSlotState `yson:"state"`
	CellID yt.NodeID       `yson:"cell_id"`
	// nodeUnavailable tells whether node is actually available for cell hosting.
	nodeUnavailable bool `yson:"-"`
}

type TabletCell struct {
	ID     yt.NodeID `yson:",value"`
	Bundle string    `yson:"tablet_cell_bundle,attr"`
}

type TabletCellBundle struct {
	Name string `yson:",value"`
	// BalancerEnabled stores a value of the corresponding user attribute.
	// Missing attribute defaults to %true.
	BalancerEnabled *YTBool `yson:"enable_bundle_balancer,attr"`
	Nodes           []*Addr `yson:"nodes,attr"`
	// Production is the old CMS compatibility flag.
	// When set to %false CMS can ignore all bundle constraints.
	// Nil value defaults to %true.
	Production *YTBool `yson:"production,attr"`

	Slots map[Addr][]*TabletSlot `yson:"-"`
}

func (b *TabletCellBundle) GetFreeSlots() int {
	cnt := 0
	for _, slots := range b.Slots {
		for _, s := range slots {
			if s.State == TabletSlotStateNone && !s.nodeUnavailable {
				cnt++
			}
		}
	}
	return cnt
}

func (b *TabletCellBundle) GetNodeSlots(addr *Addr) int {
	return len(b.Slots[*addr])
}

func (b *TabletCellBundle) GetNodeSlotStats(addr *Addr) map[TabletSlotState]int {
	counts := make(map[TabletSlotState]int)
	for _, s := range b.Slots[*addr] {
		if s.State == TabletSlotStateNone && !s.nodeUnavailable {
			counts[s.State]++
		}
	}
	return counts
}

type Chunk struct {
	ID        yt.NodeID `yson:"id,attr"`
	Confirmed bool      `yson:"confirmed,attr"`
}

type MaintenanceRequestMap map[string]*MaintenanceRequest

type MaintenanceRequest struct {
	ID           string      `yson:"id"`
	CreationTime yson.Time   `yson:"creation_time"`
	Issuer       string      `yson:"issuer"`
	Comment      string      `yson:"comment"`
	Extra        interface{} `yson:"extra"`
}

func NewMaintenanceRequest(id, issuer, comment string, extra interface{}) *MaintenanceRequest {
	return &MaintenanceRequest{
		ID:           id,
		CreationTime: yson.Time(time.Now()),
		Issuer:       issuer,
		Comment:      comment,
		Extra:        extra,
	}
}

func (r *MaintenanceRequest) GetID() string {
	if r == nil {
		return ""
	}
	return r.ID
}

type SystemMaintenanceRequestMap map[string]*SystemMaintenanceRequest

type SystemMaintenanceRequest struct {
	User      string             `yson:"user"`
	Comment   string             `yson:"comment"`
	Timestamp yson.Time          `yson:"timeout"`
	Type      yt.MaintenanceType `yson:"type"`
}

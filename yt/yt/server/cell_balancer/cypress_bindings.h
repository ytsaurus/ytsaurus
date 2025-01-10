#pragma once

#include "private.h"

#include <yt/yt/client/bundle_controller_client/bundle_controller_settings.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TSysConfig)
DECLARE_REFCOUNTED_STRUCT(TBundleInfo)
DECLARE_REFCOUNTED_STRUCT(TBundleArea)
DECLARE_REFCOUNTED_STRUCT(TChaosBundleInfo)
DECLARE_REFCOUNTED_STRUCT(TResourceQuota)
DECLARE_REFCOUNTED_STRUCT(TResourceLimits)
DECLARE_REFCOUNTED_STRUCT(THulkInstanceResources)
DECLARE_REFCOUNTED_STRUCT(TBundleConfig)
DECLARE_REFCOUNTED_STRUCT(TBundleSystemOptions)
DECLARE_REFCOUNTED_STRUCT(TBundleControllerState)
DECLARE_REFCOUNTED_STRUCT(TZoneInfo)
DECLARE_REFCOUNTED_STRUCT(TAllocationRequestSpec)
DECLARE_REFCOUNTED_STRUCT(TAllocationRequestStatus)
DECLARE_REFCOUNTED_STRUCT(TAllocationRequest)
DECLARE_REFCOUNTED_STRUCT(TDeallocationRequestSpec)
DECLARE_REFCOUNTED_STRUCT(TDeallocationRequestStatus)
DECLARE_REFCOUNTED_STRUCT(TDeallocationRequest)
DECLARE_REFCOUNTED_STRUCT(TDeallocationRequestState)
DECLARE_REFCOUNTED_STRUCT(TInstanceAnnotations)
DECLARE_REFCOUNTED_STRUCT(TTabletNodeInfo)
DECLARE_REFCOUNTED_STRUCT(TTabletNodeMemoryStatistics)
DECLARE_REFCOUNTED_STRUCT(TMemoryCategory)
DECLARE_REFCOUNTED_STRUCT(TTabletNodeStatistics)
DECLARE_REFCOUNTED_STRUCT(TTabletCellStatus)
DECLARE_REFCOUNTED_STRUCT(TTabletCellInfo)
DECLARE_REFCOUNTED_STRUCT(TTabletCellPeer)
DECLARE_REFCOUNTED_STRUCT(TTabletSlot)
DECLARE_REFCOUNTED_STRUCT(TBundleDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TRpcProxyAlive)
DECLARE_REFCOUNTED_STRUCT(TCmsMaintenanceRequest)
DECLARE_REFCOUNTED_STRUCT(TRpcProxyInfo)
DECLARE_REFCOUNTED_STRUCT(TAccountResources)
DECLARE_REFCOUNTED_STRUCT(TSystemAccount)
DECLARE_REFCOUNTED_STRUCT(TNodeTagFilterOperationState)
DECLARE_REFCOUNTED_STRUCT(TDataCenterInfo)
DECLARE_REFCOUNTED_STRUCT(TMediumThroughputLimits)
DECLARE_REFCOUNTED_STRUCT(TAbcInfo)
DECLARE_REFCOUNTED_STRUCT(TCellTagInfo)
DECLARE_REFCOUNTED_STRUCT(TGlobalCellRegistry)
DECLARE_REFCOUNTED_STRUCT(TDrillsModeOperationState)
DECLARE_REFCOUNTED_STRUCT(TDrillsModeState)

template <typename TEntryInfo>
using TIndexedEntries = THashMap<std::string, TIntrusivePtr<TEntryInfo>>;
using TChaosCellId = NObjectClient::TObjectId;

constexpr int YTRoleTypeTabNode = 1;
constexpr int YTRoleTypeRpcProxy = 3;

inline static const std::string InstanceStateOnline = "online";
inline static const std::string InstanceStateOffline = "offline";

inline static const std::string TabletSlotStateEmpty = "none";

inline static const std::string PeerStateLeading = "leading";

inline static const std::string DeallocationStrategyHulkRequest = "hulk_deallocation_request";
inline static const std::string DeallocationStrategyReturnToBB = "return_to_bundle_balancer";

inline static const std::string TrashRole = "trash-role";

////////////////////////////////////////////////////////////////////////////////

struct TSysConfig
    : public NYTree::TYsonStruct
{
    bool DisableBundleController;

    REGISTER_YSON_STRUCT(TSysConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSysConfig)

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
class TYsonStructAttributes
    : public NYTree::TYsonStruct
{
public:
    static const std::vector<std::string>& GetAttributes()
    {
        // Making sure attributes are registered.
        // YSON struct registration takes place in constructor.
        static auto holder = New<TDerived>();
        return holder->Attributes_;
    }

    template <typename TRegistrar, typename TValue>
    static auto& RegisterAttribute(TRegistrar registrar, const std::string& attribute, TValue(TDerived::*field))
    {
        Attributes_.push_back(attribute);
        // TODO(babenko): switch to std::string
        return registrar.Parameter(TString(attribute), field);
    }

private:
    inline static std::vector<std::string> Attributes_;
};

////////////////////////////////////////////////////////////////////////////////

struct TResourceQuota
    : public NYTree::TYsonStruct
{
    double Cpu;
    i64 Memory;
    // Bytes per second.
    i64 Network;

    int Vcpu() const;

    REGISTER_YSON_STRUCT(TResourceQuota);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TResourceQuota)

////////////////////////////////////////////////////////////////////////////////

struct TResourceLimits
    : public NYTree::TYsonStruct
{
    i64 TabletStaticMemory;

    REGISTER_YSON_STRUCT(TResourceLimits);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TResourceLimits)

////////////////////////////////////////////////////////////////////////////////

struct THulkInstanceResources
    : public NYTree::TYsonStruct
{
    int Vcpu;
    i64 MemoryMb;
    // Bytes per second.
    std::optional<i64> NetworkBandwidth;

    THulkInstanceResources& operator=(const NBundleControllerClient::TInstanceResources& resources);

    REGISTER_YSON_STRUCT(THulkInstanceResources);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THulkInstanceResources)

////////////////////////////////////////////////////////////////////////////////

void ConvertToInstanceResources(NBundleControllerClient::TInstanceResources& resources, const THulkInstanceResources& hulkResources);

////////////////////////////////////////////////////////////////////////////////

struct TBundleConfig
    : public NYTree::TYsonStruct
{
    int TabletNodeCount;
    int RpcProxyCount;
    NBundleControllerClient::TInstanceResourcesPtr TabletNodeResourceGuarantee;
    NBundleControllerClient::TInstanceResourcesPtr RpcProxyResourceGuarantee;
    NBundleControllerClient::TCpuLimitsPtr CpuLimits;
    NBundleControllerClient::TMemoryLimitsPtr MemoryLimits;
    THashMap<std::string, TMediumThroughputLimitsPtr> MediumThroughputLimits;
    bool InitChaosBundles;
    int AdditionalChaosCellCount;
    bool EnableDrillsMode;

    THashSet<std::string> ForbiddenDataCenters;

    REGISTER_YSON_STRUCT(TBundleConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellStatus
    : public NYTree::TYsonStruct
{
    std::string Health;
    bool Decommissioned;

    REGISTER_YSON_STRUCT(TTabletCellStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletCellStatus)

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellPeer
    : public NYTree::TYsonStruct
{
    std::string Address;
    std::string State;
    std::string LastSeenState;
    TInstant LastSeenTime;

    REGISTER_YSON_STRUCT(TTabletCellPeer);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletCellPeer)

////////////////////////////////////////////////////////////////////////////////

struct TAbcInfo
    : public NYTree::TYsonStruct
{
    std::optional<int> Id;
    std::optional<std::string> Name;
    std::optional<std::string> Slug;

    REGISTER_YSON_STRUCT(TAbcInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAbcInfo)

////////////////////////////////////////////////////////////////////////////////

struct TCellTagInfo
    : public NYTree::TYsonStruct
{
    std::string Area;
    std::string CellBundle;
    TChaosCellId CellId;

    REGISTER_YSON_STRUCT(TCellTagInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellTagInfo)

////////////////////////////////////////////////////////////////////////////////

struct TGlobalCellRegistry
    : public NYTree::TYsonStruct
{
    ui16 CellTagRangeBegin;
    ui16 CellTagRangeEnd;
    ui16 CellTagLast;

    THashMap<NObjectClient::TCellTag, TCellTagInfoPtr> CellTags;
    THashMap<NObjectClient::TCellTag, TCellTagInfoPtr> AdditionalCellTags;

    REGISTER_YSON_STRUCT(TGlobalCellRegistry);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGlobalCellRegistry)

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellInfo
    : public TYsonStructAttributes<TTabletCellInfo>
{
    std::vector<TTabletCellPeerPtr> Peers;

    REGISTER_YSON_STRUCT(TTabletCellInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletCellInfo)

////////////////////////////////////////////////////////////////////////////////

struct TBundleInfo
    : public TYsonStructAttributes<TBundleInfo>
{
    NTabletClient::ETabletCellHealth Health;
    std::string Zone;
    std::string NodeTagFilter;
    std::optional<std::string> ShortName;
    std::optional<std::string> RpcProxyRole;
    THashMap<std::string, TBundleAreaPtr> Areas;

    bool EnableBundleController;
    bool EnableInstanceAllocation;
    bool EnableTabletCellManagement;
    bool EnableNodeTagFilterManagement;
    bool EnableTabletNodeDynamicConfig;
    bool EnableRpcProxyManagement;
    bool EnableSystemAccountManagement;
    bool EnableResourceLimitsManagement;

    bool MuteTabletCellsCheck;
    bool MuteTabletCellSnapshotsCheck;

    TBundleConfigPtr TargetConfig;
    std::vector<std::string> TabletCellIds;

    TBundleSystemOptionsPtr Options;
    TResourceQuotaPtr ResourceQuota;
    TResourceLimitsPtr ResourceLimits;

    double SystemAccountQuotaMultiplier;

    std::string FolderId;
    TAbcInfoPtr Abc;
    bool BundleHotfix;

    REGISTER_YSON_STRUCT(TBundleInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleInfo)

////////////////////////////////////////////////////////////////////////////////

struct TBundleArea
    : public TYsonStructAttributes<TBundleArea>
{
    std::string Id;
    int CellCount;
    std::string NodeTagFilter;

    REGISTER_YSON_STRUCT(TBundleArea);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleArea)

////////////////////////////////////////////////////////////////////////////////

struct TChaosBundleInfo
    : public TYsonStructAttributes<TChaosBundleInfo>
{
    std::string Id;
    THashSet<TChaosCellId> ChaosCellIds;
    TBundleSystemOptionsPtr Options;
    THashMap<std::string, TBundleAreaPtr> Areas;
    THashSet<TChaosCellId> MetadataCellIds;

    REGISTER_YSON_STRUCT(TChaosBundleInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosBundleInfo)

////////////////////////////////////////////////////////////////////////////////

struct TDataCenterInfo
    : public NYTree::TYsonStruct
{
    std::string YPCluster;
    std::string TabletNodeNannyService;
    std::string RpcProxyNannyService;

    bool Forbidden;

    REGISTER_YSON_STRUCT(TDataCenterInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDataCenterInfo)

////////////////////////////////////////////////////////////////////////////////

struct TZoneInfo
    : public TYsonStructAttributes<TZoneInfo>
{
    std::string DefaultYPCluster;
    std::string DefaultTabletNodeNannyService;
    std::string DefaultRpcProxyNannyService;

    std::optional<std::string> ShortName;

    int MaxTabletNodeCount;
    int MaxRpcProxyCount;

    THashMap<std::string, NBundleControllerClient::TInstanceSizePtr> TabletNodeSizes;
    THashMap<std::string, NBundleControllerClient::TInstanceSizePtr> RpcProxySizes;

    TBundleConfigPtr SpareTargetConfig;
    std::string SpareBundleName;
    double DisruptedThresholdFactor;

    bool RequiresMinusOneRackGuarantee;
    int RedundantDataCenterCount;

    THashMap<std::string, TDataCenterInfoPtr> DataCenters;

    REGISTER_YSON_STRUCT(TZoneInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TZoneInfo)

////////////////////////////////////////////////////////////////////////////////

struct TAllocationRequestSpec
    : public NYTree::TYsonStruct
{
    std::string YPCluster;
    std::string NannyService;
    THulkInstanceResourcesPtr ResourceRequest;
    std::string PodIdTemplate;
    int InstanceRole;
    std::optional<std::string> HostTagFilter;

    REGISTER_YSON_STRUCT(TAllocationRequestSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAllocationRequestSpec)

////////////////////////////////////////////////////////////////////////////////

struct TAllocationRequestStatus
    : public NYTree::TYsonStruct
{
    std::string State;
    std::string NodeId;
    std::string PodId;

    REGISTER_YSON_STRUCT(TAllocationRequestStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAllocationRequestStatus)

////////////////////////////////////////////////////////////////////////////////

struct TAllocationRequest
    : public NYTree::TYsonStruct
{
    TAllocationRequestSpecPtr Spec;
    TAllocationRequestStatusPtr Status;

    REGISTER_YSON_STRUCT(TAllocationRequest);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAllocationRequest)

////////////////////////////////////////////////////////////////////////////////

struct TDeallocationRequestSpec
    : public NYTree::TYsonStruct
{
    std::string YPCluster;
    std::string PodId;
    int InstanceRole;

    REGISTER_YSON_STRUCT(TDeallocationRequestSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDeallocationRequestSpec)

////////////////////////////////////////////////////////////////////////////////

struct TDeallocationRequestStatus
    : public NYTree::TYsonStruct
{
    std::string State;

    REGISTER_YSON_STRUCT(TDeallocationRequestStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDeallocationRequestStatus)

////////////////////////////////////////////////////////////////////////////////

struct TDeallocationRequest
    : public NYTree::TYsonStruct
{
    TDeallocationRequestSpecPtr Spec;
    TDeallocationRequestStatusPtr Status;

    REGISTER_YSON_STRUCT(TDeallocationRequest);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDeallocationRequest)

////////////////////////////////////////////////////////////////////////////////

struct TAllocationRequestState
    : public NYTree::TYsonStruct
{
    TInstant CreationTime;
    std::string PodIdTemplate;
    std::optional<std::string> DataCenter;

    REGISTER_YSON_STRUCT(TAllocationRequestState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAllocationRequestState)

////////////////////////////////////////////////////////////////////////////////

struct TDeallocationRequestState
    : public NYTree::TYsonStruct
{
    TInstant CreationTime;
    std::string InstanceName;
    std::string Strategy;
    std::optional<std::string> DataCenter;

    bool HulkRequestCreated;

    REGISTER_YSON_STRUCT(TDeallocationRequestState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDeallocationRequestState)

////////////////////////////////////////////////////////////////////////////////

struct TRemovingTabletCellState
    : public NYTree::TYsonStruct
{
    TInstant RemovedTime;

    REGISTER_YSON_STRUCT(TRemovingTabletCellState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRemovingTabletCellState)

////////////////////////////////////////////////////////////////////////////////

struct TNodeTagFilterOperationState
    : public NYTree::TYsonStruct
{
    TInstant CreationTime;

    REGISTER_YSON_STRUCT(TNodeTagFilterOperationState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNodeTagFilterOperationState)

////////////////////////////////////////////////////////////////////////////////

struct TDrillsModeOperationState
    : public NYTree::TYsonStruct
{
    TInstant CreationTime;

    REGISTER_YSON_STRUCT(TDrillsModeOperationState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDrillsModeOperationState)

////////////////////////////////////////////////////////////////////////////////

struct TDrillsModeState
    : public NYTree::TYsonStruct
{
    TDrillsModeOperationStatePtr TurningOn;
    TDrillsModeOperationStatePtr TurningOff;

    REGISTER_YSON_STRUCT(TDrillsModeState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDrillsModeState)

////////////////////////////////////////////////////////////////////////////////

struct TBundleControllerState
    : public TYsonStructAttributes<TBundleControllerState>
{
    TIndexedEntries<TAllocationRequestState> NodeAllocations;
    TIndexedEntries<TDeallocationRequestState> NodeDeallocations;
    TIndexedEntries<TRemovingTabletCellState> RemovingCells;

    TIndexedEntries<TAllocationRequestState> ProxyAllocations;
    TIndexedEntries<TDeallocationRequestState> ProxyDeallocations;

    TIndexedEntries<TNodeTagFilterOperationState> BundleNodeAssignments;
    TIndexedEntries<TNodeTagFilterOperationState> SpareNodeAssignments;

    // Here "releasement" is used as the opposite of "assignment"
    TIndexedEntries<TNodeTagFilterOperationState> BundleNodeReleasements;
    TIndexedEntries<TNodeTagFilterOperationState> SpareNodeReleasements;

    TDrillsModeStatePtr DrillsMode;

    REGISTER_YSON_STRUCT(TBundleControllerState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleControllerState)

////////////////////////////////////////////////////////////////////////////////

struct TInstanceAnnotations
    : public NYTree::TYsonStruct
{
    std::string YPCluster;
    std::string NannyService;
    std::string AllocatedForBundle;
    bool Allocated;
    NBundleControllerClient::TInstanceResourcesPtr Resource;

    std::optional<TInstant> DeallocatedAt;
    std::string DeallocationStrategy;

    std::optional<std::string> DataCenter;

    REGISTER_YSON_STRUCT(TInstanceAnnotations);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInstanceAnnotations)

////////////////////////////////////////////////////////////////////////////////

struct TTabletSlot
    : public NYTree::TYsonStruct
{
    std::string TabletCellBundle;
    std::string CellId;
    int PeerId;
    std::string State;

    REGISTER_YSON_STRUCT(TTabletSlot);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletSlot)

////////////////////////////////////////////////////////////////////////////////

struct TCmsMaintenanceRequest
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TCmsMaintenanceRequest);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCmsMaintenanceRequest)

////////////////////////////////////////////////////////////////////////////////

struct TMemoryCategory
    : public NYTree::TYsonStruct
{
    i64 Used;
    i64 Limit;

    REGISTER_YSON_STRUCT(TMemoryCategory);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryCategory)

////////////////////////////////////////////////////////////////////////////////

struct TTabletNodeMemoryStatistics
    : public NYTree::TYsonStruct
{
    TMemoryCategoryPtr TabletDynamic;
    TMemoryCategoryPtr TabletStatic;

    REGISTER_YSON_STRUCT(TTabletNodeMemoryStatistics);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletNodeMemoryStatistics)

////////////////////////////////////////////////////////////////////////////////

struct TTabletNodeStatistics
    : public NYTree::TYsonStruct
{
    TTabletNodeMemoryStatisticsPtr Memory;

    REGISTER_YSON_STRUCT(TTabletNodeStatistics);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletNodeStatistics)

////////////////////////////////////////////////////////////////////////////////

struct TTabletNodeInfo
    : public TYsonStructAttributes<TTabletNodeInfo>
{
    bool Banned;
    bool Decommissioned;
    bool DisableTabletCells;
    std::optional<bool> EnableBundleBalancer;
    std::string Host;
    std::string State;
    THashSet<std::string> Tags;
    THashSet<std::string> UserTags;
    TInstanceAnnotationsPtr Annotations;
    std::vector<TTabletSlotPtr> TabletSlots;
    THashMap<std::string, TCmsMaintenanceRequestPtr> CmsMaintenanceRequests;
    TInstant LastSeenTime;
    TTabletNodeStatisticsPtr Statistics;
    std::string Rack;

    REGISTER_YSON_STRUCT(TTabletNodeInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletNodeInfo)

////////////////////////////////////////////////////////////////////////////////

struct TRpcProxyAlive
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TRpcProxyAlive);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyAlive)

////////////////////////////////////////////////////////////////////////////////

struct TRpcProxyInfo
    : public TYsonStructAttributes<TRpcProxyInfo>
{
    bool Banned;
    std::string Role;
    TInstanceAnnotationsPtr Annotations;
    THashMap<std::string, TCmsMaintenanceRequestPtr> CmsMaintenanceRequests;
    TInstant ModificationTime;

    TRpcProxyAlivePtr Alive;

    REGISTER_YSON_STRUCT(TRpcProxyInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyInfo)

////////////////////////////////////////////////////////////////////////////////

struct TMediumThroughputLimits
    : public NYTree::TYsonStruct
{
    i64 WriteByteRate;
    i64 ReadByteRate;

    REGISTER_YSON_STRUCT(TMediumThroughputLimits);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMediumThroughputLimits)

////////////////////////////////////////////////////////////////////////////////

struct TBundleDynamicConfig
    : public NYTree::TYsonStruct
{
    NBundleControllerClient::TCpuLimitsPtr CpuLimits;
    NBundleControllerClient::TMemoryLimitsPtr MemoryLimits;
    THashMap<std::string, TMediumThroughputLimitsPtr> MediumThroughputLimits;

    REGISTER_YSON_STRUCT(TBundleDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TAccountResources
    : public NYTree::TYsonStruct
{
    i64 ChunkCount;
    THashMap<std::string, i64> DiskSpacePerMedium;
    i64 NodeCount;

    REGISTER_YSON_STRUCT(TAccountResources);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAccountResources)

////////////////////////////////////////////////////////////////////////////////

struct TSystemAccount
    : public TYsonStructAttributes<TSystemAccount>
{
    TAccountResourcesPtr ResourceLimits;
    TAccountResourcesPtr ResourceUsage;

    REGISTER_YSON_STRUCT(TSystemAccount);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSystemAccount)

////////////////////////////////////////////////////////////////////////////////

struct TBundleSystemOptions
    : public NYTree::TYsonStruct
{
    std::string ChangelogAccount;
    std::string ChangelogPrimaryMedium;

    std::string SnapshotAccount;
    std::string SnapshotPrimaryMedium;

    int PeerCount;

    std::optional<NObjectClient::TCellTag> ClockClusterTag;

    REGISTER_YSON_STRUCT(TBundleSystemOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleSystemOptions)

////////////////////////////////////////////////////////////////////////////////

struct TAlert
{
    std::string Id;
    std::optional<std::string> BundleName;
    std::string Description;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer

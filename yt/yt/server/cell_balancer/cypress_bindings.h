#pragma once

#include "private.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TBundleInfo)
DECLARE_REFCOUNTED_STRUCT(THulkInstanceResources)
DECLARE_REFCOUNTED_STRUCT(TInstanceResources)
DECLARE_REFCOUNTED_STRUCT(TBundleConfig)
DECLARE_REFCOUNTED_STRUCT(TCpuCategoryLimits)
DECLARE_REFCOUNTED_STRUCT(TBundleControllerState)
DECLARE_REFCOUNTED_STRUCT(TZoneInfo)
DECLARE_REFCOUNTED_STRUCT(TAllocationRequestSpec)
DECLARE_REFCOUNTED_STRUCT(TAllocationRequestStatus)
DECLARE_REFCOUNTED_STRUCT(TAllocationRequest)
DECLARE_REFCOUNTED_STRUCT(TDeallocationRequestSpec)
DECLARE_REFCOUNTED_STRUCT(TDeallocationRequestStatus)
DECLARE_REFCOUNTED_STRUCT(TDeallocationRequest)
DECLARE_REFCOUNTED_STRUCT(TDeallocationRequestState)
DECLARE_REFCOUNTED_STRUCT(TTabletNodeAnnotationsInfo)
DECLARE_REFCOUNTED_STRUCT(TTabletNodeInfo)
DECLARE_REFCOUNTED_STRUCT(TTabletCellStatus)
DECLARE_REFCOUNTED_STRUCT(TTabletCellInfo)
DECLARE_REFCOUNTED_STRUCT(TTabletCellPeer)
DECLARE_REFCOUNTED_STRUCT(TTabletSlot)

template <typename TEntryInfo>
using TIndexedEntries = THashMap<TString, TIntrusivePtr<TEntryInfo>>;

constexpr int YTRoleTypeTabNode = 1;

inline static const TString InstanceStateOnline = "online";
inline static const TString InstanceStateOffline = "offline";

inline static const TString TabletSlotStateEmpty = "none";

inline static const TString PeerStateLeading = "leading";

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
class TYsonStructAttributes
    : public NYTree::TYsonStruct
{
public:
    static std::vector<TString> GetAttributes()
    {
        return Attributes_;
    }

    template <typename TRegistrar, typename TValue>
    static auto& RegisterAttribute(TRegistrar registrar, const TString& attribute, TValue(TDerived::*field))
    {
        Attributes_.push_back(attribute);
        return registrar.Parameter(attribute, field);
    }

private:
    inline static std::vector<TString> Attributes_;
};

////////////////////////////////////////////////////////////////////////////////

struct TCpuCategoryLimits
    : public NYTree::TYsonStruct
{
    int WriteThreadPoolSize;

    REGISTER_YSON_STRUCT(TCpuCategoryLimits);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCpuCategoryLimits)

////////////////////////////////////////////////////////////////////////////////

struct TInstanceResources
    : public NYTree::TYsonStruct
{
    int VCpu;
    i64 Memory;

    REGISTER_YSON_STRUCT(TInstanceResources);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInstanceResources)

////////////////////////////////////////////////////////////////////////////////

struct TBundleConfig
    : public NYTree::TYsonStruct
{
    int TabletNodeCount;
    TInstanceResourcesPtr TabletNodeResourceGuarantee;
    TCpuCategoryLimitsPtr CpuCategoryLimits;

    REGISTER_YSON_STRUCT(TBundleConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellStatus
    : public NYTree::TYsonStruct
{
    TString Health;
    bool Decommissioned;

    REGISTER_YSON_STRUCT(TTabletCellStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletCellStatus)

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellPeer
    : public NYTree::TYsonStruct
{
    TString Address;
    TString State;

    REGISTER_YSON_STRUCT(TTabletCellPeer);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletCellPeer)

////////////////////////////////////////////////////////////////////////////////

struct TTabletCellInfo
    : public TYsonStructAttributes<TTabletCellInfo>
{
    TString TabletCellBundle;
    TString TabletCellLifeStage;
    int TabletCount;
    TTabletCellStatusPtr Status;
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
    TString Zone;
    TString NodeTagFilter;

    bool EnableBundleController;
    bool EnableTabletCellManagement;
    bool EnableNodeTagFilterManagement;

    TBundleConfigPtr TargetConfig;
    TBundleConfigPtr ActualConfig;
    std::vector<TString> TabletCellIds;

    REGISTER_YSON_STRUCT(TBundleInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleInfo)

////////////////////////////////////////////////////////////////////////////////

struct TZoneInfo
    : public TYsonStructAttributes<TZoneInfo>
{
    TString YPCluster;
    TString NannyService;

    int MaxTabletNodeCount;

    TBundleConfigPtr SpareTargetConfig;
    double DisruptedThresholdFactor;

    REGISTER_YSON_STRUCT(TZoneInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TZoneInfo)

////////////////////////////////////////////////////////////////////////////////

struct THulkInstanceResources
    : public NYTree::TYsonStruct
{
    int VCpu;
    i64 MemoryMb;

    THulkInstanceResources& operator=(const TInstanceResources& resources);

    REGISTER_YSON_STRUCT(THulkInstanceResources);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THulkInstanceResources)

////////////////////////////////////////////////////////////////////////////////

struct TAllocationRequestSpec
    : public NYTree::TYsonStruct
{
    TString YPCluster;
    TString NannyService;
    THulkInstanceResourcesPtr ResourceRequest;
    TString PodIdTemplate;
    int InstanceRole;

    REGISTER_YSON_STRUCT(TAllocationRequestSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAllocationRequestSpec)

////////////////////////////////////////////////////////////////////////////////

struct TAllocationRequestStatus
    : public NYTree::TYsonStruct
{
    TString State;
    TString NodeId;
    TString PodId;

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
    TString YPCluster;
    TString PodId;
    int InstanceRole;

    REGISTER_YSON_STRUCT(TDeallocationRequestSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDeallocationRequestSpec)

////////////////////////////////////////////////////////////////////////////////

struct TDeallocationRequestStatus
    : public NYTree::TYsonStruct
{
    TString State;

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

    REGISTER_YSON_STRUCT(TAllocationRequestState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAllocationRequestState)

////////////////////////////////////////////////////////////////////////////////

struct TDeallocationRequestState
    : public NYTree::TYsonStruct
{
    TInstant CreationTime;
    TString NodeName;
    bool HulkRequestCreated;

    REGISTER_YSON_STRUCT(TDeallocationRequestState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDeallocationRequestState)

////////////////////////////////////////////////////////////////////////////////

struct TRemovingTabletCellInfo
    : public NYTree::TYsonStruct
{
    TInstant RemovedTime;

    REGISTER_YSON_STRUCT(TRemovingTabletCellInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRemovingTabletCellInfo)

////////////////////////////////////////////////////////////////////////////////

struct TBundleControllerState
    : public TYsonStructAttributes<TBundleControllerState>
{
    TIndexedEntries<TAllocationRequestState> Allocations;
    TIndexedEntries<TDeallocationRequestState> Deallocations;
    TIndexedEntries<TRemovingTabletCellInfo> RemovingCells;

    REGISTER_YSON_STRUCT(TBundleControllerState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleControllerState)

////////////////////////////////////////////////////////////////////////////////

struct TTabletNodeAnnotationsInfo
    : public TYsonStructAttributes<TTabletNodeAnnotationsInfo>
{
    TString YPCluster;
    TString NannyService;
    TString AllocatedForBundle;
    bool Allocated;

    REGISTER_YSON_STRUCT(TTabletNodeAnnotationsInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletNodeAnnotationsInfo)

////////////////////////////////////////////////////////////////////////////////

struct TTabletSlot
    : public NYTree::TYsonStruct
{
    TString TabletCellBundle;
    TString CellId;
    int PeerId;
    TString State;

    REGISTER_YSON_STRUCT(TTabletSlot);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletSlot)

////////////////////////////////////////////////////////////////////////////////

struct TTabletNodeInfo
    : public TYsonStructAttributes<TTabletNodeInfo>
{
    bool Banned;
    bool Decommissioned;
    bool DisableTabletCells;
    TString Host;
    TString State;
    THashSet<TString> Tags;
    THashSet<TString> UserTags;
    TTabletNodeAnnotationsInfoPtr Annotations;
    std::vector<TTabletSlotPtr> TabletSlots;

    REGISTER_YSON_STRUCT(TTabletNodeInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTabletNodeInfo)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer

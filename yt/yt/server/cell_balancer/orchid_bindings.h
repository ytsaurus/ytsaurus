#pragma once

#include "bundle_scheduler.h"

namespace NYT::NCellBalancer::NOrchid {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TInstanceInfo)
DECLARE_REFCOUNTED_STRUCT(TAlert)
DECLARE_REFCOUNTED_STRUCT(TAllocatingInstanceInfo)
DECLARE_REFCOUNTED_STRUCT(TBundleInfo)
DECLARE_REFCOUNTED_STRUCT(TDataCenterRacksInfo)
DECLARE_REFCOUNTED_STRUCT(TScanBundleCounter)

////////////////////////////////////////////////////////////////////////////////

struct TInstanceInfo
    : public NYTree::TYsonStruct
{
    NBundleControllerClient::TInstanceResourcesPtr Resource;

    std::string PodId;
    std::string YPCluster;

    std::optional<std::string> DataCenter;
    std::optional<bool> Removing;

    REGISTER_YSON_STRUCT(TInstanceInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInstanceInfo)

////////////////////////////////////////////////////////////////////////////////

struct TAlert
    : public NYTree::TYsonStruct
{
    std::string Id;
    std::string Description;

    REGISTER_YSON_STRUCT(TAlert);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAlert)

////////////////////////////////////////////////////////////////////////////////

struct TAllocatingInstanceInfo
    : public NYTree::TYsonStruct
{
    std::string HulkRequestState;
    std::string HulkRequestLink;
    TInstanceInfoPtr InstanceInfo;

    REGISTER_YSON_STRUCT(TAllocatingInstanceInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAllocatingInstanceInfo)

////////////////////////////////////////////////////////////////////////////////

struct TBundleInfo
    : public NYTree::TYsonStruct
{
    NBundleControllerClient::TInstanceResourcesPtr ResourceQuota;
    NBundleControllerClient::TInstanceResourcesPtr ResourceAllocated;
    NBundleControllerClient::TInstanceResourcesPtr ResourceAlive;
    NBundleControllerClient::TInstanceResourcesPtr ResourceTarget;

    THashMap<std::string, TInstanceInfoPtr> AllocatedTabletNodes;
    THashMap<std::string, TInstanceInfoPtr> AllocatedRpcProxies;

    THashMap<std::string, TAllocatingInstanceInfoPtr> AllocatingTabletNodes;
    THashMap<std::string, TAllocatingInstanceInfoPtr> AllocatingRpcProxies;

    THashMap<std::string, TInstanceInfoPtr> AssignedSpareTabletNodes;
    THashMap<std::string, TInstanceInfoPtr> AssignedSpareRpcProxies;

    int RemovingCellCount;
    int AllocatingTabletNodeCount;
    int DeallocatingTabletNodeCount;
    int AllocatingRpcProxyCount;
    int DeallocatingRpcProxyCount;

    std::vector<TAlertPtr> Alerts;

    REGISTER_YSON_STRUCT(TBundleInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleInfo)

using TBundlesInfo = THashMap<std::string, TBundleInfoPtr>;

////////////////////////////////////////////////////////////////////////////////

struct TDataCenterRacksInfo
    : public NYTree::TYsonStruct
{
    THashMap<std::string, int> RackToBundleNodes;
    THashMap<std::string, int> RackToSpareNodes;
    int RequiredSpareNodeCount;

    REGISTER_YSON_STRUCT(TDataCenterRacksInfo);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDataCenterRacksInfo)

using TDataCenterRackInfo = THashMap<std::string, TDataCenterRacksInfoPtr>;
using TZonesRacksInfo = THashMap<std::string, TDataCenterRackInfo>;

////////////////////////////////////////////////////////////////////////////////

struct TScanBundleCounter
    : public NYTree::TYsonStruct
{
    int Successful;
    int Failed;

    REGISTER_YSON_STRUCT(TScanBundleCounter);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TScanBundleCounter)

////////////////////////////////////////////////////////////////////////////////

TBundlesInfo GetBundlesInfo(const TSchedulerInputState& state, const TSchedulerMutations& mutations);

TZonesRacksInfo GetZonesRacksInfo(const TSchedulerInputState& state);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer::NOrchid

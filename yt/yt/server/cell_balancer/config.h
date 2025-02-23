#pragma once

#include "private.h"

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TCellBalancerConfig
    : public NYTree::TYsonStruct
{
    NTabletServer::TDynamicTabletManagerConfigPtr TabletManager;

    REGISTER_YSON_STRUCT(TCellBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCellBalancerMasterConnectorConfig
    : public NYTree::TYsonStruct
{
    TDuration ConnectRetryBackoffTime;

    REGISTER_YSON_STRUCT(TCellBalancerMasterConnectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChaosConfig
    : public NYTree::TYsonStruct
{
    std::vector<std::string> TabletCellClusters;
    std::vector<std::string> ChaosCellClusters;
    NObjectClient::TCellTag ClockClusterTag;

    std::string AlphaChaosCluster;
    std::string BetaChaosCluster;

    REGISTER_YSON_STRUCT(TChaosConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBundleControllerConfig
    : public NYTree::TYsonStruct
{
    std::string Cluster;
    TDuration BundleScanPeriod;
    TDuration BundleScanTransactionTimeout;
    // TODO(grachevkirill): Rename to AllocatorRequestTimeout
    TDuration HulkRequestTimeout;
    TDuration CellRemovalTimeout;
    TDuration NodeAssignmentTimeout;
    TDuration MuteTabletCellsCheckGracePeriod;

    NYPath::TYPath RootPath;

    bool HasInstanceAllocatorService;
    NYPath::TYPath HulkAllocationsPath;
    NYPath::TYPath HulkAllocationsHistoryPath;
    NYPath::TYPath HulkDeallocationsPath;
    NYPath::TYPath HulkDeallocationsHistoryPath;

    bool DecommissionReleasedNodes;

    int NodeCountPerCell;
    int ChunkCountPerCell;
    i64 JournalDiskSpacePerCell;
    i64 SnapshotDiskSpacePerCell;
    int MinNodeCount;
    int MinChunkCount;

    int ReallocateInstanceBudget;

    TDuration RemoveInstanceCypressNodeAfter;
    TDuration OfflineInstanceGracePeriod;

    bool EnableNetworkLimits;

    bool SkipJailedBundles;

    bool EnableChaosBundleManagement;
    TChaosConfigPtr ChaosConfig;

    REGISTER_YSON_STRUCT(TBundleControllerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleControllerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCellBalancerBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
    bool AbortOnUnrecognizedOptions;
    NCypressElection::TCypressElectionManagerConfigPtr ElectionManager;
    TCellBalancerMasterConnectorConfigPtr MasterConnector;
    NNodeTrackerClient::TNetworkAddressList Addresses;

    bool EnableCellBalancer;
    TCellBalancerConfigPtr CellBalancer;

    bool EnableBundleController;
    TBundleControllerConfigPtr BundleController;

    REGISTER_YSON_STRUCT(TCellBalancerBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCellBalancerProgramConfig
    : public TCellBalancerBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TCellBalancerProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerProgramConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer

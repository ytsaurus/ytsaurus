#pragma once

#include "private.h"

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/client/node_tracker_client/public.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerConfig
    : public NYTree::TYsonStruct
{
public:
    NTabletServer::TDynamicTabletManagerConfigPtr TabletManager;

    REGISTER_YSON_STRUCT(TCellBalancerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerMasterConnectorConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration ConnectRetryBackoffTime;

    REGISTER_YSON_STRUCT(TCellBalancerMasterConnectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellBalancerMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TChaosConfig
    : public NYTree::TYsonStruct
{
public:
    std::vector<TString> TabletCellClusters;
    std::vector<TString> ChaosCellClusters;
    NObjectClient::TCellTag ClockClusterTag;

    TString AlphaChaosCluster;
    TString BetaChaosCluster;

    REGISTER_YSON_STRUCT(TChaosConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosConfig)

////////////////////////////////////////////////////////////////////////////////

class TBundleControllerConfig
    : public NYTree::TYsonStruct
{
public:
    TString Cluster;
    TDuration BundleScanPeriod;
    TDuration BundleScanTransactionTimeout;
    TDuration HulkRequestTimeout;
    TDuration CellRemovalTimeout;
    TDuration NodeAssignmentTimeout;
    TDuration MuteTabletCellsCheckGracePeriod;

    NYPath::TYPath RootPath;

    NYPath::TYPath HulkAllocationsPath;
    NYPath::TYPath HulkAllocationsHistoryPath;
    NYPath::TYPath HulkDeallocationsPath;
    NYPath::TYPath HulkDeallocationsHistoryPath;

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

    bool EnableChaosBundleManagement;
    TChaosConfigPtr ChaosConfig;

    REGISTER_YSON_STRUCT(TBundleControllerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBundleControllerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellBalancerBootstrapConfig
    : public TNativeServerConfig
{
public:
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

} // namespace NYT::NCellBalancer

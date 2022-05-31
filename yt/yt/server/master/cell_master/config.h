#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra_common/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/master/chaos_server/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/tablet_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/journal_server/public.h>

#include <yt/yt/server/master/sequoia_server/public.h>

#include <yt/yt/server/master/scheduler_pool_server/public.h>

#include <yt/yt/server/lib/timestamp_server/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/program/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/bus/tcp/public.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TMasterHydraManagerConfig
    : public NHydra::TDistributedHydraManagerConfig
    , public NHydra::TLocalHydraJanitorConfig
{
public:
    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    REGISTER_YSON_STRUCT(TMasterHydraManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterHydraManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectionConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to masters.
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TMasterConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServersConfig
    : public NRpc::TBalancingChannelConfig
    , public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to discovery servers.
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TDiscoveryServersConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryServersConfig)

////////////////////////////////////////////////////////////////////////////////

class TMulticellManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Applies to follower-to-leader forwarding and cross-cell interactions.
    TMasterConnectionConfigPtr MasterConnection;

    //! Maximum time to wait before syncing with upstream cells.
    TDuration UpstreamSyncDelay;

    TMulticellManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TMulticellManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TWorldInitializerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration InitRetryPeriod;

    TDuration InitTransactionTimeout;

    TDuration UpdatePeriod;

    TWorldInitializerConfig();
};

DEFINE_REFCOUNTED_TYPE(TWorldInitializerConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicMulticellManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration CellStatisticsGossipPeriod;

    struct TMasterCellDescriptor
        : public NYTree::TYsonSerializable
    {
        TMasterCellDescriptor()
        {
            RegisterParameter("name", Name)
                .Optional();
            RegisterParameter("roles", Roles)
                .Optional();
        }

        std::optional<TString> Name;
        std::optional<EMasterCellRoles> Roles;
    };
    using TMasterCellDescriptorPtr = TIntrusivePtr<TMasterCellDescriptor>;

    THashMap<NObjectServer::TCellTag, TMasterCellDescriptorPtr> CellDescriptors;

    // COMPAT(aleksandra-zh)
    THashMap<NObjectServer::TCellTag, EMasterCellRoles> CellRoles;

    TDynamicMulticellManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicMulticellManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TCellMasterConfig
    : public TServerConfig
{
public:
    NNodeTrackerClient::TNetworkPreferenceList Networks;

    NElection::TCellConfigPtr PrimaryMaster;
    std::vector<NElection::TCellConfigPtr> SecondaryMasters;

    NElection::TDistributedElectionManagerConfigPtr ElectionManager;

    NHydra::TFileChangelogStoreConfigPtr Changelogs;
    NHydra::TLocalSnapshotStoreConfigPtr Snapshots;
    TMasterHydraManagerConfigPtr HydraManager;

    NHiveClient::TCellDirectoryConfigPtr CellDirectory;
    NHiveServer::TCellDirectorySynchronizerConfigPtr CellDirectorySynchronizer;
    NHiveServer::THiveManagerConfigPtr HiveManager;

    NNodeTrackerServer::TNodeTrackerConfigPtr NodeTracker;

    NChunkServer::TChunkManagerConfigPtr ChunkManager;

    NObjectServer::TObjectServiceConfigPtr ObjectService;

    NCypressServer::TCypressManagerConfigPtr CypressManager;

    NTabletServer::TReplicatedTableTrackerConfigPtr ReplicatedTableTracker;

    bool EnableTimestampManager;
    NTimestampServer::TTimestampManagerConfigPtr TimestampManager;

    TDiscoveryServersConfigPtr DiscoveryServer;

    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;

    NHiveServer::TTransactionSupervisorConfigPtr TransactionSupervisor;

    TMulticellManagerConfigPtr MulticellManager;

    TWorldInitializerConfigPtr WorldInitializer;

    NSecurityServer::TSecurityManagerConfigPtr SecurityManager;

    //! If |true| then |//sys/@provision_lock| is set during cluster initialization.
    bool EnableProvisionLock;

    NBus::TTcpBusConfigPtr BusClient;

    NYTree::IMapNodePtr CypressAnnotations;

    bool AbortOnUnrecognizedOptions;

    bool EnableNetworking;

    NApi::NNative::TConnectionConfigPtr ClusterConnection;

    bool UseNewHydra;

    REGISTER_YSON_STRUCT(TCellMasterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicCellMasterConfig
    : public TSingletonsDynamicConfig
{
public:
    TDuration MutationTimeCommitPeriod;

    TDuration AlertUpdatePeriod;

    THashMap<TString, double> AutomatonThreadBucketWeights;

    TDuration ExpectedMutationCommitDuration;

    REGISTER_YSON_STRUCT(TDynamicCellMasterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicClusterConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableSafeMode;
    bool EnableDescendingSortOrder;
    bool EnableDescendingSortOrderDynamic;
    bool EnableTableColumnRenaming;

    NChunkServer::TDynamicChunkManagerConfigPtr ChunkManager;
    NCellServer::TDynamicCellManagerConfigPtr CellManager;
    NTabletServer::TDynamicTabletManagerConfigPtr TabletManager;
    NChaosServer::TDynamicChaosManagerConfigPtr ChaosManager;
    NNodeTrackerServer::TDynamicNodeTrackerConfigPtr NodeTracker;
    NObjectServer::TDynamicObjectManagerConfigPtr ObjectManager;
    NSecurityServer::TDynamicSecurityManagerConfigPtr SecurityManager;
    NCypressServer::TDynamicCypressManagerConfigPtr CypressManager;
    TDynamicMulticellManagerConfigPtr MulticellManager;
    NTransactionServer::TDynamicTransactionManagerConfigPtr TransactionManager;
    TDynamicCellMasterConfigPtr CellMaster;
    NObjectServer::TDynamicObjectServiceConfigPtr ObjectService;
    NChunkServer::TDynamicChunkServiceConfigPtr ChunkService;
    NSchedulerPoolServer::TDynamicSchedulerPoolManagerConfigPtr SchedulerPoolManager;
    NSequoiaServer::TDynamicSequoiaManagerConfigPtr SequoiaManager;

    TDynamicClusterConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicClusterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/master/chaos_server/public.h>

#include <yt/yt/server/master/incumbent_server/public.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/object_server/public.h>
#include <yt/yt/server/master/object_server/config.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/tablet_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/master/journal_server/public.h>

#include <yt/yt/server/master/sequoia_server/public.h>

#include <yt/yt/server/master/scheduler_pool_server/public.h>

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra/config.h>

#include <yt/yt/server/lib/lease_server/public.h>

#include <yt/yt/server/lib/transaction_supervisor/config.h>

#include <yt/yt/server/lib/timestamp_server/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/library/program/public.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/bus/tcp/public.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

struct TMasterHydraManagerConfig
    : public NHydra::TDistributedHydraManagerConfig
    , public NHydra::TLocalHydraJanitorConfig
{
    NRpc::TResponseKeeperConfigPtr ResponseKeeper;
    int SnapshotBackgroundThreadCount;

    REGISTER_YSON_STRUCT(TMasterHydraManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterHydraManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMasterConnectionConfig
    : public NRpc::TRetryingChannelConfig
{
    //! Timeout for RPC requests to masters.
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TMasterConnectionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiscoveryServersConfig
    : public NRpc::TBalancingChannelConfig
    , public NRpc::TRetryingChannelConfig
{
    //! Timeout for RPC requests to discovery servers.
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TDiscoveryServersConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryServersConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMulticellManagerConfig
    : public NYTree::TYsonStruct
{
    //! Applies to follower-to-leader forwarding and cross-cell interactions.
    TMasterConnectionConfigPtr MasterConnection;

    //! Maximum time to wait before syncing with upstream cells.
    TDuration UpstreamSyncDelay;

    REGISTER_YSON_STRUCT(TMulticellManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMulticellManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TWorldInitializerConfig
    : public NYTree::TYsonStruct
{
    TDuration InitRetryPeriod;

    TDuration InitTransactionTimeout;

    TDuration UpdatePeriod;

    REGISTER_YSON_STRUCT(TWorldInitializerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWorldInitializerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMasterCellDescriptor
    : public NYTree::TYsonStruct
{
    std::optional<std::string> Name;
    std::optional<EMasterCellRoles> Roles;

    REGISTER_YSON_STRUCT(TMasterCellDescriptor);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TMasterCellDirectoryConfig
    : public NYTree::TYsonStruct
{
    std::vector<NApi::NNative::TMasterConnectionConfigPtr> SecondaryMasters;

    REGISTER_YSON_STRUCT(TMasterCellDirectoryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterCellDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTestConfig
    : public NYTree::TYsonStruct
{
    // NB: Temporary field to test dynamic propagation of master cells to nodes.
    TMasterCellDirectoryConfigPtr MasterCellDirectoryOverride;
    THashSet<NObjectClient::TCellTag> DiscoveredMastersCellTags;

    //! This can simulate connection instability.
    /*!
    *  Please keep in mind that this is still a part of dynamic config,
    *  which means it is being replicated using Hive. Thus, there are two anomalies:
    *  Let's call primary cell P and secondary cells S1 and S2.
    *  1. When taking an unfrozen configuration and attempting to freeze [P->S1] and [S1->S2]
    *  the latter won't be applied.
    *  2. Assuming [S1->S2] is already frozen, and a command to freeze [P->S1] is issued, the
    *  [S1->S2] connection will stay frozen, despite going unmentioned in the new frozen edge list.
    *  Both aforementioned quirks can be overcome. The latter, by expilicitly unfreezing everything, and
    *  the former by first freezing [S1->S2], waiting a bit and then freezing [P->S1];
    *  in essence by reproducing a quirk #2.
    */
    std::vector<std::vector<NObjectClient::TCellTag>> FrozenHiveEdges;

    bool AllowMasterCellRemoval;
    bool AllowMasterCellWithEmptyRole;

    REGISTER_YSON_STRUCT(TTestConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTestConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicMulticellManagerConfig
    : public NYTree::TYsonStruct
{
    TDuration CellStatisticsGossipPeriod;

    using TMasterCellDescriptorPtr = TIntrusivePtr<TMasterCellDescriptor>;

    THashMap<NObjectServer::TCellTag, TMasterCellDescriptorPtr> CellDescriptors;

    // COMPAT(aleksandra-zh)
    bool RemoveSecondaryCellDefaultRoles;
    bool AllowMasterCellRoleInvariantCheck;

    TDuration SyncHiveClocksPeriod;

    // NB: Section for testing purposes.
    TTestConfigPtr Testing;

    REGISTER_YSON_STRUCT(TDynamicMulticellManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicMulticellManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicResponseKeeperConfig
    : public NYTree::TYsonStruct
{
    TDuration ExpirationTimeout;

    int MaxResponseCountPerEvictionPass;

    TDuration EvictionPeriod;

    REGISTER_YSON_STRUCT(TDynamicResponseKeeperConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicResponseKeeperConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCellMasterBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
    //! Used to check that master is being initialized from a correct container.
    std::optional<std::string> ExpectedLocalHostName;

    NNodeTrackerClient::TNetworkPreferenceList Networks;

    NElection::TCellConfigPtr PrimaryMaster;
    std::vector<NElection::TCellConfigPtr> SecondaryMasters;

    NElection::TDistributedElectionManagerConfigPtr ElectionManager;

    NHydra::TFileChangelogStoreConfigPtr Changelogs;
    NHydra::TLocalSnapshotStoreConfigPtr Snapshots;
    TMasterHydraManagerConfigPtr HydraManager;

    NHydra::THydraDryRunConfigPtr DryRun;

    NHiveClient::TCellDirectoryConfigPtr CellDirectory;
    NHiveServer::TCellDirectorySynchronizerConfigPtr CellDirectorySynchronizer;
    NHiveServer::THiveManagerConfigPtr HiveManager;

    NNodeTrackerServer::TNodeTrackerConfigPtr NodeTracker;

    NChunkServer::TChunkManagerConfigPtr ChunkManager;

    NObjectServer::TObjectServiceConfigPtr ObjectService;

    NCellServer::TCellManagerConfigPtr CellManager;

    NTabletServer::TReplicatedTableTrackerConfigPtr ReplicatedTableTracker;
    bool EnableTimestampManager;
    NTimestampServer::TTimestampManagerConfigPtr TimestampManager;
    //! Clock server cell tag
    NObjectClient::TCellTag ClockClusterTag;

    TDiscoveryServersConfigPtr DiscoveryServer;

    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;

    NLeaseServer::TLeaseManagerConfigPtr LeaseManager;

    NTransactionSupervisor::TTransactionSupervisorConfigPtr TransactionSupervisor;

    TMulticellManagerConfigPtr MulticellManager;

    TWorldInitializerConfigPtr WorldInitializer;

    NSecurityServer::TSecurityManagerConfigPtr SecurityManager;

    NTableServer::TTableManagerConfigPtr TableManager;

    NTransactionSupervisor::TTransactionLeaseTrackerConfigPtr TransactionLeaseTracker;

    //! If |true| then |//sys/@provision_lock| is set during cluster initialization.
    bool EnableProvisionLock;

    NBus::TBusConfigPtr BusClient;

    NYTree::IMapNodePtr CypressAnnotations;

    bool AbortOnUnrecognizedOptions;

    bool ExposeTestingFacilities;

    bool DisableNodeConnections;

    REGISTER_YSON_STRUCT(TCellMasterBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellMasterBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCellMasterProgramConfig
    : public TCellMasterBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TCellMasterProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCellMasterProgramConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicCellMasterConfig
    : public TSingletonsDynamicConfig
{
    TDuration MutationTimeCommitPeriod;

    TDuration AlertUpdatePeriod;

    THashMap<TString, double> AutomatonThreadBucketWeights;

    TDuration ExpectedMutationCommitDuration;

    bool CreateLostVitalChunksSampleMap;

    TDynamicResponseKeeperConfigPtr ResponseKeeper;

    REGISTER_YSON_STRUCT(TDynamicCellMasterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicCellMasterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicQueueAgentServerConfig
    : public NYTree::TYsonStruct
{
    std::string DefaultQueueAgentStage;

    REGISTER_YSON_STRUCT(TDynamicQueueAgentServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicQueueAgentServerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicClusterConfig
    : public NYTree::TYsonStruct
{
    bool EnableSafeMode;
    bool EnableDescendingSortOrder;
    bool EnableDescendingSortOrderDynamic;
    bool EnableTableColumnRenaming;
    bool EnableDynamicTableColumnRenaming;
    bool EnableStaticTableDropColumn;
    bool EnableDynamicTableDropColumn;
    bool AllowEveryoneCreateSecondaryIndices;
    bool EnableSecondaryIndexCopy;
    bool AllowAlterKeyColumnToAny;

    NChunkServer::TDynamicChunkManagerConfigPtr ChunkManager;
    NCellServer::TDynamicCellManagerConfigPtr CellManager;
    NTableServer::TDynamicTableManagerConfigPtr TableManager;
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
    NSequoiaServer::TDynamicGroundUpdateQueueManagerConfigPtr GroundUpdateQueueManager;
    NSequoiaServer::TDynamicCypressProxyTrackerConfigPtr CypressProxyTracker;
    NIncumbentServer::TIncumbentManagerDynamicConfigPtr IncumbentManager;
    TDynamicQueueAgentServerConfigPtr QueueAgentServer;
    NHydra::TDynamicDistributedHydraManagerConfigPtr HydraManager;

    REGISTER_YSON_STRUCT(TDynamicClusterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicClusterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

#pragma once

#include "public.h"

#include <yt/server/chunk_server/config.h>

#include <yt/server/cypress_server/config.h>

#include <yt/server/hive/config.h>

#include <yt/server/hydra/config.h>

#include <yt/server/misc/config.h>

#include <yt/server/node_tracker_server/config.h>

#include <yt/server/object_server/config.h>

#include <yt/server/security_server/config.h>

#include <yt/server/tablet_server/config.h>

#include <yt/server/transaction_server/config.h>

#include <yt/server/journal_server/config.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/election/config.h>

#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/transaction_client/config.h>

#include <yt/core/bus/tcp/config.h>

#include <yt/core/rpc/config.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TMasterHydraManagerConfig
    : public NHydra::TDistributedHydraManagerConfig
{
public:
    std::optional<int> MaxSnapshotCountToKeep;
    std::optional<i64> MaxSnapshotSizeToKeep;

    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    TMasterHydraManagerConfig()
    {
        RegisterParameter("max_snapshot_count_to_keep", MaxSnapshotCountToKeep)
            .GreaterThanOrEqual(0)
            .Default(10);
        RegisterParameter("max_snapshot_size_to_keep", MaxSnapshotSizeToKeep)
            .GreaterThanOrEqual(0)
            .Default();

        RegisterParameter("response_keeper", ResponseKeeper)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TMasterHydraManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectionConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for RPC requests to masters.
    TDuration RpcTimeout;

    TMasterConnectionConfig()
    {
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(30));
    }
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TMulticellManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Applies to follower-to-leader forwarding and cross-cell interactions.
    TMasterConnectionConfigPtr MasterConnection;

    TDuration CellStatisticsGossipPeriod;

    TMulticellManagerConfig()
    {
        RegisterParameter("master_connection", MasterConnection)
            .DefaultNew();

        RegisterParameter("cell_statistics_gossip_period", CellStatisticsGossipPeriod)
            .Default(TDuration::Seconds(1));
    }
};

DEFINE_REFCOUNTED_TYPE(TMulticellManagerConfig)

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

    NTransactionServer::TTransactionManagerConfigPtr TransactionManager;

    NChunkServer::TChunkManagerConfigPtr ChunkManager;

    NJournalServer::TJournalManagerConfigPtr JournalManager;

    NObjectServer::TObjectManagerConfigPtr ObjectManager;

    NObjectServer::TObjectServiceConfigPtr ObjectService;

    NCypressServer::TCypressManagerConfigPtr CypressManager;

    NSecurityServer::TSecurityManagerConfigPtr SecurityManager;

    NTabletServer::TTabletManagerConfigPtr TabletManager;

    NTabletServer::TReplicatedTableTrackerConfigPtr ReplicatedTableTracker;

    NTransactionServer::TTimestampManagerConfigPtr TimestampManager;

    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;

    NHiveServer::TTransactionSupervisorConfigPtr TransactionSupervisor;

    TMulticellManagerConfigPtr MulticellManager;

    //! If |true| then |//sys/@provision_lock| is set during cluster initialization.
    bool EnableProvisionLock;

    NBus::TTcpBusConfigPtr BusClient;

    NYTree::IMapNodePtr CypressAnnotations;
    TCellMasterConfig();
};

DEFINE_REFCOUNTED_TYPE(TCellMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicClusterConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableSafeMode;
    NChunkServer::TDynamicChunkManagerConfigPtr ChunkManager;
    NTabletServer::TDynamicTabletManagerConfigPtr TabletManager;
    NNodeTrackerServer::TDynamicNodeTrackerConfigPtr NodeTracker;

    TDynamicClusterConfig()
    {
        RegisterParameter("enable_safe_mode", EnableSafeMode)
            .Default(false);
        RegisterParameter("chunk_manager", ChunkManager)
            .DefaultNew();
        RegisterParameter("tablet_manager", TabletManager)
            .DefaultNew();
        RegisterParameter("node_tracker", NodeTracker)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicClusterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/config.h>

#include <yt/yt/server/master/cypress_server/config.h>

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/server/lib/hydra/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/master/cell_server/config.h>

#include <yt/yt/server/master/node_tracker_server/config.h>

#include <yt/yt/server/master/object_server/config.h>

#include <yt/yt/server/master/security_server/config.h>

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/master/transaction_server/config.h>

#include <yt/yt/server/master/journal_server/config.h>

#include <yt/yt/server/master/scheduler_pool_server/config.h>

#include <yt/yt/server/lib/timestamp_server/config.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hive/config.h>

#include <yt/yt/ytlib/transaction_client/config.h>

#include <yt/yt/ytlib/program/config.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/rpc/config.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TMasterHydraManagerConfig
    : public NHydra::TDistributedHydraManagerConfig
    , public NHydra::TLocalHydraJanitorConfig
{
public:
    NRpc::TResponseKeeperConfigPtr ResponseKeeper;

    TMasterHydraManagerConfig()
    {
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

        RegisterPreprocessor([&] {
            RetryAttempts = 100;
            RetryTimeout = TDuration::Minutes(3);
        });
    }
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

    TDiscoveryServersConfig()
    {
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(30));
    }
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

    TMulticellManagerConfig()
    {
        RegisterParameter("master_connection", MasterConnection)
            .DefaultNew();
        RegisterParameter("upstream_sync_delay", UpstreamSyncDelay)
            .Default(TDuration::MilliSeconds(10));
    }
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

    TWorldInitializerConfig()
    {
        RegisterParameter("init_retry_period", InitRetryPeriod)
            .Default(TDuration::Seconds(3));
        RegisterParameter("init_transaction_timeout", InitTransactionTimeout)
            .Default(TDuration::Seconds(60));
        RegisterParameter("update_period", UpdatePeriod)
            .Default(TDuration::Minutes(5));
    }
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

    TDynamicMulticellManagerConfig()
    {
        RegisterParameter("cell_statistics_gossip_period", CellStatisticsGossipPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("cell_roles", CellRoles)
            .Default();

        RegisterParameter("cell_descriptors", CellDescriptors)
            .Default();

        RegisterPostprocessor([&] () {
            for (const auto& [cellTag, cellRoles] : CellRoles) {
                auto [it, inserted] = CellDescriptors.emplace(cellTag, New<TMasterCellDescriptor>());
                auto& roles = it->second->Roles;
                if (!roles) {
                    roles = cellRoles;
                } else {
                    roles = *roles | cellRoles;
                }
            }

            THashMap<TString, NObjectServer::TCellTag> nameToCellTag;
            for (auto& [cellTag, descriptor] : CellDescriptors) {
                if (descriptor->Roles && None(*descriptor->Roles)) {
                    THROW_ERROR_EXCEPTION("Cell %v has no roles",
                        cellTag);
                }

                if (!descriptor->Name) {
                    continue;
                }

                const auto& cellName = *descriptor->Name;

                NObjectClient::TCellTag cellTagCellName;
                if (TryFromString(cellName, cellTagCellName)) {
                    THROW_ERROR_EXCEPTION("Invalid cell name %Qv",
                        cellName);
                }

                auto [it, inserted] = nameToCellTag.emplace(cellName, cellTag);
                if (!inserted) {
                    THROW_ERROR_EXCEPTION("Duplicate cell name %Qv for cell tags %v and %v",
                        cellName,
                        cellTag,
                        it->second);
                }
            }
        });
    }
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

    TCellMasterConfig();
};

DEFINE_REFCOUNTED_TYPE(TCellMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicCellMasterConfig
    : public TSingletonsDynamicConfig
{
public:
    TDuration MutationTimeCommitPeriod;

    TDuration AlertUpdatePeriod;

    TDynamicCellMasterConfig()
    {
        RegisterParameter("mutation_time_commit_period", MutationTimeCommitPeriod)
            .Default(TDuration::Minutes(10));

        RegisterParameter("alert_update_period", AlertUpdatePeriod)
            .Default(TDuration::Seconds(30));
    }
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
    NChunkServer::TDynamicChunkManagerConfigPtr ChunkManager;
    NCellServer::TDynamicCellManagerConfigPtr CellManager;
    NTabletServer::TDynamicTabletManagerConfigPtr TabletManager;
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

    TDynamicClusterConfig();
};

DEFINE_REFCOUNTED_TYPE(TDynamicClusterConfig)

////////////////////////////////////////////////////////////////////////////////

class TSerializationDumperConfig
    : public NYTree::TYsonSerializable
{
public:
    i64 LowerLimit;
    i64 UpperLimit;

    TSerializationDumperConfig()
    {
        RegisterParameter("lower_limit", LowerLimit)
            .GreaterThanOrEqual(0)
            .Default(0);
        RegisterParameter("upper_limit", UpperLimit)
            .GreaterThanOrEqual(0)
            .Default(std::numeric_limits<i64>::max());

        RegisterPostprocessor([&] () {
            if (LowerLimit >= UpperLimit) {
                THROW_ERROR_EXCEPTION("'UpperLimit' must be greater than 'LowerLimit'")
                    << TErrorAttribute("actual_lower_limit", LowerLimit)
                    << TErrorAttribute("actual_upper_limit", UpperLimit);
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TSerializationDumperConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

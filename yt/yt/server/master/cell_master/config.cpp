#include "config.h"

#include <yt/yt/server/master/chunk_server/config.h>

#include <yt/yt/server/master/cypress_server/config.h>

#include <yt/yt/server/master/cell_server/config.h>

#include <yt/yt/server/master/chaos_server/config.h>

#include <yt/yt/server/master/node_tracker_server/config.h>

#include <yt/yt/server/master/object_server/config.h>

#include <yt/yt/server/master/security_server/config.h>

#include <yt/yt/server/master/tablet_server/config.h>

#include <yt/yt/server/master/transaction_server/config.h>

#include <yt/yt/server/master/journal_server/config.h>

#include <yt/yt/server/master/scheduler_pool_server/config.h>

#include <yt/yt/server/lib/hive/config.h>

#include <yt/yt/server/lib/election/config.h>

#include <yt/yt/server/lib/timestamp_server/config.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hive/config.h>

#include <yt/yt/ytlib/transaction_client/config.h>

#include <yt/yt/ytlib/program/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NCellMaster {

using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TMasterHydraManagerConfig::TMasterHydraManagerConfig()
{
    RegisterParameter("response_keeper", ResponseKeeper)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TMasterConnectionConfig::TMasterConnectionConfig()
{
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(30));

    RegisterPreprocessor([&] {
        RetryAttempts = 100;
        RetryTimeout = TDuration::Minutes(3);
    });
}

////////////////////////////////////////////////////////////////////////////////

TDiscoveryServersConfig::TDiscoveryServersConfig()
{
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

TMulticellManagerConfig::TMulticellManagerConfig()
{
    RegisterParameter("master_connection", MasterConnection)
        .DefaultNew();
    RegisterParameter("upstream_sync_delay", UpstreamSyncDelay)
        .Default(TDuration::MilliSeconds(10));
}

////////////////////////////////////////////////////////////////////////////////

TWorldInitializerConfig::TWorldInitializerConfig()
{
    RegisterParameter("init_retry_period", InitRetryPeriod)
        .Default(TDuration::Seconds(3));
    RegisterParameter("init_transaction_timeout", InitTransactionTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("update_period", UpdatePeriod)
        .Default(TDuration::Minutes(5));
}

////////////////////////////////////////////////////////////////////////////////

TDynamicMulticellManagerConfig::TDynamicMulticellManagerConfig()
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

////////////////////////////////////////////////////////////////////////////////

TCellMasterConfig::TCellMasterConfig()
{
    RegisterParameter("networks", Networks)
        .Default(NNodeTrackerClient::DefaultNetworkPreferences);
    RegisterParameter("primary_master", PrimaryMaster)
        .Default();
    RegisterParameter("secondary_masters", SecondaryMasters)
        .Default();
    RegisterParameter("election_manager", ElectionManager)
        .DefaultNew();
    RegisterParameter("changelogs", Changelogs);
    RegisterParameter("snapshots", Snapshots);
    RegisterParameter("hydra_manager", HydraManager)
        .DefaultNew();
    RegisterParameter("cell_directory", CellDirectory)
        .DefaultNew();
    RegisterParameter("cell_directory_synchronizer", CellDirectorySynchronizer)
        .DefaultNew();
    RegisterParameter("hive_manager", HiveManager)
        .DefaultNew();
    RegisterParameter("node_tracker", NodeTracker)
        .DefaultNew();
    RegisterParameter("chunk_manager", ChunkManager)
        .DefaultNew();
    RegisterParameter("object_service", ObjectService)
        .DefaultNew();
    RegisterParameter("cypress_manager", CypressManager)
        .DefaultNew();
    RegisterParameter("replicated_table_tracker", ReplicatedTableTracker)
        .DefaultNew();
    RegisterParameter("enable_timestamp_manager", EnableTimestampManager)
        .Default(true);
    RegisterParameter("timestamp_manager", TimestampManager)
        .DefaultNew();
    RegisterParameter("timestamp_provider", TimestampProvider);
    RegisterParameter("discovery_server", DiscoveryServer)
        .Default();
    RegisterParameter("transaction_supervisor", TransactionSupervisor)
        .DefaultNew();
    RegisterParameter("multicell_manager", MulticellManager)
        .DefaultNew();
    RegisterParameter("world_initializer", WorldInitializer)
        .DefaultNew();
    RegisterParameter("security_manager", SecurityManager)
        .DefaultNew();
    RegisterParameter("enable_provision_lock", EnableProvisionLock)
        .Default(true);
    RegisterParameter("bus_client", BusClient)
        .DefaultNew();
    RegisterParameter("cypress_annotations", CypressAnnotations)
        .Default(BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());
    RegisterParameter("abort_on_unrecognized_options", AbortOnUnrecognizedOptions)
        .Default(false);
    RegisterParameter("enable_networking", EnableNetworking)
        .Default(true);
    RegisterParameter("cluster_connection", ClusterConnection);
    RegisterParameter("use_new_hydra", UseNewHydra)
        .Default(false);

    RegisterPostprocessor([&] {
        if (SecondaryMasters.size() > MaxSecondaryMasterCells) {
            THROW_ERROR_EXCEPTION("Too many secondary master cells");
        }

        auto cellId = PrimaryMaster->CellId;
        auto primaryCellTag = CellTagFromId(PrimaryMaster->CellId);
        THashSet<TCellTag> cellTags = {primaryCellTag};
        for (const auto& cellConfig : SecondaryMasters) {
            if (ReplaceCellTagInId(cellConfig->CellId, primaryCellTag) != cellId) {
                THROW_ERROR_EXCEPTION("Invalid cell id %v specified for secondary master in server configuration",
                    cellConfig->CellId);
            }
            auto cellTag = CellTagFromId(cellConfig->CellId);
            if (!cellTags.insert(cellTag).second) {
                THROW_ERROR_EXCEPTION("Duplicate cell tag %v in server configuration",
                    cellTag);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TDynamicCellMasterConfig::TDynamicCellMasterConfig()
{
    RegisterParameter("mutation_time_commit_period", MutationTimeCommitPeriod)
        .Default(TDuration::Minutes(10));

    RegisterParameter("alert_update_period", AlertUpdatePeriod)
        .Default(TDuration::Seconds(30));

    RegisterParameter("automaton_thread_bucket_weights", AutomatonThreadBucketWeights)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TDynamicClusterConfig::TDynamicClusterConfig()
{
    RegisterParameter("enable_safe_mode", EnableSafeMode)
        .Default(false);
    RegisterParameter("enable_descending_sort_order", EnableDescendingSortOrder)
        .Default(false);
    RegisterParameter("enable_descending_sort_order_dynamic", EnableDescendingSortOrderDynamic)
        .Default(false);
    RegisterParameter("chunk_manager", ChunkManager)
        .DefaultNew();
    RegisterParameter("cell_manager", CellManager)
        .DefaultNew();
    RegisterParameter("tablet_manager", TabletManager)
        .DefaultNew();
    RegisterParameter("chaos_manager", ChaosManager)
        .DefaultNew();
    RegisterParameter("node_tracker", NodeTracker)
        .DefaultNew();
    RegisterParameter("object_manager", ObjectManager)
        .DefaultNew();
    RegisterParameter("security_manager", SecurityManager)
        .DefaultNew();
    RegisterParameter("cypress_manager", CypressManager)
        .DefaultNew();
    RegisterParameter("multicell_manager", MulticellManager)
        .DefaultNew();
    RegisterParameter("transaction_manager", TransactionManager)
        .DefaultNew();
    RegisterParameter("scheduler_pool_manager", SchedulerPoolManager)
        .DefaultNew();
    RegisterParameter("cell_master", CellMaster)
        .DefaultNew();
    RegisterParameter("object_service", ObjectService)
        .DefaultNew();
    RegisterParameter("chunk_service", ChunkService)
        .DefaultNew();

    RegisterPostprocessor([&] {
        if (EnableDescendingSortOrderDynamic && !EnableDescendingSortOrder) {
            THROW_ERROR_EXCEPTION(
                "Setting enable_descending_sort_order_dynamic requires "
                "enable_descending_sort_order to be set");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TSerializationDumperConfig::TSerializationDumperConfig()
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

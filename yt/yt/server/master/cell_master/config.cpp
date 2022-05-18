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

#include <yt/yt/server/master/sequoia_server/config.h>

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

#include <yt/yt/client/transaction_client/config.h>

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

void TMasterConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Preprocessor([] (TThis* config) {
        config->RetryAttempts = 100;
        config->RetryTimeout = TDuration::Minutes(3);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDiscoveryServersConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
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

void TCellMasterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("networks", &TThis::Networks)
        .Default(NNodeTrackerClient::DefaultNetworkPreferences);
    registrar.Parameter("primary_master", &TThis::PrimaryMaster)
        .Default();
    registrar.Parameter("secondary_masters", &TThis::SecondaryMasters)
        .Default();
    registrar.Parameter("election_manager", &TThis::ElectionManager)
        .DefaultNew();
    registrar.Parameter("changelogs", &TThis::Changelogs);
    registrar.Parameter("snapshots", &TThis::Snapshots);
    registrar.Parameter("hydra_manager", &TThis::HydraManager)
        .DefaultNew();
    registrar.Parameter("cell_directory", &TThis::CellDirectory)
        .DefaultNew();
    registrar.Parameter("cell_directory_synchronizer", &TThis::CellDirectorySynchronizer)
        .DefaultNew();
    registrar.Parameter("hive_manager", &TThis::HiveManager)
        .DefaultNew();
    registrar.Parameter("node_tracker", &TThis::NodeTracker)
        .DefaultNew();
    registrar.Parameter("chunk_manager", &TThis::ChunkManager)
        .DefaultNew();
    registrar.Parameter("object_service", &TThis::ObjectService)
        .DefaultNew();
    registrar.Parameter("cypress_manager", &TThis::CypressManager)
        .DefaultNew();
    registrar.Parameter("replicated_table_tracker", &TThis::ReplicatedTableTracker)
        .DefaultNew();
    registrar.Parameter("enable_timestamp_manager", &TThis::EnableTimestampManager)
        .Default(true);
    registrar.Parameter("timestamp_manager", &TThis::TimestampManager)
        .DefaultNew();
    registrar.Parameter("timestamp_provider", &TThis::TimestampProvider);
    registrar.Parameter("discovery_server", &TThis::DiscoveryServer)
        .Default();
    registrar.Parameter("transaction_supervisor", &TThis::TransactionSupervisor)
        .DefaultNew();
    registrar.Parameter("multicell_manager", &TThis::MulticellManager)
        .DefaultNew();
    registrar.Parameter("world_initializer", &TThis::WorldInitializer)
        .DefaultNew();
    registrar.Parameter("security_manager", &TThis::SecurityManager)
        .DefaultNew();
    registrar.Parameter("enable_provision_lock", &TThis::EnableProvisionLock)
        .Default(true);
    registrar.Parameter("bus_client", &TThis::BusClient)
        .DefaultNew();
    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("enable_networking", &TThis::EnableNetworking)
        .Default(true);
    registrar.Parameter("cluster_connection", &TThis::ClusterConnection);
    registrar.Parameter("use_new_hydra", &TThis::UseNewHydra)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (config->SecondaryMasters.size() > MaxSecondaryMasterCells) {
            THROW_ERROR_EXCEPTION("Too many secondary master cells");
        }

        auto cellId = config->PrimaryMaster->CellId;
        auto primaryCellTag = CellTagFromId(config->PrimaryMaster->CellId);
        THashSet<TCellTag> cellTags = {primaryCellTag};
        for (const auto& cellConfig : config->SecondaryMasters) {
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

void TDynamicCellMasterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("mutation_time_commit_period", &TThis::MutationTimeCommitPeriod)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("alert_update_period", &TThis::AlertUpdatePeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("automaton_thread_bucket_weights", &TThis::AutomatonThreadBucketWeights)
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
    RegisterParameter("enable_table_column_renaming", EnableTableColumnRenaming)
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
    RegisterParameter("sequoia_manager", SequoiaManager)
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

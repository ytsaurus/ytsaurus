#include "config.h"

#include <yt/client/object_client/helpers.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NCellMaster {

using namespace NObjectClient;
using namespace NYTree;

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
    RegisterParameter("tablet_manager", TabletManager)
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
    RegisterParameter("transaction_supervisor", TransactionSupervisor)
        .DefaultNew();
    RegisterParameter("multicell_manager", MulticellManager)
        .DefaultNew();
    RegisterParameter("world_initializer", WorldInitializer)
        .DefaultNew();
    RegisterParameter("enable_provision_lock", EnableProvisionLock)
        .Default(true);
    RegisterParameter("bus_client", BusClient)
        .DefaultNew();
    RegisterParameter("annotation_setter_period", AnnotationSetterPeriod)
        .Default(TDuration::Seconds(10));
    RegisterParameter("cypress_annotations", CypressAnnotations)
        .Default(BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());
    RegisterParameter("abort_on_unrecognized_options", AbortOnUnrecognizedOptions)
        .Default(false);

    RegisterPostprocessor([&] () {
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

TDynamicClusterConfig::TDynamicClusterConfig()
{
    RegisterParameter("enable_safe_mode", EnableSafeMode)
        .Default(false);
    RegisterParameter("chunk_manager", ChunkManager)
        .DefaultNew();
    RegisterParameter("tablet_manager", TabletManager)
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
    RegisterParameter("cell_master", CellMaster)
        .DefaultNew();
    RegisterParameter("object_service", ObjectService)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster

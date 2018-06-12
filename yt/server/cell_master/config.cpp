#include "config.h"

#include <yt/client/object_client/helpers.h>

namespace NYT {
namespace NCellMaster {

using namespace NObjectClient;

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
    RegisterParameter("transaction_manager", TransactionManager)
        .DefaultNew();
    RegisterParameter("chunk_manager", ChunkManager)
        .DefaultNew();
    RegisterParameter("journal_manager", JournalManager)
        .DefaultNew();
    RegisterParameter("object_manager", ObjectManager)
        .DefaultNew();
    RegisterParameter("object_service", ObjectService)
        .DefaultNew();
    RegisterParameter("cypress_manager", CypressManager)
        .DefaultNew();
    RegisterParameter("security_manager", SecurityManager)
        .DefaultNew();
    RegisterParameter("tablet_manager", TabletManager)
        .DefaultNew();
    RegisterParameter("replicated_table_manager", ReplicatedTableManager)
        .DefaultNew();
    RegisterParameter("timestamp_manager", TimestampManager)
        .DefaultNew();
    RegisterParameter("timestamp_provider", TimestampProvider);
    RegisterParameter("transaction_supervisor", TransactionSupervisor)
        .DefaultNew();
    RegisterParameter("multicell_manager", MulticellManager)
        .DefaultNew();
    RegisterParameter("enable_provision_lock", EnableProvisionLock)
        .Default(true);
    RegisterParameter("bus_client", BusClient)
        .DefaultNew();

    RegisterPostprocessor([&] () {
        if (SecondaryMasters.size() > MaxSecondaryMasterCells) {
            THROW_ERROR_EXCEPTION("Too many secondary master cells");
        }

        const auto& cellId = PrimaryMaster->CellId;
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

} // namespace NCellMaster
} // namespace NYT

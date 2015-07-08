#include "stdafx.h"
#include "config.h"

#include <ytlib/object_client/helpers.h>

namespace NYT {
namespace NCellMaster {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TCellMasterConfig::TCellMasterConfig()
{
    RegisterParameter("master", Master);
    RegisterParameter("primary_master", PrimaryMaster)
        .Default();
    RegisterParameter("secondary_masters", SecondaryMasters)
        .Default();
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
    RegisterParameter("object_manager", ObjectManager)
        .DefaultNew();
    RegisterParameter("cypress_manager", CypressManager)
        .DefaultNew();
    RegisterParameter("security_manager", SecurityManager)
        .DefaultNew();
    RegisterParameter("tablet_manager", TabletManager)
        .DefaultNew();
    RegisterParameter("timestamp_manager", TimestampManager)
        .DefaultNew();
    RegisterParameter("timestamp_provider", TimestampProvider);
    RegisterParameter("transaction_supervisor", TransactionSupervisor)
        .DefaultNew();
    RegisterParameter("rpc_port", RpcPort)
        .Default(9000);
    RegisterParameter("monitoring_port", MonitoringPort)
        .Default(10000);
    RegisterParameter("enable_provision_lock", EnableProvisionLock)
        .Default(true);

    RegisterValidator([&] () {
        if (PrimaryMaster && !SecondaryMasters.empty()) {
            THROW_ERROR_EXCEPTION("Cannot specify both \"primary_master\" and \"secondary_masters\"");
        }

        const auto& cellId = Master->CellId;
        auto cellTag = CellTagFromId(Master->CellId);
        if (PrimaryMaster && ReplaceCellTagInId(PrimaryMaster->CellId, cellTag) != cellId) {
            THROW_ERROR_EXCEPTION("Invalid cell id %v specified for primary master",
                PrimaryMaster->CellId);
        }
        for (const auto& secondaryMaster : SecondaryMasters) {
            if (ReplaceCellTagInId(secondaryMaster->CellId, cellTag) != cellId) {
                THROW_ERROR_EXCEPTION("Invalid cell id %v specified for secondary master",
                    secondaryMaster->CellId);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT

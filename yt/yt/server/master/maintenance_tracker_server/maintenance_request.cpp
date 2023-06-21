#include "maintenance_request.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NMaintenanceTrackerServer {

////////////////////////////////////////////////////////////////////////////////

// COMPAT(kvk1920): After maintenance flag removal builtin ids will become unneeded.
// Beware: Changing set of builtin ids can change mutation semantics.
// See TMaintenanceTracker::GenerateMaintenanceId().
constexpr TEnumIndexedVector<EMaintenanceType, TMaintenanceId> BuiltinMaintenanceIds = {
    // Zero id is reserved for None.
    /*Banned*/ TMaintenanceId(0, 0, 0, 1),
    /*Decommissioned*/ TMaintenanceId(0, 0, 0, 2),
    /*DisableSchedulerJobs*/ TMaintenanceId(0, 0, 0, 3),
    /*DisableWriteSessions*/ TMaintenanceId(0, 0, 0, 4),
    /*DisableTabletCells*/ TMaintenanceId(0, 0, 0, 5),
    /*PendingRestart*/ TMaintenanceId(0, 0, 0, 6),
};

TMaintenanceId GetBuiltinMaintenanceId(EMaintenanceType type)
{
    return BuiltinMaintenanceIds[type];
}

bool IsBuiltinMaintenanceId(TMaintenanceId id)
{
    return std::binary_search(
        BuiltinMaintenanceIds.begin(),
        BuiltinMaintenanceIds.end(),
        id);
}

////////////////////////////////////////////////////////////////////////////////

void TMaintenanceRequest::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, User);
    Persist(context, Type);
    Persist(context, Comment);
    Persist(context, Timestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer

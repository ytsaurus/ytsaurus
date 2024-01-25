#include "maintenance_request.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NMaintenanceTrackerServer {

////////////////////////////////////////////////////////////////////////////////

// COMPAT(kvk1920): After maintenance flag removal builtin ids will become unneeded.
// Beware: Changing the set of builtin ids can change mutation semantics.
// See TMaintenanceTracker::GenerateMaintenanceId().
static TEnumIndexedArray<EMaintenanceType, TMaintenanceId> BuiltinMaintenanceIds{
    {EMaintenanceType::Ban,                   TMaintenanceId(0, 0, 0, 1)},
    {EMaintenanceType::Decommission,          TMaintenanceId(0, 0, 0, 2)},
    {EMaintenanceType::DisableSchedulerJobs,  TMaintenanceId(0, 0, 0, 3)},
    {EMaintenanceType::DisableWriteSessions,  TMaintenanceId(0, 0, 0, 4)},
    {EMaintenanceType::DisableTabletCells,    TMaintenanceId(0, 0, 0, 5)},
    {EMaintenanceType::PendingRestart,        TMaintenanceId(0, 0, 0, 6)},
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

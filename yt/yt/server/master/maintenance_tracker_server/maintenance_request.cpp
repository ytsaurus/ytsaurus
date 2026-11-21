#include "maintenance_request.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

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

void TMaintenanceRequest::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, User);
    PHOENIX_REGISTER_FIELD(2, Type);
    PHOENIX_REGISTER_FIELD(3, Comment);
    PHOENIX_REGISTER_FIELD(4, Timestamp);
}

PHOENIX_DEFINE_TYPE(TMaintenanceRequest);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer

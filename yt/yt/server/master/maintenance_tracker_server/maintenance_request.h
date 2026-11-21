#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/client/api/helpers.h>

#include <yt/yt/core/phoenix/type_decl.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NMaintenanceTrackerServer {

////////////////////////////////////////////////////////////////////////////////

TMaintenanceId GetBuiltinMaintenanceId(EMaintenanceType type);
bool IsBuiltinMaintenanceId(TMaintenanceId id);

////////////////////////////////////////////////////////////////////////////////

struct TMaintenanceRequest
{
    std::string User;
    EMaintenanceType Type;
    std::string Comment;
    TInstant Timestamp;

    using TSaveContext = NCellMaster::TSaveContext;
    using TLoadContext = NCellMaster::TLoadContext;

    PHOENIX_DECLARE_TYPE(TMaintenanceRequest, 0x4971a873);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer

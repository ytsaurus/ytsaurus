#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/client/api/helpers.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NMaintenanceTrackerServer {

////////////////////////////////////////////////////////////////////////////////

TMaintenanceId GetBuiltinMaintenanceId(EMaintenanceType type);
bool IsBuiltinMaintenanceId(TMaintenanceId id);

////////////////////////////////////////////////////////////////////////////////

struct TMaintenanceRequest
{
    TString User;
    EMaintenanceType Type;
    TString Comment;
    TInstant Timestamp;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer

#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <library/cpp/yt/small_containers/compact_set.h>

namespace NYT::NMaintenanceTrackerServer {

////////////////////////////////////////////////////////////////////////////////

struct IMaintenanceTracker
    : public virtual TRefCounted
{
    virtual TMaintenanceId AddMaintenance(
        EMaintenanceComponent component,
        const TString& address,
        EMaintenanceType type,
        const TString& comment) = 0;

    virtual TMaintenanceCounts RemoveMaintenance(
        EMaintenanceComponent component,
        const TString& address,
        const std::optional<TCompactSet<TMaintenanceId, TypicalMaintenanceRequestCount>> ids,
        std::optional<TStringBuf> user,
        std::optional<EMaintenanceType> type) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMaintenanceTracker)

////////////////////////////////////////////////////////////////////////////////

IMaintenanceTrackerPtr CreateMaintenanceTracker(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer

#pragma once

#include <yt/yt/client/api/helpers.h>
#include <yt/yt/client/api/public.h>

namespace NYT::NMaintenanceTrackerServer {

////////////////////////////////////////////////////////////////////////////////

using NApi::EMaintenanceComponent;
using NApi::EMaintenanceType;
using NApi::TMaintenanceId;
using NApi::TMaintenanceCounts;
using NApi::TMaintenanceIdPerTarget;
using NApi::TMaintenanceCountsPerTarget;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IMaintenanceTracker)

class TClusterProxyNode;

////////////////////////////////////////////////////////////////////////////////

constexpr int TypicalMaintenanceRequestCount = 6;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer

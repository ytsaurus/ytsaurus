#pragma once

#include "maintenance_target.h"

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NMaintenanceTrackerServer {

////////////////////////////////////////////////////////////////////////////////

void SerializeMaintenanceRequestsOf(
    const TNontemplateMaintenanceTargetBase* maintenanceTarget,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMaintenanceTrackerServer

#pragma once

#include "bundle_scheduler.h"
#include "bundle_sensors.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct ICellDowntimeTracker
    : public TRefCounted
{
    using TBundleSensorProvider = std::function<TBundleSensorsPtr(const std::string& bundleName)>;

    virtual void HandleState(
        const TSchedulerInputState& inputState,
        TBundleSensorProvider sensorsProvider) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICellDowntimeTracker)

////////////////////////////////////////////////////////////////////////////////

ICellDowntimeTrackerPtr CreateCellDowntimeTracker();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer

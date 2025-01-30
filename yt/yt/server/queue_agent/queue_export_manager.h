#pragma once

#include "private.h"
#include "config.h"

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

struct IQueueExportManager
    : public TRefCounted
{
    virtual NConcurrency::IThroughputThrottlerPtr GetExportThrottler() const = 0;

    virtual void OnDynamicConfigChanged(
        const TQueueExportManagerDynamicConfigPtr& oldConfig,
        const TQueueExportManagerDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueueExportManager)

IQueueExportManagerPtr CreateQueueExportManager(
    const TQueueExportManagerDynamicConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

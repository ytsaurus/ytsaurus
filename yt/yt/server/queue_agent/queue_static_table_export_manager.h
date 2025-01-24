#pragma once

#include "private.h"
#include "config.h"

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

struct IQueueStaticTableExportManager
    : public TRefCounted
{
    virtual NConcurrency::IThroughputThrottlerPtr GetExportThrottler() const = 0;

    virtual void OnDynamicConfigChanged(
        const TQueueStaticTableExportManagerDynamicConfigPtr& oldConfig,
        const TQueueStaticTableExportManagerDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueueStaticTableExportManager)

IQueueStaticTableExportManagerPtr CreateQueueStaticTableExportManager(
    const TQueueStaticTableExportManagerDynamicConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

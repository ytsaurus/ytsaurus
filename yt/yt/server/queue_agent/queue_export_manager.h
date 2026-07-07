#pragma once

#include "private.h"
#include "config.h"

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

struct IQueueExportManager
    : public TRefCounted
{
    virtual NConcurrency::IThroughputThrottlerPtr GetExportThrottler() const = 0;
    virtual const NHiveClient::TClientDirectoryPtr& GetQueueExportClientDirectory() const = 0;

    virtual void Reconfigure(
        const TQueueExportManagerDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueueExportManager)

IQueueExportManagerPtr CreateQueueExportManager(
    NApi::NNative::IConnectionPtr nativeConnection,
    std::string defaultQueueAgentUser,
    TQueueExportManagerConfigPtr staticConfig,
    TQueueExportManagerDynamicConfigPtr dynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

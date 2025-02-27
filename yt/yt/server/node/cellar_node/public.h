#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TMasterConnectorConfig)
DECLARE_REFCOUNTED_STRUCT(TMasterConnectorDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TCellarNodeConfig)
DECLARE_REFCOUNTED_STRUCT(TCellarNodeDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TBundleDynamicConfigManager)

DECLARE_REFCOUNTED_STRUCT(TCpuLimits)
DECLARE_REFCOUNTED_STRUCT(TMemoryLimits)
DECLARE_REFCOUNTED_STRUCT(TMediumThroughputLimits)
DECLARE_REFCOUNTED_STRUCT(TBundleDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IBootstrap)
DECLARE_REFCOUNTED_STRUCT(IMasterConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode

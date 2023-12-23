#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap;

DECLARE_REFCOUNTED_CLASS(TMasterConnectorConfig)
DECLARE_REFCOUNTED_CLASS(TMasterConnectorDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TCellarNodeConfig)
DECLARE_REFCOUNTED_CLASS(TCellarNodeDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TBundleDynamicConfigManager)

DECLARE_REFCOUNTED_STRUCT(TCpuLimits)
DECLARE_REFCOUNTED_STRUCT(TMemoryLimits)
DECLARE_REFCOUNTED_STRUCT(TBundleDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IMasterConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode

#pragma once

#include <yt/yt/core/misc/intrusive_ptr.h>

namespace NYT::NCellarNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMasterConnectorConfig)
DECLARE_REFCOUNTED_CLASS(TMasterConnectorDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TCellarNodeConfig)
DECLARE_REFCOUNTED_CLASS(TCellarNodeDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IMasterConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarNode

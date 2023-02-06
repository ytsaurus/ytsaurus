#pragma once

#include "object.h"

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! Returns true if new controller was created.
bool UpdateConsumerController(
    IObjectControllerPtr& controller,
    bool leading,
    const NQueueClient::TConsumerTableRow& row,
    const IObjectStore* store,
    TQueueControllerDynamicConfigPtr dynamicConfig,
    NHiveClient::TClientDirectoryPtr clientDirectory,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

#pragma once

#include "object.h"

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! Returns true if new controller was created.
bool UpdateQueueController(
    IObjectControllerPtr& controller,
    bool leading,
    const NQueueClient::TQueueTableRow& row,
    const IObjectStore* store,
    TQueueControllerDynamicConfigPtr dynamicConfig,
    NHiveClient::TClientDirectoryPtr clientDirectory,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

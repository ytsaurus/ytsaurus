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
    const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& replicatedTableMappingRow,
    const IObjectStore* store,
    TQueueControllerDynamicConfigPtr dynamicConfig,
    TQueueAgentClientDirectoryPtr clientDirectory,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

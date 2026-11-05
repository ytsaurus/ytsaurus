#pragma once

#include "object.h"

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! Returns true if new controller was created.
bool UpdateConsumerController(
    IObjectControllerPtr& controller,
    bool leading,
    NQueueClient::TConsumerReference reference,
    NQueueClient::TConsumerTableRowConstPtr tableRow,
    const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& replicatedTableMappingRow,
    const IObjectStore* store,
    const TQueueControllerDynamicConfigPtr& dynamicConfig,
    const TQueueAgentClientDirectoryPtr& clientDirectory,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

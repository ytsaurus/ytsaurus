#pragma once

#include "private.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/queue_client/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! Returns true if new controller was created.
bool UpdateMultiConsumerController(
    IObjectControllerPtr& controller,
    NQueueClient::TConsumerTableRowConstPtr row,
    const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& replicatedTableMappingRow,
    const IObjectStore* store,
    const TQueueControllerDynamicConfigPtr& dynamicConfig,
    const TQueueAgentClientDirectoryPtr& clientDirectory,
    IInvokerPtr invoker,
    NQueueClient::TDynamicStatePtr dynamicState);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

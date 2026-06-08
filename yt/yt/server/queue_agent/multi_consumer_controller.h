#pragma once

#include "private.h"

#include <yt/yt/ytlib/hive/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! Returns true if new controller was created.
bool UpdateMultiConsumerController(
    IObjectControllerPtr& controller,
    const TIntrusivePtr<NQueueClient::TConsumerTableRow>& row,
    const std::optional<NQueueClient::TReplicatedTableMappingTableRow>& replicatedTableMappingRow,
    const TQueueControllerDynamicConfigPtr& dynamicConfig,
    const TQueueAgentClientDirectoryPtr& clientDirectory,
    IInvokerPtr invoker,
    NQueueClient::TDynamicStatePtr dynamicState);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent

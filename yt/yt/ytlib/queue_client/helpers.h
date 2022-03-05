#pragma once

#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

//! Creates YPath service for queue agent orchid subtree representing queue/consumer #cluster:#objectPath.
//! Object kind can be either "queue" or "consumer".
NYTree::IYPathServicePtr CreateQueueAgentYPathService(
    NRpc::IChannelPtr queueAgentChannel,
    const TString& cluster,
    const TString& objectKind,
    const NYPath::TYPath& objectPath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

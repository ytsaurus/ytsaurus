#pragma once

#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

//! Creates YPath service for queue agent orchid subtree representing queue #cluster:#queuePath.
NYTree::IYPathServicePtr CreateQueueYPathService(
    NRpc::IChannelPtr queueAgentChannel,
    TString cluster,
    NYPath::TYPath queuePath);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

#pragma once

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/cypress_client/public.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

NCypressClient::TNodeId CreateQueueProducerNode(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& path,
    const NApi::TCreateNodeOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

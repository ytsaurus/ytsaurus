#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

NCypressClient::TNodeId CreatePipelineNode(
    const NApi::IClientPtr& client,
    const NYPath::TYPath& path,
    const NApi::TCreateNodeOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

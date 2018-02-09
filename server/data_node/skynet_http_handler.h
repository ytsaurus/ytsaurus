#pragma once

#include <yt/core/http/public.h>

#include <yt/server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

NHttp::IHttpHandlerPtr MakeSkynetHttpHandler(NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

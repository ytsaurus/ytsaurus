#pragma once

#include <yt/ytlib/monitoring/http_server.h>

#include <yt/server/cell_node/bootstrap.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

NXHttp::TServer::TAsyncHandler MakeSkynetHttpHandler(NCellNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

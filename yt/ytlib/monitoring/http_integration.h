#pragma once

#include "common.h"
#include "http_server.h"

#include "../ytree/ypath_service.h"

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

NHTTP::TServer::TAsyncHandler::TPtr GetYPathHttpHandler(
     NYTree::TYPathServiceAsyncProvider::TPtr asyncProvider);

NHTTP::TServer::TSyncHandler::TPtr GetProfilingHttpHandler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT

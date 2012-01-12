#pragma once

#include "common.h"
#include "http_server.h"

#include <ytlib/ytree/ypath_service.h>

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

NHttp::TServer::TAsyncHandler::TPtr GetYPathHttpHandler(
    NYTree::TYPathServiceProvider* provider,
    IInvoker* invoker);
NHttp::TServer::TSyncHandler::TPtr GetProfilingHttpHandler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT

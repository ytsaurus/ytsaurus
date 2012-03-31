#pragma once

#include "common.h"
#include "http_server.h"

#include <ytlib/ytree/ypath_service.h>

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

NHttp::TServer::TAsyncHandler GetYPathHttpHandler(
    NYTree::IYPathServicePtr service);

NHttp::TServer::TAsyncHandler GetYPathHttpHandler(
    NYTree::TYPathServiceProducer producer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT

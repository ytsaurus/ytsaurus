#pragma once

#include "common.h"
#include "http_server.h"

#include <ytlib/ytree/ypath_service.h>

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

NHttp::TServer::TAsyncHandler::TPtr GetYPathHttpHandler(
    NYTree::IYPathService* service);

NHttp::TServer::TAsyncHandler::TPtr GetYPathHttpHandler(
    NYTree::TYPathServiceProducer producer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT

#pragma once

#include "public.h"
#include "http_server.h"

#include <yt/core/ytree/ypath_service.h>
#include <yt/core/http/public.h>

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

NXHttp::TServer::TAsyncHandler GetYPathHttpHandler(
    NYTree::IYPathServicePtr service);

NHttp::IHttpHandlerPtr GetOrchidYPathHttpHandler(
    const NYTree::IYPathServicePtr& service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT

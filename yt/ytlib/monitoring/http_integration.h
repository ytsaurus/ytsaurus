#pragma once

#include "public.h"
#include "http_server.h"

#include <yt/core/ytree/ypath_service.h>

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

NHttp::TServer::TAsyncHandler GetYPathHttpHandler(
    NYTree::IYPathServicePtr service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT

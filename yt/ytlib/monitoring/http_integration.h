#pragma once

#include "public.h"

#include <yt/core/ytree/ypath_service.h>
#include <yt/core/http/public.h>

namespace NYT::NMonitoring {

////////////////////////////////////////////////////////////////////////////////

NHttp::IHttpHandlerPtr GetOrchidYPathHttpHandler(
    const NYTree::IYPathServicePtr& service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMonitoring

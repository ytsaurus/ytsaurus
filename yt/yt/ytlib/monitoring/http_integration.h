#pragma once

#include "public.h"

#include <yt/core/ytree/ypath_service.h>

#include <yt/core/http/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

namespace NYT::NMonitoring {

////////////////////////////////////////////////////////////////////////////////

void Initialize(
    const NHttp::IServerPtr& monitoringServer,
    TMonitoringManagerPtr* manager,
    NYTree::IMapNodePtr* orchidRoot,
    const NProfiling::TSolomonExporterConfigPtr& solomonConfig);

NHttp::IHttpHandlerPtr CreateTracingHttpHandler();
    
NHttp::IHttpHandlerPtr GetOrchidYPathHttpHandler(
    const NYTree::IYPathServicePtr& service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMonitoring

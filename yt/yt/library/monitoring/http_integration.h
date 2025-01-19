#pragma once

#include "public.h"

#include <yt/yt/core/ytree/ypath_service.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

namespace NYT::NMonitoring {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): refactor all these helpers
void Initialize(
    const NHttp::IServerPtr& monitoringServer,
    const NProfiling::TSolomonExporterConfigPtr& solomonExporterConfig,
    IMonitoringManagerPtr* monitoringManager,
    NYTree::IMapNodePtr* orchidRoot);
void Initialize(
    const NHttp::IServerPtr& monitoringServer,
    const NProfiling::TSolomonExporterPtr& solomonExporter,
    IMonitoringManagerPtr* monitoringManager,
    NYTree::IMapNodePtr* orchidRoot);

NHttp::IHttpHandlerPtr GetOrchidYPathHttpHandler(
    const NYTree::IYPathServicePtr& service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMonitoring

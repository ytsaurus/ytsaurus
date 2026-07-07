#pragma once

#include "public.h"

#include <yt/yt/core/ytree/ypath_service.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

namespace NYT::NMonitoring {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): refactor all these helpers
void Initialize(
    const NHttp::IServerPtr& monitoringHttpServer,
    const NProfiling::TSolomonExporterConfigPtr& solomonExporterConfig,
    IMonitoringManagerPtr* monitoringManager,
    NYTree::IMapNodePtr* orchidRoot);
void Initialize(
    const NHttp::IServerPtr& monitoringHttpServer,
    const NProfiling::TSolomonExporterPtr& solomonExporter,
    IMonitoringManagerPtr* monitoringManager,
    NYTree::IMapNodePtr* orchidRoot);

//! Same as above but also registers monitoring handlers on an optional HTTPS server.
//! |monitoringHttpsServer| may be null, in which case it is ignored.
void Initialize(
    const NHttp::IServerPtr& monitoringHttpServer,
    const NHttp::IServerPtr& monitoringHttpsServer,
    const NProfiling::TSolomonExporterConfigPtr& solomonExporterConfig,
    IMonitoringManagerPtr* monitoringManager,
    NYTree::IMapNodePtr* orchidRoot);
void Initialize(
    const NHttp::IServerPtr& monitoringHttpServer,
    const NHttp::IServerPtr& monitoringHttpsServer,
    const NProfiling::TSolomonExporterPtr& solomonExporter,
    IMonitoringManagerPtr* monitoringManager,
    NYTree::IMapNodePtr* orchidRoot);

NHttp::IHttpHandlerPtr GetOrchidYPathHttpHandler(
    const NYTree::IYPathServicePtr& service);

//! Registers monitoring handlers (solomon, orchid, ytprof, backtrace) on a server.
//! Use this when the primary monitoring server is already initialized via Initialize()
//! and you want to expose the same monitoring endpoints on an extra server (e.g. HTTPS).
void RegisterMonitoringHandlers(
    const NHttp::IServerPtr& monitoringServer,
    const NProfiling::TSolomonExporterPtr& solomonExporter,
    const NYTree::IMapNodePtr& orchidRoot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NMonitoring

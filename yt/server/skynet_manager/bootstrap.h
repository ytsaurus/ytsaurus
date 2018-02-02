#pragma once

#include "public.h"

#include "config.h"

#include <yt/ytlib/monitoring/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/net/public.h>

#include <yt/core/http/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

struct TCluster
{
    NConcurrency::IThroughputThrottlerPtr UserRequestThrottler;
    NConcurrency::IThroughputThrottlerPtr BackgroundThrottler;

    TCypressSyncPtr CypressSync;

    TClusterConnectionConfigPtr Config;

    void ThrottleBackground() const;
};

struct TBootstrap
    : public TRefCounted
{
    explicit TBootstrap(TSkynetManagerConfigPtr config);

    void Run();
    void DoRun();

    const TCluster& GetCluster(const TString& name) const;

    TSkynetManagerConfigPtr Config;

    NConcurrency::IPollerPtr Poller;

    NNet::IListenerPtr HttpListener;
    NHttp::IServerPtr HttpServer;

    NHttp::IClientPtr HttpClient;

    NYTree::IMapNodePtr OrchidRoot;
    NMonitoring::TMonitoringManagerPtr MonitoringManager;
    NHttp::IServerPtr MonitoringHttpServer;

    NConcurrency::TActionQueuePtr SkynetApiActionQueue;    
    ISkynetApiPtr SkynetApi;
    TShareCachePtr ShareCache;

    std::vector<TCluster> Clusters;

    TSkynetManagerPtr Manager;
};

DEFINE_REFCOUNTED_TYPE(TBootstrap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT

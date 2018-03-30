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
public:
    explicit TBootstrap(TSkynetManagerConfigPtr config);

    void Run();
    void DoRun();

    const TCluster& GetCluster(const TString& name) const;
    size_t GetClustersCount() const;

    IInvokerPtr GetInvoker() const;
    const ISkynetApiPtr& GetSkynetApi() const;
    const TSkynetManagerConfigPtr& GetConfig() const;
    const NHttp::IServerPtr& GetHttpServer() const;
    const NHttp::IClientPtr& GetHttpClient() const;
    const TShareCachePtr& GetShareCache() const;

private:
    TSkynetManagerConfigPtr Config_;

    NConcurrency::IPollerPtr Poller_;

    NNet::IListenerPtr HttpListener_;
    NHttp::IServerPtr HttpServer_;

    NHttp::IClientPtr HttpClient_;

    NYTree::IMapNodePtr OrchidRoot_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NHttp::IServerPtr MonitoringHttpServer_;

    NConcurrency::TActionQueuePtr SkynetApiActionQueue_;
    ISkynetApiPtr SkynetApi_;
    TShareCachePtr ShareCache_;

    std::vector<TCluster> Clusters_;

    TSkynetManagerPtr Manager_;
};

DEFINE_REFCOUNTED_TYPE(TBootstrap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT

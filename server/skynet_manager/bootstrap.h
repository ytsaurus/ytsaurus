#pragma once

#include "public.h"

#include "config.h"

#include <yt/ytlib/monitoring/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/net/public.h>

#include <yt/core/http/public.h>

#include <yt/core/ytree/public.h>

namespace NYT::NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
    : public TRefCounted
{
public:
    explicit TBootstrap(TSkynetManagerConfigPtr config);

    void Run();

    IInvokerPtr GetInvoker() const;
    const TSkynetManagerConfigPtr& GetConfig() const;
    const NHttp::IServerPtr& GetHttpServer() const;
    const NHttp::IClientPtr& GetHttpClient() const;
    const TAnnouncerPtr& GetAnnouncer() const;
    const NNet::IListenerPtr& GetPeerListener() const;
    const std::vector<TClusterConnectionPtr>& GetClusters() const;

private:
    TSkynetManagerConfigPtr Config_;

    NConcurrency::IPollerPtr Poller_;

    NNet::IListenerPtr HttpListener_;
    NHttp::IServerPtr HttpServer_;

    NHttp::IClientPtr HttpClient_;

    NYTree::IMapNodePtr OrchidRoot_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NHttp::IServerPtr MonitoringHttpServer_;

    NConcurrency::TActionQueuePtr ActionQueue_;

    NNet::IListenerPtr PeerListener_;
    TAnnouncerPtr Announcer_;

    std::vector<TClusterConnectionPtr> Clusters_;

    TSkynetServicePtr SkynetService_;
};

DEFINE_REFCOUNTED_TYPE(TBootstrap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkynetManager

#pragma once

#include <yt/server/http_proxy/public.h>

#include <yt/client/api/public.h>

#include <yt/core/http/server.h>

#include <yt/core/concurrency/public.h>

#include "private.h"

namespace NYT::NHttpProxy::NClickHouse {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseHandler
    : public NHttp::IHttpHandler
{
public:
    explicit TClickHouseHandler(TBootstrap* bootstrap);

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

    // It combines all metrics in one structure.
    struct TClickHouseProxyMetrics
    {
        NProfiling::TAggregateGauge ResolveAliasTime{"/query_time/resolve_alias"};
        NProfiling::TAggregateGauge DiscoveryForceUpdateTime{"/query_time/discovery_force_update"};
        NProfiling::TAggregateGauge FindDiscoveryTime{"/query_time/find_discovery"};
        NProfiling::TAggregateGauge IssueProxiedRequestTime{"/query_time/issue_proxied_request"};
        NProfiling::TAggregateGauge AuthenticateTime{"/query_time/authenticate"};
        NProfiling::TAggregateGauge CreateDiscoveryTime{"/query_time/create_discovery"};
        NProfiling::TAggregateGauge ForwardProxiedResponseTime{"/query_time/forward_proxied_response"};

        NProfiling::TAggregateGauge TotalQueryTime{"/total_query_time"};

        NProfiling::TMonotonicCounter QueryCount{"/query_count"};
        NProfiling::TMonotonicCounter ForceUpdateCount{"/force_update_count"};
        NProfiling::TMonotonicCounter BannedCount{"/banned_count"};
    };

private:
    TBootstrap* const Bootstrap_;
    const TCoordinatorPtr Coordinator_;
    const TClickHouseConfigPtr Config_;
    const NHttp::IClientPtr HttpClient_;
    const NApi::IClientPtr Client_;


    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;
    IInvokerPtr ControlInvoker_;

    THashMap<TString, int> UserToRunningQueryCount_;

    TCliqueCachePtr CliqueCache_;

    TClickHouseProxyMetrics Metrics_;

    //! Change internal user -> query count mapping value, which is used in profiling.
    /*!
     *  \note Invoker affinity: Control invoker
     */
    void AdjustQueryCount(const TString& user, int delta);

    void OnProfiling();
};

DEFINE_REFCOUNTED_TYPE(TClickHouseHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse

#pragma once

#include "public.h"

#include "config.h"
#include "helpers.h"
#include "private.h"
#include "component_discovery.h"

#include <yt/yt/ytlib/api/public.h>
#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/http/http.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

struct TLiveness
    : public NYTree::TYsonStruct
{
    TInstant UpdatedAt;
    double LoadAverage;
    double NetworkCoef;
    double UserCpu, SystemCpu, CpuWait;
    int ConcurrentRequests;

    REGISTER_YSON_STRUCT(TLiveness);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLiveness)

struct TProxyEntry
    : public NYTree::TYsonStruct
{
    TString Endpoint;
    std::string Role;

    TLivenessPtr Liveness;

    bool IsBanned;
    std::optional<TString> BanMessage;

    TString GetHost() const;

    REGISTER_YSON_STRUCT(TProxyEntry);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyEntry)

struct TCoordinatorProxy
    : public TRefCounted
{
    const TProxyEntryPtr Entry;
    std::atomic<i64> Dampening = 0;

    explicit TCoordinatorProxy(const TProxyEntryPtr& proxyEntry);
};

DEFINE_REFCOUNTED_TYPE(TCoordinatorProxy)

////////////////////////////////////////////////////////////////////////////////

class TCoordinator
    : public TRefCounted
{
public:
    TCoordinator(
        TProxyConfigPtr config,
        TBootstrap* bootstrap);

    void Start();

    bool IsBanned() const;
    bool CanHandleHeavyRequests() const;

    std::vector<TProxyEntryPtr> ListProxyEntries(std::optional<std::string> roleFilter, bool includeDeadAndBanned = false);
    TProxyEntryPtr AllocateProxy(const std::string& role);
    TProxyEntryPtr GetSelf();

    const TCoordinatorConfigPtr& GetConfig() const;
    NTracing::TSamplerPtr GetTraceSampler();

    bool IsDead(const TProxyEntryPtr& proxy, TInstant at) const;
    bool IsUnavailable(TInstant at) const;

    //! Raised when proxy role changes.
    DEFINE_SIGNAL(void(const std::string&), OnSelfRoleChanged);

    TDuration GetDeathAge() const;

private:
    const TCoordinatorConfigPtr Config_;
    const NTracing::TSamplerPtr Sampler_;
    TBootstrap* const Bootstrap_;
    const NApi::IClientPtr Client_;
    const NConcurrency::TPeriodicExecutorPtr UpdateStateExecutor_;

    ICypressRegistrarPtr CypressRegistrar_;
    NProfiling::TGauge BannedGauge_ = HttpProxyProfiler.Gauge("/banned");

    TPromise<void> FirstUpdateIterationFinished_ = NewPromise<void>();

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SelfLock_);
    TCoordinatorProxyPtr Self_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ProxiesLock_);
    std::vector<TCoordinatorProxyPtr> Proxies_;

    TInstant StatisticsUpdatedAt_;
    std::optional<TNetworkStatistics> LastStatistics_;
    TAtomicObject<TInstant> AvailableAt_;

    // TODO(aleksandra-zh): replace with the time read-only mode was entered.
    bool MastersInReadOnly_ = false;

    void UpdateReadOnly();

    void UpdateState();
    std::vector<TCoordinatorProxyPtr> ListCypressProxies();
    std::vector<TCoordinatorProxyPtr> ListProxies(std::optional<std::string> roleFilter, bool includeDeadAndBanned = false);

    TLivenessPtr GetSelfLiveness();
};

DEFINE_REFCOUNTED_TYPE(TCoordinator)

////////////////////////////////////////////////////////////////////////////////

class THostsHandler
    : public NHttp::IHttpHandler
{
public:
    explicit THostsHandler(TCoordinatorPtr coordinator);

    void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

private:
    const TCoordinatorPtr Coordinator_;
};

DEFINE_REFCOUNTED_TYPE(THostsHandler)

////////////////////////////////////////////////////////////////////////////////

class TClusterConnectionHandler
    : public NHttp::IHttpHandler
{
public:
    explicit TClusterConnectionHandler(NApi::IClientPtr client);

    void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

private:
    const NApi::IClientPtr Client_;
};

DEFINE_REFCOUNTED_TYPE(TClusterConnectionHandler)

////////////////////////////////////////////////////////////////////////////////

class TPingHandler
    : public NHttp::IHttpHandler
{
public:
    explicit TPingHandler(TCoordinatorPtr coordinator);

    void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

private:
    const TCoordinatorPtr Coordinator_;
};

DEFINE_REFCOUNTED_TYPE(TPingHandler)

////////////////////////////////////////////////////////////////////////////////

class TDiscoverVersionsHandler
    : public NHttp::IHttpHandler
    , public TComponentDiscoverer
{
public:
    TDiscoverVersionsHandler(NApi::NNative::IClientPtr client, TComponentDiscoveryOptions componentDiscoveryOptions);

    void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;
};

DEFINE_REFCOUNTED_TYPE(TDiscoverVersionsHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy

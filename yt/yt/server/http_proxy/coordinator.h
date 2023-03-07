#pragma once

#include "public.h"

#include "config.h"
#include "helpers.h"

#include <yt/ytlib/api/public.h>
#include <yt/ytlib/api/native/public.h>

#include <yt/core/actions/signal.h>

#include <yt/core/http/http.h>

#include <yt/core/tracing/sampler.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

struct TLiveness
    : public NYTree::TYsonSerializable
{
    TInstant UpdatedAt;
    double LoadAverage;
    double NetworkCoef;
    double UserCpu, SystemCpu, CpuWait;
    int ConcurrentRequests;

    std::atomic<i64> Dampening{0};

    TLiveness();
};

DEFINE_REFCOUNTED_TYPE(TLiveness)

struct TProxyEntry
    : public NYTree::TYsonSerializable
{
    TString Endpoint;
    TString Role;

    TLivenessPtr Liveness;

    bool IsBanned;
    std::optional<TString> BanMessage;

    TProxyEntry();

    TString GetHost() const;
};

DEFINE_REFCOUNTED_TYPE(TProxyEntry)

////////////////////////////////////////////////////////////////////////////////

class TCoordinator
    : public TRefCounted
{
public:
    TCoordinator(
        const TProxyConfigPtr& config,
        TBootstrap* bootstrap);

    void Start();

    bool IsBanned() const;
    bool CanHandleHeavyRequests() const;

    std::vector<TProxyEntryPtr> ListProxies(std::optional<TString> roleFilter, bool includeDeadAndBanned = false);
    TProxyEntryPtr AllocateProxy(const TString& role);
    TProxyEntryPtr GetSelf();

    const TCoordinatorConfigPtr& GetConfig() const;
    TDynamicConfigPtr GetDynamicConfig();
    NTracing::TSampler* GetTraceSampler();

    NYTree::IYPathServicePtr CreateOrchidService();

    bool IsDead(const TProxyEntryPtr& proxy, TInstant at) const;

    //! Raised when proxy role changes.
    DEFINE_SIGNAL(void(const TString), OnSelfRoleChanged);

private:
    const TCoordinatorConfigPtr Config_;
    TBootstrap* const Bootstrap_;
    const NApi::IClientPtr Client_;
    const NConcurrency::TPeriodicExecutorPtr UpdateStateExecutor_;
    const NConcurrency::TPeriodicExecutorPtr UpdateDynamicConfigExecutor_;

    TPromise<void> FirstUpdateIterationFinished_ = NewPromise<void>();
    bool Initialized_ = false;

    TSpinLock Lock_;
    TProxyEntryPtr Self_;
    TDynamicConfigPtr DynamicConfig_;
    NTracing::TSampler Sampler_;
    std::vector<TProxyEntryPtr> Proxies_;

    TInstant StatisticsUpdatedAt_;
    std::optional<TNetworkStatistics> LastStatistics_;

    void UpdateState();
    std::vector<TProxyEntryPtr> ListCypressProxies();

    TLivenessPtr GetSelfLiveness();

    void UpdateDynamicConfig();
    void SetDynamicConfig(TDynamicConfigPtr config);

    void BuildOrchid(NYson::IYsonConsumer* consumer);
};

DEFINE_REFCOUNTED_TYPE(TCoordinator)

////////////////////////////////////////////////////////////////////////////////

class THostsHandler
    : public NHttp::IHttpHandler
{
public:
    explicit THostsHandler(TCoordinatorPtr coordinator);

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

private:
    const TCoordinatorPtr Coordinator_;
};

DEFINE_REFCOUNTED_TYPE(THostsHandler)

////////////////////////////////////////////////////////////////////////////////

class TPingHandler
    : public NHttp::IHttpHandler
{
public:
    explicit TPingHandler(TCoordinatorPtr coordinator);

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

private:
    const TCoordinatorPtr Coordinator_;
};

DEFINE_REFCOUNTED_TYPE(TPingHandler)

////////////////////////////////////////////////////////////////////////////////

struct TInstance {
    TString Type, Address, Version, StartTime;

    bool Banned = false;
    bool Online = true;
    TString State;
    TError Error;
};

class TDiscoverVersionsHandler
    : public NHttp::IHttpHandler
{
public:
    TDiscoverVersionsHandler(
        NApi::NNative::IConnectionPtr connection,
        NApi::IClientPtr client,
        const TCoordinatorConfigPtr config);

protected:
    const NApi::NNative::IConnectionPtr Connection_;
    const NApi::IClientPtr Client_;
    const TCoordinatorConfigPtr Config_;

    std::vector<TString> GetInstances(const TString& path, bool fromSubdirectories = false);
    std::vector<TInstance> ListComponent(const TString& component, const TString& type);
    std::vector<TInstance> ListProxies(const TString& component, const TString& type);
    std::vector<TInstance> GetAttributes(
        const TString& path,
        const std::vector<TString>& instances,
        const TString& type);
};

DEFINE_REFCOUNTED_TYPE(TDiscoverVersionsHandler)

////////////////////////////////////////////////////////////////////////////////

class TDiscoverVersionsHandlerV1
    : public TDiscoverVersionsHandler
{
public:
    using TDiscoverVersionsHandler::TDiscoverVersionsHandler;

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;
};

DEFINE_REFCOUNTED_TYPE(TDiscoverVersionsHandlerV1)

////////////////////////////////////////////////////////////////////////////////

class TDiscoverVersionsHandlerV2
    : public TDiscoverVersionsHandler
{
public:
    using TDiscoverVersionsHandler::TDiscoverVersionsHandler;

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;
};

DEFINE_REFCOUNTED_TYPE(TDiscoverVersionsHandlerV2)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy

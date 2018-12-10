#pragma once

#include "public.h"

#include "helpers.h"

#include <yt/core/http/http.h>

#include <yt/ytlib/api/public.h>
#include <yt/ytlib/api/native/public.h>


#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

struct TLiveness
    : public NYTree::TYsonSerializable
{
    TInstant UpdatedAt;
    double LoadAverage;
    double NetworkCoef;

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

    bool IsDead(const TProxyEntryPtr& proxy, TInstant at) const;

private:
    TCoordinatorConfigPtr Config_;
    const TBootstrap* Bootstrap_;
    NApi::IClientPtr Client_;
    NConcurrency::TPeriodicExecutorPtr Periodic_;

    TPromise<void> FirstUpdateIterationFinished_ = NewPromise<void>();
    bool IsInitialized_ = false;

    TSpinLock Lock_;
    TProxyEntryPtr Self_;
    std::vector<TProxyEntryPtr> Proxies_;

    void Update();
    std::vector<TProxyEntryPtr> ListCypressProxies();

    TInstant StatisticsUpdatedAt_;
    std::optional<TNetworkStatistics> LastStatistics_;

    TLivenessPtr GetSelfLiveness();
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

class TDiscoverVersionsHandler
    : public NHttp::IHttpHandler
{
public:
    TDiscoverVersionsHandler(NApi::NNative::IConnectionPtr connection, NApi::IClientPtr client);

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

private:
    const NApi::NNative::IConnectionPtr Connection_;
    const NApi::IClientPtr Client_;

    NYson::TYsonString ListComponent(const TString& component, bool isDataNode);
    std::vector<TString> GetInstances(const TString& path, bool fromSubdirectories = false);
    NYson::TYsonString GetAttributes(const TString& path, const std::vector<TString>& instances);
};

DEFINE_REFCOUNTED_TYPE(TDiscoverVersionsHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT

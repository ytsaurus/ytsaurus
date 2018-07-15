#pragma once

#include "public.h"

#include "tables.h"

#include <yt/core/net/public.h>

#include <yt/core/http/public.h>

#include <yt/core/misc/async_expiring_cache.h>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

class TShareOperation
    : public TRefCounted
{
public:
    TShareOperation(
        TRequestKey request,
        const TClusterConnectionPtr& cluster,
        const TAnnouncerPtr& announcer);

    void Start(const IInvokerPtr& invoker);

private:
    const TRequestKey Request_;
    TClusterConnectionPtr Cluster_;
    TAnnouncerPtr Announcer_;

    NLogging::TLogger Logger;

    void Run();
};

DEFINE_REFCOUNTED_TYPE(TShareOperation)

////////////////////////////////////////////////////////////////////////////////

class TClusterConnection
    : public TRefCounted
{
public:
    TClusterConnection(
        const TClusterConnectionConfigPtr& config,
        const NApi::IClientPtr& client,
        const NHttp::IClientPtr& httpClient);

    TString GetName() const;
    const TTablesPtr& GetTables() const;
    const NConcurrency::IThroughputThrottlerPtr& GetBackgroundThrottler() const;

    bool IsHealthy() const;

    std::vector<TTableShard> ReadSkynetMetaFromTable(
        const NYPath::TYPath& path,
        const std::vector<TString>& keyColumns,
        const TProgressCallback& progressCallback);

    std::vector<TRowRangeLocation> FetchSkynetPartsLocations(
        const NYPath::TYPath& path);

    TErrorOr<i64> CheckTableAttributes(
        const NYPath::TRichYPath& path);

private:
    const TClusterConnectionConfigPtr Config_;
    const NConcurrency::IThroughputThrottlerPtr BackgroundThrottler_;

    NApi::IClientPtr Client_;
    NHttp::IClientPtr HttpClient_;

    TTablesPtr Tables_;
};

DEFINE_REFCOUNTED_TYPE(TClusterConnection)

////////////////////////////////////////////////////////////////////////////////

struct TCachedResource
    : public TRefCounted
{
    NYPath::TYPath TableRange;
    NProto::TResource Resource;
};

DECLARE_REFCOUNTED_STRUCT(TCachedResource)
DEFINE_REFCOUNTED_TYPE(TCachedResource)

typedef std::pair<TString, TResourceId> TCacheKey;

////////////////////////////////////////////////////////////////////////////////

class TSkynetService
    : public virtual TRefCounted
    , public TAsyncExpiringCache<TCacheKey, TCachedResourcePtr>
{
public:
    TSkynetService(TBootstrap* bootstrap, const TString& peerId);

    void Start();

    void HandleShare(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);
    void HandleHealthCheck(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);
    void HandleDebugLinks(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);

    void HandlePeerConnection(TPeerConnectionPtr peer);

private:
    const TSkynetManagerConfigPtr Config_;

    TAnnouncerPtr Announcer_;
    NNet::IListenerPtr PeerListener_;
    IInvokerPtr Invoker_;
    std::vector<TClusterConnectionPtr> Clusters_;
    TString SelfPeerId_;
    TString SelfPeerName_;

    void AcceptPeers();
    void SyncResourcesLoop(TClusterConnectionPtr cluster);
    void ReapRemovedTablesLoop(TClusterConnectionPtr cluster);

    TClusterConnectionPtr GetCluster(const TString& clusterName) const;

    void WriteShareReply(
        const NHttp::IResponseWriterPtr& rsp,
        const TRequestKey& request,
        const TRequestState& state);

    virtual TFuture<TCachedResourcePtr> DoGet(const TCacheKey& key) override;
};

DEFINE_REFCOUNTED_TYPE(TSkynetService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT

#include "chaos_residency_cache.h"

#include "chaos_node_service_proxy.h"
#include "chaos_cell_directory_synchronizer.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChaosClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NHiveClient;
using namespace NProto;

using NNative::IClientPtr;
using NNative::IConnectionPtr;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TChaosResidencyCache
    : public IChaosResidencyCache
    , public TAsyncExpiringCache<TObjectId, TCellTag>
{
public:
    TChaosResidencyCache(
        TChaosResidencyCacheConfigPtr config,
        IConnectionPtr connection,
        const NLogging::TLogger& logger);

    TFuture<TCellTag> GetChaosResidency(TObjectId objectId) override;
    void ForceRefresh(TObjectId objectId, TCellTag cellTag) override;
    void Clear() override;
    void UpdateReplicationCardResidency(NObjectClient::TObjectId objectId, NObjectClient::TCellTag cellTag) override;
    void RemoveReplicationCardResidency(NObjectClient::TObjectId objectId) override;
    void PingReplicationCardResidency(NObjectClient::TObjectId objectId) override;

protected:
    class TGetSession;

    const TChaosResidencyCacheConfigPtr Config_;
    const TWeakPtr<NNative::IConnection> Connection_;
    const NLogging::TLogger Logger;

    TFuture<TCellTag> DoGet(
        const TObjectId& objectId,
        const TErrorOr<TCellTag>* oldValue,
        EUpdateReason updateReason) noexcept override;

    TFuture<TCellTag> DoGet(
        const TObjectId& objectId,
        bool isPeriodicUpdate) noexcept override;

    TFuture<TCellTag> DoGet(
        const TObjectId& objectId,
        const TErrorOr<TCellTag>* oldValue);
};

////////////////////////////////////////////////////////////////////////////////

class TChaosResidencyCache::TGetSession
    : public TRefCounted
{
public:
    TGetSession(
        TChaosResidencyCache* owner,
        TObjectId objectId,
        TCellTag cellTag,
        const NLogging::TLogger& logger)
        : Owner_(owner)
        , ObjectId_(objectId)
        , Type_(ToString(TypeFromId(objectId)))
        , CellTag_(cellTag)
        , Logger(logger
            .WithTag("ObjectId: %v, Type: %v, CacheSessionId: %v",
                ObjectId_,
                Type_,
                TGuid::Create()))
    { }

    TCellTag Run()
    {
        auto connection = Owner_->Connection_.Lock();
        if (!connection) {
            THROW_ERROR_EXCEPTION("Unable to locate %v %v: connection terminated",
                Type_,
                ObjectId_);
        }

        const auto& cellDirectory = connection->GetCellDirectory();
        if (auto channel = cellDirectory->FindChannelByCellTag(CellTag_); !channel) {
            const auto& synchronizer = connection->GetChaosCellDirectorySynchronizer();
            synchronizer->AddCellTag(CellTag_);
            WaitFor(synchronizer->Sync())
                .ThrowOnError();
        }

        if (auto channel = cellDirectory->FindChannelByCellTag(CellTag_)) {
            auto proxy = TChaosNodeServiceProxy(channel);
            auto req = proxy.FindReplicationCard();
            ToProto(req->mutable_replication_card_id(), ObjectId_);

            auto rspOrError = WaitFor(req->Invoke());

            if (rspOrError.IsOK()) {
                return CellTag_;
            }
        }

        using TResponse = TIntrusivePtr<TChaosNodeServiceProxy::TRspFindReplicationCard>;
        std::vector<TFuture<TResponse>> futureFoundReplicationCards;
        std::vector<TCellTag> futureCellTags;
        auto chaosCellTags = GetChaosCellTags(cellDirectory);

        for (auto cellTag : chaosCellTags) {
            auto channel = cellDirectory->FindChannelByCellTag(cellTag);
            if (!channel) {
                continue;
            }

            auto proxy = TChaosNodeServiceProxy(channel);
            auto req = proxy.FindReplicationCard();
            ToProto(req->mutable_replication_card_id(), ObjectId_);

            futureFoundReplicationCards.push_back(req->Invoke());
            futureCellTags.push_back(cellTag);
        }

        YT_LOG_DEBUG("Looking for %v on chaos cells (ChaosCellTags: %v)",
            Type_,
            futureCellTags);

        auto resultOrError = WaitFor(AnyNSucceeded(futureFoundReplicationCards, 1));
        if (!resultOrError.IsOK()) {
            if (auto resolveError = resultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                THROW_ERROR *resolveError;
            }

            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Unable to locate %v %v",
                Type_,
                ObjectId_)
                << resultOrError;
        }

        for (int index = 0; index < std::ssize(futureFoundReplicationCards); ++index) {
            const auto& future = futureFoundReplicationCards[index];
            if (!future.IsSet()) {
                continue;
            }
            if (const auto& result = future.Get(); result.IsOK()) {
                return futureCellTags[index];
            }
        }

        YT_ABORT();
    }

private:
    const TIntrusivePtr<TChaosResidencyCache> Owner_;
    const TObjectId ObjectId_;
    const TString Type_;
    const TCellTag CellTag_;

    const NLogging::TLogger Logger;


    std::vector<TCellTag> GetChaosCellTags(const ICellDirectoryPtr& cellDirectory)
    {
        std::vector<TCellTag> chaosCellTags;

        auto cellInfos = cellDirectory->GetRegisteredCells();
        for (auto cellInfo : cellInfos) {
            if (TypeFromId(cellInfo.CellId) == EObjectType::ChaosCell) {
                chaosCellTags.push_back(CellTagFromId(cellInfo.CellId));
            }
        }

        return chaosCellTags;
    }
};

////////////////////////////////////////////////////////////////////////////////

TChaosResidencyCache::TChaosResidencyCache(
    TChaosResidencyCacheConfigPtr config,
    NNative::IConnectionPtr connection,
    const NLogging::TLogger& logger)
    : TAsyncExpiringCache(config)
    , Config_(std::move(config))
    , Connection_(connection)
    , Logger(logger)
{ }

TFuture<TCellTag> TChaosResidencyCache::GetChaosResidency(TObjectId key)
{
    return TAsyncExpiringCache::Get(key);
}

TFuture<TCellTag> TChaosResidencyCache::DoGet(
    const TObjectId& objectId,
    const TErrorOr<TCellTag>* oldValue,
    EUpdateReason /*updateReason*/) noexcept
{
    return DoGet(objectId, oldValue);
}

TFuture<TCellTag> TChaosResidencyCache::DoGet(
    const TObjectId& objectId,
    bool /*isPeriodicUpdate*/) noexcept
{
    return DoGet(objectId, nullptr);
}

TFuture<TCellTag> TChaosResidencyCache::DoGet(
    const TObjectId& objectId,
    const TErrorOr<TCellTag>* oldValue)
{
    auto cellTag = oldValue && oldValue->IsOK()
        ? oldValue->Value()
        : CellTagFromId(objectId);

    auto connection = Connection_.Lock();
    if (!connection) {
        return MakeFuture<TCellTag>(
            TError("Unable to locate %v: connection terminated",
                TypeFromId(objectId))
                << TErrorAttribute("object_id", objectId));
    }

    auto invoker = connection->GetInvoker();
    auto session = New<TGetSession>(this, objectId, cellTag, Logger);

    return BIND(&TGetSession::Run, std::move(session))
        .AsyncVia(std::move(invoker))
        .Run();
}

void TChaosResidencyCache::ForceRefresh(TObjectId objectId, TCellTag cellTag)
{
    TAsyncExpiringCache::ForceRefresh(objectId, cellTag);
}

void TChaosResidencyCache::Clear()
{
    TAsyncExpiringCache::Clear();
}

void TChaosResidencyCache::UpdateReplicationCardResidency(NObjectClient::TObjectId objectId, NObjectClient::TCellTag cellTag)
{
    TAsyncExpiringCache::Set(objectId, cellTag);
}

void TChaosResidencyCache::RemoveReplicationCardResidency(NObjectClient::TObjectId objectId)
{
    TAsyncExpiringCache::InvalidateActive(objectId);
}

void TChaosResidencyCache::PingReplicationCardResidency(NObjectClient::TObjectId objectId)
{
    Ping(objectId);
}

////////////////////////////////////////////////////////////////////////////////

IChaosResidencyCachePtr CreateChaosResidencyCache(
    TChaosResidencyCacheConfigPtr config,
    IConnectionPtr connection,
    const NLogging::TLogger& logger)
{
    return New<TChaosResidencyCache>(
        std::move(config),
        std::move(connection),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

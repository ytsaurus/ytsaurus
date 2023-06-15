#include "replication_card_residency_cache.h"

#include "chaos_node_service_proxy.h"
#include "chaos_cell_directory_synchronizer.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

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

///////////////////////////////////////////////////////////////////////////////

class TReplicationCardResidencyCache
    : public IReplicationCardResidencyCache
    , public TAsyncExpiringCache<TReplicationCardId, TCellTag>
{
public:
    TReplicationCardResidencyCache(
        TReplicationCardResidencyCacheConfigPtr config,
        IConnectionPtr connection,
        const NLogging::TLogger& logger);

    TFuture<TCellTag> GetReplicationCardResidency(TReplicationCardId replicationCardId) override;
    void ForceRefresh(TReplicationCardId replicationCardId, TCellTag cellTag) override;
    void Clear() override;

protected:
    class TGetSession;

    const TReplicationCardResidencyCacheConfigPtr Config_;
    const TWeakPtr<NNative::IConnection> Connection_;
    const NLogging::TLogger Logger;

    TFuture<TCellTag> DoGet(
        const TReplicationCardId& replicationCardId,
        const TErrorOr<TCellTag>* oldValue,
        EUpdateReason updateReason) noexcept override;

    TFuture<TCellTag> DoGet(
        const TReplicationCardId& replicationCardId,
        bool isPeriodicUpdate) noexcept override;

    TFuture<TCellTag> DoGet(
        const TReplicationCardId& replicationCardId,
        const TErrorOr<TCellTag>* oldValue);
};

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardResidencyCache::TGetSession
    : public TRefCounted
{
public:
    TGetSession(
        TReplicationCardResidencyCache* owner,
        TReplicationCardId replicationCardId,
        TCellTag cellTag,
        const NLogging::TLogger& logger)
        : Owner_(owner)
        , ReplicationCardId_(replicationCardId)
        , CellTag_(cellTag)
        , Logger(logger
            .WithTag("ReplicationCardId: %v, CacheSessionId: %v",
                ReplicationCardId_,
                TGuid::Create()))
    { }

    TCellTag Run()
    {
        auto connection = Owner_->Connection_.Lock();
        if (!connection) {
            THROW_ERROR_EXCEPTION("Unable to locate replication card: connection terminated");
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
            ToProto(req->mutable_replication_card_id(), ReplicationCardId_);

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
            ToProto(req->mutable_replication_card_id(), ReplicationCardId_);

            futureFoundReplicationCards.push_back(req->Invoke());
            futureCellTags.push_back(cellTag);
        }

        YT_LOG_DEBUG("Looking for replication card on chaos cells (ChaosCellTags: %v)",
            futureCellTags);

        auto resultOrError = WaitFor(AnyNSucceeded(futureFoundReplicationCards, 1));
        if (!resultOrError.IsOK()) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Unable to locate replication card")
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

        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Unable to locate replication card %v",
            ReplicationCardId_);
    }

private:
    const TIntrusivePtr<TReplicationCardResidencyCache> Owner_;
    const TReplicationCardId ReplicationCardId_;
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

TReplicationCardResidencyCache::TReplicationCardResidencyCache(
    TReplicationCardResidencyCacheConfigPtr config,
    NNative::IConnectionPtr connection,
    const NLogging::TLogger& logger)
    : TAsyncExpiringCache(config)
    , Config_(std::move(config))
    , Connection_(connection)
    , Logger(logger)
{ }

TFuture<TCellTag> TReplicationCardResidencyCache::GetReplicationCardResidency(TReplicationCardId key)
{
    return TAsyncExpiringCache::Get(key);
}

TFuture<TCellTag> TReplicationCardResidencyCache::DoGet(
    const TReplicationCardId& replicationCardId,
    const TErrorOr<TCellTag>* oldValue,
    EUpdateReason /*updateReason*/) noexcept
{
    return DoGet(replicationCardId, oldValue);
}

TFuture<TCellTag> TReplicationCardResidencyCache::DoGet(
    const TReplicationCardId& replicationCardId,
    bool /*isPeriodicUpdate*/) noexcept
{
    return DoGet(replicationCardId, nullptr);
}

TFuture<TCellTag> TReplicationCardResidencyCache::DoGet(
    const TReplicationCardId& replicationCardId,
    const TErrorOr<TCellTag>* oldValue)
{
    auto cellTag = oldValue && oldValue->IsOK()
        ? oldValue->Value()
        : CellTagFromId(replicationCardId);

    auto connection = Connection_.Lock();
    if (!connection) {
        return MakeFuture<TCellTag>(
            TError("Unable to locate replication card: connection terminated")
                << TErrorAttribute("replication_card_id", replicationCardId));
    }

    auto invoker = connection->GetInvoker();
    auto session = New<TGetSession>(this, replicationCardId, cellTag, Logger);

    return BIND(&TGetSession::Run, std::move(session))
        .AsyncVia(std::move(invoker))
        .Run();
}

void TReplicationCardResidencyCache::ForceRefresh(TReplicationCardId replicationCardId, TCellTag cellTag)
{
    TAsyncExpiringCache::ForceRefresh(replicationCardId, cellTag);
}

void TReplicationCardResidencyCache::Clear()
{
    TAsyncExpiringCache::Clear();
}

////////////////////////////////////////////////////////////////////////////////

IReplicationCardResidencyCachePtr CreateReplicationCardResidencyCache(
    TReplicationCardResidencyCacheConfigPtr config,
    IConnectionPtr connection,
    const NLogging::TLogger& logger)
{
    return New<TReplicationCardResidencyCache>(
        std::move(config),
        std::move(connection),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

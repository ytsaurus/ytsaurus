#include "chaos_residency_cache.h"

#include "chaos_node_service_proxy.h"
#include "chaos_cell_directory_synchronizer.h"
#include "config.h"
#include "master_cache_channel.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

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
using NNative::EChaosResidencyCacheType;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TChaosResidencyCacheBase
    : public IChaosResidencyCache
    , public TAsyncExpiringCache<TObjectId, TCellTag>
{
public:
    TChaosResidencyCacheBase(
        TChaosResidencyCacheConfigPtr config,
        IConnectionPtr connection,
        const NLogging::TLogger& logger);

    TFuture<TCellTag> GetChaosResidency(TObjectId objectId) override;
    void ForceRefresh(TObjectId objectId, TCellTag cellTag) override;
    void Clear() override;
    void UpdateReplicationCardResidency(NObjectClient::TObjectId objectId, NObjectClient::TCellTag cellTag) override;
    void RemoveReplicationCardResidency(NObjectClient::TObjectId objectId) override;
    void PingReplicationCardResidency(NObjectClient::TObjectId objectId) override;
    void Reconfigure(TChaosResidencyCacheConfigPtr config) override;

protected:
    class TGetSessionBase;

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
        const TErrorOr<TCellTag>* oldValue,
        bool forceRefresh);

    virtual TIntrusivePtr<TGetSessionBase> CreateGetSession(
        const TObjectId& objectId,
        const TCellTag& oldValue,
        bool forceRefresh) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChaosResidencyMasterCache
    : public TChaosResidencyCacheBase
{
public:
    using TChaosResidencyCacheBase::TChaosResidencyCacheBase;

protected:
    class TGetSession;

    TIntrusivePtr<TGetSessionBase> CreateGetSession(
        const TObjectId& objectId,
        const TCellTag& oldValue,
        bool forceRefresh) override;
};

////////////////////////////////////////////////////////////////////////////////

class TChaosResidencyClientCache
    : public TChaosResidencyCacheBase
{
public:
    TChaosResidencyClientCache(
        TChaosResidencyCacheConfigPtr config,
        IConnectionPtr connection,
        IChannelPtr chaosCacheChannel,
        const NLogging::TLogger& logger);

protected:
    class TGetSession;

    const IChannelPtr ChaosCacheChannel_;

    TIntrusivePtr<TGetSessionBase> CreateGetSession(
        const TObjectId& objectId,
        const TCellTag& oldValue,
        bool forceRefresh) override;
};

////////////////////////////////////////////////////////////////////////////////

class TChaosResidencyCompoundCache
    : public IChaosResidencyCache
{
public:
    TChaosResidencyCompoundCache(
        IChaosResidencyCachePtr masterCache,
        IChaosResidencyCachePtr clientCache,
        TChaosResidencyCacheConfigPtr config);

    TFuture<TCellTag> GetChaosResidency(TObjectId objectId) override;
    void ForceRefresh(TObjectId objectId, TCellTag cellTag) override;
    void Clear() override;
    void UpdateReplicationCardResidency(NObjectClient::TObjectId objectId, NObjectClient::TCellTag cellTag) override;
    void RemoveReplicationCardResidency(NObjectClient::TObjectId objectId) override;
    void PingReplicationCardResidency(NObjectClient::TObjectId objectId) override;
    void Reconfigure(TChaosResidencyCacheConfigPtr config) override;

private:
    const IChaosResidencyCachePtr MasterCache_;
    const IChaosResidencyCachePtr ClientCache_;

    std::atomic<bool> ActiveCacheIsClient_;
};

////////////////////////////////////////////////////////////////////////////////

class TChaosResidencyCacheBase::TGetSessionBase
    : public TRefCounted
{
public:
    TGetSessionBase(
        TObjectId objectId,
        TCellTag cellTag,
        const NLogging::TLogger& logger)
        : ObjectId_(objectId)
        , Type_(ToString(TypeFromId(objectId)))
        , CellTag_(cellTag)
        , Logger(logger
            .WithTag("ObjectId: %v, Type: %v, CacheSessionId: %v",
                ObjectId_,
                Type_,
                TGuid::Create()))
    { }

    virtual TFuture<TCellTag> Run() = 0;

protected:
    const TObjectId ObjectId_;
    const TString Type_;
    const TCellTag CellTag_;

    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

TChaosResidencyCacheBase::TChaosResidencyCacheBase(
    TChaosResidencyCacheConfigPtr config,
    NNative::IConnectionPtr connection,
    const NLogging::TLogger& logger)
    : TAsyncExpiringCache(config)
    , Connection_(connection)
    , Logger(logger)
{ }

TFuture<TCellTag> TChaosResidencyCacheBase::GetChaosResidency(TObjectId key)
{
    return TAsyncExpiringCache::Get(key);
}

void TChaosResidencyCacheBase::ForceRefresh(TObjectId objectId, TCellTag cellTag)
{
    TAsyncExpiringCache::ForceRefresh(objectId, cellTag);
}

void TChaosResidencyCacheBase::Clear()
{
    TAsyncExpiringCache::Clear();
}

void TChaosResidencyCacheBase::UpdateReplicationCardResidency(
    NObjectClient::TObjectId objectId,
    NObjectClient::TCellTag cellTag)
{
    TAsyncExpiringCache::Set(objectId, cellTag);
}

void TChaosResidencyCacheBase::RemoveReplicationCardResidency(NObjectClient::TObjectId objectId)
{
    TAsyncExpiringCache::InvalidateActive(objectId);
}

void TChaosResidencyCacheBase::PingReplicationCardResidency(NObjectClient::TObjectId objectId)
{
    TAsyncExpiringCache::Ping(objectId);
}

void TChaosResidencyCacheBase::Reconfigure(TChaosResidencyCacheConfigPtr config)
{
    TAsyncExpiringCache::Reconfigure(std::move(config));
}

TFuture<TCellTag> TChaosResidencyCacheBase::DoGet(
    const TObjectId& objectId,
    const TErrorOr<TCellTag>* oldValue,
    EUpdateReason updateReason) noexcept
{
    return DoGet(objectId, oldValue, updateReason == EUpdateReason::ForcedUpdate);
}

TFuture<TCellTag> TChaosResidencyCacheBase::DoGet(
    const TObjectId& objectId,
    bool /*isPeriodicUpdate*/) noexcept
{
    return DoGet(objectId, nullptr, false);
}

TFuture<TCellTag> TChaosResidencyCacheBase::DoGet(
    const TObjectId& objectId,
    const TErrorOr<TCellTag>* oldValue,
    bool forceRefresh)
{
    auto cellTag = oldValue && oldValue->IsOK()
        ? oldValue->Value()
        : CellTagFromId(objectId);

    auto connection = Connection_.Lock();
    if (!connection) {
        return MakeFuture<TCellTag>(
            TError("Unable to locate %Qlv: connection terminated",
                TypeFromId(objectId))
                << TErrorAttribute("object_id", objectId));
    }

    auto invoker = connection->GetInvoker();
    auto session = CreateGetSession(objectId, cellTag, forceRefresh);

    return BIND(&TGetSessionBase::Run, std::move(session))
        .AsyncVia(std::move(invoker))
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

class TChaosResidencyMasterCache::TGetSession
    : public TGetSessionBase
{
public:
    TGetSession(
        TChaosResidencyMasterCache* owner,
        TObjectId objectId,
        TCellTag cellTag,
        const NLogging::TLogger& logger)
        : TGetSessionBase(objectId, cellTag, logger)
        , Owner_(owner)
    { }

    TFuture<TCellTag> Run() override
    {
        auto connection = Owner_->Connection_.Lock();
        if (!connection) {
            THROW_ERROR_EXCEPTION("Unable to locate %v %v: connection terminated",
                Type_,
                ObjectId_);
        }

        auto defaultTimeout = connection->GetConfig()->DefaultChaosNodeServiceTimeout;
        auto channelFuture = EnsureChaosCellChannel(connection, CellTag_);
        auto checkLastSeenResidencyFuture = channelFuture.IsSet()
            ? CheckLastSeenResidency(
                ObjectId_,
                CellTag_,
                defaultTimeout,
                std::move(channelFuture.GetUnique()
                    .ValueOrDefault(nullptr)))
            : channelFuture.ApplyUnique(BIND(
                TGetSession::CheckLastSeenResidency,
                ObjectId_,
                CellTag_,
                defaultTimeout));

        auto fullLookupFuture = checkLastSeenResidencyFuture.ApplyUnique(BIND(
            [
                this,
                this_ = MakeStrong(this),
                connection = std::move(connection),
                defaultTimeout
            ] (TErrorOr<TCellTag>&& sameResidency)
            {
                auto sameResidencyValue = sameResidency.ValueOrDefault(InvalidCellTag);
                if (sameResidencyValue != InvalidCellTag) {
                    return MakeFuture(sameResidencyValue);
                }

                return LookForCardOnAllChaosCells(connection->GetCellDirectory(), defaultTimeout);
            }
        ));

        return fullLookupFuture;
    }

private:
    const TIntrusivePtr<TChaosResidencyMasterCache> Owner_;

    static TFuture<TCellTag> CheckLastSeenResidency(
        const TObjectId& objectId,
        TCellTag cellTag,
        TDuration timeout,
        IChannelPtr&& channel)
    {
        if (!channel) {
            return MakeFuture(InvalidCellTag);
        }

        auto proxy = TChaosNodeServiceProxy(channel);
        proxy.SetDefaultTimeout(timeout);
        auto req = proxy.FindReplicationCard();
        ToProto(req->mutable_replication_card_id(), objectId);

        return req->Invoke()
            .ApplyUnique(BIND(
                [
                    cellTag = cellTag
                ] (TErrorOr<TChaosNodeServiceProxy::TRspFindReplicationCardPtr>&& rspOrError)
                {
                    return rspOrError.IsOK() ? cellTag : InvalidCellTag;
                }
            ));
    }

    TFuture<TCellTag> LookForCardOnAllChaosCells(const NHiveClient::ICellDirectoryPtr& cellDirectory, TDuration timeout)
    {
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
            proxy.SetDefaultTimeout(timeout);
            auto req = proxy.FindReplicationCard();
            ToProto(req->mutable_replication_card_id(), ObjectId_);

            futureFoundReplicationCards.push_back(req->Invoke());
            futureCellTags.push_back(cellTag);
        }

        YT_LOG_DEBUG("Looking for %v on chaos cells (ChaosCellTags: %v)",
            Type_,
            futureCellTags);

        return AnyNSucceeded(futureFoundReplicationCards, 1).ApplyUnique(BIND(
            [
                type = Type_,
                objectId = ObjectId_,
                futureFoundReplicationCards = std::move(futureFoundReplicationCards),
                futureCellTags = std::move(futureCellTags)
            ] (
                TErrorOr<std::vector<TResponse>>&& resultOrError
            )
            {
                if (!resultOrError.IsOK()) {
                    if (auto resolveError = resultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                        THROW_ERROR *resolveError;
                    }

                    THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Unable to locate %v %v",
                        type,
                        objectId)
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
        ));
    }

    TFuture<IChannelPtr> EnsureChaosCellChannel(IConnectionPtr connection, TCellTag /* cellTag */)
    {
        auto cellDirectoryPtr = connection->GetCellDirectory();
        auto channel = cellDirectoryPtr->FindChannelByCellTag(CellTag_);
        if (!channel) {
            const auto& synchronizer = connection->GetChaosCellDirectorySynchronizer();
            synchronizer->AddCellTag(CellTag_);
            return synchronizer->Sync().Apply(BIND([
                cellTag = CellTag_,
                cellDirectoryPtr = std::move(cellDirectoryPtr)
            ] (const TErrorOr<void>& /* syncResult */) {
                return cellDirectoryPtr->FindChannelByCellTag(cellTag);
            }));
        }

        return MakeFuture(std::move(channel));
    }

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

TIntrusivePtr<TChaosResidencyCacheBase::TGetSessionBase> TChaosResidencyMasterCache::CreateGetSession(
    const TObjectId& objectId,
    const TCellTag& oldValue,
    bool /*forceRefresh*/)
{
    return New<TGetSession>(this, objectId, oldValue, Logger);
}

////////////////////////////////////////////////////////////////////////////////

class TChaosResidencyClientCache::TGetSession
    : public TGetSessionBase
{
public:
    TGetSession(
        TChaosResidencyClientCache* owner,
        TObjectId objectId,
        TCellTag cellTag,
        bool forceRefresh,
        const NLogging::TLogger& logger)
        : TGetSessionBase(objectId, cellTag, logger)
        , Owner_(owner)
        , ForceRefresh_(forceRefresh)
    { }

    TFuture<TCellTag> Run() override
    {
        auto connection = Owner_->Connection_.Lock();
        if (!connection) {
            THROW_ERROR_EXCEPTION("Unable to locate %v %v: connection terminated",
                Type_,
                ObjectId_);
        }

        auto proxy = TChaosNodeServiceProxy(Owner_->ChaosCacheChannel_);
        proxy.SetDefaultTimeout(connection->GetConfig()->DefaultChaosNodeServiceTimeout);
        auto req = proxy.GetReplicationCardResidency();
        ToProto(req->mutable_replication_card_id(), ObjectId_);
        if (ForceRefresh_) {
            req->set_force_refresh_replication_card_cell_tag(ToProto<ui32>(CellTag_));
        }

        SetChaosCacheStickyGroupBalancingHint(
            ObjectId_,
            req->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext));

        YT_LOG_DEBUG("Requesting master cache");
        return req->Invoke().ApplyUnique(BIND(
            [
                type = Type_,
                objectId = ObjectId_
            ] (TErrorOr<TChaosNodeServiceProxy::TRspGetReplicationCardResidencyPtr>&& resultOrError)
        {
            if (!resultOrError.IsOK()) {
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Unable to locate %v %v",
                    type,
                    objectId)
                    << resultOrError;
            }

            return FromProto<TCellTag>(resultOrError.Value()->replication_card_cell_tag());
        }));
    }

private:
    const TIntrusivePtr<TChaosResidencyClientCache> Owner_;
    const bool ForceRefresh_;
};

////////////////////////////////////////////////////////////////////////////////

TChaosResidencyClientCache::TChaosResidencyClientCache(
    TChaosResidencyCacheConfigPtr config,
    NNative::IConnectionPtr connection,
    IChannelPtr chaosCacheChannel,
    const NLogging::TLogger& logger)
    : TChaosResidencyCacheBase(
        std::move(config),
        connection,
        std::move(logger))
    , ChaosCacheChannel_(std::move(chaosCacheChannel))
{ }


TIntrusivePtr<TChaosResidencyCacheBase::TGetSessionBase> TChaosResidencyClientCache::CreateGetSession(
    const TObjectId& objectId,
    const TCellTag& oldValue,
    bool forceRefresh)
{
    return New<TGetSession>(this, objectId, oldValue, forceRefresh, Logger);
}

////////////////////////////////////////////////////////////////////////////////

TChaosResidencyCompoundCache::TChaosResidencyCompoundCache(
    IChaosResidencyCachePtr masterCache,
    IChaosResidencyCachePtr clientCache,
    TChaosResidencyCacheConfigPtr config)
    : MasterCache_(std::move(masterCache))
    , ClientCache_(std::move(clientCache))
    , ActiveCacheIsClient_(config->IsClientModeActive)
{ }

TFuture<TCellTag> TChaosResidencyCompoundCache::GetChaosResidency(TObjectId objectId)
{
    return ActiveCacheIsClient_
        ? ClientCache_->GetChaosResidency(objectId)
        : MasterCache_->GetChaosResidency(objectId);
}

void TChaosResidencyCompoundCache::ForceRefresh(TObjectId objectId, TCellTag cellTag)
{
    if (ActiveCacheIsClient_) {
        ClientCache_->ForceRefresh(objectId, cellTag);
    } else {
        MasterCache_->ForceRefresh(objectId, cellTag);
    }
}

void TChaosResidencyCompoundCache::Clear()
{
    ClientCache_->Clear();
    MasterCache_->Clear();
}

void TChaosResidencyCompoundCache::UpdateReplicationCardResidency(
    NObjectClient::TObjectId objectId,
    NObjectClient::TCellTag cellTag)
{
    return ActiveCacheIsClient_
        ? ClientCache_->UpdateReplicationCardResidency(objectId, cellTag)
        : MasterCache_->UpdateReplicationCardResidency(objectId, cellTag);
}

void TChaosResidencyCompoundCache::RemoveReplicationCardResidency(NObjectClient::TObjectId objectId)
{
    if (ActiveCacheIsClient_) {
        ClientCache_->RemoveReplicationCardResidency(objectId);
    } else {
        MasterCache_->RemoveReplicationCardResidency(objectId);
    }
}

void TChaosResidencyCompoundCache::PingReplicationCardResidency(NObjectClient::TObjectId objectId)
{
    if (ActiveCacheIsClient_) {
        ClientCache_->PingReplicationCardResidency(objectId);
    } else {
        MasterCache_->PingReplicationCardResidency(objectId);
    }
}

void TChaosResidencyCompoundCache::Reconfigure(TChaosResidencyCacheConfigPtr config)
{
    ActiveCacheIsClient_ = config->IsClientModeActive;
    ClientCache_->Reconfigure(config);
    MasterCache_->Reconfigure(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

IChaosResidencyCachePtr CreateChaosResidencyMasterCache(
    TChaosResidencyCacheConfigPtr config,
    IConnectionPtr connection,
    const NLogging::TLogger& logger)
{
    return New<TChaosResidencyMasterCache>(
        std::move(config),
        std::move(connection),
        logger);
}

IChaosResidencyCachePtr CreateChaosResidencyClientCache(
    TChaosResidencyCacheConfigPtr config,
    TChaosCacheChannelConfigPtr chaosCacheChannelConfig,
    IConnectionPtr connection,
    const NLogging::TLogger& logger)
{
    auto chaosCacheChannel = CreateChaosCacheChannel(connection, std::move(chaosCacheChannelConfig));
    return New<TChaosResidencyClientCache>(
        std::move(config),
        std::move(connection),
        std::move(chaosCacheChannel),
        logger);
}

IChaosResidencyCachePtr CreateChaosResidencyCache(
    TChaosResidencyCacheConfigPtr config,
    TChaosCacheChannelConfigPtr chaosCacheChannelConfig,
    IConnectionPtr connection,
    EChaosResidencyCacheType mode,
    const NLogging::TLogger& logger)
{
    if (mode == EChaosResidencyCacheType::MasterCache || !chaosCacheChannelConfig) {
        return CreateChaosResidencyMasterCache(
            std::move(config),
            std::move(connection),
            logger);
    }

    return New<TChaosResidencyCompoundCache>(
        CreateChaosResidencyMasterCache(
            config,
            connection,
            logger),
        CreateChaosResidencyClientCache(
            config,
            std::move(chaosCacheChannelConfig),
            std::move(connection),
            logger),
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

#include "chaos_residency_cache.h"

#include "chaos_cell_directory_synchronizer.h"
#include "chaos_node_service_proxy.h"
#include "config.h"
#include "master_cache_channel.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/object_client/helpers.h>
#include <yt/yt/client/chaos_client/helpers.h>

#include <yt/yt/core/misc/async_expiring_cache.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/retrying_channel.h>

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
    void UpdateChaosObjectResidency(TObjectId objectId, TCellTag cellTag) override;
    void RemoveChaosObjectResidency(TObjectId objectId) override;
    void PingChaosObjectResidency(TObjectId objectId) override;
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
    TChaosResidencyMasterCache(
        TChaosResidencyCacheConfigPtr config,
        IConnectionPtr connection,
        const NLogging::TLogger& logger);

    void Reconfigure(TChaosResidencyCacheConfigPtr config) override;

    TChaosResidencyCacheConfigPtr GetCacheConfig() const;

protected:
    class TGetSession;

    TIntrusivePtr<TGetSessionBase> CreateGetSession(
        const TObjectId& objectId,
        const TCellTag& oldValue,
        bool forceRefresh) override;

private:
    TAtomicIntrusivePtr<TChaosResidencyCacheConfig> Config_;
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
    IConnectionPtr connection,
    const NLogging::TLogger& logger)
    : TAsyncExpiringCache(std::move(config), NRpc::TDispatcher::Get()->GetHeavyInvoker())
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

void TChaosResidencyCacheBase::UpdateChaosObjectResidency(
    TObjectId objectId,
    TCellTag cellTag)
{
    TAsyncExpiringCache::Set(objectId, cellTag);
}

void TChaosResidencyCacheBase::RemoveChaosObjectResidency(TObjectId objectId)
{
    TAsyncExpiringCache::InvalidateActive(objectId);
}

void TChaosResidencyCacheBase::PingChaosObjectResidency(TObjectId objectId)
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

class TChaosObjectLocationResult
{
public:
    explicit TChaosObjectLocationResult(TCellTag cellTag)
        : CellTag_(cellTag)
    { }

    static TChaosObjectLocationResult FromCellTag(TCellTag cellTag)
    {
        return TChaosObjectLocationResult(cellTag);
    }

    static TChaosObjectLocationResult Absent()
    {
        return FromCellTag(InvalidCellTag);
    }

    static TChaosObjectLocationResult CellUnavailable()
    {
        return FromCellTag(CellUnavailableSentinel);
    }

    static TChaosObjectLocationResult NonExistent()
    {
        return FromCellTag(NonExistentReplicationCardSentinel);
    }

    bool IsAbsent() const
    {
        return CellTag_ == InvalidCellTag;
    }

    bool IsCellUnavailable() const
    {
        return CellTag_ == CellUnavailableSentinel;
    }

    bool IsNonExistent() const
    {
        return CellTag_ == NonExistentReplicationCardSentinel;
    }

    bool IsPresent() const
    {
        return !IsAbsent() && !IsNonExistent();
    }

    TCellTag GetCellTag() const
    {
        YT_VERIFY(CellTag_ >= MinValidCellTag && CellTag_ <= MaxValidCellTag);
        return CellTag_;
    }

private:
    inline static constexpr auto NonExistentReplicationCardSentinel = NotReplicatedCellTagSentinel;
    inline static constexpr auto CellUnavailableSentinel = PrimaryMasterCellTagSentinel;

    TCellTag CellTag_;
};

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
            THROW_ERROR_EXCEPTION("Unable to locate %Qlv %v: connection terminated",
                Type_,
                ObjectId_);
        }

        auto defaultTimeout = connection->GetConfig()->DefaultChaosNodeServiceTimeout;
        auto channelFuture = EnsureChaosCellChannel(connection, CellTag_);
        // COMPAT(osidorkin)
        if (Owner_->GetCacheConfig()->UseHasChaosObject) {
            return RunWithHasChaosObject(connection, std::move(channelFuture), defaultTimeout);
        }

        auto checkLastSeenResidencyFuture = channelFuture.IsSet()
            ? CheckLastSeenResidency(
                ObjectId_,
                CellTag_,
                defaultTimeout,
                std::move(channelFuture.AsUnique().Get()
                    .ValueOrDefault(nullptr)))
            : channelFuture.AsUnique().Apply(BIND(
                TGetSession::CheckLastSeenResidency,
                ObjectId_,
                CellTag_,
                defaultTimeout));

        auto fullLookupFuture = checkLastSeenResidencyFuture.AsUnique().Apply(BIND(
            [
                this,
                this_ = MakeStrong(this),
                connection = std::move(connection),
                defaultTimeout
            ] (TErrorOr<TCellTag>&& sameResidency) {
                auto sameResidencyValue = sameResidency.ValueOrDefault(InvalidCellTag);
                if (sameResidencyValue != InvalidCellTag) {
                    return MakeFuture(sameResidencyValue);
                }

                return LookForObjectOnAllChaosCells(
                    connection->GetCellDirectory(),
                    defaultTimeout);
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

        auto req = proxy.FindChaosObject();
        ToProto(req->mutable_chaos_object_id(), objectId);

        return req->Invoke()
            .AsUnique().Apply(BIND(
                [
                    cellTag = cellTag
                ] (TErrorOr<TChaosNodeServiceProxy::TRspFindChaosObjectPtr>&& rspOrError) {
                    return rspOrError.IsOK() ? cellTag : InvalidCellTag;
                }
            ));
    }

    TFuture<TCellTag> LookForObjectOnAllChaosCells(
        const ICellDirectoryPtr& cellDirectory,
        TDuration timeout)
    {
        std::vector<TFuture<void>> foundFutures;
        std::vector<TCellTag> futureCellTags;
        auto chaosCellTags = GetChaosCellTags(cellDirectory);

        for (auto cellTag : chaosCellTags) {
            auto channel = cellDirectory->FindChannelByCellTag(cellTag);
            if (!channel) {
                continue;
            }

            auto proxy = TChaosNodeServiceProxy(channel);
            proxy.SetDefaultTimeout(timeout);

            auto req = proxy.FindChaosObject();
            ToProto(req->mutable_chaos_object_id(), ObjectId_);
            foundFutures.push_back(req->Invoke().AsVoid());

            futureCellTags.push_back(cellTag);
        }

        YT_LOG_DEBUG("Looking for %v on chaos cells (ChaosCellTags: %v)",
            Type_,
            futureCellTags);

        return AnyNSucceeded(foundFutures, 1).Apply(BIND(
            [
                type = Type_,
                objectId = ObjectId_,
                foundFutures = std::move(foundFutures),
                futureCellTags = std::move(futureCellTags)
            ] (const TErrorOr<void>& errorOr) {
                if (!errorOr.IsOK()) {
                    if (auto resolveError = errorOr.FindMatching(NYTree::EErrorCode::ResolveError)) {
                        THROW_ERROR *resolveError;
                    }

                    THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Unable to locate %Qlv %v",
                        type,
                        objectId)
                    << errorOr;
                }

                for (int index = 0; index < std::ssize(foundFutures); ++index) {
                    const auto& future = foundFutures[index];
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

    TFuture<TCellTag> RunWithHasChaosObject(
        const IConnectionPtr& connection,
        TFuture<IChannelPtr>&& channelFuture,
        TDuration defaultTimeout)
    {
        auto checkLastSeenResidencyFuture = channelFuture.IsSet()
            ? CheckLastSeenResidencyViaIsChaosObjectExistent(
                ObjectId_,
                CellTag_,
                defaultTimeout,
                std::move(channelFuture.AsUnique().Get()
                    .ValueOrDefault(nullptr)))
            : channelFuture.AsUnique().Apply(BIND(
                TGetSession::CheckLastSeenResidencyViaIsChaosObjectExistent,
                ObjectId_,
                CellTag_,
                defaultTimeout));

        auto fullLookupFuture = checkLastSeenResidencyFuture.AsUnique().Apply(BIND(
            [
                this,
                this_ = MakeStrong(this),
                connection = std::move(connection),
                defaultTimeout
            ] (TErrorOr<TChaosObjectLocationResult>&& sameResidency) {
                if (sameResidency.IsOK()) {
                    const auto& sameResidencyValue = sameResidency.Value();
                    if (sameResidencyValue.IsNonExistent()) {
                        THROW_ERROR_EXCEPTION(NYTree::EErrorCode::ResolveError, "No such chaos object")
                            << TErrorAttribute("chaos_object_id", ObjectId_)
                            << TErrorAttribute("chaos_object_type", TypeFromId(ObjectId_));
                    }

                    if (sameResidencyValue.IsPresent()) {
                        return MakeFuture(sameResidencyValue.GetCellTag());
                    }
                }

                // Cell is unavailable or replication card is absent.
                return LookForObjectOnAllChaosCellsViaIsChaosObjectExistent(
                    connection->GetCellDirectory(),
                    defaultTimeout);
            }
        ));

        return fullLookupFuture;
    }

    static TFuture<TChaosObjectLocationResult> CheckResidency(
        IChannelPtr&& channel,
        const TObjectId& objectId,
        TDuration timeout,
        TCellTag cellTag)
    {
        auto proxy = TChaosNodeServiceProxy(std::move(channel));
        proxy.SetDefaultTimeout(timeout);

        auto req = proxy.IsChaosObjectExistent();
        ToProto(req->mutable_chaos_object_id(), objectId);

        return req->Invoke().AsUnique().Apply(BIND(
            [
                cellTag = cellTag
            ] (TErrorOr<TChaosNodeServiceProxy::TRspIsChaosObjectExistentPtr>&& rspOrError) {
                const auto& value = rspOrError.ValueOrThrow();
                if (value->has_chaos_object_exists()) {
                    return TChaosObjectLocationResult::FromCellTag(cellTag);
                }

                if (value->has_chaos_object_does_not_exist()) {
                    return TChaosObjectLocationResult::NonExistent();
                }

                return TChaosObjectLocationResult::Absent();
            }
        ));
    }

    static TFuture<TChaosObjectLocationResult> CheckLastSeenResidencyViaIsChaosObjectExistent(
        const TObjectId& objectId,
        TCellTag cellTag,
        TDuration timeout,
        IChannelPtr&& channel)
    {
        if (!channel) {
            return MakeFuture(TChaosObjectLocationResult::CellUnavailable());
        }

        return CheckResidency(std::move(channel), objectId, timeout, cellTag);
    }

    TFuture<TCellTag> LookForObjectOnAllChaosCellsViaIsChaosObjectExistent(
        const ICellDirectoryPtr& cellDirectory,
        TDuration timeout)
    {
        std::vector<TFuture<TChaosObjectLocationResult>> foundFutures;
        std::vector<TCellTag> futureCellTags;
        auto chaosCellTags = GetChaosCellTags(cellDirectory);

        for (auto cellTag : chaosCellTags) {
            auto channel = cellDirectory->FindChannelByCellTag(cellTag);
            if (!channel) {
                continue;
            }

            foundFutures.push_back(
                CheckResidency(std::move(channel), ObjectId_, timeout, cellTag)
                .AsUnique().Apply(BIND([] (TErrorOr<TChaosObjectLocationResult>&& result) {
                    if (!result.IsOK() || !result.Value().IsAbsent()) {
                        return result;
                    }

                    return TErrorOr<TChaosObjectLocationResult>(
                        TError(NRpc::EErrorCode::Unavailable, "Unable to locate object"));
                })));
            futureCellTags.push_back(cellTag);
        }

        YT_LOG_DEBUG("Looking for %v on chaos cells (ChaosCellTags: %v)",
            Type_,
            futureCellTags);

        return AnySucceeded(std::move(foundFutures))
            .AsUnique().Apply(BIND(CombinedLookupHandler, ObjectId_, Type_));
    }

    static TErrorOr<TCellTag> CombinedLookupHandler(
        TChaosObjectId objectId,
        const TString& type,
        TErrorOr<TChaosObjectLocationResult>&& errorOrCellTag)
    {
        if (!errorOrCellTag.IsOK()) {
            return TError(NRpc::EErrorCode::Unavailable, "Unable to locate %Qlv %v", type, objectId)
                << errorOrCellTag;
        }

        const auto& locationResult = errorOrCellTag.Value();
        if (locationResult.IsNonExistent()) {
            return TError(NYTree::EErrorCode::ResolveError, "No such chaos object")
                << TErrorAttribute("chaos_object_id", objectId)
                << TErrorAttribute("chaos_object_type", TypeFromId(objectId));
        }

        return locationResult.GetCellTag();
    }

    static TFuture<IChannelPtr> EnsureChaosCellChannel(IConnectionPtr connection, TCellTag cellTag)
    {
        auto cellDirectoryPtr = connection->GetCellDirectory();
        auto channel = cellDirectoryPtr->FindChannelByCellTag(cellTag);
        if (!channel) {
            const auto& synchronizer = connection->GetChaosCellDirectorySynchronizer();
            synchronizer->AddCellTag(cellTag);
            return synchronizer->Sync().Apply(BIND([
                cellTag = cellTag,
                cellDirectoryPtr = std::move(cellDirectoryPtr)
            ] (const TErrorOr<void>& /* syncResult */) {
                return cellDirectoryPtr->FindChannelByCellTag(cellTag);
            }));
        }

        return MakeFuture(std::move(channel));
    }

    std::vector<TCellTag> GetChaosCellTags(const ICellDirectoryPtr& cellDirectory)
    {
        if (IsChaosLeaseType(TypeFromId(ObjectId_))) {
            auto originCellTag = CellTagFromId(ObjectId_);
            return {originCellTag, GetSiblingChaosCellTag(originCellTag)};
        }

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

TChaosResidencyMasterCache::TChaosResidencyMasterCache(
    TChaosResidencyCacheConfigPtr config,
    IConnectionPtr connection,
    const NLogging::TLogger& logger)
    : TChaosResidencyCacheBase(
        config,
        std::move(connection),
        logger)
    , Config_(std::move(config))
{ }

void TChaosResidencyMasterCache::Reconfigure(TChaosResidencyCacheConfigPtr config)
{
    Config_.Store(config);
    TChaosResidencyCacheBase::Reconfigure(std::move(config));
}

TChaosResidencyCacheConfigPtr TChaosResidencyMasterCache::GetCacheConfig() const
{
    return Config_.Acquire();
}

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
            THROW_ERROR_EXCEPTION("Unable to locate %Qlv %v: connection terminated",
                Type_,
                ObjectId_);
        }

        auto proxy = TChaosNodeServiceProxy(Owner_->ChaosCacheChannel_);
        proxy.SetDefaultTimeout(connection->GetConfig()->DefaultChaosNodeServiceTimeout);
        auto req = proxy.GetChaosObjectResidency();
        ToProto(req->mutable_chaos_object_id(), ObjectId_);
        if (ForceRefresh_) {
            req->set_force_refresh_chaos_object_cell_tag(ToProto(CellTag_));
        }

        SetChaosCacheStickyGroupBalancingHint(
            ObjectId_,
            req->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext));

        YT_LOG_DEBUG("Requesting master cache");
        return req->Invoke().AsUnique().Apply(BIND(
            [
                type = Type_,
                objectId = ObjectId_
            ] (TErrorOr<TChaosNodeServiceProxy::TRspGetChaosObjectResidencyPtr>&& resultOrError) {
            if (!resultOrError.IsOK()) {
                THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Unable to locate %Qlv %v",
                    type,
                    objectId)
                    << resultOrError;
            }

            return FromProto<TCellTag>(resultOrError.Value()->chaos_object_cell_tag());
        }));
    }

private:
    const TIntrusivePtr<TChaosResidencyClientCache> Owner_;
    const bool ForceRefresh_;
};

////////////////////////////////////////////////////////////////////////////////

TChaosResidencyClientCache::TChaosResidencyClientCache(
    TChaosResidencyCacheConfigPtr config,
    IConnectionPtr connection,
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
    bool isClientCache = mode == EChaosResidencyCacheType::Client;
    bool chaosChannelConfigAddressesInvalid = isClientCache &&
        chaosCacheChannelConfig &&
        chaosCacheChannelConfig->Addresses &&
        chaosCacheChannelConfig->Addresses->empty();

    if (chaosChannelConfigAddressesInvalid) {
        const auto& Logger = logger;
        YT_LOG_WARNING(
            "Chaos cache channel addresses are present but empty, "
            "falling back to master cache variant of chaos residency cache");
    }

    if (!isClientCache || !chaosCacheChannelConfig || chaosChannelConfigAddressesInvalid) {
        return CreateChaosResidencyMasterCache(
            std::move(config),
            std::move(connection),
            logger);
    }

    return CreateChaosResidencyClientCache(
        std::move(config),
        std::move(chaosCacheChannelConfig),
        std::move(connection),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

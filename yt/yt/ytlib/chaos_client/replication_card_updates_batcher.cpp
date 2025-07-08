#include "replication_card_updates_batcher.h"
#include "replication_card_updates_batcher_serialization.h"
#include "config.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>
#include <yt/yt/ytlib/chaos_client/chaos_residency_cache.h>
#include <yt/yt/ytlib/chaos_client/master_cache_channel.h>

#include <yt/yt/client/chaos_client/replication_card.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/client/api/client_common.h>

#include <yt/yt/core/actions/future.h>

#include "yt/yt/core/concurrency/async_barrier.h"
#include <yt/yt/core/concurrency/async_rw_lock.h>

namespace NYT::NChaosClient {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NRpc;
using namespace NThreading;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

DECLARE_REFCOUNTED_CLASS(TBarrierLockGuard)

class TBarrierLockGuard
    : public TRefCounted
{
public:
    TBarrierLockGuard(
        IReplicationCardUpdatesBatcherPtr owner,
        TAsyncBarrier& barrier)
        : Owner_(std::move(owner))
        , Barrier_(&barrier)
        , Cookie_(barrier.Insert())
    { }

    ~TBarrierLockGuard()
    {
        Barrier_->Remove(Cookie_);
    }

private:
    const IReplicationCardUpdatesBatcherPtr Owner_;
    TAsyncBarrier* const Barrier_;
    TAsyncBarrierCookie Cookie_;
};

DEFINE_REFCOUNTED_TYPE(TBarrierLockGuard)

class TReplicationProgressUpdatesSerializer
{
public:
    explicit TReplicationProgressUpdatesSerializer(TReplicationProgress progress)
        : StateLock_(New<TAsyncSemaphore>(1))
        , Progress_(std::move(progress))
    { }

    TFuture<void> AsyncUpdate(
        TReplicationProgress progressUpdate,
        TBarrierLockGuardPtr barrierLockGuard,
        const TLogger& logger)
    {
        return StateLock_->AsyncAcquire(1)
            .ApplyUnique(BIND(
                [
                    progressUpdate = std::move(progressUpdate),
                    barrierLockGuard = std::move(barrierLockGuard),
                    Logger = logger,
                    this
                ] (TAsyncSemaphoreGuard&& /*guard*/) {
                    if (Progress_.Segments.empty()) {
                        Progress_ = std::move(progressUpdate);
                    } else {
                        YT_LOG_DEBUG("Updating replication progress (CurrentProgress: %v, ProgressUpdate: %v)",
                            Progress_,
                            progressUpdate);

                        Progress_ = BuildMaxProgress(Progress_, progressUpdate);

                        YT_LOG_DEBUG("Resulting progress (ResultingProgress: %v)",
                            Progress_);
                    }
                })
            .AsyncVia(GetCurrentInvoker()));
    }

    TReplicationProgress ExtractProgress()
    {
        return std::move(Progress_);
    }

private:
    const TAsyncSemaphorePtr StateLock_;
    TReplicationProgress Progress_;
};

class TAsyncReplicationCardUpdatesAccumulator
{
public:
    TFuture<TReplicationCardPtr> AddReplicationCardUpdate(
        TReplicationCardProgressUpdate replicationCardProgressUpdate,
        TBarrierLockGuardPtr barrierLockGuard,
        const TLogger& logger)
    {
        std::vector<TFuture<void>> futures;
        futures.reserve(replicationCardProgressUpdate.ReplicaProgressUpdates.size());

        auto guard = Guard(StateLock_);
        if (!FetchOptions_) {
            FetchOptions_ = std::move(replicationCardProgressUpdate.FetchOptions);
        } else if (replicationCardProgressUpdate.FetchOptions) {
            *FetchOptions_ |= *replicationCardProgressUpdate.FetchOptions;
        }

        for (auto& [replicaId, progressUpdate] : replicationCardProgressUpdate.ReplicaProgressUpdates) {
            futures.push_back(AsyncUpdateReplica(
                replicaId,
                std::move(progressUpdate),
                logger,
                std::move(barrierLockGuard),
                guard));
        }

        guard.Release();
        return ToReplicationCardFuture(AllSet(std::move(futures)).AsVoid());
    }

    TFuture<TReplicationCardPtr> AddReplicaUpdate(
        TReplicaId replicaId,
        TReplicationProgress progressUpdate,
        TBarrierLockGuardPtr barrierLockGuard,
        const TLogger& logger)
    {
        auto guard = Guard(StateLock_);
        auto updateFuture = AsyncUpdateReplica(
            replicaId,
            std::move(progressUpdate),
            logger,
            std::move(barrierLockGuard),
            guard);

        guard.Release();
        return ToReplicationCardFuture(std::move(updateFuture));
    }

    TPromise<TReplicationCardPtr> ExtractCardPromise()
    {
        return CardPromise_;
    }

    std::vector<TReplicaProgressUpdate> ExtractProgressByReplicaId()
    {
        std::vector<TReplicaProgressUpdate> result;
        result.reserve(ReplicaProgressUpdates_.size());
        for (auto& [replicaId, progress] : ReplicaProgressUpdates_) {
            result.emplace_back(replicaId, std::move(progress->ExtractProgress()));
        }

        return result;
    }

    std::optional<TReplicationCardFetchOptions> ExtractFetchOptions()
    {
        return FetchOptions_;
    }

    int GetSize() const
    {
        auto guard = Guard(StateLock_);
        return static_cast<ssize_t>(ReplicaProgressUpdates_.size());
    }

    void Reserve(int size)
    {
        auto guard = Guard(StateLock_);
        ReplicaProgressUpdates_.reserve(size);
    }

private:
    const TPromise<TReplicationCardPtr> CardPromise_ = NewPromise<TReplicationCardPtr>();

    YT_DECLARE_SPIN_LOCK(TSpinLock, StateLock_) ;
    // Replica count is usually small so use plain vector.
    std::vector<std::pair<TReplicaId, std::unique_ptr<TReplicationProgressUpdatesSerializer>>> ReplicaProgressUpdates_;
    std::optional<TReplicationCardFetchOptions> FetchOptions_;

    TFuture<void> AsyncUpdateReplica(
        const TReplicaId& replicaId,
        TReplicationProgress progressUpdate,
        const TLogger& logger,
        TBarrierLockGuardPtr barrierLockGuard,
        const TGuard<TSpinLock>& /*guard*/)
    {
        for (auto& progressByReplicaId : ReplicaProgressUpdates_) {
            if (replicaId == progressByReplicaId.first) {
                return progressByReplicaId.second->AsyncUpdate(
                    std::move(progressUpdate),
                    barrierLockGuard,
                    logger);
            }
        }

        ReplicaProgressUpdates_.emplace_back(
            replicaId,
            std::make_unique<TReplicationProgressUpdatesSerializer>(std::move(progressUpdate)));

        return VoidFuture;
    }

    TFuture<TReplicationCardPtr> ToReplicationCardFuture(TFuture<void>&& prerequisite) const
    {
        return prerequisite.Apply(BIND([future = CardPromise_.ToFuture()] {
            return future;
        }));
    }
};

DECLARE_REFCOUNTED_CLASS(TReplicationCardsUpdatesAccumulator)

class TReplicationCardsUpdatesAccumulator
    : public TRefCounted
{
public:
    using TBulkUpdateResult = IReplicationCardUpdatesBatcher::TBulkUpdateResult;

    using TMultipleReplicationCardProgressesUpdates =
        THashMap<TReplicationCardId, NDetail::TAsyncReplicationCardUpdatesAccumulator>;

    TFuture<TReplicationCardPtr> AddReplicaProgressesUpdate(
        TReplicationCardId replicationCardId,
        TReplicaId replicaId,
        TReplicationProgress replicaProgressUpdate,
        TBarrierLockGuardPtr barrierLock,
        const TLogger& logger)
    {
        auto& asyncAccumulator = GetAsyncAccumulator(replicationCardId);
        return asyncAccumulator.AddReplicaUpdate(
            replicaId,
            std::move(replicaProgressUpdate),
            std::move(barrierLock),
            logger);
    }

    TFuture<TReplicationCardPtr> AddReplicationCardProgressesUpdate(
        TReplicationCardProgressUpdate replicationCardProgressUpdate,
        TBarrierLockGuardPtr barrierLock,
        const TLogger& logger)
    {
        auto& asyncAccumulator = GetAsyncAccumulator(replicationCardProgressUpdate.ReplicationCardId);
        return asyncAccumulator.AddReplicationCardUpdate(
            std::move(replicationCardProgressUpdate),
            std::move(barrierLock),
            logger);
    }

    TBulkUpdateResult AddBulkReplicationCardProgressesUpdate(
        TReplicationCardProgressUpdatesBatch replicationCardProgressUpdateBatch,
        TBarrierLockGuardPtr barrierLock,
        const TLogger& logger)
    {
        using TAsyncAccumulatorWithUpdate = std::pair<
            NDetail::TAsyncReplicationCardUpdatesAccumulator*,
            TReplicationCardProgressUpdate>;

        const auto& replicationCardProgressUpdates = replicationCardProgressUpdateBatch.ReplicationCardProgressUpdates;

        // Call reserves for all collection to avoid memory allocations under UpdatesLock_ lock.
        std::vector<TAsyncAccumulatorWithUpdate> matchedEntries;
        matchedEntries.reserve(replicationCardProgressUpdates.size());

        std::vector<TReplicationCardProgressUpdate> missingEntries;
        matchedEntries.reserve(replicationCardProgressUpdates.size());

        {
            auto readerGuard = ReaderGuard(UpdatesLock_);

            for (const auto& replicationCardProgressUpdate : replicationCardProgressUpdates) {
                if (auto it = Updates_.find(replicationCardProgressUpdate.ReplicationCardId); it != Updates_.end()) {
                    matchedEntries.emplace_back(&it->second, std::move(replicationCardProgressUpdate));
                } else {
                    missingEntries.push_back(std::move(replicationCardProgressUpdate));
                }
            }
        }

        if (!missingEntries.empty()) {
            auto writerGuard = WriterGuard(UpdatesLock_);

            for (const auto& replicationCardProgressUpdate : missingEntries) {
                matchedEntries.emplace_back(
                    &Updates_[replicationCardProgressUpdate.ReplicationCardId],
                    std::move(replicationCardProgressUpdate));
            }
        }

        TBulkUpdateResult result;
        result.resize(replicationCardProgressUpdateBatch.ReplicationCardProgressUpdates.size());
        for (auto& [batchingEntry, replicationCardProgressUpdate] : matchedEntries) {
            auto replicationCardId = replicationCardProgressUpdate.ReplicationCardId;
            result.emplace_back(
                replicationCardId,
                batchingEntry->AddReplicationCardUpdate(
                    std::move(replicationCardProgressUpdate),
                    barrierLock,
                    logger));
        }

        return result;
    }

    TReplicationCardsUpdatesAccumulatorPtr CreateNext()
    {
        auto guard = ReaderGuard(UpdatesLock_);
        auto next = New<TReplicationCardsUpdatesAccumulator>();
        // Fill next accumulator with previous keys and call reserve for values
        // to reduce spin lock contention during future updates.
        next->Updates_.reserve(Updates_.size());
        for (const auto& [replicationCardId, batchingEntry] : Updates_) {
            if (int entrySize = batchingEntry.GetSize(); entrySize != 0) {
                next->Updates_[replicationCardId].Reserve(entrySize);
            }
        }

        return next;
    }

    TMultipleReplicationCardProgressesUpdates ExtractUpdates()
    {
        return std::move(Updates_);
    }

private:
    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, UpdatesLock_);
    TMultipleReplicationCardProgressesUpdates Updates_;

    NDetail::TAsyncReplicationCardUpdatesAccumulator& GetAsyncAccumulator(const TReplicationCardId& replicationCardId)
    {
        {
            auto readerGuard = ReaderGuard(UpdatesLock_);
            auto it = Updates_.find(replicationCardId);
            if (it != Updates_.end()) {
                return it->second;
            }
        }

        auto writerGuard = WriterGuard(UpdatesLock_);
        return Updates_[replicationCardId];
    }
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardsUpdatesAccumulator)

struct IReplicationCardUpdatesAccumulatorFlushSession
{
    using TMultipleReplicationCardProgressesUpdates =
        TReplicationCardsUpdatesAccumulator::TMultipleReplicationCardProgressesUpdates;

    virtual ~IReplicationCardUpdatesAccumulatorFlushSession() = default;

    virtual void FlushProgressUpdatesBatch(
        const IConnectionPtr& connection,
        TMultipleReplicationCardProgressesUpdates multipleReplicationCardProgressesUpdates) const = 0;
};

class TReplicationCardUpdatesBatcherBase
    : public IReplicationCardUpdatesBatcher
{
public:
    TReplicationCardUpdatesBatcherBase(
        const TChaosReplicationCardUpdatesBatcherConfigPtr& config,
        IConnectionPtr connection,
        const IInvokerPtr& invoker,
        TLogger logger)
        : Logger(std::move(logger))
        , Connection_(std::move(connection))
        , SubmittingExecutor_(New<TPeriodicExecutor>(
            invoker,
            BIND(&TReplicationCardUpdatesBatcherBase::SubmitBatch, MakeWeak(this)),
            config->FlushPeriod))
        , Accumulator_(New<TReplicationCardsUpdatesAccumulator>())
    { }

    TBulkUpdateResult AddBulkReplicationCardProgressesUpdate(
        TReplicationCardProgressUpdatesBatch replicationCardProgressUpdateBatch) override
    {
        auto barrierGuard = New<TBarrierLockGuard>(this, WorkerBarrier_);
        ValidateRunning(barrierGuard);

        return Accumulator_->AddBulkReplicationCardProgressesUpdate(
            std::move(replicationCardProgressUpdateBatch),
            std::move(barrierGuard),
            Logger);
    }

    TFuture<TReplicationCardPtr> AddReplicationCardProgressesUpdate(
        TReplicationCardProgressUpdate replicationCardProgressUpdate) override
    {
        auto barrierGuard = New<TBarrierLockGuard>(this, WorkerBarrier_);
        ValidateRunning(barrierGuard);

        return Accumulator_->AddReplicationCardProgressesUpdate(
            std::move(replicationCardProgressUpdate),
            std::move(barrierGuard),
            Logger);
    }

    TFuture<TReplicationCardPtr> AddTabletProgressUpdate(
        TReplicationCardId replicationCardId,
        TReplicaId replicaId,
        const TReplicationProgress& tabletReplicationProgress) override
    {
        auto barrierGuard = New<TBarrierLockGuard>(this, WorkerBarrier_);
        ValidateRunning(barrierGuard);

        return Accumulator_->AddReplicaProgressesUpdate(
            replicationCardId,
            replicaId,
            std::move(tabletReplicationProgress),
            std::move(barrierGuard),
            Logger);
    }

    void Start() override
    {
        if (Stopped_) {
            SubmittingExecutor_->Start();
            Stopped_ = false;
        }
    }

    void Stop() override
    {
        if (Stopped_) {
            return;
        }

        Stopped_ = true;

        // Wait for all updates to be settled in the batch.
        auto barrierFutureWaitResult = WaitFor(WorkerBarrier_.GetBarrierFuture());
        if (!barrierFutureWaitResult.IsOK()) {
            YT_LOG_WARNING(barrierFutureWaitResult, "Failed to wait for a barrier on shutdown");
        }

        SubmittingExecutor_->ScheduleOutOfBand();

        auto futureStop = WaitFor(SubmittingExecutor_->Stop());
        if (!futureStop.IsOK()) {
            YT_LOG_WARNING(futureStop, "Failed to stop submitting executor");
        }

    }

protected:
    const TLogger Logger;

    virtual const IReplicationCardUpdatesAccumulatorFlushSession& GetFlushSession() const = 0;

    void SetPeriod(TDuration period)
    {
        SubmittingExecutor_->SetPeriod(period);
    }

private:
    const TWeakPtr<IConnection> Connection_;
    const TPeriodicExecutorPtr SubmittingExecutor_;

    std::atomic<bool> Stopped_ = true;
    TReplicationCardsUpdatesAccumulatorPtr Accumulator_;
    TAsyncBarrier WorkerBarrier_;

    void SubmitBatch()
    {
        YT_LOG_DEBUG("Started progress updates batch flush");

        auto connection = Connection_.Lock();
        if (!connection) {
            YT_LOG_DEBUG("Connections is not available");
            return;
        }

        auto previousAccumulator = Accumulator_;
        Accumulator_ = previousAccumulator->CreateNext();
        WaitFor(WorkerBarrier_.GetBarrierFuture())
            .ThrowOnError();

        GetFlushSession().FlushProgressUpdatesBatch(
            connection,
            std::move(previousAccumulator->ExtractUpdates()));

        YT_LOG_DEBUG("Finished progress updates batch flush");
    }

    void ValidateRunning(const TBarrierLockGuardPtr /*barrierLock*/) const
    {
        if (Stopped_) {
            THROW_ERROR_EXCEPTION("Batcher is not running");
        }
    }
};

class TMasterCacheUpdatesAccumulatorFlushSession
    : public IReplicationCardUpdatesAccumulatorFlushSession
{
public:
    explicit TMasterCacheUpdatesAccumulatorFlushSession(TLogger logger)
        : Logger(std::move(logger))
    { }

    void FlushProgressUpdatesBatch(
        const IConnectionPtr& connection,
        TMultipleReplicationCardProgressesUpdates replicationCardProgressUpdatesEntries) const override
    {
        auto timeout = connection->GetConfig()->DefaultChaosNodeServiceTimeout;

        auto replicationCardUpdateByChaosCells = GroupRepicationCardUpdatesByChaosCells(
            connection,
            replicationCardProgressUpdatesEntries);

        YT_LOG_DEBUG("Updates grouping finishe (BatchSize: %v, ChaosCellsCount: %v, ResolvingErrorCount: %v)",
            replicationCardProgressUpdatesEntries.size(),
            replicationCardUpdateByChaosCells.ReplicationCardIdsByChaosCells.size(),
            replicationCardUpdateByChaosCells.ResolvingErrors.size());

        const auto& replicationCardIdsByChaosCells = replicationCardUpdateByChaosCells.ReplicationCardIdsByChaosCells;
        for (const auto& [cellTag, replicationCardIds] : replicationCardIdsByChaosCells) {
            auto cellChannel = connection->GetChaosChannelByCellTag(cellTag);
            SubmitProgressesUpdateToChaosNode(
                cellChannel,
                replicationCardIds,
                replicationCardProgressUpdatesEntries,
                timeout);
        }

        for (const auto& [missingId, error] : replicationCardUpdateByChaosCells.ResolvingErrors) {
            auto it = replicationCardProgressUpdatesEntries.find(missingId);
            if (it != replicationCardProgressUpdatesEntries.end()) {
                it->second.ExtractCardPromise().Set(error);
            }
        }
    }

private:
    struct TRepicationCardUpdatesByChaosCells
    {
        THashMap<TCellTag, std::vector<TReplicationCardId>> ReplicationCardIdsByChaosCells;
        THashMap<TReplicationCardId, TError> ResolvingErrors;
    };

    const TLogger Logger;

    void SubmitProgressesUpdateToChaosNode(
        const IChannelPtr& channel,
        const std::vector<TReplicationCardId>& replicationCardIds,
        TMultipleReplicationCardProgressesUpdates& batch,
        TDuration timeout) const
    {
        THashMap<TReplicationCardId, TPromise<TReplicationCardPtr>> promises;
        promises.reserve(replicationCardIds.size());

        auto proxy = TChaosNodeServiceProxy(channel);
        auto req = proxy.UpdateMultipleTableProgresses();
        req->SetTimeout(timeout);
        req->mutable_replication_card_progress_updates()->Reserve(replicationCardIds.size());

        for (const auto& replicationCardId : replicationCardIds) {
            auto& entry = batch.find(replicationCardId)->second;
            EmplaceOrCrash(promises, replicationCardId, entry.ExtractCardPromise());

            auto update = TReplicationCardProgressUpdate();
            update.ReplicationCardId = replicationCardId;
            update.ReplicaProgressUpdates = entry.ExtractProgressByReplicaId();
            update.FetchOptions = entry.ExtractFetchOptions();

            auto* progressUpdate = req->add_replication_card_progress_updates();
            ToProto(progressUpdate->mutable_replication_card_id(), replicationCardId);
            ToProto(progressUpdate, update);
        }

        req->Invoke()
            .SubscribeUnique(BIND(
                [
                    promises = std::move(promises),
                    Logger = Logger
                ] (TErrorOr<TChaosNodeServiceProxy::TRspUpdateMultipleTableProgressesPtr>&& response) {
                    if (!response.IsOK()) {
                        for (const auto& [replicationCardId, promise] : promises) {
                            promise.Set(response);
                        }
                    } else {
                        const auto& responses = response.Value()->replication_card_progress_update_results();
                        for (const auto& response : responses) {
                            auto replicationCardId = FromProto<TReplicationCardId>(response.replication_card_id());
                            auto it = promises.find(replicationCardId);
                            if (it == promises.end()) {
                                YT_LOG_WARNING(
                                    "Received response for unknown replication card (ReplicationCardId: %v)",
                                    replicationCardId);
                                continue;
                            }

                            YT_LOG_DEBUG("Received response for replication card (ReplicationCardId: %v)",
                                replicationCardId);

                            if (response.has_error()) {
                                it->second.Set(FromProto<TError>(response.error()));
                            } else {
                                it->second.Set(FromProto(response.result()));
                            }
                        }
                    }
                }));
    }

    TRepicationCardUpdatesByChaosCells GroupRepicationCardUpdatesByChaosCells(
        const IConnectionPtr& connection,
        const TMultipleReplicationCardProgressesUpdates& batch) const
    {
        const auto residencyCache = connection->GetChaosResidencyCache();

        std::vector<std::pair<TReplicationCardId, TFuture<TCellTag>>> futureTagById;
        std::vector<TFuture<TCellTag>> futures;
        futureTagById.reserve(batch.size());
        futures.reserve(batch.size());

        for (const auto& [replicationCardId, entry] : batch) {
            auto cellTagFuture = residencyCache->GetChaosResidency(replicationCardId);
            futureTagById.emplace_back(replicationCardId, cellTagFuture);
            futures.push_back(std::move(cellTagFuture));
        }

        WaitForFast(AllSet(std::move(futures)))
            .ThrowOnError();

        TRepicationCardUpdatesByChaosCells result;
        for (const auto& [replicationCardId, future] : futureTagById) {
            if (auto cellTagOrError = future.Get(); !cellTagOrError.IsOK()) {
                YT_LOG_DEBUG(cellTagOrError,
                    "Failed to get cell tag for replication card, update skipped (ReplicationCardId: %v)",
                    replicationCardId);

                result.ResolvingErrors.emplace(replicationCardId, cellTagOrError);
            } else {
                result.ReplicationCardIdsByChaosCells[cellTagOrError.Value()].push_back(replicationCardId);
            }
        }

        return result;
    }
};

class TTabNodeUpdatesAccumulatorFlushSession
    : public IReplicationCardUpdatesAccumulatorFlushSession
{
public:
    TTabNodeUpdatesAccumulatorFlushSession(
        IChannelPtr chaosCacheChannel,
        TLogger logger)
        : ChaosCacheChannel_(std::move(chaosCacheChannel))
        , Logger(std::move(logger))
    { }

    void FlushProgressUpdatesBatch(
        const IConnectionPtr& connection,
        TMultipleReplicationCardProgressesUpdates replicationCardProgressUpdatesEntries) const override
    {
        auto timeout = connection->GetConfig()->DefaultChaosNodeServiceTimeout;

        auto proxy = TChaosNodeServiceProxy(ChaosCacheChannel_);
        for (auto& [replicationCardId, entry] : replicationCardProgressUpdatesEntries) {
            auto update = TReplicationCardProgressUpdate();
            update.ReplicationCardId = replicationCardId;
            update.ReplicaProgressUpdates = entry.ExtractProgressByReplicaId();
            update.FetchOptions = entry.ExtractFetchOptions();

            YT_LOG_DEBUG("Sending update for replication card (ReplicationCardId: %v, ProgressUpdates: %v",
                replicationCardId,
                update.ReplicaProgressUpdates);

            auto req = proxy.UpdateTableProgress();
            req->SetTimeout(timeout);
            SetChaosCacheStickyGroupBalancingHint(replicationCardId,
                req->Header().MutableExtension(NRpc::NProto::TBalancingExt::balancing_ext));
            auto* updateItem = req->mutable_replication_card_progress_update();
            ToProto(updateItem, update);

            entry.ExtractCardPromise()
                .SetFrom(req->Invoke()
                    .ApplyUnique(BIND([] (TChaosNodeServiceProxy::TRspUpdateTableProgressPtr&& response) {
                        return FromProto(*response);
                    })));
        }
    }

private:
    const IChannelPtr ChaosCacheChannel_;
    const TLogger Logger;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TMasterCacheReplicationCardUpdatesBatcher
    : public NDetail::TReplicationCardUpdatesBatcherBase
{
public:
    TMasterCacheReplicationCardUpdatesBatcher(
        const TChaosReplicationCardUpdatesBatcherConfigPtr& config,
        IConnectionPtr connection,
        const IInvokerPtr& invoker,
        TLogger logger)
        : TReplicationCardUpdatesBatcherBase(
            config,
            std::move(connection),
            invoker,
            logger)
        , MasterCacheUpdatesAccumulatorFlushSession_(std::move(logger))
    { }

    bool Enabled() const override
    {
        return true;
    }

    void Reconfigure(const TChaosReplicationCardUpdatesBatcherConfigPtr& /*config*/) override
    { }

protected:
    const NDetail::IReplicationCardUpdatesAccumulatorFlushSession& GetFlushSession() const override
    {
        return MasterCacheUpdatesAccumulatorFlushSession_;
    }

private:
    const NDetail::TMasterCacheUpdatesAccumulatorFlushSession MasterCacheUpdatesAccumulatorFlushSession_;
};

class TClientReplicationCardUpdatesBatcher
    : public NDetail::TReplicationCardUpdatesBatcherBase

{
public:
    TClientReplicationCardUpdatesBatcher(
        const TChaosReplicationCardUpdatesBatcherConfigPtr& config,
        IConnectionPtr connection,
        const IInvokerPtr& invoker,
        TLogger logger,
        IChannelPtr chaosCacheChannel)
        : TReplicationCardUpdatesBatcherBase(
            config,
            std::move(connection),
            invoker,
            logger)
        , TabNodeAccumulatorFlushSession_(
            std::move(chaosCacheChannel),
            std::move(logger))
    { }

    bool Enabled() const override
    {
        return Enabled_;
    }

    void Reconfigure(const TChaosReplicationCardUpdatesBatcherConfigPtr& config) override
    {
        if (const auto enableOptional = config->Enable) {
            Enabled_ = *enableOptional;
        }

        SetPeriod(config->FlushPeriod);
    }

protected:
    const NDetail::IReplicationCardUpdatesAccumulatorFlushSession& GetFlushSession() const override
    {
        return TabNodeAccumulatorFlushSession_;
    }

private:
    const NDetail::TTabNodeUpdatesAccumulatorFlushSession TabNodeAccumulatorFlushSession_;
    std::atomic<bool> Enabled_ = false;
};

////////////////////////////////////////////////////////////////////////////////

IReplicationCardUpdatesBatcherPtr CreateMasterCacheReplicationCardUpdatesBatcher(
    const TChaosReplicationCardUpdatesBatcherConfigPtr& config,
    NApi::NNative::IConnectionPtr connection,
    const IInvokerPtr& invoker,
    TLogger logger)
{
    return New<TMasterCacheReplicationCardUpdatesBatcher>(
        config,
        std::move(connection),
        invoker,
        std::move(logger));
}

IReplicationCardUpdatesBatcherPtr CreateClientReplicationCardUpdatesBatcher(
    const TChaosReplicationCardUpdatesBatcherConfigPtr& config,
    NApi::NNative::IConnectionPtr connection,
    TLogger logger)
{
    if (connection->GetStaticConfig()->ReplicationCardCache == nullptr) {
        return nullptr;
    }

    auto invoker = connection->GetInvoker();
    auto chaosCacheChannel = CreateChaosCacheChannel(connection, connection->GetStaticConfig()->ReplicationCardCache);
    return New<TClientReplicationCardUpdatesBatcher>(
        config,
        std::move(connection),
        invoker,
        std::move(logger),
        std::move(chaosCacheChannel));
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilderBase* builder,
    const TReplicaProgressUpdate& replicaProgressUpdates,
    TStringBuf /*spec*/)
{
    builder->AppendFormat("{ReplicaId: %v, ReplicationProgressUpdate: %v}",
        replicaProgressUpdates.ReplicaId,
        replicaProgressUpdates.ReplicationProgressUpdate);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TReplicationCardProgressUpdate& replicationCardProgressUpdate,
    TStringBuf /*spec*/)
{
    builder->AppendFormat("{ReplicationCardId: %v, ReplicationProgressUpdates: %v, FetchOptions: %v}",
        replicationCardProgressUpdate.ReplicationCardId,
        replicationCardProgressUpdate.ReplicaProgressUpdates,
        replicationCardProgressUpdate.FetchOptions);
}

void FormatValue(
    TStringBuilderBase* builder,
    const TReplicationCardProgressUpdatesBatch& replicationCardProgressUpdatesBatch,
    TStringBuf /*spec*/)
{
    builder->AppendFormat("{ReplicationCardProgressUpdates: %v}",
        replicationCardProgressUpdatesBatch.ReplicationCardProgressUpdates);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient

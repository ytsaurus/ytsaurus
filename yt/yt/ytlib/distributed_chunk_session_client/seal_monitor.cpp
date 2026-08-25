#include "seal_monitor.h"

#include "config.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/backoff_strategy.h>

#include <algorithm>
#include <atomic>
#include <deque>
#include <memory>
#include <utility>

namespace NYT::NDistributedChunkSessionClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSealSubscriptionState
    : public TRefCounted
{
    struct TCellState
    {
        std::deque<TChunkId> ReadyChunkIds;
        bool EnqueuedForPolling = false;
    };

    const TDistributedChunkSessionSealedCallback Callback;

    std::atomic<bool> Active = true;

    THashSet<TChunkId> PendingChunkIds;
    THashMap<TCellTag, TCellState> CellStates;

    explicit TSealSubscriptionState(TDistributedChunkSessionSealedCallback callback)
        : Callback(std::move(callback))
    { }
};

using TSealSubscriptionStatePtr = TIntrusivePtr<TSealSubscriptionState>;

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionSealMonitor;

class TDistributedChunkSessionSealSubscription final
    : public IDistributedChunkSessionSealSubscription
{
public:
    TDistributedChunkSessionSealSubscription(
        TWeakPtr<TDistributedChunkSessionSealMonitor> monitor,
        TSealSubscriptionStatePtr subscription)
        : Monitor_(std::move(monitor))
        , Subscription_(std::move(subscription))
    { }

    ~TDistributedChunkSessionSealSubscription() final;

    void TrackChunks(std::vector<TChunkId> chunkIds) final;

private:
    const TWeakPtr<TDistributedChunkSessionSealMonitor> Monitor_;
    const TSealSubscriptionStatePtr Subscription_;
};

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionSealMonitor
    : public IDistributedChunkSessionSealMonitor
{
public:
    TDistributedChunkSessionSealMonitor(
        TDistributedChunkSessionSealMonitorConfigPtr config,
        TDistributedChunkSessionSealSummaryFetchCallback fetchSealSummaries,
        IInvokerPtr invoker,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , FetchSealSummaries_(std::move(fetchSealSummaries))
        , Invoker_(CreateSerializedInvoker(std::move(invoker)))
        , Logger(std::move(logger))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(FetchSealSummaries_);
        YT_VERIFY(Invoker_);
    }

    TDistributedChunkSessionSealSubscriptionPtr Subscribe(
        TDistributedChunkSessionSealedCallback callback) final
    {
        YT_VERIFY(callback);

        auto subscription = New<TSealSubscriptionState>(std::move(callback));
        return std::make_unique<TDistributedChunkSessionSealSubscription>(
            MakeWeak(this),
            std::move(subscription));
    }

    void Reconfigure(TDistributedChunkSessionSealMonitorConfigPtr config) final
    {
        YT_VERIFY(config);
        Invoker_->Invoke(BIND_NO_PROPAGATE(
            &TDistributedChunkSessionSealMonitor::DoReconfigure,
            MakeWeak(this),
            Passed(std::move(config))));
    }

    void TrackChunks(
        const TSealSubscriptionStatePtr& subscription,
        std::vector<TChunkId> chunkIds)
    {
        if (!subscription->Active.load() || chunkIds.empty()) {
            return;
        }

        Invoker_->Invoke(BIND_NO_PROPAGATE(
            &TDistributedChunkSessionSealMonitor::DoTrackChunks,
            MakeWeak(this),
            subscription,
            Passed(std::move(chunkIds))));
    }

    void Unsubscribe(const TSealSubscriptionStatePtr& subscription)
    {
        subscription->Active.store(false);
        Invoker_->Invoke(BIND_NO_PROPAGATE(
            &TDistributedChunkSessionSealMonitor::DoUnsubscribe,
            MakeWeak(this),
            subscription));
    }

private:
    struct TPollEntry
    {
        TSealSubscriptionStatePtr Subscription;
        TChunkId ChunkId;
    };

    struct TDelayedPollBatch
    {
        TInstant ReadyAt;
        std::vector<TPollEntry> Entries;
    };

    struct TCellPollState
    {
        std::deque<TSealSubscriptionStatePtr> SubscriptionsReadyForPolling;
        std::deque<TDelayedPollBatch> DelayedBatches;

        bool FetchInProgress = false;
        TDelayedExecutorCookie PumpCookie;
        i64 PumpGeneration = 0;
        TInstant RetryDeadline;
        TBackoffStrategy ErrorBackoff;

        explicit TCellPollState(const TExponentialBackoffOptions& errorBackoffOptions)
            : ErrorBackoff(errorBackoffOptions)
        { }
    };

    TDistributedChunkSessionSealMonitorConfigPtr Config_;
    const TDistributedChunkSessionSealSummaryFetchCallback FetchSealSummaries_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;

    THashMap<TChunkId, TSealSubscriptionStatePtr> SubscriptionByChunkId_;
    THashMap<TCellTag, std::unique_ptr<TCellPollState>> CellPollStates_;

    void DoTrackChunks(
        const TSealSubscriptionStatePtr& subscription,
        std::vector<TChunkId> chunkIds)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (!subscription->Active.load()) {
            return;
        }

        THashSet<TCellTag> affectedCellTags;
        for (auto chunkId : chunkIds) {
            auto [ownerIt, ownerInserted] = SubscriptionByChunkId_.emplace(chunkId, subscription);
            if (!ownerInserted) {
                YT_VERIFY(!ownerIt->second->Active.load());
                ownerIt->second = subscription;
            }

            YT_VERIFY(subscription->PendingChunkIds.insert(chunkId).second);
            auto cellTag = CellTagFromId(chunkId);
            subscription->CellStates[cellTag].ReadyChunkIds.push_back(chunkId);
            affectedCellTags.insert(cellTag);
        }

        for (auto cellTag : affectedCellTags) {
            EnqueueSubscription(cellTag, subscription);
            ScheduleCellPump(cellTag, TInstant::Now());
        }

        YT_TLOG_DEBUG("Distributed chunk session chunks registered for seal monitoring")
            .With("AddedChunkCount", chunkIds.size())
            .With("PendingChunkCount", subscription->PendingChunkIds.size());
    }

    void DoUnsubscribe(const TSealSubscriptionStatePtr& subscription)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto pendingChunkCount = subscription->PendingChunkIds.size();
        for (auto chunkId : subscription->PendingChunkIds) {
            auto ownerIt = SubscriptionByChunkId_.find(chunkId);
            if (ownerIt != SubscriptionByChunkId_.end() &&
                ownerIt->second == subscription)
            {
                SubscriptionByChunkId_.erase(ownerIt);
            }
        }
        subscription->PendingChunkIds.clear();
        subscription->CellStates.clear();

        YT_TLOG_DEBUG("Distributed chunk session seal monitoring subscription removed")
            .With("DroppedChunkCount", pendingChunkCount);
    }

    void DoReconfigure(TDistributedChunkSessionSealMonitorConfigPtr config)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        Config_ = std::move(config);
        for (const auto& [cellTag, cellState] : CellPollStates_) {
            cellState->ErrorBackoff.UpdateOptions(Config_->ErrorBackoff);
            cellState->ErrorBackoff.Restart();
        }
    }

    TCellPollState& GetOrCreateCellPollState(TCellTag cellTag)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto it = CellPollStates_.find(cellTag);
        if (it == CellPollStates_.end()) {
            it = EmplaceOrCrash(
                CellPollStates_,
                cellTag,
                std::make_unique<TCellPollState>(Config_->ErrorBackoff));
        }
        return *it->second;
    }

    void EnqueueSubscription(
        TCellTag cellTag,
        const TSealSubscriptionStatePtr& subscription)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto& subscriptionCellState = GetOrCrash(subscription->CellStates, cellTag);
        if (subscriptionCellState.EnqueuedForPolling ||
            subscriptionCellState.ReadyChunkIds.empty())
        {
            return;
        }

        subscriptionCellState.EnqueuedForPolling = true;
        GetOrCreateCellPollState(cellTag).SubscriptionsReadyForPolling.push_back(subscription);
    }

    bool IsPollEntryPending(const TPollEntry& entry) const
    {
        const auto& subscription = entry.Subscription;
        if (!subscription->Active.load()) {
            return false;
        }

        YT_VERIFY(subscription->PendingChunkIds.contains(entry.ChunkId));
        YT_VERIFY(GetOrCrash(SubscriptionByChunkId_, entry.ChunkId) == subscription);
        return true;
    }

    void MakePollEntryReady(const TPollEntry& entry)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (!IsPollEntryPending(entry)) {
            return;
        }

        auto cellTag = CellTagFromId(entry.ChunkId);
        GetOrCrash(entry.Subscription->CellStates, cellTag).ReadyChunkIds.push_back(entry.ChunkId);
        EnqueueSubscription(cellTag, entry.Subscription);
    }

    void DelayPollEntries(
        TCellTag cellTag,
        std::vector<TPollEntry> entries,
        TInstant readyAt)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        std::vector<TPollEntry> pendingEntries;
        pendingEntries.reserve(entries.size());
        for (auto& entry : entries) {
            YT_VERIFY(CellTagFromId(entry.ChunkId) == cellTag);
            if (IsPollEntryPending(entry)) {
                pendingEntries.push_back(std::move(entry));
            }
        }

        if (pendingEntries.empty()) {
            return;
        }

        auto& delayedBatches = GetOrCrash(CellPollStates_, cellTag)->DelayedBatches;
        auto insertIt = std::upper_bound(
            delayedBatches.begin(),
            delayedBatches.end(),
            readyAt,
            [] (TInstant lhs, const TDelayedPollBatch& rhs) {
                return lhs < rhs.ReadyAt;
            });
        delayedBatches.insert(insertIt, TDelayedPollBatch{
            .ReadyAt = readyAt,
            .Entries = std::move(pendingEntries),
        });
    }

    void MoveReadyDelayedBatches(TCellTag cellTag)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto& delayedBatches = GetOrCrash(CellPollStates_, cellTag)->DelayedBatches;
        auto now = TInstant::Now();
        while (!delayedBatches.empty() && delayedBatches.front().ReadyAt <= now) {
            auto entries = std::move(delayedBatches.front().Entries);
            delayedBatches.pop_front();
            for (const auto& entry : entries) {
                MakePollEntryReady(entry);
            }
        }
    }

    void ScheduleCellPump(TCellTag cellTag, TInstant deadline)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto& cellState = *GetOrCrash(CellPollStates_, cellTag);
        if (cellState.FetchInProgress) {
            return;
        }

        deadline = std::max(deadline, cellState.RetryDeadline);
        TDelayedExecutor::CancelAndClear(cellState.PumpCookie);
        auto generation = ++cellState.PumpGeneration;
        cellState.PumpCookie = TDelayedExecutor::Submit(
            BIND_NO_PROPAGATE(
                &TDistributedChunkSessionSealMonitor::OnCellPump,
                MakeWeak(this),
                cellTag,
                generation),
            deadline,
            Invoker_);
    }

    void OnCellPump(TCellTag cellTag, i64 generation)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto& cellState = *GetOrCrash(CellPollStates_, cellTag);
        if (generation != cellState.PumpGeneration) {
            return;
        }

        cellState.PumpCookie.Reset();
        PumpCell(cellTag);
    }

    void PumpCell(TCellTag cellTag)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto& cellState = *GetOrCrash(CellPollStates_, cellTag);
        if (cellState.FetchInProgress) {
            return;
        }

        MoveReadyDelayedBatches(cellTag);
        auto now = TInstant::Now();
        if (cellState.RetryDeadline > now) {
            ScheduleCellPump(cellTag, cellState.RetryDeadline);
            return;
        }
        auto entries = SelectPollEntries(cellTag);
        if (entries.empty()) {
            if (!cellState.DelayedBatches.empty()) {
                ScheduleCellPump(cellTag, cellState.DelayedBatches.front().ReadyAt);
            }
            return;
        }

        std::vector<TChunkId> chunkIds;
        chunkIds.reserve(entries.size());
        for (const auto& entry : entries) {
            YT_VERIFY(CellTagFromId(entry.ChunkId) == cellTag);
            chunkIds.push_back(entry.ChunkId);
        }
        YT_VERIFY(std::ssize(chunkIds) <= Config_->MaxChunksPerFetch);

        cellState.FetchInProgress = true;

        TFuture<std::vector<TDistributedChunkSessionSealSummary>> fetchFuture;
        try {
            fetchFuture = FetchSealSummaries_(std::move(chunkIds));
        } catch (const std::exception& ex) {
            fetchFuture = MakeFuture<std::vector<TDistributedChunkSessionSealSummary>>(TError(ex));
        }

        fetchFuture.Subscribe(BIND_NO_PROPAGATE(
            &TDistributedChunkSessionSealMonitor::OnSealSummariesFetched,
            MakeWeak(this),
            cellTag,
            Passed(std::move(entries)))
            .Via(Invoker_));
    }

    std::vector<TPollEntry> SelectPollEntries(TCellTag cellTag)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto& subscriptions = GetOrCrash(CellPollStates_, cellTag)->SubscriptionsReadyForPolling;
        std::vector<TPollEntry> entries;
        entries.reserve(Config_->MaxChunksPerFetch);

        while (std::ssize(entries) < Config_->MaxChunksPerFetch &&
            !subscriptions.empty())
        {
            auto subscription = std::move(subscriptions.front());
            subscriptions.pop_front();

            auto subscriptionCellIt = subscription->CellStates.find(cellTag);
            if (subscriptionCellIt == subscription->CellStates.end()) {
                continue;
            }

            auto& subscriptionCellState = subscriptionCellIt->second;
            YT_VERIFY(std::exchange(subscriptionCellState.EnqueuedForPolling, false));

            if (!subscription->Active.load()) {
                subscriptionCellState.ReadyChunkIds.clear();
                continue;
            }

            if (subscriptionCellState.ReadyChunkIds.empty()) {
                continue;
            }

            auto chunkId = subscriptionCellState.ReadyChunkIds.front();
            subscriptionCellState.ReadyChunkIds.pop_front();
            YT_VERIFY(CellTagFromId(chunkId) == cellTag);
            TPollEntry entry{
                .Subscription = subscription,
                .ChunkId = chunkId,
            };
            if (IsPollEntryPending(entry)) {
                entries.push_back(std::move(entry));
            }

            EnqueueSubscription(cellTag, subscription);
        }

        return entries;
    }

    void OnSealSummariesFetched(
        TCellTag cellTag,
        std::vector<TPollEntry> entries,
        const TErrorOr<std::vector<TDistributedChunkSessionSealSummary>>& summariesOrError) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto& cellState = *GetOrCrash(CellPollStates_, cellTag);
        YT_VERIFY(std::exchange(cellState.FetchInProgress, false));

        if (!summariesOrError.IsOK()) {
            for (const auto& entry : entries) {
                MakePollEntryReady(entry);
            }
            cellState.ErrorBackoff.Next();
            auto backoff = cellState.ErrorBackoff.GetBackoff();
            cellState.RetryDeadline = TInstant::Now() + backoff;

            YT_TLOG_WARNING("Failed to fetch distributed chunk session seal summaries; retrying")
                .With("CellTag", cellTag)
                .With("ChunkCount", entries.size())
                .With("Backoff", backoff)
                .With(static_cast<const TError&>(summariesOrError));

            ScheduleCellPump(cellTag, cellState.RetryDeadline);
            return;
        }

        cellState.ErrorBackoff.Restart();
        cellState.RetryDeadline = {};
        auto unsealedEntries = ProcessSealSummaries(entries, summariesOrError.Value());
        DelayPollEntries(
            cellTag,
            std::move(unsealedEntries),
            TInstant::Now() + Config_->PollPeriod);

        ScheduleCellPump(cellTag, TInstant::Now());
    }

    std::vector<TPollEntry> ProcessSealSummaries(
        const std::vector<TPollEntry>& entries,
        const std::vector<TDistributedChunkSessionSealSummary>& summaries) noexcept
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        THashMap<TChunkId, TDistributedChunkSessionSealSummary> summaryByChunkId;
        for (const auto& summary : summaries) {
            EmplaceOrCrash(summaryByChunkId, summary.ChunkId, summary);
        }

        THashMap<TSealSubscriptionStatePtr, std::vector<TDistributedChunkSessionSealSummary>> deliveries;
        std::vector<TPollEntry> unsealedEntries;
        int matchedSummaryCount = 0;
        int sealedChunkCount = 0;
        int staleChunkCount = 0;
        for (const auto& entry : entries) {
            auto summaryIt = summaryByChunkId.find(entry.ChunkId);
            bool hasSealSummary = summaryIt != summaryByChunkId.end();
            if (hasSealSummary) {
                ++matchedSummaryCount;
            }

            const auto& subscription = entry.Subscription;
            if (!IsPollEntryPending(entry)) {
                ++staleChunkCount;
                continue;
            }

            if (!hasSealSummary) {
                unsealedEntries.push_back(entry);
                continue;
            }

            YT_VERIFY(subscription->PendingChunkIds.erase(entry.ChunkId) == 1);
            YT_VERIFY(SubscriptionByChunkId_.erase(entry.ChunkId) == 1);
            deliveries[subscription].push_back(summaryIt->second);
            ++sealedChunkCount;
        }
        YT_VERIFY(matchedSummaryCount == std::ssize(summaryByChunkId));

        for (auto& [subscription, results] : deliveries) {
            if (!subscription->Active.load()) {
                continue;
            }

            i64 resultCount = std::ssize(results);
            try {
                subscription->Callback(std::move(results));
            } catch (const std::exception& ex) {
                YT_TLOG_WARNING("Distributed chunk session seal callback failed")
                    .With("ResultCount", resultCount)
                    .With(TError(ex));
            }
        }

        YT_TLOG_DEBUG("Distributed chunk session seal summaries processed")
            .With("PolledChunkCount", entries.size())
            .With("FetchedSealSummaryCount", summaries.size())
            .With("SealedChunkCount", sealedChunkCount)
            .With("UnsealedChunkCount", unsealedEntries.size())
            .With("StaleChunkCount", staleChunkCount)
            .With("SubscriptionCount", deliveries.size());

        return unsealedEntries;
    }
};

TDistributedChunkSessionSealSubscription::~TDistributedChunkSessionSealSubscription()
{
    if (auto monitor = Monitor_.Lock()) {
        monitor->Unsubscribe(Subscription_);
    }
}

void TDistributedChunkSessionSealSubscription::TrackChunks(
    std::vector<TChunkId> chunkIds)
{
    if (auto monitor = Monitor_.Lock()) {
        monitor->TrackChunks(Subscription_, std::move(chunkIds));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionSealMonitorPtr CreateDistributedChunkSessionSealMonitor(
    TDistributedChunkSessionSealMonitorConfigPtr config,
    TDistributedChunkSessionSealSummaryFetchCallback fetchSealSummaries,
    IInvokerPtr invoker,
    NLogging::TLogger logger)
{
    return New<TDistributedChunkSessionSealMonitor>(
        std::move(config),
        std::move(fetchSealSummaries),
        std::move(invoker),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient

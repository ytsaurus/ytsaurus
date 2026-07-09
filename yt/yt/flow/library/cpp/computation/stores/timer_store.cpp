#include "timer_store.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/inflight_tracker.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/flow/library/cpp/common/message_migration.h>

#include <yt/yt/flow/library/cpp/tables/timers.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>
#include <library/cpp/iterator/iterate_values.h>

#include <yt/yt/client/table_client/logical_type.h>

#include <util/digest/multi.h>
#include <util/generic/hash.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TMessageHashMapOpsByStreamAndKeyAndTriggerTs
{
    bool operator()(const TInputTimerConstPtr& a, const TInputTimerConstPtr& b) const
    {
        return std::tie(a->StreamId, a->Key, a->TriggerTimestamp) == std::tie(b->StreamId, b->Key, b->TriggerTimestamp);
    }

    size_t operator()(const TInputTimerConstPtr& a) const
    {
        return MultiHash(a->StreamId, a->Key, a->TriggerTimestamp);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCompareTimerByTriggerTimestampAndMessageId
{
    bool operator()(const TInputTimerConstPtr& a, const TInputTimerConstPtr& b) const
    {
        return std::tie(a->TriggerTimestamp, a->MessageId) < std::tie(b->TriggerTimestamp, b->MessageId);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTimerStore
    : public ITimerStore
{
public:
    explicit TTimerStore(TTimerStoreContextPtr context, TDynamicTimerStoreContextPtr dynamicContext)
        : Context_(std::move(context))
        , Table_(Context_->TimersTable)
        , Logger(Context_->Logger)
        , InflightStore_(New<TMultiInflightTracker>(Context_->Profiler.WithPrefix("/timer_streams"), GetKeys(Context_->TimerSpecs), Context_->WatermarkPercentileSpec))
    {
        YT_VERIFY(Table_);
        Reconfigure(std::move(dynamicContext));
    }

    void Reconfigure(TDynamicTimerStoreContextPtr dynamicContext) override
    {
        Draining_ = dynamicContext->Draining;
        Table_->Reconfigure(dynamicContext->DynamicTimerStoreSpec->TableRequest);
    }

    std::vector<TInputTimerConstPtr> GetNextBatch(const THashSet<TStreamId>& allowedStreams, i64 maxRows, i64 maxByteSize) override
    {
        if (Draining_) {
            return {};
        }

        using TPriority = std::pair<TSystemTimestamp, const TMessageId&>;
        std::vector<std::pair<TSortedByTriggerTimestampTimerSet*, std::function<TPriority()>>> triggeredTimers;
        triggeredTimers.reserve(TriggeredTimers_.size());
        for (auto& [streamId, timers] : TriggeredTimers_) {
            if (allowedStreams.contains(streamId)) {
                triggeredTimers.emplace_back(&timers, [timersPtr = &timers] () -> TPriority {
                    YT_ASSERT(!timersPtr->empty());
                    const auto& t = *timersPtr->begin();
                    return {t->TriggerTimestamp, t->MessageId};
                });
            }
        }

        std::vector<TInputTimerConstPtr> timers;
        timers.reserve(maxRows);
        TMessageBatchLimiter limiter(maxRows, maxByteSize);
        MergingExtractBatch(
            std::move(triggeredTimers),
            [] (const TInputTimerConstPtr& timer) -> const TInputTimerConstPtr& {
                return timer;
            },
            limiter,
            [&] (const TInputTimerConstPtr& timer) {
                // Remove extracted timer from deduplication index so that
                // newly registered timers with the same (Key, TriggerTimestamp)
                // do not evict an already-extracted timer.
                UnregisterTimerKeyIndex(timer);
                timers.push_back(timer);
            });

        return timers;
    }

    void UpdateWatermarkState(TWatermarkStatePtr watermarkState) override
    {
        WatermarkState_ = std::move(watermarkState);
        TriggerTimers();
    }

    void RegisterTimerKeyIndex(const TInputTimerConstPtr& timer)
    {
        if (!GetOrCrash(Context_->TimerSpecs, timer->StreamId)->DeduplicateEqualTimestamps) {
            return;
        }

        auto [it, emplaced] = TimerDeduplicationState_.emplace(timer);

        if (emplaced) {
            return;
        }

        TInputTimerConstPtr timerToUnregister = timer;
        if (timerToUnregister->EventTimestamp < (*it)->EventTimestamp) {
            timerToUnregister = *it;
            // Replace timer in TimerDeduplicationState_.
            TimerDeduplicationState_.erase(it);
            TimerDeduplicationState_.emplace(timer);
        }
        Unregister(std::vector<TInputTimerConstPtr>{timerToUnregister});
        Deduplicated_.insert(timerToUnregister);
    }

    void UnregisterTimerKeyIndex(const TInputTimerConstPtr& timer)
    {
        // Idempotent: if the timer is not in the index (e.g. already removed
        // during extract in GetNextBatch), do nothing.
        // Do not check DeduplicateEqualTimestamps, because erasing from empty map is cheap and no-op.
        auto it = TimerDeduplicationState_.find(timer);
        if (it == TimerDeduplicationState_.end() || (*it)->MessageId != timer->MessageId) {
            return;
        }
        TimerDeduplicationState_.erase(it);
    }

    void Register(std::vector<TTimer>&& timers) override
    {
        for (auto& timer : timers) {
            if (!Context_->TimerSpecs.contains(timer.StreamId)) {
                THROW_ERROR_EXCEPTION("Unknown timer stream %Qv during register",
                    timer.StreamId);
            }

            auto inputTimer = New<TInputTimer>(std::move(timer), Context_->KeySchema);
            InflightStore_->Register(inputTimer);

            SortedTimers_[inputTimer->StreamId].insert(inputTimer);

            YT_LOG_DEBUG("MessageLifeCycle.TimerStore: timer was registered (MessageId: %v, StreamId: %v)",
                inputTimer->MessageId,
                inputTimer->StreamId);
            ModificationQueue_.emplace_back(inputTimer, true);

            RegisterTimerKeyIndex(inputTimer);
        }
        InflightStore_->SyncCounters();
    }

    void Unregister(const std::vector<TInputTimerConstPtr>& timers) override
    {
        for (const auto& timer : timers) {
            if (Deduplicated_.contains(timer)) {
                // Already unregistered.
                continue;
            }
            if (!Context_->TimerSpecs.contains(timer->StreamId)) {
                THROW_ERROR_EXCEPTION("Unknown timer stream %Qv during unregister",
                    timer->StreamId);
            }
            InflightStore_->Unregister(*timer);
            ModificationQueue_.emplace_back(timer, false);
            UnregisterTimerKeyIndex(timer);
            SortedTimers_[timer->StreamId].erase(timer);
            TriggeredTimers_[timer->StreamId].erase(timer);
            YT_LOG_DEBUG("MessageLifeCycle.TimerStore: timer was unregistered (MessageId: %v, StreamId: %v)",
                timer->MessageId,
                timer->StreamId);
        }
        InflightStore_->SyncCounters();
    }

    void RegisterLoaded(std::vector<TTimer> timers)
    {
        NConcurrency::TForbidContextSwitchGuard contextSwitchGuard;
        for (auto& timer : timers) {
            timer.KeySchema = Context_->KeySchema;
        }
        YT_LOG_DEBUG("Timers loaded (Count: %v)",
            timers.size());
        Register(std::move(timers));
        ModificationQueue_.clear();
        YT_LOG_DEBUG("Timer store init completed");
    }

    TFuture<void> Init() override
    {
        auto loadTimers = Table_->LoadAll({
            .ComputationId = Context_->Partition->ComputationId,
            .LowerKey = Context_->Partition->LowerKey,
            .ExactKey = Context_->Partition->SourceKey,
            .UpperKey = Context_->Partition->UpperKey,
        });
        return loadTimers.AsUnique().Apply(BIND([weakThis = MakeWeak(this)] (std::vector<std::pair<NTables::TTimers::TTableKey, TTimer>>&& keyedTimers) {
            if (auto strongThis = weakThis.Lock()) {
                std::vector<TTimer> timers;
                timers.reserve(keyedTimers.size());
                for (auto& [tableKey, timer] : keyedTimers) {
                    timers.push_back(std::move(timer));
                }
                strongThis->RegisterLoaded(timers);
            } else {
                THROW_ERROR_EXCEPTION("Interrupted");
            }
        }));
    }

    void Sync(NApi::IDynamicTableTransactionPtr tx) override
    {
        DeduplicateModificationQueue();
        std::vector<TTimer> toPersist;
        std::vector<NTables::TTimers::TTableKey> toErase;
        for (const auto& [timer, add] : ModificationQueue_) {
            if (add) {
                toPersist.push_back(TTimer(*timer));
            } else {
                toErase.push_back({
                    .ComputationId = Context_->Partition->ComputationId,
                    .Key = timer->Key,
                    .MessageId = timer->MessageId,
                });
            }
        }
        Table_->Write(tx, Context_->Partition->ComputationId, toPersist);
        Table_->Erase(tx, toErase);
        Deduplicated_.clear();
        ModificationQueue_.clear();
    }

    i64 GetByteSize() const override
    {
        return InflightStore_->GetTotalByteSize();
    }

    i64 GetCount() const override
    {
        return InflightStore_->GetTotalCount();
    }

    THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() override
    {
        auto inflights = InflightStore_->BuildInflights();
        for (auto& [_, inflight] : inflights) {
            inflight->Suspended = Draining_;
        }
        return inflights;
    }

private:
    using TSortedByTriggerTimestampTimerSet = std::set<TInputTimerConstPtr, TCompareTimerByTriggerTimestampAndMessageId>;
    using TTimerSet = absl::flat_hash_set<TInputTimerConstPtr, TMessageHashMapOpsByMessageId, TMessageHashMapOpsByMessageId>;

    const TTimerStoreContextPtr Context_;
    const NTables::ITimersPtr Table_;
    const NLogging::TLogger Logger;
    // Do not increase due to possible problems with transactions.
    const TMultiInflightTrackerPtr InflightStore_;
    bool Draining_ = false;

    TWatermarkStatePtr WatermarkState_;

    // Used only if deduplication is enabled.
    absl::flat_hash_set<TInputTimerConstPtr, TMessageHashMapOpsByStreamAndKeyAndTriggerTs, TMessageHashMapOpsByStreamAndKeyAndTriggerTs> TimerDeduplicationState_;

    THashMap<TStreamId, TSortedByTriggerTimestampTimerSet> SortedTimers_;
    THashMap<TStreamId, TSortedByTriggerTimestampTimerSet> TriggeredTimers_;

    TTimerSet Deduplicated_;
    std::deque<std::pair<TInputTimerConstPtr, bool>> ModificationQueue_;

private:
    void TriggerTimers()
    {
        for (auto& [timerStreamId, timers] : SortedTimers_) {
            auto timerSpec = GetOrCrash(Context_->TimerSpecs, timerStreamId);
            TSystemTimestamp watermark = InfinitySystemTimestamp;
            if (timerSpec->StreamsWithDelays) {
                for (const auto& [streamId, delay] : *timerSpec->StreamsWithDelays) {
                    auto streamWatermark = WatermarkState_->GetWatermark(streamId, timerSpec->TimeType);
                    auto delayedWatermark = TSystemTimestamp(std::max(streamWatermark.Underlying(), delay.Seconds()) - delay.Seconds());
                    watermark = std::min(watermark, delayedWatermark);
                }
            } else if (timerSpec->Streams) {
                for (const auto& streamId : *timerSpec->Streams) {
                    watermark = std::min(watermark, WatermarkState_->GetWatermark(streamId, timerSpec->TimeType));
                }
            } else {
                for (const auto& streamId : Context_->StreamsDependency.at(timerStreamId)) {
                    watermark = std::min(watermark, WatermarkState_->GetWatermark(streamId, timerSpec->TimeType));
                }
            }

            auto& triggeredTimers = TriggeredTimers_[timerStreamId];
            while (!timers.empty() && (*timers.begin())->TriggerTimestamp <= watermark) {
                const auto& timer = *timers.begin();
                YT_LOG_DEBUG("MessageLifeCycle.TimerStore: timer was triggered (MessageId: %v, StreamId: %v)",
                    timer->MessageId,
                    timer->StreamId);
                triggeredTimers.insert(timer);
                timers.erase(timers.begin());
            }
        }
    }

    void DeduplicateModificationQueue()
    {
        absl::flat_hash_map<TInputTimerConstPtr, bool, TMessageHashMapOpsByMessageId, TMessageHashMapOpsByMessageId> finalState;
        for (const auto& [timer, state] : ModificationQueue_) {
            if (!finalState.contains(timer)) {
                finalState[timer] = state;
            } else {
                YT_VERIFY(finalState[timer] == true);
                YT_VERIFY(state == false);
                finalState.erase(timer);
            }
        }
        std::deque<std::pair<TInputTimerConstPtr, bool>> filteredModificationQueue;
        for (const auto& [timer, state] : ModificationQueue_) {
            if (auto iter = finalState.find(timer); iter != finalState.end() && iter->second == state) {
                filteredModificationQueue.push_back({timer, state});
            }
        }
        ModificationQueue_ = std::move(filteredModificationQueue);
    }
};

////////////////////////////////////////////////////////////////////////////////

ITimerStorePtr CreateTimerStore(TTimerStoreContextPtr context, TDynamicTimerStoreContextPtr dynamicContext)
{
    return New<TTimerStore>(std::move(context), std::move(dynamicContext));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

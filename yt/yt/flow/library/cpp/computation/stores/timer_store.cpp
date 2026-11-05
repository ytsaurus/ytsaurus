#include "timer_store.h"

#include "paged_timer_store.h"
#include "timer_page_catalogue.h"
#include "timer_store_conversion.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/inflight_tracker.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/flow/library/cpp/common/message_migration.h>

#include <yt/yt/flow/library/cpp/tables/timer_pages.h>
#include <yt/yt/flow/library/cpp/tables/timers.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>
#include <library/cpp/iterator/iterate_values.h>

#include <yt/yt/client/table_client/logical_type.h>

#include <util/digest/multi.h>
#include <util/generic/hash.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TFuture<void> ITimerStore::Preload()
{
    return OKFuture;
}

bool ITimerStore::IsEmpty() const
{
    return GetCount() == 0;
}

i64 ITimerStore::GetInMemoryByteSize() const
{
    return GetByteSize();
}

i64 ITimerStore::GetInMemoryCount() const
{
    return GetCount();
}

i64 ITimerStore::GetByteSize() const
{
    return GetInMemoryByteSize();
}

using namespace NConcurrency;

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

std::vector<std::vector<TStreamId>> BuildTimerStreamGroups(const TTimerTriggerConditions& conditions)
{
    THashMap<std::pair<ETimeType, TStreamId>, TStreamId> firstTimerByCondition;
    THashMap<TStreamId, std::vector<TStreamId>> neighbours;
    for (const auto& [timerId, condition] : conditions) {
        neighbours[timerId];
        auto addCondition = [&] (const TStreamId& inputId) {
            auto [it, inserted] = firstTimerByCondition.emplace(std::pair(condition.TimeType, inputId), timerId);
            if (!inserted && it->second != timerId) {
                neighbours[timerId].push_back(it->second);
                neighbours[it->second].push_back(timerId);
            }
        };
        if (condition.TimeType == ETimeType::CurrentTime) {
            addCondition(TStreamId());
        } else {
            for (const auto& [inputId, delay] : condition.StreamsWithDelays) {
                addCondition(inputId);
            }
        }
    }

    std::vector<std::vector<TStreamId>> groups;
    THashSet<TStreamId> visited;
    for (const auto& [timerId, adjacent] : neighbours) {
        if (!visited.insert(timerId).second) {
            continue;
        }
        auto& group = groups.emplace_back();
        group.push_back(timerId);
        for (int index = 0; index < std::ssize(group); ++index) {
            for (const auto& neighbour : GetOrCrash(neighbours, group[index])) {
                if (visited.insert(neighbour).second) {
                    group.push_back(neighbour);
                }
            }
        }
    }
    return groups;
}

////////////////////////////////////////////////////////////////////////////////

class TTimerStore
    : public ITimerStore
{
public:
    explicit TTimerStore(TTimerStoreContextPtr context, TDynamicTimerStoreContextPtr dynamicContext)
        : Context_(std::move(context))
        , TriggerConditions_(BuildTimerTriggerConditions(Context_->TimerSpecs, Context_->StreamsDependency))
        , TimerStreamGroups_(BuildTimerStreamGroups(TriggerConditions_))
        , Table_(Context_->TimersTable)
        , Logger(Context_->Logger)
        , InflightStore_(New<TMultiInflightTracker>(
            Context_->Profiler.WithPrefix("/timer_streams"),
            GetKeys(Context_->TimerSpecs),
            Context_->WatermarkPercentileSpec,
            TStreamLimitUsageStateMap{}))
    {
        YT_VERIFY(Table_);
        Reconfigure(std::move(dynamicContext));
    }

    void Reconfigure(TDynamicTimerStoreContextPtr dynamicContext) override
    {
        DynamicContext_ = dynamicContext;
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
        i64 reserveSize = 0;
        for (const auto& group : TimerStreamGroups_) {
            bool groupAllowed = std::all_of(group.begin(), group.end(), [&] (const TStreamId& streamId) {
                return allowedStreams.contains(streamId);
            });
            if (!groupAllowed) {
                continue;
            }
            for (const auto& streamId : group) {
                auto it = TriggeredTimers_.find(streamId);
                if (it == TriggeredTimers_.end()) {
                    continue;
                }
                auto* timers = &it->second;
                reserveSize += std::min<i64>(std::ssize(*timers), maxRows - reserveSize);
                triggeredTimers.emplace_back(timers, [timers] () -> TPriority {
                    YT_ASSERT(!timers->empty());
                    const auto& t = *timers->begin();
                    return {t->TriggerTimestamp, t->MessageId};
                });
            }
        }

        std::vector<TInputTimerConstPtr> timers;
        timers.reserve(reserveSize);
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
                InflightStore_->MarkTaken(*timer);
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
        DoUnregister(std::vector<TInputTimerConstPtr>{timerToUnregister});
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
        RegisterImpl(std::move(timers), /*restored*/ false);
    }

    void RegisterImpl(std::vector<TTimer>&& timers, bool restored)
    {
        for (auto& timer : timers) {
            if (!Context_->TimerSpecs.contains(timer.StreamId)) {
                THROW_ERROR_EXCEPTION("Unknown timer stream %Qv during register",
                    timer.StreamId);
            }

            auto inputTimer = New<TInputTimer>(std::move(timer), Context_->KeySchema);
            if (restored) {
                InflightStore_->Restore(inputTimer);
            } else {
                InflightStore_->Register(inputTimer);
            }

            SortedTimers_[inputTimer->StreamId].insert(inputTimer);

            YT_TLOG_DEBUG("MessageLifeCycle.TimerStore: timer was registered")
                .With("MessageId", inputTimer->MessageId)
                .With("StreamId", inputTimer->StreamId);
            ModificationQueue_.emplace_back(inputTimer, true);

            RegisterTimerKeyIndex(inputTimer);
        }
        InflightStore_->SyncCounters();
    }

    void Unregister(const std::vector<TInputTimerConstPtr>& timers) override
    {
        DoUnregister(timers);
    }

    void DoUnregister(const std::vector<TInputTimerConstPtr>& timers)
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
            YT_TLOG_DEBUG("MessageLifeCycle.TimerStore: timer was unregistered")
                .With("MessageId", timer->MessageId)
                .With("StreamId", timer->StreamId);
        }
        InflightStore_->SyncCounters();
    }

    void RegisterLoaded(std::vector<TTimer> timers)
    {
        NConcurrency::TForbidContextSwitchGuard contextSwitchGuard;
        for (auto& timer : timers) {
            timer.KeySchema = Context_->KeySchema;
        }
        YT_TLOG_DEBUG("Timers loaded")
            .With("Count", timers.size());
        RegisterImpl(std::move(timers), /*restored*/ true);
        ModificationQueue_.clear();
        YT_TLOG_DEBUG("Timer store init completed");
    }

    TFuture<void> Preload() override
    {
        return OKFuture;
    }

    TFuture<void> Init() override
    {
        return BIND(&TTimerStore::ExportPages, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run()
            .Apply(BIND(&TTimerStore::Load, MakeStrong(this)));
    }

    //! Timers left in pages by a switch from the paged backend go back to the |timers| table.
    void ExportPages()
    {
        if (Context_->TimerSpecs.empty() || !Context_->PageCatalogue || !Context_->TimerPagesTable) {
            return;
        }
        if (!WaitFor(Context_->TimerPagesTable->TablesExist()).ValueOrThrow()) {
            return;
        }
        // A direct probe rather than the worker catalogue: a pipeline that never used the paged
        // backend does not depend on the catalogue then. Empty pages count as well: an export
        // leaves none behind.
        auto range = GetPartitionKeyRange(*Context_->Partition);
        auto pages = WaitFor(Context_->TimerPagesTable->LoadMeta({.ComputationId = Context_->Partition->ComputationId}))
            .ValueOrThrow();
        bool hasPages = std::any_of(pages.begin(), pages.end(), [&] (const TTimerPageMeta& page) {
            return Context_->TimerSpecs.contains(page.StreamId) && page.KeyRange.Lower < range.Upper && range.Lower < page.KeyRange.Upper;
        });
        if (!hasPages) {
            return;
        }
        YT_TLOG_INFO("Exporting timer pages into the timers table");
        // The export extracts the timers through #GetNextBatch(), which draining would block.
        auto dynamicContext = New<TDynamicTimerStoreContext>();
        dynamicContext->DynamicTimerStoreSpec = DynamicContext_->DynamicTimerStoreSpec;
        dynamicContext->MaxRowsPerBatch = DynamicContext_->MaxRowsPerBatch;
        auto pagedStore = CreatePagedTimerStore(Context_, dynamicContext);
        pagedStore->InitSync();
        ExportPagesIntoTimers(Context_, pagedStore);
    }

    TFuture<void> Load()
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

    void Commit() override
    {
        InflightStore_->Commit();
    }

    bool IsEmpty() const override
    {
        return InflightStore_->GetTotalCount() == 0;
    }

    i64 GetCount() const override
    {
        return InflightStore_->GetTotalCount();
    }

    i64 GetInMemoryByteSize() const override
    {
        return InflightStore_->GetTotalByteSize();
    }

    i64 GetInMemoryCount() const override
    {
        return InflightStore_->GetTotalCount();
    }

    THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() override
    {
        auto inflights = InflightStore_->BuildInflights();
        for (auto& [streamId, inflight] : inflights) {
            inflight->Suspended = Draining_;
            inflight->MinTriggerTimestamp = GetMinTriggerTimestamp(streamId);
        }
        return inflights;
    }

private:
    using TSortedByTriggerTimestampTimerSet = std::set<TInputTimerConstPtr, TCompareTimerByTriggerTimestampAndMessageId>;
    using TTimerSet = absl::flat_hash_set<TInputTimerConstPtr, TMessageHashMapOpsByMessageId, TMessageHashMapOpsByMessageId>;

    const TTimerStoreContextPtr Context_;
    const TTimerTriggerConditions TriggerConditions_;
    const std::vector<std::vector<TStreamId>> TimerStreamGroups_;
    const NTables::ITimersPtr Table_;
    const NLogging::TLogger Logger;
    // Do not increase due to possible problems with transactions.
    const TMultiInflightTrackerPtr InflightStore_;
    TDynamicTimerStoreContextPtr DynamicContext_;
    bool Draining_ = false;

    TWatermarkStatePtr WatermarkState_;

    // Used only if deduplication is enabled.
    absl::flat_hash_set<TInputTimerConstPtr, TMessageHashMapOpsByStreamAndKeyAndTriggerTs, TMessageHashMapOpsByStreamAndKeyAndTriggerTs> TimerDeduplicationState_;

    THashMap<TStreamId, TSortedByTriggerTimestampTimerSet> SortedTimers_;
    THashMap<TStreamId, TSortedByTriggerTimestampTimerSet> TriggeredTimers_;

    TTimerSet Deduplicated_;
    std::deque<std::pair<TInputTimerConstPtr, bool>> ModificationQueue_;

private:
    // Taken timers are not tracked: callers unregister a taken batch before building inflights.
    std::optional<TSystemTimestamp> GetMinTriggerTimestamp(const TStreamId& streamId) const
    {
        std::optional<TSystemTimestamp> result;
        for (const auto* timerSets : {&SortedTimers_, &TriggeredTimers_}) {
            auto it = timerSets->find(streamId);
            if (it == timerSets->end() || it->second.empty()) {
                continue;
            }

            auto triggerTimestamp = (*it->second.begin())->TriggerTimestamp;
            result = result ? std::min(*result, triggerTimestamp) : triggerTimestamp;
        }

        return result;
    }

    void TriggerTimers()
    {
        bool offered = false;
        for (auto& [timerStreamId, timers] : SortedTimers_) {
            const auto& condition = GetOrCrash(TriggerConditions_, timerStreamId);
            TSystemTimestamp watermark = InfinitySystemTimestamp;
            if (condition.TimeType == ETimeType::CurrentTime) {
                // Watched streams and their delays are stream-time coordinates and do not apply to current time.
                watermark = WatermarkState_->GetCurrentTimestamp();
            } else {
                for (const auto& [streamId, delay] : condition.StreamsWithDelays) {
                    auto streamWatermark = WatermarkState_->GetWatermark(streamId, condition.TimeType);
                    auto delayedWatermark = TSystemTimestamp(std::max(streamWatermark.Underlying(), delay.Seconds()) - delay.Seconds());
                    watermark = std::min(watermark, delayedWatermark);
                }
            }

            auto& triggeredTimers = TriggeredTimers_[timerStreamId];
            while (!timers.empty() && (*timers.begin())->TriggerTimestamp <= watermark) {
                const auto& timer = *timers.begin();
                YT_TLOG_DEBUG("MessageLifeCycle.TimerStore: timer was triggered")
                    .With("MessageId", timer->MessageId)
                    .With("StreamId", timer->StreamId);
                triggeredTimers.insert(timer);
                InflightStore_->MarkOffered(*timer);
                offered = true;
                timers.erase(timers.begin());
            }
        }
        if (offered) {
            InflightStore_->SyncCounters();
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
    switch (context->Backend) {
        case ETimerStoreBackend::InMemory:
            return New<TTimerStore>(std::move(context), std::move(dynamicContext));
        case ETimerStoreBackend::Paged:
            return CreatePagedTimerStore(std::move(context), std::move(dynamicContext));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

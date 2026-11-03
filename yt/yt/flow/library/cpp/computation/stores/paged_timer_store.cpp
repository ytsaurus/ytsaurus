#include "paged_timer_store.h"

#include "timer_page_catalogue.h"
#include "timer_page_data.h"
#include "timer_store_conversion.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>
#include <yt/yt/flow/library/cpp/common/traverse.h>

#include <yt/yt/flow/library/cpp/misc/counter.h>
#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/flow/library/cpp/tables/timer_pages.h>
#include <yt/yt/flow/library/cpp/tables/transaction_manager.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <algorithm>
#include <limits>
#include <map>
#include <queue>
#include <ranges>
#include <set>
#include <tuple>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TKeyRange GetPartitionKeyRange(const TPartition& partition)
{
    if (partition.SourceKey) {
        // The successor of the exact key: the key followed by a Max sentinel.
        TUnversionedOwningRowBuilder builder;
        for (auto value : partition.SourceKey->Underlying().Get()) {
            builder.AddValue(value);
        }
        builder.AddValue(MakeUnversionedSentinelValue(EValueType::Max));
        return TKeyRange{
            .Lower = *partition.SourceKey,
            .Upper = TKey(TCompactUnversionedOwningRow(builder.FinishRow())),
        };
    }
    return TKeyRange{
        .Lower = partition.LowerKey.value_or(MinKey()),
        .Upper = partition.UpperKey.value_or(MaxKey()),
    };
}

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto PageKind = ETimerPageKind::Trigger;
// Bijective mixing of family ids, so that families created at the same time land in
// different tablets of |timer_pages|.
constexpr ui64 FamilyIdMultiplier = 0x9E3779B97F4A7C15ull;
constexpr i64 DefaultRowByteSize = 120;
constexpr auto RowFormWaitPeriod = TDuration::Seconds(1);
constexpr int MinSharedPageBatchSize = 4;
constexpr int MaxSharedPageBatchSize = 64;
constexpr int MaxPageMaintenanceOperations = 4;
constexpr int TimerCacheMultiplier = 2;

//! The firing order of the in-memory store: by trigger, then by id.
struct TCompareTimersByOrder
{
    bool operator()(const TInputTimerConstPtr& lhs, const TInputTimerConstPtr& rhs) const
    {
        if (lhs->TriggerTimestamp != rhs->TriggerTimestamp) {
            return lhs->TriggerTimestamp < rhs->TriggerTimestamp;
        }
        return lhs->MessageId.Underlying() < rhs->MessageId.Underlying();
    }
};

//! The deduplication slot: one timer per key and trigger within a stream.
struct TSlotKey
{
    TKey Key;
    TSystemTimestamp TriggerTimestamp;

    bool operator<(const TSlotKey& other) const
    {
        if (Key != other.Key) {
            return Key < other.Key;
        }
        return TriggerTimestamp < other.TriggerTimestamp;
    }
};

//! The deduplication rule of the in-memory store: the lower event timestamp wins, a tie goes
//! to the incumbent, the stored or the earlier registered timer.
bool DeduplicationReplaces(const TTimer& candidate, const TTimer& incumbent)
{
    return candidate.EventTimestamp < incumbent.EventTimestamp;
}

//! The rule between two stored timers of one slot, where neither is the incumbent: the lower
//! event timestamp, then the lower system one, then the id.
bool DeduplicationPrefers(const TTimer& candidate, const TTimer& incumbent)
{
    if (candidate.EventTimestamp != incumbent.EventTimestamp) {
        return candidate.EventTimestamp < incumbent.EventTimestamp;
    }
    if (candidate.SystemTimestamp != incumbent.SystemTimestamp) {
        return candidate.SystemTimestamp < incumbent.SystemTimestamp;
    }
    return candidate.MessageId.Underlying() < incumbent.MessageId.Underlying();
}

////////////////////////////////////////////////////////////////////////////////

class TPagedTimerStore
    : public IPagedTimerStore
{
public:
    TPagedTimerStore(TTimerStoreContextPtr context, TDynamicTimerStoreContextPtr dynamicContext)
        : Context_(std::move(context))
        , Tables_(Context_->TimerPagesTable)
        , Catalogue_(Context_->PageCatalogue)
        , TimeProvider_(Context_->TimeProvider)
        , ComputationId_(Context_->Partition->ComputationId)
        , PartitionRange_(GetPartitionKeyRange(*Context_->Partition))
        , TimerStreamGroups_(BuildTimerStreamGroups(BuildTimerTriggerConditions(Context_->TimerSpecs, Context_->StreamsDependency)))
        , PageSize_(Context_->TimerPageSize)
        , Logger(Context_->Logger)
        , Metrics_{.Profiler = Context_->Profiler.WithPrefix("/paged_timer_store")}
    {
        YT_VERIFY(Tables_);
        YT_VERIFY(Catalogue_);
        YT_VERIFY(TimeProvider_);
        YT_VERIFY(PageSize_ > 0);
        for (const auto& [streamId, spec] : Context_->TimerSpecs) {
            Streams_[streamId].Spec = spec;
        }
        Reconfigure(std::move(dynamicContext));
    }

    void Reconfigure(TDynamicTimerStoreContextPtr dynamicContext) override
    {
        const auto& spec = dynamicContext->DynamicTimerStoreSpec;
        Draining_ = dynamicContext->Draining;
        ResidentLimit_ = SaturateCount(static_cast<long double>(TimerCacheMultiplier) * std::max<i64>(1, dynamicContext->MaxRowsPerBatch));
        TimerWriteMultiplier_ = spec->TimerWriteMultiplier;
        Tables_->Reconfigure(spec->TableRequest);
    }

    TFuture<void> Init() override
    {
        return BIND(&TPagedTimerStore::DoInit, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    TFuture<void> Preload() override
    {
        return BIND(&TPagedTimerStore::DoPreload, MakeStrong(this))
            .AsyncVia(GetCurrentInvoker())
            .Run()
            .AsVoid();
    }

    void InitSync() override
    {
        // The conversions run inside |Init| and must not enqueue onto their current invoker.
        DoInit();
    }

    bool PreloadSync() override
    {
        return DoPreload();
    }

    void DropEmptyPages() override
    {
        // An export must leave no empty page behind, or the next start would export again.
        for (auto& [streamId, stream] : Streams_) {
            PruneEmptyPages(&stream);
        }
    }

    void TriggerAll() override
    {
        for (auto& [streamId, stream] : Streams_) {
            stream.Watermark = InfinitySystemTimestamp;
            TriggerTimers(streamId, &stream);
        }
    }

    void SyncAndCommit(const std::function<void(const NApi::IDynamicTableTransactionPtr&)>& modify) override
    {
        const auto& manager = Context_->TransactionManager;
        if (!manager) {
            // Unit tests over mocks: the tables apply the changes directly.
            modify(nullptr);
            Sync(nullptr);
            Commit();
            return;
        }
        IRetryableTransactionPtr transaction = manager->CreateTransaction();
        NApi::IDynamicTableTransactionPtr dynamicTransaction = transaction;
        modify(dynamicTransaction);
        Sync(dynamicTransaction);
        WaitFor(manager->CommitTransaction(transaction)).ThrowOnError();
        Commit();
    }

    void UpdateWatermarkState(TWatermarkStatePtr watermarkState) override
    {
        WatermarkState_ = std::move(watermarkState);
        for (auto& [streamId, stream] : Streams_) {
            stream.Watermark = ComputeWatermark(streamId, stream);
            TriggerTimers(streamId, &stream);
        }
    }

    std::vector<TInputTimerConstPtr> GetNextBatch(const THashSet<TStreamId>& allowedStreams, i64 maxRows, i64 maxByteSize) override
    {
        if (Draining_) {
            return {};
        }

        using TPriority = std::pair<TSystemTimestamp, const TMessageId&>;
        std::vector<std::pair<TTriggeredSet*, std::function<TPriority()>>> triggeredTimers;
        auto frontier = InfinitySystemTimestamp;
        for (const auto& group : TimerStreamGroups_) {
            if (!std::all_of(group.begin(), group.end(), [&] (const TStreamId& streamId) {
                    return allowedStreams.contains(streamId);
                })) {
                continue;
            }
            for (const auto& streamId : group) {
                auto& stream = GetOrCrash(Streams_, streamId);
                auto streamFrontier = GetFrontier(stream);
                if (streamFrontier <= stream.Watermark) {
                    frontier = std::min(frontier, streamFrontier);
                }
                triggeredTimers.emplace_back(&stream.Triggered, [timers = &stream.Triggered] () -> TPriority {
                    YT_ASSERT(!timers->empty());
                    const auto& timer = *timers->begin();
                    return {timer->TriggerTimestamp, timer->MessageId};
                });
            }
        }

        std::vector<TInputTimerConstPtr> timers;
        TMessageBatchLimiter limiter(maxRows, maxByteSize);
        MergingExtractBatch(
            std::move(triggeredTimers),
            [] (const TInputTimerConstPtr& timer) -> const TInputTimerConstPtr& {
                return timer;
            },
            limiter,
            [&] (const TInputTimerConstPtr& timer) {
                auto& stream = GetOrCrash(Streams_, timer->StreamId);
                stream.ReadyByteSize -= timer->ByteSize;
                timers.push_back(timer);
            },
            [frontier] (const TPriority& priority) {
                return priority.first < frontier;
            });
        return timers;
    }

    void Register(std::vector<TTimer>&& timers) override
    {
        // Duplicates of one iteration are resolved in memory first.
        std::vector<TInputTimerConstPtr> candidates;
        TSlotIndex slots;
        for (auto& timer : timers) {
            auto& stream = GetStreamOrThrow(timer.StreamId, "register");
            auto inputTimer = New<TInputTimer>(std::move(timer), Context_->KeySchema);
            stream.RegisteredCounter.Inc();
            if (!stream.Spec->DeduplicateEqualTimestamps) {
                candidates.push_back(std::move(inputTimer));
                continue;
            }
            AddResolvingSlot(&candidates, &slots, std::move(inputTimer), &DeduplicationReplaces);
        }

        auto existing = LookupSlots(candidates);
        for (int index = 0; index < std::ssize(candidates); ++index) {
            const auto& timer = candidates[index];
            if (const auto& found = existing[index]) {
                if (DeduplicationReplaces(*timer, found->Timer)) {
                    EvictStoredTimer(*found);
                } else {
                    LogDeduplicated(*timer);
                    continue;
                }
            }
            InsertTimer(*timer);
        }
    }

    void Unregister(const std::vector<TInputTimerConstPtr>& timers) override
    {
        for (const auto& timer : timers) {
            if (Deduplicated_.erase(timer->MessageId) > 0) {
                continue;
            }
            auto& stream = GetStreamOrThrow(timer->StreamId, "unregister");
            auto location = FindResidentTimer(stream, *timer);
            if (!location) {
                THROW_ERROR_EXCEPTION("Timer %v of stream %Qv is not resident during unregister",
                    timer->MessageId,
                    timer->StreamId);
            }
            EraseTimer(location->Family, location->Page, *timer);
            stream.Offered.erase(timer->MessageId);
            stream.ProcessedCounter.Inc();
            YT_TLOG_DEBUG("MessageLifeCycle.TimerStore: timer was unregistered")
                .With("MessageId", timer->MessageId)
                .With("StreamId", timer->StreamId);
        }
    }

    void Sync(NApi::IDynamicTableTransactionPtr tx) override
    {
        if (!Pending_.RequiredWrites) {
            i64 requiredWrites = 0;
            for (const auto& [key, change] : Pending_.Rows) {
                requiredWrites += change.Write.has_value();
            }
            auto allowance = static_cast<long double>(TimerWriteMultiplier_ - 1) * requiredWrites;
            i64 budget = SaturateCount(allowance);
            Canonicalize(budget);
            Pending_.RequiredWrites = requiredWrites;
            int splitBudget = MaxPageMaintenanceOperations;
            // Resident pages are cut from data, so they are split at twice the page size.
            SplitOversizedPages(&splitBudget, SaturateCount(2.0L * PageSize_), /*residentOnly*/ true);
            for (auto& [streamId, stream] : Streams_) {
                PruneEmptyPages(&stream);
            }
        }

        // Erasures go first: a slot fired and re-registered in one iteration is deleted and
        // written within one transaction.
        std::vector<TTimerPageRowKey> erases;
        std::vector<TTimerPageRow> writes;
        for (const auto& [rowKey, change] : Pending_.Rows) {
            if (change.Erase) {
                erases.push_back(rowKey);
            }
            if (change.Write) {
                writes.push_back(TTimerPageRow{.RowKey = rowKey, .Timer = *change.Write});
            }
        }
        auto writeLimit = static_cast<long double>(TimerWriteMultiplier_) * *Pending_.RequiredWrites;
        YT_VERIFY(std::ssize(writes) <= writeLimit);
        Tables_->EraseRows(tx, erases);
        Tables_->WriteRows(tx, writes);
        for (const auto& page : Pending_.DeletedPages) {
            Tables_->ErasePage(tx, page.ComputationId, page.FamilyId, page.Kind, page.Begin);
            Pending_.Journal.push_back(TTimerJournalEntry::Delete(page));
        }
        for (auto& [streamId, stream] : Streams_) {
            for (auto& [familyId, family] : stream.Families) {
                for (auto& [begin, page] : family.Pages) {
                    if (page.StaleMinima) {
                        RefreshResidentMinima(family, &page);
                        page.StaleMinima = false;
                    }
                    if (page.Created) {
                        Tables_->WritePage(tx, page.Meta);
                        Pending_.Journal.push_back(TTimerJournalEntry::Create(page.Meta));
                        continue;
                    }
                    NTables::ITimerPages::TPageUpdate update{
                        .ComputationId = ComputationId_,
                        .FamilyId = familyId,
                        .Kind = page.Meta.Kind,
                        .Begin = page.Meta.Begin,
                        .Flags = page.PendingFlags,
                        .CountDelta = page.PendingCountDelta,
                    };
                    if (page.PendingMaxTimestamp) {
                        update.MaxTimestamp = page.Meta.MaxTimestamp;
                    }
                    if (page.PendingRange) {
                        update.End.emplace(page.Meta.End);
                    }
                    if (page.PendingMinima) {
                        update.Minima = std::pair(page.Meta.MinEvent, page.Meta.MinSystem);
                    }
                    auto journaled = page.PendingFlags;
                    journaled.DirtyMinima.reset();
                    if (!journaled.IsEmpty()) {
                        Pending_.Journal.push_back(TTimerJournalEntry::SetFlags(page.Meta, journaled));
                    }
                    Tables_->UpdatePage(tx, update);
                }
            }
        }
        Tables_->WriteJournal(tx, Pending_.Journal);
        Metrics_.WrittenRows.Increment(std::ssize(writes));
        Metrics_.ErasedRows.Increment(std::ssize(erases));
    }

    void Commit() override
    {
        for (auto& [streamId, stream] : Streams_) {
            for (auto& [familyId, family] : stream.Families) {
                for (auto& [begin, page] : family.Pages) {
                    page.Created = false;
                    page.PendingCountDelta = 0;
                    page.PendingRange = false;
                    page.PendingFlags = {};
                    page.PendingMinima = false;
                    page.PendingMaxTimestamp = false;
                }
            }
        }
        Pending_ = {};
        Deduplicated_.clear();
    }

    THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() override
    {
        THashMap<TStreamId, TInflightStreamTraverseDataPtr> inflights;
        i64 residentCount = 0;
        i64 residentBytes = 0;
        i64 familyCount = 0;
        for (auto& [streamId, stream] : Streams_) {
            auto summary = Summarize(stream);
            auto inflight = New<TInflightStreamTraverseData>();
            inflight->Suspended = Draining_;
            inflight->Empty = summary.CountHigh == 0;
            inflight->MinEventTimestamp = summary.MinEvent;
            inflight->MinSystemTimestamp = summary.MinSystem;
            inflight->MinTriggerTimestamp = GetMinTriggerTimestamp(stream);
            inflight->InflightMetrics->Count = summary.CountHigh;
            inflight->InflightMetrics->ByteSize = summary.CountHigh * summary.AverageRowByteSize;
            inflight->InflightMetrics->NewCountPerSec = stream.RegisteredCounter.GetDecayedRate();
            inflight->InflightMetrics->ReadyCount = std::ssize(stream.Triggered);
            inflight->InflightMetrics->ReadyByteSize = stream.ReadyByteSize;
            inflight->InflightMetrics->OfferedCountPerSec = stream.OfferedCounter.GetDecayedRate();
            inflight->InflightMetrics->ProcessedCountPerSec = stream.ProcessedCounter.GetDecayedRate();
            inflights[streamId] = std::move(inflight);
            residentCount += summary.ResidentCount;
            residentBytes += summary.ResidentByteSize;
            familyCount += std::ssize(stream.Families);
        }
        Metrics_.Families.Update(familyCount);
        Metrics_.ResidentCount.Update(residentCount);
        Metrics_.ResidentByteSize.Update(residentBytes);
        return inflights;
    }

    bool IsEmpty() const override
    {
        // Proven emptiness only: a non-resident shared page with rows keeps the store non-empty.
        return GetCount() == 0;
    }

    i64 GetCount() const override
    {
        i64 result = 0;
        for (const auto& [streamId, stream] : Streams_) {
            result += Summarize(stream).CountHigh;
        }
        return result;
    }

    i64 GetInMemoryByteSize() const override
    {
        i64 result = 0;
        for (const auto& [streamId, stream] : Streams_) {
            result += Summarize(stream).ResidentByteSize;
        }
        return result;
    }

    i64 GetInMemoryCount() const override
    {
        i64 result = 0;
        for (const auto& [streamId, stream] : Streams_) {
            result += Summarize(stream).ResidentCount;
        }
        return result;
    }

private:
    using TTriggeredSet = std::set<TInputTimerConstPtr, TCompareTimersByOrder>;

    struct TPageState
    {
        TTimerPageMeta Meta;
        ETimerPageClass Class = ETimerPageClass::Shared;
        std::optional<TTimerPageStatistics> Resident;
        //! Created in this iteration; written whole at |Sync|.
        bool Created = false;
        i64 PendingCountDelta = 0;
        bool PendingRange = false;
        TTimerPageFlagsUpdate PendingFlags;
        bool PendingMinima = false;
        // |Meta.MaxTimestamp| has been raised and is not stored yet.
        bool PendingMaxTimestamp = false;
        // Retry an uncuttable page only when its count changes.
        std::optional<i64> UncuttableAtCount;
        // Resident minima need refreshing after an erase.
        bool StaleMinima = false;
        //! Known upper bound of this partition's stored timestamps; it may fall below the stored
        //! |Meta.MaxTimestamp| after the page is loaded.
        std::optional<TSystemTimestamp> MaxTimestamp;

        bool IsOwned() const
        {
            return Class != ETimerPageClass::Shared;
        }
    };

    struct TFamilyState
    {
        TTimerFamilyId FamilyId;
        TKeyRange KeyRange;
        //! Indexed by slot for a deduplicating stream.
        TTimerPageData Data;
        //! By |Begin|.
        std::map<TSystemTimestamp, TPageState> Pages;
        int SharedPageBatchSize = MinSharedPageBatchSize;

        bool HasPagesOfClass(ETimerPageClass pageClass) const
        {
            for (const auto& [begin, page] : Pages) {
                if (page.Class == pageClass) {
                    return true;
                }
            }
            return false;
        }
    };

    struct TStreamState
    {
        TTimerSpecPtr Spec;
        std::map<TTimerFamilyId, TFamilyState> Families;

        TSystemTimestamp Watermark = ZeroSystemTimestamp;
        //! Offered and not yet taken.
        TTriggeredSet Triggered;
        i64 ReadyByteSize = 0;
        //! Offered or taken and not yet unregistered.
        THashSet<TMessageId> Offered;

        TSimpleEmaCounter RegisteredCounter;
        TSimpleEmaCounter OfferedCounter;
        TSimpleEmaCounter ProcessedCounter;

        // Validated on lookup and rebuilt on a miss.
        std::map<TSystemTimestamp, TTimerFamilyId> CanonicalIndex;
    };

    struct TTimerLocation
    {
        TFamilyState* Family = nullptr;
        TPageState* Page = nullptr;
    };

    struct TStoredTimer
    {
        TTimerFamilyId FamilyId;
        TTimer Timer;
    };

    struct TStreamSummary
    {
        i64 CountHigh = 0;
        i64 ResidentCount = 0;
        i64 ResidentByteSize = 0;
        i64 AverageRowByteSize = DefaultRowByteSize;
        std::optional<TSystemTimestamp> MinEvent;
        std::optional<TSystemTimestamp> MinSystem;
    };

    // Net changes per row; an erase precedes a write.
    struct TRowChange
    {
        bool Erase = false;
        std::optional<TTimer> Write;
    };

    struct TPending
    {
        std::map<TTimerPageRowKey, TRowChange> Rows;
        std::optional<i64> RequiredWrites;
        std::vector<TTimerPageMeta> DeletedPages;
        std::vector<TTimerJournalEntry> Journal;
    };

    struct TMetrics
    {
        NProfiling::TProfiler Profiler;
        NProfiling::TCounter PageReads = Profiler.Counter("/page_reads");
        NProfiling::TCounter LookupKeys = Profiler.Counter("/lookup_keys");
        NProfiling::TEventTimer LookupTime = Profiler.Timer("/lookup_time");
        NProfiling::TCounter WrittenRows = Profiler.Counter("/written_rows");
        NProfiling::TCounter ErasedRows = Profiler.Counter("/erased_rows");
        NProfiling::TCounter Merges = Profiler.Counter("/merges");
        NProfiling::TCounter Splits = Profiler.Counter("/splits");
        NProfiling::TCounter PageClosures = Profiler.Counter("/page_closures");
        NProfiling::TCounter Evictions = Profiler.Counter("/evictions");
        NProfiling::TCounter MaintenanceRows = Profiler.Counter("/maintenance_rows");
        NProfiling::TCounter CanonicalizedRows = Profiler.Counter("/canonicalized_rows");
        NProfiling::TGauge ResidentCount = Profiler.Gauge("/resident_count");
        NProfiling::TGauge ResidentByteSize = Profiler.Gauge("/resident_byte_size");
        NProfiling::TGauge Families = Profiler.Gauge("/families");
    };

    const TTimerStoreContextPtr Context_;
    const NTables::ITimerPagesPtr Tables_;
    const TTimerPageCataloguePtr Catalogue_;
    const ITimeProviderPtr TimeProvider_;
    const TComputationId ComputationId_;
    const TKeyRange PartitionRange_;
    const std::vector<std::vector<TStreamId>> TimerStreamGroups_;
    const i64 PageSize_;
    const NLogging::TLogger Logger;
    const TMetrics Metrics_;

    bool Draining_ = false;
    i64 ResidentLimit_ = 2000;
    double TimerWriteMultiplier_ = 2.0;

    TWatermarkStatePtr WatermarkState_;
    std::map<TStreamId, TStreamState> Streams_;
    TPending Pending_;
    //! Evicted by a deduplication while offered or taken; their unregister is a no-op.
    THashSet<TMessageId> Deduplicated_;
    i64 PageReads_ = 0;
    //! The family served last by the canonicalization; the next round starts after it.
    std::optional<std::pair<TStreamId, TTimerFamilyId>> CanonicalizationCursor_;

    static i64 SaturateCount(long double value)
    {
        return value >= static_cast<long double>(std::numeric_limits<i64>::max())
            ? std::numeric_limits<i64>::max()
            : static_cast<i64>(value);
    }

    TStreamState& GetStreamOrThrow(const TStreamId& streamId, TStringBuf action)
    {
        auto it = Streams_.find(streamId);
        if (it == Streams_.end()) {
            THROW_ERROR_EXCEPTION("Unknown timer stream %Qv during %v",
                streamId,
                action);
        }
        return it->second;
    }

    using TSlotIndex = std::map<std::pair<TStreamId, TSlotKey>, int>;

    //! Adds |timer| to |candidates|; if its slot is already taken, keeps whichever of the two
    //! |beats| prefers and drops the other.
    void AddResolvingSlot(
        std::vector<TInputTimerConstPtr>* candidates,
        TSlotIndex* slots,
        TInputTimerConstPtr timer,
        bool (*beats)(const TTimer&, const TTimer&))
    {
        auto slot = std::pair(timer->StreamId, TSlotKey{timer->Key, timer->TriggerTimestamp});
        auto [it, inserted] = slots->emplace(std::move(slot), std::ssize(*candidates));
        if (inserted) {
            candidates->push_back(std::move(timer));
        } else if (beats(*timer, *(*candidates)[it->second])) {
            LogDeduplicated(*(*candidates)[it->second]);
            (*candidates)[it->second] = std::move(timer);
        } else {
            LogDeduplicated(*timer);
        }
    }

    void LogDeduplicated(const TTimer& timer)
    {
        YT_TLOG_DEBUG("MessageLifeCycle.TimerStore: timer was deduplicated")
            .With("MessageId", timer.MessageId)
            .With("StreamId", timer.StreamId);
    }

    void DoInit()
    {
        WaitFor(Catalogue_->Synchronize()).ThrowOnError();
        auto snapshot = Catalogue_->GetSnapshot();

        struct TLoadedFamily
        {
            TKeyRange KeyRange;
            std::vector<TTimerPageMeta> Pages;
        };

        // The metadata of every family is read and checked before any of it is applied.
        THashMap<TStreamId, std::map<TTimerFamilyId, TLoadedFamily>> loaded;
        for (const auto& [streamId, stream] : Streams_) {
            // The catalogue names the pages; their private fields come from the table.
            std::map<TTimerFamilyId, TKeyRange> familyRanges;
            for (const auto& page : snapshot.FindPages(ComputationId_, streamId, PartitionRange_)) {
                familyRanges.emplace(page.FamilyId, page.KeyRange);
            }
            for (const auto& [familyId, keyRange] : familyRanges) {
                auto pages = WaitFor(Tables_->LoadMeta({.ComputationId = ComputationId_, .FamilyId = familyId})).ValueOrThrow();
                ValidateFormatVersions(pages);
                loaded[streamId].emplace(familyId, TLoadedFamily{.KeyRange = keyRange, .Pages = std::move(pages)});
            }
        }
        for (auto& [streamId, stream] : Streams_) {
            for (const auto& [familyId, family] : loaded[streamId]) {
                AddFamily(&stream, familyId, family.KeyRange, family.Pages);
            }
            AcceptCanonicalPages(&stream);
            YT_TLOG_INFO("Timer families loaded")
                .With("StreamId", streamId)
                .With("Families", stream.Families.size())
                .With("CanonicalPages", CountCanonicalPages(stream));
        }
        ConvertRowForms();
        DoPreload();
        // A store the in-memory backend creates to export its pages must not import the rows
        // an interrupted export has already moved.
        if (Context_->Backend == ETimerStoreBackend::Paged && Context_->TimersTable) {
            ImportTimersIntoPages(Context_, MakeStrong(this));
        }
        YT_TLOG_DEBUG("Timer store init completed");
    }

    static void ValidateFormatVersions(const std::vector<TTimerPageMeta>& pages)
    {
        for (const auto& page : pages) {
            if (page.FormatVersion > CurrentTimerPageFormatVersion) {
                THROW_ERROR_EXCEPTION("Timer page format version %v is not supported; the current version is %v",
                    page.FormatVersion,
                    CurrentTimerPageFormatVersion)
                    .With("family_id", page.FamilyId)
                    .With("begin_timestamp", page.Begin)
                    .With("computation_id", page.ComputationId);
            }
        }
    }

    void ConvertRowForms()
    {
        for (auto& [streamId, stream] : Streams_) {
            bool target = stream.Spec->DeduplicateEqualTimestamps;
            std::map<TTimerFamilyId, std::vector<TSystemTimestamp>> waiting;
            std::vector<std::pair<TTimerFamilyId, TSystemTimestamp>> pages;
            for (const auto& [familyId, family] : stream.Families) {
                for (const auto& [begin, page] : family.Pages) {
                    pages.emplace_back(familyId, begin);
                }
            }
            for (const auto& [familyId, begin] : pages) {
                auto familyIt = stream.Families.find(familyId);
                if (familyIt == stream.Families.end()) {
                    continue;
                }
                auto& family = familyIt->second;
                auto pageIt = family.Pages.find(begin);
                if (pageIt == family.Pages.end() || pageIt->second.Meta.Flags.DedupSlots == target) {
                    continue;
                }
                auto& page = pageIt->second;
                if (page.IsOwned() || PartitionRange_.Contains(family.KeyRange.Lower)) {
                    ConvertRowForm(streamId, family, &page, target);
                } else if (page.Meta.Count != 0) {
                    waiting[familyId].push_back(begin);
                }
            }
            for (const auto& [familyId, pageBegins] : waiting) {
                WaitForRowForm(streamId, &stream, familyId, pageBegins, target);
            }
        }
    }

    void ConvertRowForm(const TStreamId& streamId, const TFamilyState& family, TPageState* page, bool target)
    {
        YT_VERIFY(!page->Resident);
        auto rows = WaitFor(Tables_->ReadPage(page->Meta, std::nullopt)).ValueOrThrow();
        Metrics_.PageReads.Increment();
        ++PageReads_;
        // An empty once-shared page is the collector's: a flag written after its collection
        // would resurrect the metadata row. A page with a nonzero count is flipped even when
        // the read is empty, so that the partitions waiting for its row form are released.
        if (rows.empty() && page->Meta.Flags.OnceShared && page->Meta.Count == 0) {
            YT_TLOG_INFO("Empty shared timer page left unconverted")
                .With("StreamId", streamId)
                .With("FamilyId", family.FamilyId)
                .With("Begin", page->Meta.Begin);
            return;
        }
        for (const auto& row : rows) {
            EraseRow(row.RowKey);
        }
        std::vector<TTimer> timers;
        std::map<TSlotKey, int> slots;
        for (auto& row : rows) {
            auto timer = std::move(row.Timer);
            timer.StreamId = streamId;
            if (!target) {
                timers.push_back(std::move(timer));
                continue;
            }
            auto [it, inserted] = slots.emplace(TSlotKey{timer.Key, timer.TriggerTimestamp}, std::ssize(timers));
            if (inserted) {
                timers.push_back(std::move(timer));
            } else if (DeduplicationPrefers(timer, timers[it->second])) {
                LogDeduplicated(timers[it->second]);
                timers[it->second] = std::move(timer);
            } else {
                LogDeduplicated(timer);
            }
        }
        page->Meta.Flags.DedupSlots = target;
        page->PendingFlags.DedupSlots = target;
        TTimerPageStatistics statistics;
        for (const auto& timer : timers) {
            WriteRow(MakeRowKey(family, *page, timer), timer);
            statistics.Add(timer, PageKind);
        }
        // Deduplication may collapse rows, so the delta is taken against the rows read here.
        i64 countDelta = std::ssize(timers) - std::ssize(rows);
        page->PendingCountDelta += countDelta;
        page->Meta.Count += countDelta;
        if (statistics.Count) {
            page->Meta.MinEvent = statistics.MinEvent;
            page->Meta.MinSystem = statistics.MinSystem;
            page->PendingMinima = true;
        }
        if (page->Meta.Flags.DirtyMinima) {
            page->Meta.Flags.DirtyMinima = false;
            page->PendingFlags.DirtyMinima = false;
        }
        YT_TLOG_INFO("Timer page row form converted")
            .With("StreamId", streamId)
            .With("FamilyId", family.FamilyId)
            .With("Begin", page->Meta.Begin)
            .With("DedupSlots", target)
            .With("Rows", rows.size())
            .With("Timers", timers.size());
        SyncAndCommit([] (const NApi::IDynamicTableTransactionPtr&) {
        });
    }

    //! Waits until the owner has converted every listed page of the family, then reloads it once.
    void WaitForRowForm(
        const TStreamId& streamId,
        TStreamState* stream,
        TTimerFamilyId familyId,
        const std::vector<TSystemTimestamp>& pageBegins,
        bool target)
    {
        auto started = TInstant::Now();
        while (true) {
            WaitFor(Catalogue_->Synchronize()).ThrowOnError();
            auto snapshot = Catalogue_->GetSnapshot();
            std::optional<TSystemTimestamp> pending;
            for (auto pageBegin : pageBegins) {
                auto page = snapshot.FindPage(familyId, PageKind, pageBegin);
                if (page && page->DedupSlots != target) {
                    pending = pageBegin;
                    break;
                }
            }
            if (!pending) {
                break;
            }
            if (TInstant::Now() - started > Context_->RowFormWaitTimeout) {
                THROW_ERROR_EXCEPTION("Timed out waiting for the owner to convert the row form of a shared timer page")
                    .With("family_id", familyId)
                    .With("begin_timestamp", *pending)
                    .With("owner_partition_key", GetOrCrash(stream->Families, familyId).KeyRange.Lower)
                    .With("timeout", Context_->RowFormWaitTimeout);
            }
            YT_TLOG_INFO("Waiting for the owner to convert the row form of a shared timer page")
                .With("StreamId", streamId)
                .With("FamilyId", familyId)
                .With("Begin", *pending)
                .With("Elapsed", TInstant::Now() - started);
            TDelayedExecutor::WaitForDuration(RowFormWaitPeriod);
        }
        // The pages' private fields changed under the conversion: reload the family.
        auto pages = WaitFor(Tables_->LoadMeta({.ComputationId = ComputationId_, .FamilyId = familyId})).ValueOrThrow();
        ValidateFormatVersions(pages);
        auto keyRange = GetOrCrash(stream->Families, familyId).KeyRange;
        EraseOrCrash(stream->Families, familyId);
        AddFamily(stream, familyId, keyRange, pages);
    }

    void AddFamily(TStreamState* stream, TTimerFamilyId familyId, const TKeyRange& keyRange, const std::vector<TTimerPageMeta>& pages)
    {
        TFamilyState family{
            .FamilyId = familyId,
            .KeyRange = keyRange,
            .Data = TTimerPageData(PageKind, stream->Spec->DeduplicateEqualTimestamps),
        };
        bool onceShared = false;
        for (const auto& page : pages) {
            if (page.Kind != PageKind || page.FamilyId != familyId) {
                continue;
            }
            if (page.KeyRange != family.KeyRange) {
                THROW_ERROR_EXCEPTION("Timer page %v of family %v has the key range %v while the family has %v",
                    page.Begin,
                    familyId,
                    page.KeyRange,
                    family.KeyRange);
            }
            onceShared = onceShared || page.Flags.OnceShared;
            if (auto [it, inserted] = family.Pages.emplace(page.Begin, TPageState{.Meta = page, .MaxTimestamp = page.MaxTimestamp}); !inserted) {
                THROW_ERROR_EXCEPTION("Timer pages %v and %v of family %v begin at the same timestamp %v",
                    it->second.Meta.Begin,
                    page.Begin,
                    familyId,
                    page.Begin);
            }
        }
        if (family.Pages.empty()) {
            return;
        }
        // The flag is set on the whole family at once, so one page stands for all.
        auto pageClass = ClassifyTimerPage(family.KeyRange, onceShared, PartitionRange_);
        for (auto& [begin, page] : family.Pages) {
            page.Meta.KeyRange = family.KeyRange;
            page.Class = pageClass;
        }
        ValidateNoOverlap(family);

        if (pageClass == ETimerPageClass::Shared && !onceShared && PartitionRange_.Contains(family.KeyRange.Lower)) {
            // The partition holding the family's key begin fences it against a later adoption.
            for (auto& [begin, page] : family.Pages) {
                page.Meta.Flags.OnceShared = true;
                page.PendingFlags.OnceShared = true;
            }
            YT_TLOG_INFO("Timer family flagged once shared")
                .With("FamilyId", familyId)
                .With("KeyRange", family.KeyRange);
        }
        EmplaceOrCrash(stream->Families, familyId, std::move(family));
    }

    void ValidateNoOverlap(const TFamilyState& family)
    {
        std::optional<TSystemTimestamp> previousEnd;
        bool first = true;
        for (const auto& [begin, page] : family.Pages) {
            THROW_ERROR_EXCEPTION_IF((!first && previousEnd != begin) || (page.Meta.End && *page.Meta.End <= begin),
                "Timer family %v has overlapping pages or a gap at %v",
                family.FamilyId,
                begin);
            first = false;
            previousEnd = page.Meta.End;
        }
        auto range = family.Pages.begin()->second.Meta;
        if (range.Begin != ZeroSystemTimestamp) {
            range.End = range.Begin;
            range.Begin = ZeroSystemTimestamp;
            THROW_ERROR_EXCEPTION_IF(WaitFor(Tables_->Aggregate(range)).ValueOrThrow().Count,
                "Timer family %v has rows before its first page",
                family.FamilyId);
        }
        if (previousEnd) {
            range.Begin = *previousEnd;
            range.End.reset();
            THROW_ERROR_EXCEPTION_IF(WaitFor(Tables_->Aggregate(range)).ValueOrThrow().Count,
                "Timer family %v has rows after its last page",
                family.FamilyId);
        }
    }

    void AcceptCanonicalPages(TStreamState* stream)
    {
        std::vector<TFamilyState*> candidates;
        for (auto& [familyId, family] : stream->Families) {
            if (family.HasPagesOfClass(ETimerPageClass::Canonical)) {
                candidates.push_back(&family);
            }
        }
        std::sort(candidates.begin(), candidates.end(), [] (const TFamilyState* lhs, const TFamilyState* rhs) {
            if (lhs->Pages.size() != rhs->Pages.size()) {
                return lhs->Pages.size() > rhs->Pages.size();
            }
            return lhs->FamilyId < rhs->FamilyId;
        });

        std::map<TSystemTimestamp, std::optional<TSystemTimestamp>> accepted;
        for (auto* family : candidates) {
            for (auto& [begin, page] : family->Pages) {
                bool overlaps = false;
                auto next = accepted.upper_bound(begin);
                if (next != accepted.end() && (!page.Meta.End || next->first < *page.Meta.End)) {
                    overlaps = true;
                }
                if (next != accepted.begin()) {
                    auto previous = std::prev(next);
                    if (!previous->second || begin < *previous->second) {
                        overlaps = true;
                    }
                }
                if (overlaps) {
                    page.Class = ETimerPageClass::Nested;
                    YT_TLOG_WARNING("Timer page overlaps an accepted canonical page and stays nested")
                        .With("FamilyId", family->FamilyId)
                        .With("Begin", page.Meta.Begin);
                } else {
                    accepted[begin] = page.Meta.End;
                }
            }
        }
    }

    i64 CountCanonicalPages(const TStreamState& stream) const
    {
        i64 count = 0;
        for (const auto& [familyId, family] : stream.Families) {
            for (const auto& [begin, page] : family.Pages) {
                count += page.Class == ETimerPageClass::Canonical;
            }
        }
        return count;
    }

    // Reports read progress even when shared pages contain no timers of this partition.
    bool DoPreload()
    {
        i64 pageReads = PageReads_;
        std::vector<TStreamId> streamIds;
        streamIds.reserve(Streams_.size());
        for (auto& [streamId, stream] : Streams_) {
            for (auto& [familyId, family] : stream.Families) {
                EvictFamily(stream, &family);
            }
            FetchPages({streamId}, /*explore*/ true);
            streamIds.push_back(streamId);
        }
        if (streamIds.size() > 1) {
            FetchPages(streamIds, /*explore*/ false);
        }
        Maintain();
        for (auto& [streamId, stream] : Streams_) {
            PruneEmptyPages(&stream);
        }
        return PageReads_ != pageReads;
    }

    void EvictFamily(const TStreamState& stream, TFamilyState* family)
    {
        i64 resident = 0;
        bool withinWindow = true;
        for (auto& [begin, page] : family->Pages) {
            if (!page.Resident) {
                withinWindow = false;
                continue;
            }
            if (withinWindow && resident < ResidentLimit_) {
                resident += page.Resident->Count;
                continue;
            }
            if (page.Created || page.PendingCountDelta != 0 || page.PendingRange || !page.PendingFlags.IsEmpty() || page.PendingMinima ||
                page.PendingMaxTimestamp || page.StaleMinima)
            {
                continue;
            }
            bool offered = false;
            for (const auto& entry : family->Data.GetEntries(page.Meta.Begin, page.Meta.End)) {
                if (stream.Offered.contains(entry.MessageId)) {
                    offered = true;
                    break;
                }
            }
            if (offered) {
                continue;
            }
            YT_TLOG_DEBUG("Timer page evicted")
                .With("FamilyId", family->FamilyId)
                .With("Begin", page.Meta.Begin)
                .With("Rows", page.Resident->Count);
            family->Data.EraseInterval(page.Meta.Begin, page.Meta.End);
            page.Resident.reset();
            Metrics_.Evictions.Increment();
        }
    }

    void FetchPages(const std::vector<TStreamId>& streamIds, bool explore)
    {
        using TTimerOrder = std::tuple<TSystemTimestamp, TMessageId, TStreamId>;
        std::set<TTimerOrder> nearest;
        auto include = [&] (TTimerOrder order) {
            nearest.insert(std::move(order));
            if (std::ssize(nearest) > ResidentLimit_) {
                nearest.erase(std::prev(nearest.end()));
            }
        };
        auto includeEntries = [&] (const TStreamId& streamId, TTimerPageData::TEntryRange entries) {
            for (const auto& entry : entries) {
                TTimerOrder order{entry.GetTimestamp(), entry.MessageId, streamId};
                if (std::ssize(nearest) == ResidentLimit_ && order >= *nearest.rbegin()) {
                    break;
                }
                include(std::move(order));
            }
        };
        for (const auto& [key, change] : Pending_.Rows) {
            if (change.Write && std::ranges::find(streamIds, change.Write->StreamId) != streamIds.end()) {
                include({change.Write->TriggerTimestamp, change.Write->MessageId, change.Write->StreamId});
            }
        }

        struct TFamilyCursor
        {
            TFamilyState* Family = nullptr;
            std::map<TSystemTimestamp, TPageState>::iterator NextPageIt;
            int SharedPagesRead = 0;
            bool FoundTimers = false;
        };

        std::vector<TFamilyCursor> cursors;
        for (const auto& streamId : streamIds) {
            auto& stream = GetOrCrash(Streams_, streamId);
            for (auto& [familyId, family] : stream.Families) {
                includeEntries(streamId, family.Data.GetEntries());
                cursors.push_back({.Family = &family, .NextPageIt = family.Pages.begin()});
            }
        }
        using TPageOrder = std::pair<TSystemTimestamp, int>;
        std::priority_queue<TPageOrder, std::vector<TPageOrder>, std::greater<TPageOrder>> pages;
        auto enqueue = [&] (int index) {
            auto& cursor = cursors[index];
            while (cursor.NextPageIt != cursor.Family->Pages.end() && cursor.NextPageIt->second.Resident) {
                ++cursor.NextPageIt;
            }
            if (cursor.NextPageIt != cursor.Family->Pages.end()) {
                pages.emplace(cursor.NextPageIt->first, index);
            }
        };
        // The second pass revisits pages skipped before their reading became necessary.
        for (int pass = explore ? 0 : 1; pass < 2; ++pass) {
            pages = {};
            for (int index = 0; index < std::ssize(cursors); ++index) {
                enqueue(index);
            }
            while (!pages.empty()) {
                auto [begin, index] = pages.top();
                pages.pop();
                // Equal triggers still need merging by ID.
                bool beforeKnown = !nearest.empty() && begin <= std::get<0>(*nearest.rbegin());
                if (!beforeKnown && (pass == 1 || std::ssize(nearest) == ResidentLimit_)) {
                    break;
                }
                auto& cursor = cursors[index];
                auto& page = cursor.NextPageIt->second;
                if (!page.IsOwned() && !beforeKnown && cursor.SharedPagesRead >= cursor.Family->SharedPageBatchSize) {
                    continue;
                }
                LoadPage(cursor.Family, &page);
                includeEntries(page.Meta.StreamId, cursor.Family->Data.GetEntries(page.Meta.Begin, page.Meta.End));
                if (!page.IsOwned()) {
                    ++cursor.SharedPagesRead;
                    cursor.FoundTimers |= page.Resident->Count > 0;
                }
                ++cursor.NextPageIt;
                enqueue(index);
            }
        }
        for (auto& cursor : cursors) {
            auto& batchSize = cursor.Family->SharedPageBatchSize;
            if (cursor.FoundTimers) {
                batchSize = MinSharedPageBatchSize;
            } else if (explore && cursor.SharedPagesRead >= batchSize) {
                batchSize = std::min(2 * batchSize, MaxSharedPageBatchSize);
            }
        }
    }

    void LoadPage(TFamilyState* family, TPageState* page)
    {
        std::optional<TKeyRange> keyRange;
        if (!page->IsOwned()) {
            keyRange = PartitionRange_;
        }
        auto rows = WaitFor(Tables_->ReadPage(page->Meta, keyRange)).ValueOrThrow();
        Metrics_.PageReads.Increment();
        ++PageReads_;
        OverlayPendingRows(*page, &rows);
        for (const auto& row : rows) {
            family->Data.Insert(row.Timer);
        }
        page->Resident = family->Data.GetStatistics(page->Meta.Begin, page->Meta.End);
        page->MaxTimestamp = page->Resident->Count ? page->Resident->MaxTimestamp : page->Meta.Begin;

        if (page->IsOwned() && !page->Meta.Flags.OnceShared) {
            if (page->Resident->Count != page->Meta.Count) {
                YT_TLOG_WARNING("Timer page count mismatch, corrected")
                    .With("FamilyId", family->FamilyId)
                    .With("Begin", page->Meta.Begin)
                    .With("StoredCount", page->Meta.Count)
                    .With("ActualCount", page->Resident->Count);
                page->PendingCountDelta += page->Resident->Count - page->Meta.Count;
                page->Meta.Count = page->Resident->Count;
            }
            // A legacy page gets its bound here; a stored bound is never lowered.
            EstablishMaxTimestamp(page, *page->MaxTimestamp);
            RaiseMaxTimestamp(page, *page->MaxTimestamp);
            RefreshResidentMinima(*family, page);
            if (page->Meta.Flags.DirtyMinima) {
                page->Meta.Flags.DirtyMinima = false;
                page->PendingFlags.DirtyMinima = false;
            }
        }
        YT_TLOG_DEBUG("Timer page loaded")
            .With("FamilyId", family->FamilyId)
            .With("Begin", page->Meta.Begin)
            .With("Rows", rows.size());
    }

    auto GetPendingRows(const TTimerPageMeta& page) const
    {
        auto begin = Pending_.Rows.lower_bound(TTimerPageRowKey{
            .FamilyId = page.FamilyId,
            .Kind = page.Kind,
            .Timestamp = page.Begin,
            .Key = MinKey(),
        });
        return std::ranges::subrange(begin, Pending_.Rows.end()) | std::views::take_while([&page] (const auto& entry) {
            return entry.first.FamilyId == page.FamilyId && entry.first.Kind == page.Kind &&
                (!page.End || entry.first.Timestamp < *page.End);
        });
    }

    bool HasPendingRows(const TTimerPageMeta& page) const
    {
        return std::ranges::any_of(GetPendingRows(page), [] (const auto& entry) {
            return entry.second.Erase || entry.second.Write.has_value();
        });
    }

    // Init may load a destination page before its imported rows commit.
    void OverlayPendingRows(const TPageState& page, std::vector<TTimerPageRow>* rows)
    {
        std::vector<TTimerPageRow> overlaid;
        for (const auto& row : *rows) {
            auto it = Pending_.Rows.find(row.RowKey);
            if (it == Pending_.Rows.end() || (!it->second.Erase && !it->second.Write)) {
                overlaid.push_back(row);
            }
        }
        for (const auto& [key, change] : GetPendingRows(page.Meta)) {
            if (change.Write && (page.IsOwned() || PartitionRange_.Contains(key.Key))) {
                overlaid.push_back(TTimerPageRow{.RowKey = key, .Timer = *change.Write});
            }
        }
        *rows = std::move(overlaid);
    }

    void RefreshResidentMinima(const TFamilyState& family, TPageState* page)
    {
        page->Resident = family.Data.GetStatistics(page->Meta.Begin, page->Meta.End);
        if (page->IsOwned() && !page->Meta.Flags.OnceShared && page->Resident->Count) {
            page->Meta.MinEvent = page->Resident->MinEvent;
            page->Meta.MinSystem = page->Resident->MinSystem;
            page->PendingMinima = true;
        }
        page->StaleMinima = false;
    }

    void Maintain()
    {
        int budget = MaxPageMaintenanceOperations;
        for (auto& [streamId, stream] : Streams_) {
            for (auto& [familyId, family] : stream.Families) {
                for (auto& [begin, page] : family.Pages) {
                    if (budget > 0 && !page.Resident && page.IsOwned() && !page.Meta.Flags.OnceShared && page.Meta.Flags.DirtyMinima && !HasPendingRows(page.Meta)) {
                        auto statistics = WaitFor(Tables_->Aggregate(page.Meta)).ValueOrThrow();
                        if (statistics.Count) {
                            page.Meta.MinEvent = statistics.MinEvent;
                            page.Meta.MinSystem = statistics.MinSystem;
                            page.PendingMinima = true;
                        }
                        page.Meta.Flags.DirtyMinima = false;
                        page.PendingFlags.DirtyMinima = false;
                        --budget;
                    }
                }
            }
        }
        CloseFullOpenPages();
        SplitOversizedPages(&budget, PageSize_, /*residentOnly*/ true);
        // A nonresident page below twice the page size is not worth a table scan: it becomes
        // resident for firing within the horizon anyway.
        SplitOversizedPages(&budget, SaturateCount(2.0L * PageSize_), /*residentOnly*/ false);
        for (auto& [streamId, stream] : Streams_) {
            for (auto& [familyId, family] : stream.Families) {
                MergeSmallPages(&family, &budget);
            }
        }
    }

    //! Closes the full nonresident open pages after their last known timestamp; touches metadata only.
    void CloseFullOpenPages()
    {
        for (auto& [streamId, stream] : Streams_) {
            for (auto& [familyId, family] : stream.Families) {
                for (auto& [begin, page] : family.Pages) {
                    // Closing at the maximum plus one must stay below infinity.
                    if (page.IsOwned() && page.Class == ETimerPageClass::Canonical && !page.Meta.Flags.OnceShared &&
                        !page.Resident && !page.Meta.End && page.MaxTimestamp && page.Meta.Count > PageSize_ &&
                        page.MaxTimestamp->Underlying() < InfinitySystemTimestamp.Underlying() - 1 &&
                        !HasPendingRows(page.Meta))
                    {
                        AppendPage(&family, &page, TSystemTimestamp(page.MaxTimestamp->Underlying() + 1));
                    }
                }
            }
        }
    }

    void SplitOversizedPages(int* budget, i64 threshold, bool residentOnly)
    {
        while (*budget > 0) {
            TFamilyState* family = nullptr;
            TPageState* candidate = nullptr;
            for (auto& [streamId, stream] : Streams_) {
                for (auto& [familyId, current] : stream.Families) {
                    for (auto& [begin, page] : current.Pages) {
                        if (!page.IsOwned() || page.Meta.Flags.OnceShared || page.Meta.Count <= threshold ||
                            page.UncuttableAtCount == page.Meta.Count ||
                            (!page.Resident && (residentOnly || HasPendingRows(page.Meta))) ||
                            (candidate && page.Meta.Count <= candidate->Meta.Count))
                        {
                            continue;
                        }
                        family = &current;
                        candidate = &page;
                    }
                }
            }
            if (!candidate) {
                break;
            }
            SplitPage(family, candidate);
            if (!candidate->Resident || candidate->UncuttableAtCount != candidate->Meta.Count) {
                --*budget;
            }
        }
    }

    void Canonicalize(i64 budget)
    {
        if (budget <= 0) {
            return;
        }
        std::map<TStreamId, TTimerFamilyId> targets;
        std::vector<std::pair<TStreamId, TTimerFamilyId>> foreign;
        for (const auto& [streamId, stream] : Streams_) {
            i64 mostPages = 0;
            for (const auto& [familyId, family] : stream.Families) {
                i64 count = 0;
                for (const auto& [begin, page] : family.Pages) {
                    count += page.Class == ETimerPageClass::Canonical;
                }
                if (count > mostPages) {
                    targets[streamId] = familyId;
                    mostPages = count;
                }
            }
            for (const auto& [familyId, family] : stream.Families) {
                if (!targets.contains(streamId) || familyId != targets[streamId] ||
                    family.HasPagesOfClass(ETimerPageClass::Nested) || family.HasPagesOfClass(ETimerPageClass::Shared))
                {
                    foreign.emplace_back(streamId, familyId);
                }
            }
        }
        if (foreign.empty()) {
            return;
        }
        if (CanonicalizationCursor_) {
            auto next = std::upper_bound(foreign.begin(), foreign.end(), *CanonicalizationCursor_);
            std::rotate(foreign.begin(), next == foreign.end() ? foreign.begin() : next, foreign.end());
        }
        i64 share = std::max<i64>(1, budget / std::ssize(foreign));
        for (const auto& [streamId, familyId] : foreign) {
            auto moved = CanonicalizeFamily(streamId, &GetOrCrash(Streams_, streamId), familyId, std::min(share, budget), targets.contains(streamId) && familyId != targets[streamId] ? std::optional(targets[streamId]) : std::nullopt);
            CanonicalizationCursor_ = std::pair(streamId, familyId);
            budget -= moved;
            Metrics_.MaintenanceRows.Increment(moved);
            Metrics_.CanonicalizedRows.Increment(moved);
            if (budget <= 0) {
                break;
            }
        }
    }

    i64 CanonicalizeFamily(const TStreamId& streamId, TStreamState* stream, TTimerFamilyId familyId, i64 limit, std::optional<TTimerFamilyId> destinationFamily)
    {
        std::vector<TInputTimerConstPtr> moving;
        {
            auto& family = GetOrCrash(stream->Families, familyId);
            for (auto& [begin, page] : family.Pages) {
                if (std::ssize(moving) >= limit) {
                    break;
                }
                if (!page.Resident || (page.Class == ETimerPageClass::Canonical && (!destinationFamily || HasPendingRows(page.Meta)))) {
                    continue;
                }
                bool consolidating = page.Class == ETimerPageClass::Canonical;
                for (const auto& entry : family.Data.GetEntries(page.Meta.Begin, page.Meta.End)) {
                    if (std::ssize(moving) >= limit) {
                        break;
                    }
                    if (stream->Offered.contains(entry.MessageId)) {
                        continue;
                    }
                    auto timer = entry.ToTimer(PageKind);
                    timer.StreamId = streamId;
                    timer.KeySchema = Context_->KeySchema;
                    if (consolidating) {
                        auto oldClass = page.Class;
                        page.Class = ETimerPageClass::Nested;
                        if (!FindOrMakeCanonicalPage(streamId, stream, timer.TriggerTimestamp, destinationFamily)) {
                            page.Class = oldClass;
                            continue;
                        }
                    }
                    moving.push_back(New<TInputTimer>(std::move(timer), Context_->KeySchema));
                }
            }
        }
        if (moving.empty()) {
            return 0;
        }

        // The rows leave their pages first, so that the lookup does not see them as stored.
        for (const auto& timer : moving) {
            auto& family = GetOrCrash(stream->Families, familyId);
            auto* page = FindPageByTimestamp(family, GetTimerPageTimestamp(PageKind, *timer));
            YT_VERIFY(page && page->Resident);
            EraseTimer(&family, page, *timer);
        }
        i64 movedCount = std::ssize(moving);
        if (stream->Spec->DeduplicateEqualTimestamps) {
            // An unconverted old-form page may hold several timers of one slot.
            std::vector<TInputTimerConstPtr> resolved;
            TSlotIndex slots;
            for (auto& timer : moving) {
                AddResolvingSlot(&resolved, &slots, std::move(timer), &DeduplicationPrefers);
            }
            moving = std::move(resolved);
        }
        auto existing = LookupSlots(moving);
        for (int index = 0; index < std::ssize(moving); ++index) {
            const auto& timer = *moving[index];
            if (const auto& found = existing[index]) {
                if (DeduplicationReplaces(timer, found->Timer)) {
                    EvictStoredTimer(*found);
                } else {
                    LogDeduplicated(timer);
                    continue;
                }
            }
            InsertTimer(timer);
            YT_TLOG_DEBUG("Timer canonicalized")
                .With("MessageId", timer.MessageId)
                .With("StreamId", streamId)
                .With("FamilyId", familyId);
        }
        return movedCount;
    }

    void SplitPage(TFamilyState* family, TPageState* page)
    {
        TSystemTimestamp cut;
        TTimerPageStatistics head;
        TTimerPageStatistics tail;
        if (page->Resident) {
            auto split = family->Data.FindSplitPoint(PageSize_, page->Meta.Begin, page->Meta.End);
            if (!split) {
                page->UncuttableAtCount = page->Meta.Count;
                return;
            }
            cut = *split;
            head = family->Data.GetStatistics(page->Meta.Begin, cut);
            tail = family->Data.GetStatistics(cut, page->Meta.End);
        } else {
            auto median = WaitFor(Tables_->FindMedianTimestamp(page->Meta)).ValueOrThrow();
            if (!median) {
                // The stored count overstates the rows; a load corrects it.
                YT_TLOG_WARNING("Nonempty timer page has no median row; leaving it alone")
                    .With("FamilyId", family->FamilyId)
                    .With("Begin", page->Meta.Begin)
                    .With("StoredCount", page->Meta.Count);
                page->UncuttableAtCount = page->Meta.Count;
                return;
            }
            cut = *median;
            std::tie(head, tail) = WaitFor(Tables_->AggregateSplit(page->Meta, cut)).ValueOrThrow();
            // A majority at the minimum timestamp must stay together on the left.
            if (!head.Count && tail.Count && tail.MinTimestamp < tail.MaxTimestamp) {
                cut = TSystemTimestamp(tail.MinTimestamp.Underlying() + 1);
                std::tie(head, tail) = WaitFor(Tables_->AggregateSplit(page->Meta, cut)).ValueOrThrow();
            }
            page->MaxTimestamp = std::max(head.MaxTimestamp, tail.MaxTimestamp);
            if (!head.Count || !tail.Count) {
                page->UncuttableAtCount = page->Meta.Count;
                return;
            }
        }

        TPageState second{
            .Meta = page->Meta,
            .Class = page->Class,
            .Created = true,
            .MaxTimestamp = tail.MaxTimestamp,
        };
        second.Meta.Begin = cut;
        second.Meta.Count = tail.Count;
        second.Meta.MinEvent = tail.MinEvent;
        second.Meta.MinSystem = tail.MinSystem;
        second.Meta.MaxTimestamp = tail.MaxTimestamp;
        second.Meta.Flags.DirtyMinima = false;
        if (page->Resident) {
            second.Resident = tail;
            page->Resident = head;
        }
        page->Meta.End = cut;
        page->Meta.Count = head.Count;
        page->Meta.MinEvent = head.MinEvent;
        page->Meta.MinSystem = head.MinSystem;
        page->Meta.Flags.DirtyMinima = false;
        page->PendingFlags.DirtyMinima = false;
        page->PendingRange = true;
        page->PendingMinima = true;
        page->PendingCountDelta -= tail.Count;
        page->MaxTimestamp = head.MaxTimestamp;
        EstablishMaxTimestamp(page, head.MaxTimestamp);
        Metrics_.Splits.Increment();
        EmplaceOrCrash(family->Pages, cut, std::move(second));
    }

    TSystemTimestamp ComputeWatermark(const TStreamId& streamId, const TStreamState& stream) const
    {
        const auto& spec = stream.Spec;
        if (spec->TimeType == ETimeType::CurrentTime) {
            // Watched streams and their delays are stream-time coordinates and do not apply to current time.
            return WatermarkState_->GetCurrentTimestamp();
        }
        auto watermark = InfinitySystemTimestamp;
        if (spec->StreamsWithDelays) {
            for (const auto& [inputStreamId, delay] : *spec->StreamsWithDelays) {
                auto streamWatermark = WatermarkState_->GetWatermark(inputStreamId, spec->TimeType);
                auto delayed = TSystemTimestamp(std::max(streamWatermark.Underlying(), delay.Seconds()) - delay.Seconds());
                watermark = std::min(watermark, delayed);
            }
        } else if (spec->Streams) {
            for (const auto& inputStreamId : *spec->Streams) {
                watermark = std::min(watermark, WatermarkState_->GetWatermark(inputStreamId, spec->TimeType));
            }
        } else {
            for (const auto& inputStreamId : GetOrCrash(Context_->StreamsDependency, streamId)) {
                watermark = std::min(watermark, WatermarkState_->GetWatermark(inputStreamId, spec->TimeType));
            }
        }
        return watermark;
    }

    //! The frontier of a family: its timers below it are all resident. Infinity once every
    //! page is loaded.
    static TSystemTimestamp GetFrontier(const TFamilyState& family)
    {
        for (const auto& [begin, page] : family.Pages) {
            if (!page.Resident) {
                return begin;
            }
        }
        return InfinitySystemTimestamp;
    }

    //! The timestamp below which the resident prefixes of every family are complete.
    static TSystemTimestamp GetFrontier(const TStreamState& stream)
    {
        auto frontier = InfinitySystemTimestamp;
        for (const auto& [familyId, family] : stream.Families) {
            frontier = std::min(frontier, GetFrontier(family));
        }
        return frontier;
    }

    struct TFiringTimer
    {
        TFamilyState* Family = nullptr;
        TPageState* Page = nullptr;
        TTimer Timer;
    };

    void TriggerTimers(const TStreamId& streamId, TStreamState* stream)
    {
        // Timers past the frontier are not offered even when resident: an unread page of
        // another family may hold earlier ones.
        auto frontier = GetFrontier(*stream);
        std::vector<TFiringTimer> firing;
        for (auto& [familyId, family] : stream->Families) {
            for (auto& [begin, page] : family.Pages) {
                if (!page.Resident) {
                    break;
                }
                bool exhausted = false;
                for (const auto& entry : family.Data.GetEntries(page.Meta.Begin, page.Meta.End)) {
                    if (entry.GetTimestamp() > stream->Watermark || entry.GetTimestamp() >= frontier) {
                        exhausted = true;
                        break;
                    }
                    if (stream->Offered.contains(entry.MessageId)) {
                        continue;
                    }
                    auto timer = entry.ToTimer(PageKind);
                    timer.StreamId = streamId;
                    timer.KeySchema = Context_->KeySchema;
                    firing.push_back(TFiringTimer{.Family = &family, .Page = &page, .Timer = std::move(timer)});
                }
                if (exhausted) {
                    break;
                }
            }
        }

        if (stream->Spec->DeduplicateEqualTimestamps) {
            DeduplicateFiring(&firing);
        }
        for (auto& candidate : firing) {
            auto inputTimer = New<TInputTimer>(std::move(candidate.Timer), Context_->KeySchema);
            stream->Offered.insert(inputTimer->MessageId);
            stream->ReadyByteSize += inputTimer->ByteSize;
            stream->OfferedCounter.Inc();
            YT_TLOG_DEBUG("MessageLifeCycle.TimerStore: timer was triggered")
                .With("MessageId", inputTimer->MessageId)
                .With("StreamId", streamId);
            InsertOrCrash(stream->Triggered, std::move(inputTimer));
        }
    }

    //! Keeps one timer per slot among the firing ones; the losers are erased. Pages of
    //! different families may hold the same slot after a deduplication mode change.
    void DeduplicateFiring(std::vector<TFiringTimer>* firing)
    {
        std::map<TSlotKey, int> winners;
        std::vector<bool> lost(firing->size());
        for (int index = 0; index < std::ssize(*firing); ++index) {
            const auto& candidate = (*firing)[index];
            auto [it, inserted] = winners.emplace(TSlotKey{candidate.Timer.Key, candidate.Timer.TriggerTimestamp}, index);
            if (inserted) {
                continue;
            }
            if (DeduplicationPrefers(candidate.Timer, (*firing)[it->second].Timer)) {
                lost[it->second] = true;
                it->second = index;
            } else {
                lost[index] = true;
            }
        }
        std::vector<TFiringTimer> kept;
        for (int index = 0; index < std::ssize(*firing); ++index) {
            auto& candidate = (*firing)[index];
            if (!lost[index]) {
                kept.push_back(std::move(candidate));
                continue;
            }
            LogDeduplicated(candidate.Timer);
            EraseTimer(candidate.Family, candidate.Page, candidate.Timer);
        }
        *firing = std::move(kept);
    }

    std::vector<std::optional<TStoredTimer>> LookupSlots(const std::vector<TInputTimerConstPtr>& candidates)
    {
        struct TResidentLookup
        {
            TFamilyState* Family = nullptr;
            THashMap<TKey, int> Candidates;
        };

        std::vector<std::optional<TStoredTimer>> result(candidates.size());
        std::vector<TTimerPageRowKey> rowKeys;
        std::vector<int> owners;
        std::map<std::pair<TTimerFamilyId, TSystemTimestamp>, TResidentLookup> residentLookups;
        auto found = [&] (int index, const TTimerPageRowKey& rowKey, const TTimer& timer) {
            auto& slot = result[index];
            if (slot) {
                // A slot must be stored once; keep the winner and drop the other row.
                YT_TLOG_WARNING("Timer slot is stored twice")
                    .With("StreamId", candidates[index]->StreamId)
                    .With("Key", rowKey.Key)
                    .With("TriggerTimestamp", rowKey.Timestamp);
                if (DeduplicationPrefers(timer, slot->Timer)) {
                    EvictStoredTimer(*slot);
                } else {
                    auto loser = TStoredTimer{rowKey.FamilyId, timer};
                    loser.Timer.StreamId = candidates[index]->StreamId;
                    EvictStoredTimer(loser);
                    return;
                }
            }
            slot = TStoredTimer{.FamilyId = rowKey.FamilyId, .Timer = timer};
            slot->Timer.StreamId = candidates[index]->StreamId;
        };
        for (int index = 0; index < std::ssize(candidates); ++index) {
            const auto& timer = candidates[index];
            auto& stream = GetOrCrash(Streams_, timer->StreamId);
            if (!stream.Spec->DeduplicateEqualTimestamps) {
                continue;
            }
            for (auto& [familyId, family] : stream.Families) {
                if (!family.KeyRange.Contains(timer->Key)) {
                    continue;
                }
                auto* page = FindPageByTimestamp(family, timer->TriggerTimestamp);
                // A page storing rows with ids holds no slots; its duplicates fire out.
                if (!page || !page->Meta.Flags.DedupSlots) {
                    continue;
                }
                auto rowKey = MakeTimerPageRowKey(familyId, PageKind, *timer, /*dedupSlots*/ true);
                // A row changed in this iteration is known without a lookup: a pending write is
                // the slot's timer, a pending erasure means the stored row is gone.
                if (auto it = Pending_.Rows.find(rowKey); it != Pending_.Rows.end()) {
                    if (it->second.Write) {
                        found(index, rowKey, *it->second.Write);
                    }
                    continue;
                }
                // Past the page's last row the slot cannot be stored: the common case of
                // timers armed ahead of every stored one.
                if (page->MaxTimestamp && *page->MaxTimestamp < timer->TriggerTimestamp) {
                    continue;
                }
                if (page->Resident) {
                    auto& lookup = residentLookups[{familyId, timer->TriggerTimestamp}];
                    lookup.Family = &family;
                    lookup.Candidates.emplace(timer->Key, index);
                    continue;
                }
                rowKeys.push_back(std::move(rowKey));
                owners.push_back(index);
            }
        }
        for (const auto& [group, lookup] : residentLookups) {
            auto [familyId, timestamp] = group;
            std::vector<std::pair<int, TTimer>> matches;
            for (const auto& [key, index] : lookup.Candidates) {
                for (auto& timer : lookup.Family->Data.FindEntriesByKey(timestamp, key)) {
                    matches.emplace_back(index, std::move(timer));
                }
            }
            // A slot is normally stored once; the order only matters for a stored duplicate.
            std::sort(matches.begin(), matches.end(), [] (const auto& lhs, const auto& rhs) {
                return std::tie(lhs.first, lhs.second.MessageId) < std::tie(rhs.first, rhs.second.MessageId);
            });
            // Resolving duplicates can erase entries from the resident tree.
            for (const auto& [index, timer] : matches) {
                found(index, MakeTimerPageRowKey(familyId, PageKind, timer, /*dedupSlots*/ true), timer);
            }
        }
        if (rowKeys.empty()) {
            return result;
        }

        NProfiling::TWallTimer timer;
        auto rows = WaitFor(Tables_->LookupRows(rowKeys)).ValueOrThrow();
        Metrics_.LookupTime.Record(timer.GetElapsedTime());
        Metrics_.LookupKeys.Increment(std::ssize(rowKeys));
        for (int index = 0; index < std::ssize(rows); ++index) {
            if (const auto& row = rows[index]) {
                found(owners[index], row->RowKey, row->Timer);
            }
        }
        return result;
    }

    //! Removes a stored timer that lost a deduplication.
    void EvictStoredTimer(const TStoredTimer& stored)
    {
        auto& stream = GetOrCrash(Streams_, stored.Timer.StreamId);
        auto& family = GetOrCrash(stream.Families, stored.FamilyId);
        auto* page = FindPageByTimestamp(family, stored.Timer.TriggerTimestamp);
        if (!page) {
            THROW_ERROR_EXCEPTION("Timer page %v of family %v is unknown during deduplication",
                stored.Timer.TriggerTimestamp,
                stored.FamilyId);
        }
        auto timer = stored.Timer;
        timer.KeySchema = Context_->KeySchema;
        if (stream.Offered.erase(timer.MessageId) > 0) {
            // Offered or taken: a later unregister must not look for it.
            auto inputTimer = New<TInputTimer>(TTimer(timer), Context_->KeySchema);
            if (auto it = stream.Triggered.find(inputTimer); it != stream.Triggered.end()) {
                stream.ReadyByteSize -= (*it)->ByteSize;
                stream.Triggered.erase(it);
            }
            Deduplicated_.insert(timer.MessageId);
        }
        EraseTimer(&family, page, timer);
        LogDeduplicated(timer);
    }

    //! The page of the family whose interval holds the timestamp, if any.
    static TPageState* FindPageByTimestamp(TFamilyState& family, TSystemTimestamp timestamp)
    {
        auto it = family.Pages.upper_bound(timestamp);
        if (it == family.Pages.begin()) {
            return nullptr;
        }
        --it;
        const auto& end = it->second.Meta.End;
        return !end || timestamp < *end ? &it->second : nullptr;
    }

    std::optional<TTimerLocation> FindResidentTimer(TStreamState& stream, const TTimer& timer)
    {
        for (auto& [familyId, family] : stream.Families) {
            auto* page = FindPageByTimestamp(family, GetTimerPageTimestamp(PageKind, timer));
            if (page && page->Resident && family.Data.Contains(timer)) {
                return TTimerLocation{.Family = &family, .Page = page};
            }
        }
        return std::nullopt;
    }

    TTimerPageRowKey MakeRowKey(const TFamilyState& family, const TPageState& page, const TTimer& timer) const
    {
        // The row form follows the page, never the spec: the spec may have changed since the
        // row was written.
        return MakeTimerPageRowKey(family.FamilyId, PageKind, timer, page.Meta.Flags.DedupSlots);
    }

    //! Puts a timer into the canonical page holding its trigger.
    void InsertTimer(const TTimer& timer)
    {
        auto& stream = GetOrCrash(Streams_, timer.StreamId);
        auto timestamp = GetTimerPageTimestamp(PageKind, timer);
        auto location = *FindOrMakeCanonicalPage(timer.StreamId, &stream, timestamp);
        if (!location.Page->Meta.End && location.Page->Meta.Count >= PageSize_ &&
            location.Page->MaxTimestamp && timestamp > *location.Page->MaxTimestamp)
        {
            location.Page = AppendPage(location.Family, location.Page, timestamp);
        }
        auto& page = *location.Page;

        if (page.Resident) {
            i64 bytes = location.Family->Data.GetByteSize();
            if (!location.Family->Data.Insert(timer)) {
                THROW_ERROR_EXCEPTION("Timer %v of stream %Qv is registered twice",
                    timer.MessageId,
                    timer.StreamId);
            }
            page.Resident->Add(timer, PageKind);
            page.Resident->ByteSize += location.Family->Data.GetByteSize() - bytes;
        }
        WriteRow(MakeRowKey(*location.Family, page, timer), timer);
        ++page.Meta.Count;
        ++page.PendingCountDelta;
        if (page.MaxTimestamp) {
            page.MaxTimestamp = std::max(*page.MaxTimestamp, timestamp);
        }
        RaiseMaxTimestamp(&page, timestamp);
        if (page.Meta.Count == 1 || timer.EventTimestamp < page.Meta.MinEvent || timer.SystemTimestamp < page.Meta.MinSystem) {
            page.Meta.MinEvent = page.Meta.Count == 1 ? timer.EventTimestamp : std::min(page.Meta.MinEvent, timer.EventTimestamp);
            page.Meta.MinSystem = page.Meta.Count == 1 ? timer.SystemTimestamp : std::min(page.Meta.MinSystem, timer.SystemTimestamp);
            page.PendingMinima = true;
        }
        YT_TLOG_DEBUG("MessageLifeCycle.TimerStore: timer was registered")
            .With("MessageId", timer.MessageId)
            .With("StreamId", timer.StreamId);
    }

    //! Raises the stored upper bound of the page's row timestamps; an unknown bound stays unknown.
    static void RaiseMaxTimestamp(TPageState* page, TSystemTimestamp timestamp)
    {
        if (page->Meta.MaxTimestamp && timestamp > *page->Meta.MaxTimestamp) {
            page->Meta.MaxTimestamp = timestamp;
            page->PendingMaxTimestamp = true;
        }
    }

    //! Stores an upper bound for a page that has none; |timestamp| must cover all of its rows.
    static void EstablishMaxTimestamp(TPageState* page, TSystemTimestamp timestamp)
    {
        if (!page->Meta.MaxTimestamp) {
            page->Meta.MaxTimestamp = timestamp;
            page->PendingMaxTimestamp = true;
        }
    }

    TPageState* AppendPage(TFamilyState* family, TPageState* page, TSystemTimestamp begin)
    {
        YT_VERIFY(page->Class == ETimerPageClass::Canonical && !page->Meta.Flags.OnceShared && !page->Meta.End);
        TPageState tail{
            .Meta = page->Meta,
            .Class = page->Class,
            .Resident = TTimerPageStatistics{},
            .Created = true,
            .MaxTimestamp = begin,
        };
        tail.Meta.Begin = begin;
        tail.Meta.Count = 0;
        tail.Meta.MaxTimestamp = begin;
        tail.Meta.Flags.DirtyMinima = false;
        page->Meta.End = begin;
        page->PendingRange = true;
        Metrics_.PageClosures.Increment();
        return &EmplaceOrCrash(family->Pages, begin, std::move(tail))->second;
    }

    std::optional<TTimerLocation> FindOrMakeCanonicalPage(
        const TStreamId& streamId,
        TStreamState* stream,
        TSystemTimestamp timestamp,
        std::optional<TTimerFamilyId> destinationFamily = {})
    {
        if (auto location = FindIndexedCanonicalPage(*stream, timestamp)) {
            return !destinationFamily || location->Family->FamilyId == *destinationFamily ? location : std::nullopt;
        }
        RebuildCanonicalIndex(stream);
        if (auto location = FindIndexedCanonicalPage(*stream, timestamp)) {
            return !destinationFamily || location->Family->FamilyId == *destinationFamily ? location : std::nullopt;
        }

        TTimerLocation previous;
        TTimerLocation next;
        for (auto& [familyId, family] : stream->Families) {
            for (auto& [begin, page] : family.Pages) {
                if (page.Class != ETimerPageClass::Canonical) {
                    continue;
                }
                if (begin <= timestamp) {
                    if (!page.Meta.End || timestamp < *page.Meta.End) {
                        return !destinationFamily || familyId == *destinationFamily
                            ? std::optional(TTimerLocation{.Family = &family, .Page = &page})
                            : std::nullopt;
                    }
                    if (!previous.Page || previous.Page->Meta.Begin < begin) {
                        previous = TTimerLocation{.Family = &family, .Page = &page};
                    }
                } else if (!next.Page || begin < next.Page->Meta.Begin) {
                    next = TTimerLocation{.Family = &family, .Page = &page};
                }
            }
        }

        std::optional<TSystemTimestamp> previousEnd;
        if (previous.Page) {
            auto familyNext = std::next(previous.Family->Pages.find(previous.Page->Meta.Begin));
            if (familyNext != previous.Family->Pages.end()) {
                previousEnd = familyNext->first;
            }
            if (next.Page && (!previousEnd || next.Page->Meta.Begin < *previousEnd)) {
                previousEnd = next.Page->Meta.Begin;
            }
        }
        bool previousFits = previous.Page && (!destinationFamily || previous.Family->FamilyId == *destinationFamily) && (!previousEnd || timestamp < *previousEnd);

        auto nextBegin = ZeroSystemTimestamp;
        if (next.Page) {
            auto it = next.Family->Pages.find(next.Page->Meta.Begin);
            if (it != next.Family->Pages.begin()) {
                nextBegin = std::prev(it)->second.Meta.End.value_or(nextBegin);
            }
            if (previous.Page && previous.Page->Meta.End) {
                nextBegin = std::max(nextBegin, *previous.Page->Meta.End);
            }
        }
        bool nextFits = next.Page && (!destinationFamily || next.Family->FamilyId == *destinationFamily) && nextBegin <= timestamp;

        if (!previousFits && !nextFits) {
            if (destinationFamily) {
                return std::nullopt;
            }
            // The gap between the canonical neighbours is free of canonical pages.
            auto begin = previous.Page ? *previous.Page->Meta.End : ZeroSystemTimestamp;
            std::optional<TSystemTimestamp> end;
            if (next.Page) {
                end = next.Page->Meta.Begin;
            }
            YT_VERIFY(begin <= timestamp && (!end || timestamp < *end));
            return CreateCanonicalFamily(streamId, stream, begin, end);
        }

        if (previousFits && (!nextFits || timestamp.Underlying() - previous.Page->Meta.End->Underlying() <= next.Page->Meta.Begin.Underlying() - timestamp.Underlying())) {
            auto& page = *previous.Page;
            page.Meta.End = previousEnd;
            page.PendingRange = true;
            YT_TLOG_DEBUG("Timer page extended forward")
                .With("FamilyId", previous.Family->FamilyId)
                .With("Begin", page.Meta.Begin)
                .With("End", previousEnd);
            return previous;
        }

        auto& family = *next.Family;
        auto it = family.Pages.find(next.Page->Meta.Begin);
        auto state = std::move(it->second);
        family.Pages.erase(it);
        if (!state.Created) {
            Pending_.DeletedPages.push_back(state.Meta);
        }
        state.Created = true;
        state.Meta.Begin = nextBegin;
        state.PendingRange = true;
        auto* page = &EmplaceOrCrash(family.Pages, nextBegin, std::move(state))->second;
        YT_TLOG_DEBUG("Timer page extended backward")
            .With("FamilyId", family.FamilyId)
            .With("Begin", nextBegin);
        return TTimerLocation{.Family = &family, .Page = page};
    }

    static std::optional<TTimerLocation> FindIndexedCanonicalPage(TStreamState& stream, TSystemTimestamp timestamp)
    {
        auto it = stream.CanonicalIndex.upper_bound(timestamp);
        if (it == stream.CanonicalIndex.begin()) {
            return std::nullopt;
        }
        --it;
        auto familyIt = stream.Families.find(it->second);
        if (familyIt == stream.Families.end()) {
            return std::nullopt;
        }
        auto pageIt = familyIt->second.Pages.find(it->first);
        if (pageIt == familyIt->second.Pages.end()) {
            return std::nullopt;
        }
        auto& page = pageIt->second;
        if (page.Class != ETimerPageClass::Canonical || (page.Meta.End && *page.Meta.End <= timestamp)) {
            return std::nullopt;
        }
        return TTimerLocation{.Family = &familyIt->second, .Page = &page};
    }

    static void RebuildCanonicalIndex(TStreamState* stream)
    {
        stream->CanonicalIndex.clear();
        for (const auto& [familyId, family] : stream->Families) {
            for (const auto& [begin, page] : family.Pages) {
                if (page.Class == ETimerPageClass::Canonical) {
                    stream->CanonicalIndex[begin] = familyId;
                }
            }
        }
    }

    TTimerLocation CreateCanonicalFamily(
        const TStreamId& streamId,
        TStreamState* stream,
        TSystemTimestamp begin,
        std::optional<TSystemTimestamp> end)
    {
        auto familySeqNo = WaitFor(TimeProvider_->GenerateGlobalUniqueSeqNo()).ValueOrThrow();
        auto familyId = TTimerFamilyId(familySeqNo.UniqueSeqNo.Underlying() * FamilyIdMultiplier);

        TPageState page{
            .Meta = TTimerPageMeta{
                .ComputationId = ComputationId_,
                .FamilyId = familyId,
                .StreamId = streamId,
                .KeyRange = PartitionRange_,
                .Kind = PageKind,
                .Begin = begin,
                .End = end,
                .Flags = {.DedupSlots = stream->Spec->DeduplicateEqualTimestamps},
                .MaxTimestamp = begin,
            },
            .Class = ETimerPageClass::Canonical,
            .Created = true,
            .MaxTimestamp = begin,
        };
        page.Resident.emplace();
        TFamilyState family{
            .FamilyId = familyId,
            .KeyRange = PartitionRange_,
            .Data = TTimerPageData(PageKind, stream->Spec->DeduplicateEqualTimestamps),
        };
        EmplaceOrCrash(family.Pages, begin, std::move(page));
        YT_TLOG_INFO("Canonical timer family created")
            .With("StreamId", streamId)
            .With("FamilyId", familyId)
            .With("Begin", begin)
            .With("End", end);
        auto& inserted = EmplaceOrCrash(stream->Families, familyId, std::move(family))->second;
        return TTimerLocation{.Family = &inserted, .Page = &inserted.Pages.begin()->second};
    }

    void EraseTimer(TFamilyState* family, TPageState* page, const TTimer& timer)
    {
        if (page->Resident) {
            i64 bytes = family->Data.GetByteSize();
            YT_VERIFY(family->Data.Erase(timer));
            --page->Resident->Count;
            page->Resident->ByteSize += family->Data.GetByteSize() - bytes;
        }
        EraseRow(MakeRowKey(*family, *page, timer));
        --page->Meta.Count;
        --page->PendingCountDelta;

        if (page->Resident) {
            // Recomputing on every erase would scan the page per fired timer; once per page at
            // the sync is enough.
            page->StaleMinima = true;
        } else if (page->IsOwned() && !page->Meta.Flags.OnceShared &&
            (timer.EventTimestamp == page->Meta.MinEvent || timer.SystemTimestamp == page->Meta.MinSystem) && !page->Meta.Flags.DirtyMinima) {
            // A cross-family replacement may raise either minimum of the source page.
            page->Meta.Flags.DirtyMinima = true;
            page->PendingFlags.DirtyMinima = true;
        }
    }

    using TPageIterator = std::map<TSystemTimestamp, TPageState>::iterator;

    static bool HaveSameRowForm(const TPageState& lhs, const TPageState& rhs)
    {
        return lhs.Meta.FormatVersion == rhs.Meta.FormatVersion &&
            lhs.Meta.Flags.DedupSlots == rhs.Meta.Flags.DedupSlots &&
            lhs.Meta.Flags.EventDone == rhs.Meta.Flags.EventDone &&
            // A stored bound cannot be unset, so it must not cover a page that has none.
            lhs.Meta.MaxTimestamp.has_value() == rhs.Meta.MaxTimestamp.has_value();
    }

    void MergePages(TFamilyState* family, TPageIterator first, TPageIterator second)
    {
        auto& page = first->second;
        auto& tail = second->second;
        YT_VERIFY(page.Meta.End == tail.Meta.Begin);
        YT_VERIFY(page.Class == tail.Class);
        if (page.Resident && tail.Resident) {
            page.Resident->Add(*tail.Resident);
        } else if (!page.Meta.Count) {
            page.Resident = tail.Resident;
        } else if (tail.Meta.Count) {
            YT_VERIFY(!page.Resident && !tail.Resident);
        }
        if (!tail.Created) {
            Pending_.DeletedPages.push_back(tail.Meta);
        }
        page.Meta.End = tail.Meta.End;
        page.PendingRange = true;
        if (tail.Meta.Count) {
            page.Meta.MinEvent = page.Meta.Count ? std::min(page.Meta.MinEvent, tail.Meta.MinEvent) : tail.Meta.MinEvent;
            page.Meta.MinSystem = page.Meta.Count ? std::min(page.Meta.MinSystem, tail.Meta.MinSystem) : tail.Meta.MinSystem;
        }
        page.Meta.Count += tail.Meta.Count;
        page.PendingCountDelta += tail.Meta.Count;
        page.PendingMinima = true;
        page.Meta.Flags.DirtyMinima |= tail.Meta.Flags.DirtyMinima;
        page.PendingFlags.DirtyMinima = page.Meta.Flags.DirtyMinima;
        page.StaleMinima = page.Resident && (page.StaleMinima || tail.StaleMinima);
        if (tail.Meta.MaxTimestamp) {
            RaiseMaxTimestamp(&page, *tail.Meta.MaxTimestamp);
        }
        if (page.MaxTimestamp && tail.MaxTimestamp) {
            page.MaxTimestamp = std::max(*page.MaxTimestamp, *tail.MaxTimestamp);
        } else {
            // The stored bound is an upper bound, which is all the consumers need.
            page.MaxTimestamp = page.Meta.MaxTimestamp;
        }
        family->Pages.erase(second);
        Metrics_.Merges.Increment();
    }

    void MergeSmallPages(TFamilyState* family, int* budget)
    {
        for (auto first = family->Pages.begin(); first != family->Pages.end() && *budget > 0;) {
            auto second = std::next(first);
            if (second == family->Pages.end()) {
                break;
            }
            auto& page = first->second;
            auto& tail = second->second;
            if (page.IsOwned() && !page.Meta.Flags.OnceShared && page.Class == tail.Class && HaveSameRowForm(page, tail) &&
                page.Meta.End == tail.Meta.Begin &&
                page.Meta.Count + tail.Meta.Count <= PageSize_ / 2 &&
                page.Resident.has_value() == tail.Resident.has_value())
            {
                MergePages(family, first, second);
                --*budget;
            } else {
                first = second;
            }
        }
    }

    //! Checks a nonresident stored page with a zero count against the table before it is deleted
    //! outright; restores the count and minima from the table and returns true when rows exist.
    bool RestoreCountIfRowsExist(TPageState* page)
    {
        if (page->Created || page->Resident) {
            return false;
        }
        auto statistics = WaitFor(Tables_->Aggregate(page->Meta)).ValueOrThrow();
        // The rows erased in this iteration are still stored.
        i64 pendingErases = std::ranges::count_if(GetPendingRows(page->Meta), [] (const auto& entry) {
            return entry.second.Erase;
        });
        i64 rowCount = statistics.Count - pendingErases;
        if (rowCount <= 0) {
            return false;
        }
        YT_TLOG_WARNING("Timer page count understates its rows; keeping the page")
            .With("FamilyId", page->Meta.FamilyId)
            .With("Begin", page->Meta.Begin)
            .With("StoredCount", page->Meta.Count)
            .With("ActualCount", rowCount);
        page->PendingCountDelta += rowCount - page->Meta.Count;
        page->Meta.Count = rowCount;
        page->Meta.MinEvent = statistics.MinEvent;
        page->Meta.MinSystem = statistics.MinSystem;
        page->PendingMinima = true;
        return true;
    }

    void PruneEmptyPages(TStreamState* stream)
    {
        for (auto familyIt = stream->Families.begin(); familyIt != stream->Families.end();) {
            auto& family = familyIt->second;
            for (auto it = family.Pages.begin(); it != family.Pages.end();) {
                auto& page = it->second;
                if (!page.IsOwned() || page.Meta.Flags.OnceShared) {
                    if (page.Resident && !page.Resident->Count && !page.PendingCountDelta &&
                        page.PendingFlags.IsEmpty() && !page.PendingRange && !page.PendingMinima)
                    {
                        it = family.Pages.erase(it);
                    } else {
                        ++it;
                    }
                    continue;
                }
                if (page.Meta.Count) {
                    ++it;
                    continue;
                }
                // An empty open tail after a full page is the insertion point of the family.
                if (!page.Meta.End && it != family.Pages.begin() && std::prev(it)->second.Meta.Count >= PageSize_) {
                    ++it;
                    continue;
                }
                if (family.Pages.size() > 1) {
                    auto previous = it == family.Pages.begin() ? family.Pages.end() : std::prev(it);
                    auto next = std::next(it);
                    auto canMerge = [&] (TPageIterator neighbor) {
                        if (neighbor == family.Pages.end() || neighbor->second.Class != page.Class ||
                            !HaveSameRowForm(neighbor->second, page))
                        {
                            return false;
                        }
                        // A gap is left by a once-shared page dropped from memory.
                        return neighbor == previous
                            ? neighbor->second.Meta.End == page.Meta.Begin
                            : page.Meta.End == neighbor->second.Meta.Begin;
                    };
                    auto first = previous;
                    if (!canMerge(first)) {
                        first = canMerge(next) ? it : family.Pages.end();
                    }
                    if (first != family.Pages.end()) {
                        MergePages(&family, first, std::next(first));
                        it = first;
                        continue;
                    }
                    // Trim mixed-class empty edges; keep interior coverage and pending conversions.
                    if (previous != family.Pages.end() && next != family.Pages.end()) {
                        ++it;
                        continue;
                    }
                    auto neighbor = previous == family.Pages.end() ? next : previous;
                    if (neighbor->second.Class == page.Class) {
                        ++it;
                        continue;
                    }
                }
                if (RestoreCountIfRowsExist(&page)) {
                    ++it;
                    continue;
                }
                if (!page.Created) {
                    Pending_.DeletedPages.push_back(page.Meta);
                }
                it = family.Pages.erase(it);
            }
            if (family.Pages.empty()) {
                familyIt = stream->Families.erase(familyIt);
            } else {
                ++familyIt;
            }
        }
    }

    void WriteRow(TTimerPageRowKey rowKey, const TTimer& timer)
    {
        Pending_.Rows[std::move(rowKey)].Write = timer;
    }

    void EraseRow(TTimerPageRowKey rowKey)
    {
        auto& change = Pending_.Rows[std::move(rowKey)];
        if (change.Write) {
            // The row was written in this iteration: nothing is stored yet.
            change.Write.reset();
        } else {
            change.Erase = true;
        }
    }

    //! A lower bound of the trigger timestamps of the stream's unprocessed timers, offered and triggered ones included.
    static std::optional<TSystemTimestamp> GetMinTriggerTimestamp(const TStreamState& stream)
    {
        std::optional<TSystemTimestamp> result;
        for (const auto& [familyId, family] : stream.Families) {
            for (const auto& [begin, page] : family.Pages) {
                std::optional<TSystemTimestamp> pageMin;
                if (page.Resident) {
                    if (page.Resident->Count) {
                        pageMin = page.StaleMinima
                            ? family.Data.GetStatistics(page.Meta.Begin, page.Meta.End).MinTimestamp
                            : page.Resident->MinTimestamp;
                    }
                } else if (page.Meta.Count > 0) {
                    // |Begin| is a lower bound of the rows, so the lag may be over-reported but never under-reported.
                    pageMin = page.Meta.Begin;
                }

                if (pageMin) {
                    result = result ? std::min(*result, *pageMin) : *pageMin;
                    break;
                }
            }
        }

        return result;
    }

    TStreamSummary Summarize(const TStreamState& stream) const
    {
        TStreamSummary summary;
        auto lowerEvent = [&] (TSystemTimestamp value) {
            summary.MinEvent = summary.MinEvent ? std::min(*summary.MinEvent, value) : value;
        };
        auto lowerSystem = [&] (TSystemTimestamp value) {
            summary.MinSystem = summary.MinSystem ? std::min(*summary.MinSystem, value) : value;
        };
        for (const auto& [familyId, family] : stream.Families) {
            for (const auto& [begin, page] : family.Pages) {
                if (page.Resident) {
                    summary.CountHigh += page.Resident->Count;
                    summary.ResidentCount += page.Resident->Count;
                    summary.ResidentByteSize += page.Resident->ByteSize;
                    if (page.Resident->Count) {
                        auto statistics = page.StaleMinima
                            ? family.Data.GetStatistics(page.Meta.Begin, page.Meta.End)
                            : *page.Resident;
                        lowerEvent(statistics.MinEvent);
                        lowerSystem(statistics.MinSystem);
                    }
                } else if (page.Meta.Count > 0) {
                    // A shared page counts whole: an upper bound with a lower bound on the minima.
                    summary.CountHigh += page.Meta.Count;
                    lowerEvent(page.Meta.MinEvent);
                    lowerSystem(page.Meta.MinSystem);
                }
            }
        }
        for (const auto& timer : stream.Triggered) {
            summary.ResidentByteSize += timer->ByteSize;
        }
        if (summary.ResidentCount > 0) {
            summary.AverageRowByteSize = summary.ResidentByteSize / summary.ResidentCount;
        }
        return summary;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

IPagedTimerStorePtr CreatePagedTimerStore(TTimerStoreContextPtr context, TDynamicTimerStoreContextPtr dynamicContext)
{
    return New<TPagedTimerStore>(std::move(context), std::move(dynamicContext));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/tables/timer_pages.h>

#include <library/cpp/yt/system/spin_lock.h>

#include <functional>
#include <map>
#include <tuple>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

// In-memory implementation of ITimerPages for unit testing.
// Transactions are ignored — every write is applied immediately; the |count| sum and |max_timestamp| max
// aggregates are emulated. Thread-safe: the store and the page catalogue use it from different threads.
class TInMemoryTimerPages
    : public ITimerPages
{
public:
    void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) override;
    TFuture<bool> TablesExist() override;

    TFuture<std::vector<TTimerPageRow>> ReadPage(
        TTimerPageMeta page,
        std::optional<TKeyRange> keyRange) override;

    TFuture<TTimerPageStatistics> Aggregate(TTimerPageMeta page) override;
    TFuture<std::optional<TSystemTimestamp>> FindMedianTimestamp(TTimerPageMeta page) override;
    TFuture<std::pair<TTimerPageStatistics, TTimerPageStatistics>> AggregateSplit(
        TTimerPageMeta page,
        TSystemTimestamp cut) override;

    TFuture<std::vector<std::optional<TTimerPageRow>>> LookupRows(
        std::vector<TTimerPageRowKey> rowKeys) override;

    void WriteRows(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TTimerPageRow>& rows) override;

    void EraseRows(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TTimerPageRowKey>& rowKeys) override;

    TFuture<std::vector<TTimerPageMeta>> LoadMeta(TMetaFilter filter) override;

    void WritePage(
        NApi::IDynamicTableTransactionPtr transaction,
        const TTimerPageMeta& page) override;

    void UpdatePage(
        NApi::IDynamicTableTransactionPtr transaction,
        const TPageUpdate& update) override;

    void ErasePage(
        NApi::IDynamicTableTransactionPtr transaction,
        const TComputationId& computationId,
        TTimerFamilyId familyId,
        ETimerPageKind kind,
        TSystemTimestamp begin) override;

    void WriteJournal(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TTimerJournalEntry>& entries) override;

    TFuture<TJournalRowRange> GetJournalRowRange() override;

    TFuture<TJournalReadResult> ReadJournal(i64 fromRowIndex, i64 limit) override;

    // Test helpers.
    i64 GetRowCount() const;
    i64 GetPageReadCount() const;
    i64 GetLookupCount() const;
    i64 GetAggregateCount() const;
    i64 GetRowWriteCount() const;
    //! The number of metadata writes, erasures and updates that change something.
    i64 GetPageWriteCount() const;
    //! Drops the journal rows before |rowCount| like a trim would.
    void TrimJournal(i64 rowCount);
    //! Called at the start of |ReadPage|, |LoadMeta|, |ReadJournal| and |GetJournalRowRange| with the
    //! method name, outside the lock: lets a test interleave writes with a reader.
    void SetHook(std::function<void(TStringBuf method)> hook);

private:
    using TMetaKey = std::tuple<TComputationId, TTimerFamilyId, ETimerPageKind, TSystemTimestamp>;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    std::function<void(TStringBuf)> Hook_;
    std::map<TTimerPageRowKey, TTimer> Rows_;
    std::map<TMetaKey, TTimerPageMeta> Pages_;
    std::vector<TTimerJournalEntry> Journal_;
    i64 JournalTrimmedRowCount_ = 0;

    i64 PageReadCount_ = 0;
    i64 LookupCount_ = 0;
    i64 AggregateCount_ = 0;
    i64 RowWriteCount_ = 0;
    i64 PageWriteCount_ = 0;

    std::vector<TTimerPageBucket> DoAggregate(const TTimerPageMeta& page, std::optional<TSystemTimestamp> cut);
    void CallHook(TStringBuf method);
};

DEFINE_REFCOUNTED_TYPE(TInMemoryTimerPages);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables

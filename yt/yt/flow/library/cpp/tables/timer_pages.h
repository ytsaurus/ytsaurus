#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/timer_page.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

//! Access to the three tables of the paged timer store: |timer_pages| rows, |timer_page_meta|
//! pages, and the |timer_page_journal| queue of page operations.
struct ITimerPages
    : public TRefCounted
{
    //! All pages, those of one computation, or those of one family of a computation.
    struct TMetaFilter
    {
        std::optional<TComputationId> ComputationId;
        std::optional<TTimerFamilyId> FamilyId;
    };

    //! Partial update of a page row; absent fields keep their values.
    struct TPageUpdate
    {
        TComputationId ComputationId;
        TTimerFamilyId FamilyId;
        ETimerPageKind Kind = ETimerPageKind::Trigger;
        TSystemTimestamp Begin;
        std::optional<std::optional<TSystemTimestamp>> End;
        TTimerPageFlagsUpdate Flags;
        std::optional<std::pair<TSystemTimestamp, TSystemTimestamp>> Minima;
        //! Added to the |count| aggregate.
        i64 CountDelta = 0;
        //! Max-merged into the |max_timestamp| aggregate.
        std::optional<TSystemTimestamp> MaxTimestamp;
    };

    struct TJournalRowRange
    {
        i64 TrimmedRowCount = 0;
        i64 TotalRowCount = 0;
    };

    struct TJournalReadResult
    {
        std::vector<TTimerJournalEntry> Entries;
        //! Row index to continue from; entries may start past |fromRowIndex| after a trim.
        i64 NextRowIndex = 0;
    };

    virtual void Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec) = 0;

    //! Whether the pipeline has the page tables at all.
    virtual TFuture<bool> TablesExist() = 0;

    //! Reads the rows of one page in table order, restricted to |keyRange| when given.
    virtual TFuture<std::vector<TTimerPageRow>> ReadPage(
        TTimerPageMeta page,
        std::optional<TKeyRange> keyRange) = 0;

    virtual TFuture<TTimerPageStatistics> Aggregate(TTimerPageMeta page) = 0;
    //! Returns the timestamp of the middle row in table order, or null for an empty page.
    virtual TFuture<std::optional<TSystemTimestamp>> FindMedianTimestamp(TTimerPageMeta page) = 0;
    //! Statistics of the ranges before |cut| and starting at |cut|.
    virtual TFuture<std::pair<TTimerPageStatistics, TTimerPageStatistics>> AggregateSplit(
        TTimerPageMeta page,
        TSystemTimestamp cut) = 0;

    //! Looks up rows by exact keys; the result is aligned with |rowKeys|, missing rows are null.
    virtual TFuture<std::vector<std::optional<TTimerPageRow>>> LookupRows(
        std::vector<TTimerPageRowKey> rowKeys) = 0;

    virtual void WriteRows(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TTimerPageRow>& rows) = 0;

    virtual void EraseRows(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TTimerPageRowKey>& rowKeys) = 0;

    //! Reads the page rows matching |filter| at one timestamp.
    virtual TFuture<std::vector<TTimerPageMeta>> LoadMeta(TMetaFilter filter) = 0;

    //! Writes every column of the page row; |Count| is stored as is, not added.
    virtual void WritePage(
        NApi::IDynamicTableTransactionPtr transaction,
        const TTimerPageMeta& page) = 0;

    virtual void UpdatePage(
        NApi::IDynamicTableTransactionPtr transaction,
        const TPageUpdate& update) = 0;

    virtual void ErasePage(
        NApi::IDynamicTableTransactionPtr transaction,
        const TComputationId& computationId,
        TTimerFamilyId familyId,
        ETimerPageKind kind,
        TSystemTimestamp begin) = 0;

    //! Appends the entries to the single journal tablet in order.
    virtual void WriteJournal(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<TTimerJournalEntry>& entries) = 0;

    virtual TFuture<TJournalRowRange> GetJournalRowRange() = 0;

    //! Reads up to |limit| journal entries with row index at least |fromRowIndex|, in row order.
    virtual TFuture<TJournalReadResult> ReadJournal(i64 fromRowIndex, i64 limit) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITimerPages);

////////////////////////////////////////////////////////////////////////////////

class TTimerPages
    : public ITimerPages
{
public:
    TTimerPages(TContextPtr context, TDynamicTableRequestSpecPtr dynamicSpec);

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

private:
    struct TTableMetrics
    {
        NProfiling::TProfiler Profiler;
        NProfiling::TCounter SelectRows = Profiler.Counter("/select_rows");
        NProfiling::TCounter SelectBytes = Profiler.Counter("/select_bytes");
        NProfiling::TEventTimer SelectTime = Profiler.Timer("/select_time");
        NProfiling::TCounter LookupRows = Profiler.Counter("/lookup_rows");
        NProfiling::TCounter LookupBytes = Profiler.Counter("/lookup_bytes");
        NProfiling::TEventTimer LookupTime = Profiler.Timer("/lookup_time");
        NProfiling::TCounter WriteRows = Profiler.Counter("/write_rows");
        NProfiling::TCounter WriteBytes = Profiler.Counter("/write_bytes");
        NProfiling::TCounter EraseRows = Profiler.Counter("/erase_rows");
    };

    struct TTable
    {
        TContextPtr Context;
        NYPath::TYPath Path;
        std::string Tag;
        TTableMetrics Metrics;
    };

    const TTable Pages_;
    const TTable Meta_;
    const TTable Journal_;

    TDynamicTableRequestSpecPtr DynamicSpec_;

    static TTable MakeTable(const TContextPtr& context, TStringBuf tableName);

    std::vector<TTimerPageRow> DoReadPage(
        TTimerPageMeta page,
        const std::optional<TKeyRange>& keyRange);
    std::vector<TTimerPageBucket> DoAggregate(const TTimerPageMeta& page, std::optional<TSystemTimestamp> cut);
    std::vector<std::optional<TTimerPageRow>> DoLookupRows(const std::vector<TTimerPageRowKey>& rowKeys);
    std::vector<TTimerPageMeta> DoLoadMeta(const TMetaFilter& filter);
    TJournalReadResult DoReadJournal(i64 fromRowIndex, i64 limit);
};

DEFINE_REFCOUNTED_TYPE(TTimerPages);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables

#include "timer_pages.h"

#include "common.h"
#include "context.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/dynamic_table_client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/string/join.h>

#include <algorithm>
#include <limits>

namespace NYT::NFlow::NTables {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int JournalTabletIndex = 0;

TYsonString KeyToYson(const TKey& key)
{
    return ConvertToYsonString(key.Underlying());
}

std::string KeyToYsonLiteral(const TKey& key)
{
    return Format("yson_string_to_any(%Qv)", ConvertToYsonString(key, EYsonFormat::Text));
}

//! Checks whether |range| is the exact-key range of |range|.Lower, i.e. its upper bound is the
//! lower key followed by a Max sentinel.
bool IsExactKeyRange(const TKeyRange& range)
{
    auto lower = range.Lower.Underlying().Get();
    auto upper = range.Upper.Underlying().Get();
    if (!lower || !upper || upper.GetCount() != lower.GetCount() + 1 ||
        upper[lower.GetCount()].Type != EValueType::Max)
    {
        return false;
    }
    for (int index = 0; index < static_cast<int>(lower.GetCount()); ++index) {
        if (CompareRowValues(lower[index], upper[index]) != 0) {
            return false;
        }
    }
    return true;
}

TKey KeyFromYson(TStringBuf yson)
{
    return ConvertTo<TKey>(TYsonStringBuf(yson));
}

template <class T>
std::optional<T> OptionalFromValue(TUnversionedValue value)
{
    if (value.Type == EValueType::Null) {
        return std::nullopt;
    }
    return FromUnversionedValue<T>(value);
}

template <class T>
T ValueOrThrow(TUnversionedValue value, TStringBuf column)
{
    auto result = OptionalFromValue<T>(value);
    THROW_ERROR_EXCEPTION_IF(!result, "Timer page column %Qv is missing", column);
    return *result;
}

i64 SumSizes(const std::vector<i64>& sizes)
{
    i64 total = 0;
    for (auto size : sizes) {
        total += size;
    }
    return total;
}

//! Builds the rows of one |ModifyRows| call: every row is added column by column and captured
//! into the shared buffer. Keeps the writers below free of name table bookkeeping.
class TRowsBuilder
{
public:
    void Begin()
    {
        Builder_ = TUnversionedRowBuilder();
    }

    void AddString(TStringBuf column, TStringBuf value)
    {
        Builder_.AddValue(RowBuffer_->CaptureValue(MakeUnversionedStringValue(value, Id(column))));
    }

    void AddAny(TStringBuf column, const TYsonString& value)
    {
        Builder_.AddValue(RowBuffer_->CaptureValue(MakeUnversionedAnyValue(value.AsStringBuf(), Id(column))));
    }

    void AddUint64(TStringBuf column, ui64 value, EValueFlags flags = EValueFlags::None)
    {
        Builder_.AddValue(MakeUnversionedUint64Value(value, Id(column), flags));
    }

    void AddOptionalUint64(TStringBuf column, std::optional<ui64> value)
    {
        Builder_.AddValue(value ? MakeUnversionedUint64Value(*value, Id(column)) : MakeUnversionedNullValue(Id(column)));
    }

    void AddInt64(TStringBuf column, i64 value, EValueFlags flags = EValueFlags::None)
    {
        Builder_.AddValue(MakeUnversionedInt64Value(value, Id(column), flags));
    }

    void AddBoolean(TStringBuf column, bool value)
    {
        Builder_.AddValue(MakeUnversionedBooleanValue(value, Id(column)));
    }

    void AddOptionalBoolean(TStringBuf column, std::optional<bool> value)
    {
        if (value) {
            AddBoolean(column, *value);
        }
    }

    //! Captures the row built so far; returns its data weight.
    i64 EndWrite()
    {
        return End(/*write*/ true);
    }

    i64 EndDelete()
    {
        return End(/*write*/ false);
    }

    std::vector<TUnversionedRow> TakeRows()
    {
        return std::move(Rows_);
    }

    void ModifyRows(const IDynamicTableTransactionPtr& transaction, const NYPath::TYPath& path)
    {
        transaction->ModifyRows(path, NameTable_, MakeSharedRange(std::move(Modifications_), RowBuffer_));
    }

    const TNameTablePtr& GetNameTable() const
    {
        return NameTable_;
    }

    const TRowBufferPtr& GetRowBuffer() const
    {
        return RowBuffer_;
    }

private:
    const TNameTablePtr NameTable_ = New<TNameTable>();
    const TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    TUnversionedRowBuilder Builder_;
    std::vector<TUnversionedRow> Rows_;
    std::vector<TRowModification> Modifications_;

    int Id(TStringBuf column)
    {
        return NameTable_->GetIdOrRegisterName(column);
    }

    i64 End(bool write)
    {
        auto row = RowBuffer_->CaptureRow(Builder_.GetRow(), /*captureValues*/ false);
        Rows_.push_back(row);
        Modifications_.push_back(write ? TRowModification(NRowModifications::TWriteRow(row)) : TRowModification(NRowModifications::TDeleteRow(row)));
        return GetDataWeight(row);
    }
};

void AddRowKey(TRowsBuilder* builder, const TTimerPageRowKey& rowKey)
{
    builder->AddUint64("family_id", rowKey.FamilyId.Underlying());
    builder->AddString("kind", FormatEnum(rowKey.Kind));
    builder->AddAny("key", KeyToYson(rowKey.Key));
    builder->AddUint64("timestamp", rowKey.Timestamp.Underlying());
    builder->AddString("row_id", rowKey.RowId.Underlying());
}

void AddPageKey(TRowsBuilder* builder, const TComputationId& computationId, TTimerFamilyId familyId, ETimerPageKind kind, TSystemTimestamp begin)
{
    builder->AddString("computation_id", computationId.Underlying());
    builder->AddUint64("family_id", familyId.Underlying());
    builder->AddString("kind", FormatEnum(kind));
    builder->AddUint64("begin_timestamp", begin.Underlying());
}

//! The value columns of a |timer_pages| row.
struct TRowValueFields
{
    int TriggerTimestamp;
    int EventTimestamp;
    int SystemTimestamp;
    int MessageId;

    explicit TRowValueFields(const TTableSchema& schema)
        : TriggerTimestamp(schema.GetColumnIndexOrThrow("trigger_timestamp"))
        , EventTimestamp(schema.GetColumnIndexOrThrow("event_timestamp"))
        , SystemTimestamp(schema.GetColumnIndexOrThrow("system_timestamp"))
        , MessageId(schema.GetColumnIndexOrThrow("message_id"))
    { }

    TTimer ReadTimer(TUnversionedRow row, const TKey& key) const
    {
        TTimer timer;
        timer.Key = key;
        timer.TriggerTimestamp = ValueOrThrow<TSystemTimestamp>(row[TriggerTimestamp], "trigger_timestamp");
        timer.EventTimestamp = ValueOrThrow<TSystemTimestamp>(row[EventTimestamp], "event_timestamp");
        timer.SystemTimestamp = ValueOrThrow<TSystemTimestamp>(row[SystemTimestamp], "system_timestamp");
        // The alignment timestamp is the persist timestamp.
        timer.AlignmentTimestamp = timer.SystemTimestamp;
        timer.MessageId = ValueOrThrow<TMessageId>(row[MessageId], "message_id");
        return timer;
    }
};

//! The columns of a |timer_page_meta| row.
struct TMetaFields
{
    int ComputationId;
    int FamilyId;
    int StreamId;
    int KeyBegin;
    int KeyEnd;
    int Kind;
    int Begin;
    int End;
    int OnceShared;
    int DirtyMinima;
    int EventDone;
    int DedupSlots;
    int Count;
    int MinEvent;
    int MinSystem;
    int MaxTimestamp;
    int FormatVersion;

    explicit TMetaFields(const TTableSchema& schema)
        : ComputationId(schema.GetColumnIndexOrThrow("computation_id"))
        , FamilyId(schema.GetColumnIndexOrThrow("family_id"))
        , StreamId(schema.GetColumnIndexOrThrow("stream_id"))
        , KeyBegin(schema.GetColumnIndexOrThrow("key_begin"))
        , KeyEnd(schema.GetColumnIndexOrThrow("key_end"))
        , Kind(schema.GetColumnIndexOrThrow("kind"))
        , Begin(schema.GetColumnIndexOrThrow("begin_timestamp"))
        , End(schema.GetColumnIndexOrThrow("end_timestamp"))
        , OnceShared(schema.GetColumnIndexOrThrow("once_shared"))
        , DirtyMinima(schema.GetColumnIndexOrThrow("dirty_minima"))
        , EventDone(schema.GetColumnIndexOrThrow("event_done"))
        , DedupSlots(schema.GetColumnIndexOrThrow("dedup_slots"))
        , Count(schema.GetColumnIndexOrThrow("count"))
        , MinEvent(schema.GetColumnIndexOrThrow("min_event"))
        , MinSystem(schema.GetColumnIndexOrThrow("min_system"))
        , MaxTimestamp(schema.GetColumnIndexOrThrow("max_timestamp"))
        , FormatVersion(schema.GetColumnIndexOrThrow("format_version"))
    { }

    //! A blind aggregate update of a page that the collector has already erased upserts a row
    //! without |stream_id|; such a row is not a page.
    bool IsGhost(TUnversionedRow row) const
    {
        return row[StreamId].Type == EValueType::Null;
    }

    std::tuple<TComputationId, TTimerFamilyId, ETimerPageKind, TSystemTimestamp> ReadKey(TUnversionedRow row) const
    {
        return {
            FromUnversionedValue<TComputationId>(row[ComputationId]),
            FromUnversionedValue<TTimerFamilyId>(row[FamilyId]),
            ParseEnum<ETimerPageKind>(ValueOrThrow<TStringBuf>(row[Kind], "kind")),
            ValueOrThrow<TSystemTimestamp>(row[Begin], "begin_timestamp"),
        };
    }

    TTimerPageMeta ReadPage(TUnversionedRow row) const
    {
        return TTimerPageMeta{
            .ComputationId = FromUnversionedValue<TComputationId>(row[ComputationId]),
            .FamilyId = FromUnversionedValue<TTimerFamilyId>(row[FamilyId]),
            .StreamId = ValueOrThrow<TStreamId>(row[StreamId], "stream_id"),
            .KeyRange = TKeyRange{
                .Lower = KeyFromYson(ValueOrThrow<TStringBuf>(row[KeyBegin], "key_begin")),
                .Upper = KeyFromYson(ValueOrThrow<TStringBuf>(row[KeyEnd], "key_end")),
            },
            .Kind = ParseEnum<ETimerPageKind>(ValueOrThrow<TStringBuf>(row[Kind], "kind")),
            .Begin = ValueOrThrow<TSystemTimestamp>(row[Begin], "begin_timestamp"),
            .End = OptionalFromValue<TSystemTimestamp>(row[End]),
            .Flags = TTimerPageFlags{
                .OnceShared = OptionalFromValue<bool>(row[OnceShared]).value_or(false),
                .DirtyMinima = OptionalFromValue<bool>(row[DirtyMinima]).value_or(false),
                .EventDone = OptionalFromValue<bool>(row[EventDone]).value_or(false),
                .DedupSlots = OptionalFromValue<bool>(row[DedupSlots]).value_or(false),
            },
            .Count = OptionalFromValue<i64>(row[Count]).value_or(0),
            .MinEvent = OptionalFromValue<TSystemTimestamp>(row[MinEvent]).value_or(ZeroSystemTimestamp),
            .MinSystem = OptionalFromValue<TSystemTimestamp>(row[MinSystem]).value_or(ZeroSystemTimestamp),
            .MaxTimestamp = OptionalFromValue<TSystemTimestamp>(row[MaxTimestamp]),
            .FormatVersion = OptionalFromValue<i64>(row[FormatVersion]).value_or(1),
        };
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTimerPages::TTimerPages(
    TContextPtr context,
    TDynamicTableRequestSpecPtr dynamicSpec)
    : Pages_(MakeTable(context, TimerPagesTableName))
    , Meta_(MakeTable(context, TimerPageMetaTableName))
    , Journal_(MakeTable(context, TimerPageJournalTableName))
    , DynamicSpec_(std::move(dynamicSpec))
{ }

TTimerPages::TTable TTimerPages::MakeTable(const TContextPtr& context, TStringBuf tableName)
{
    auto tableContext = context->WithTableName(tableName);
    auto tag = tableContext->Tag.value_or(std::string(tableName));
    return TTable{
        .Context = tableContext,
        .Path = NYPath::YPathJoin(tableContext->PipelinePath.GetPath(), tableName),
        .Tag = tag,
        .Metrics = {.Profiler = tableContext->Profiler.WithTag("tag", tag)},
    };
}

void TTimerPages::Reconfigure(TDynamicTableRequestSpecPtr dynamicSpec)
{
    DynamicSpec_ = std::move(dynamicSpec);
}

TFuture<bool> TTimerPages::TablesExist()
{
    // The three tables are created together, so one of them stands for all.
    return Meta_.Context->Client->NodeExists(Meta_.Path);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<std::vector<TTimerPageRow>> TTimerPages::ReadPage(
    TTimerPageMeta page,
    std::optional<TKeyRange> keyRange)
{
    return BIND([this, this_ = MakeStrong(this), page = std::move(page), keyRange = std::move(keyRange)] {
        return DoReadPage(page, keyRange);
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

std::vector<TTimerPageRow> TTimerPages::DoReadPage(
    TTimerPageMeta page,
    const std::optional<TKeyRange>& keyRange)
{
    std::vector<std::string> baseConditions;
    baseConditions.push_back(Format("family_id = %vu and kind = %Qv and timestamp >= %vu",
        page.FamilyId.Underlying(),
        FormatEnum(page.Kind),
        page.Begin.Underlying()));
    if (page.End) {
        baseConditions.push_back(Format("timestamp < %vu", page.End->Underlying()));
    }
    if (keyRange && IsExactKeyRange(*keyRange)) {
        // The QL comparator cannot compare a key with the Max sentinel of the successor literal.
        baseConditions.push_back(Format("key = %v", KeyToYsonLiteral(keyRange->Lower)));
    } else {
        if (keyRange && keyRange->Lower != MinKey()) {
            baseConditions.push_back(Format("key >= %v", KeyToYsonLiteral(keyRange->Lower)));
        }
        if (keyRange && keyRange->Upper != MaxKey()) {
            baseConditions.push_back(Format("key < %v", KeyToYsonLiteral(keyRange->Upper)));
        }
    }

    std::vector<TTimerPageRow> rows;
    std::optional<TTimerPageRowKey> offsetExclusive;
    TSelectLimiter limiter(DynamicSpec_);
    const i64 maxLimit = DynamicSpec_->SelectMaxLimit;
    while (true) {
        // The count is exact for an owned page when nothing is pending, so one select reads it
        // and the extra row lets that select come back short. The limiter keeps growing past a
        // stale count. A shared page is read by a key range, and its count covers other
        // partitions' rows, which would over-charge the throttler.
        i64 limit = limiter.Get();
        if (!keyRange) {
            limit = std::min(maxLimit, std::max(limit, page.Count + 1 - std::ssize(rows)));
        }
        WaitFor(Pages_.Context->LoadThroughputThrottler->ThrottleKeys(Pages_.Tag, limit)).ThrowOnError();

        auto conditions = baseConditions;
        if (offsetExclusive) {
            conditions.push_back(Format("(timestamp, key, row_id) > (%vu, %v, %Qv)",
                offsetExclusive->Timestamp.Underlying(),
                KeyToYsonLiteral(offsetExclusive->Key),
                offsetExclusive->RowId));
        }
        auto query = Format("key, timestamp, row_id, trigger_timestamp, event_timestamp, system_timestamp, message_id "
            "from [%v] where %v limit %v",
            Pages_.Path,
            JoinSeq(" and ", conditions),
            limit);
        TSelectRowsOptions selectRowsOptions;
        selectRowsOptions.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;

        NProfiling::TWallTimer selectTimer;
        auto rowset = WaitFor(Pages_.Context->Client->SelectRows(query, selectRowsOptions)).ValueOrThrow().Rowset;
        Pages_.Metrics.SelectTime.Record(selectTimer.GetElapsedTime());

        const auto schema = rowset->GetSchema();
        const int keyField = schema->GetColumnIndexOrThrow("key");
        const int timestampField = schema->GetColumnIndexOrThrow("timestamp");
        const int rowIdField = schema->GetColumnIndexOrThrow("row_id");
        const TRowValueFields valueFields(*schema);

        std::vector<i64> sizes;
        for (auto row : rowset->GetRows()) {
            TTimerPageRowKey rowKey{
                .FamilyId = page.FamilyId,
                .Kind = page.Kind,
                .Timestamp = FromUnversionedValue<TSystemTimestamp>(row[timestampField]),
                .Key = KeyFromYson(FromUnversionedValue<TYsonStringBuf>(row[keyField]).AsStringBuf()),
                .RowId = FromUnversionedValue<TMessageId>(row[rowIdField]),
            };
            auto timer = valueFields.ReadTimer(row, rowKey.Key);
            rows.push_back(TTimerPageRow{.RowKey = std::move(rowKey), .Timer = std::move(timer)});
            sizes.push_back(GetDataWeight(row));
        }
        Pages_.Context->LoadThroughputThrottler->RegisterKeys(Pages_.Tag, sizes);
        Pages_.Metrics.SelectRows.Increment(std::ssize(sizes));
        Pages_.Metrics.SelectBytes.Increment(SumSizes(sizes));

        if (std::ssize(rowset->GetRows()) < limit) {
            break;
        }
        offsetExclusive = rows.back().RowKey;
    }
    return rows;
}

TFuture<TTimerPageStatistics> TTimerPages::Aggregate(TTimerPageMeta page)
{
    return BIND([this, this_ = MakeStrong(this), page = std::move(page)] {
        auto buckets = DoAggregate(page, std::nullopt);
        return buckets.empty() ? TTimerPageStatistics{} : buckets.front().Statistics;
    }).AsyncVia(GetCurrentInvoker())
        .Run();
}

TFuture<std::optional<TSystemTimestamp>> TTimerPages::FindMedianTimestamp(TTimerPageMeta page)
{
    return BIND([this, this_ = MakeStrong(this), page = std::move(page)] () -> std::optional<TSystemTimestamp> {
        auto condition = Format("family_id = %vu and kind = %Qv and timestamp >= %vu",
            page.FamilyId.Underlying(),
            FormatEnum(page.Kind),
            page.Begin.Underlying());
        if (page.End) {
            condition += Format(" and timestamp < %vu", page.End->Underlying());
        }
        auto query = Format("timestamp from [%v] where %v offset %v limit 1",
            Pages_.Path,
            condition,
            page.Count / 2);
        WaitFor(Pages_.Context->LoadThroughputThrottler->ThrottleKeys(Pages_.Tag, 1)).ThrowOnError();
        TSelectRowsOptions options;
        options.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
        options.InputRowLimit = std::numeric_limits<i64>::max();
        options.OutputRowLimit = page.Count / 2 + 1;
        NProfiling::TWallTimer timer;
        auto rowset = WaitFor(Pages_.Context->Client->SelectRows(query, options)).ValueOrThrow().Rowset;
        Pages_.Metrics.SelectTime.Record(timer.GetElapsedTime());
        std::vector<i64> sizes;
        for (auto row : rowset->GetRows()) {
            sizes.push_back(GetDataWeight(row));
        }
        Pages_.Context->LoadThroughputThrottler->RegisterKeys(Pages_.Tag, sizes);
        Pages_.Metrics.SelectRows.Increment(std::ssize(sizes));
        Pages_.Metrics.SelectBytes.Increment(SumSizes(sizes));
        if (rowset->GetRows().empty()) {
            return std::nullopt;
        }
        const int field = rowset->GetSchema()->GetColumnIndexOrThrow("timestamp");
        return FromUnversionedValue<TSystemTimestamp>(rowset->GetRows().Front()[field]);
    }).AsyncVia(GetCurrentInvoker())
        .Run();
}

TFuture<std::pair<TTimerPageStatistics, TTimerPageStatistics>> TTimerPages::AggregateSplit(
    TTimerPageMeta page,
    TSystemTimestamp cut)
{
    return BIND([this, this_ = MakeStrong(this), page = std::move(page), cut] {
        std::pair<TTimerPageStatistics, TTimerPageStatistics> result;
        for (const auto& bucket : DoAggregate(page, cut)) {
            (bucket.Begin < cut ? result.first : result.second) = bucket.Statistics;
        }
        return result;
    }).AsyncVia(GetCurrentInvoker())
        .Run();
}

std::vector<TTimerPageBucket> TTimerPages::DoAggregate(const TTimerPageMeta& page, std::optional<TSystemTimestamp> cut)
{
    auto condition = Format("family_id = %vu and kind = %Qv and timestamp >= %vu",
        page.FamilyId.Underlying(),
        FormatEnum(page.Kind),
        page.Begin.Underlying());
    if (page.End) {
        condition += Format(" and timestamp < %vu", page.End->Underlying());
    }
    std::string bucketExpression = "0u";
    if (cut) {
        bucketExpression = Format("if(timestamp < %vu, 0u, 1u)", cut->Underlying());
    }
    auto query = Format("bucket, sum(1) as count, min(timestamp) as min_timestamp, max(timestamp) as max_timestamp, "
        "min(event_timestamp) as min_event, min(system_timestamp) as min_system "
        "from [%v] where %v group by %v as bucket",
        Pages_.Path,
        condition,
        bucketExpression);
    WaitFor(Pages_.Context->LoadThroughputThrottler->ThrottleKeys(Pages_.Tag, 1)).ThrowOnError();
    TSelectRowsOptions options;
    options.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
    options.InputRowLimit = std::numeric_limits<i64>::max();
    options.OutputRowLimit = std::numeric_limits<i64>::max();
    NProfiling::TWallTimer timer;
    auto rowset = WaitFor(Pages_.Context->Client->SelectRows(query, options)).ValueOrThrow().Rowset;
    Pages_.Metrics.SelectTime.Record(timer.GetElapsedTime());
    const auto& schema = *rowset->GetSchema();
    const int bucketField = schema.GetColumnIndexOrThrow("bucket");
    const int countField = schema.GetColumnIndexOrThrow("count");
    const int minField = schema.GetColumnIndexOrThrow("min_timestamp");
    const int maxField = schema.GetColumnIndexOrThrow("max_timestamp");
    const int eventField = schema.GetColumnIndexOrThrow("min_event");
    const int systemField = schema.GetColumnIndexOrThrow("min_system");
    std::vector<TTimerPageBucket> result;
    std::vector<i64> sizes;
    for (auto row : rowset->GetRows()) {
        i64 count = OptionalFromValue<i64>(row[countField]).value_or(0);
        if (!count) {
            continue;
        }
        result.push_back({
            .Begin = cut && FromUnversionedValue<ui64>(row[bucketField]) ? *cut : page.Begin,
            .Statistics = {
                .Count = count,
                .MinTimestamp = FromUnversionedValue<TSystemTimestamp>(row[minField]),
                .MaxTimestamp = FromUnversionedValue<TSystemTimestamp>(row[maxField]),
                .MinEvent = FromUnversionedValue<TSystemTimestamp>(row[eventField]),
                .MinSystem = FromUnversionedValue<TSystemTimestamp>(row[systemField]),
            },
        });
        sizes.push_back(GetDataWeight(row));
    }
    Pages_.Context->LoadThroughputThrottler->RegisterKeys(Pages_.Tag, sizes);
    Pages_.Metrics.SelectRows.Increment(std::ssize(sizes));
    Pages_.Metrics.SelectBytes.Increment(SumSizes(sizes));
    std::sort(result.begin(), result.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.Begin < rhs.Begin;
    });
    return result;
}

TFuture<std::vector<std::optional<TTimerPageRow>>> TTimerPages::LookupRows(std::vector<TTimerPageRowKey> rowKeys)
{
    return BIND([this, this_ = MakeStrong(this), rowKeys = std::move(rowKeys)] {
        return DoLookupRows(rowKeys);
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

std::vector<std::optional<TTimerPageRow>> TTimerPages::DoLookupRows(const std::vector<TTimerPageRowKey>& rowKeys)
{
    std::vector<std::optional<TTimerPageRow>> result(rowKeys.size());
    if (rowKeys.empty()) {
        return result;
    }

    WaitFor(Pages_.Context->LoadThroughputThrottler->ThrottleRows(Pages_.Tag, std::ssize(rowKeys))).ThrowOnError();

    TRowsBuilder builder;
    for (const auto& rowKey : rowKeys) {
        builder.Begin();
        AddRowKey(&builder, rowKey);
        builder.EndWrite();
    }

    TLookupRowsOptions lookupRowsOptions;
    lookupRowsOptions.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
    lookupRowsOptions.KeepMissingRows = true;

    NProfiling::TWallTimer lookupTimer;
    auto rowset = WaitFor(Pages_.Context->Client->LookupRows(
        Pages_.Path,
        builder.GetNameTable(),
        MakeSharedRange(builder.TakeRows(), builder.GetRowBuffer()),
        lookupRowsOptions))
        .ValueOrThrow()
        .Rowset;
    Pages_.Metrics.LookupTime.Record(lookupTimer.GetElapsedTime());

    const TRowValueFields valueFields(*rowset->GetSchema());
    const auto rows = rowset->GetRows();
    YT_VERIFY(std::ssize(rows) == std::ssize(rowKeys));

    std::vector<i64> sizes;
    for (int index = 0; index < std::ssize(rows); ++index) {
        auto row = rows[index];
        if (!row) {
            continue;
        }
        result[index] = TTimerPageRow{.RowKey = rowKeys[index], .Timer = valueFields.ReadTimer(row, rowKeys[index].Key)};
        sizes.push_back(GetDataWeight(row));
    }
    Pages_.Context->LoadThroughputThrottler->RegisterRows(Pages_.Tag, sizes);
    Pages_.Metrics.LookupRows.Increment(std::ssize(sizes));
    Pages_.Metrics.LookupBytes.Increment(SumSizes(sizes));
    return result;
}

void TTimerPages::WriteRows(
    NApi::IDynamicTableTransactionPtr transaction,
    const std::vector<TTimerPageRow>& rows)
{
    if (rows.empty()) {
        return;
    }

    TRowsBuilder builder;
    i64 totalBytes = 0;
    for (const auto& [rowKey, timer] : rows) {
        builder.Begin();
        AddRowKey(&builder, rowKey);
        builder.AddUint64("trigger_timestamp", timer.TriggerTimestamp.Underlying());
        builder.AddUint64("event_timestamp", timer.EventTimestamp.Underlying());
        builder.AddUint64("system_timestamp", timer.SystemTimestamp.Underlying());
        builder.AddString("message_id", timer.MessageId.Underlying());
        totalBytes += builder.EndWrite();
    }
    builder.ModifyRows(transaction, Pages_.Path);

    Pages_.Metrics.WriteRows.Increment(std::ssize(rows));
    Pages_.Metrics.WriteBytes.Increment(totalBytes);
}

void TTimerPages::EraseRows(
    NApi::IDynamicTableTransactionPtr transaction,
    const std::vector<TTimerPageRowKey>& rowKeys)
{
    if (rowKeys.empty()) {
        return;
    }

    TRowsBuilder builder;
    for (const auto& rowKey : rowKeys) {
        builder.Begin();
        AddRowKey(&builder, rowKey);
        builder.EndDelete();
    }
    builder.ModifyRows(transaction, Pages_.Path);

    Pages_.Metrics.EraseRows.Increment(std::ssize(rowKeys));
}

////////////////////////////////////////////////////////////////////////////////

TFuture<std::vector<TTimerPageMeta>> TTimerPages::LoadMeta(TMetaFilter filter)
{
    return BIND([this, this_ = MakeStrong(this), filter = std::move(filter)] {
        return DoLoadMeta(filter);
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

std::vector<TTimerPageMeta> TTimerPages::DoLoadMeta(const TMetaFilter& filter)
{
    YT_VERIFY(!filter.FamilyId || filter.ComputationId);
    auto timestamp = WaitFor(Meta_.Context->Client->GetTimestampProvider()->GenerateTimestamps()).ValueOrThrow();
    std::vector<std::string> baseConditions;
    if (filter.ComputationId) {
        baseConditions.push_back(Format("computation_id = %Qv", *filter.ComputationId));
    }
    if (filter.FamilyId) {
        baseConditions.push_back(Format("family_id = %vu", filter.FamilyId->Underlying()));
    }

    const auto& Logger = Meta_.Context->Logger;
    std::vector<TTimerPageMeta> pages;
    // The last row read, which may be a skipped one.
    std::optional<std::tuple<TComputationId, TTimerFamilyId, ETimerPageKind, TSystemTimestamp>> lastKey;
    TSelectLimiter limiter(DynamicSpec_);
    while (true) {
        auto limit = limiter.Get();
        WaitFor(Meta_.Context->LoadThroughputThrottler->ThrottleKeys(Meta_.Tag, limit)).ThrowOnError();

        auto conditions = baseConditions;
        if (lastKey) {
            const auto& [computationId, familyId, kind, begin] = *lastKey;
            conditions.push_back(Format("(computation_id, family_id, kind, begin_timestamp) > (%Qv, %vu, %Qv, %vu)",
                computationId,
                familyId.Underlying(),
                FormatEnum(kind),
                begin.Underlying()));
        }
        auto query = Format("computation_id, family_id, stream_id, key_begin, key_end, kind, "
            "begin_timestamp, end_timestamp, once_shared, dirty_minima, event_done, dedup_slots, "
            "count, min_event, min_system, max_timestamp, format_version from [%v] %v limit %v",
            Meta_.Path,
            conditions.empty() ? "" : "where " + JoinSeq(" and ", conditions),
            limit);
        TSelectRowsOptions selectRowsOptions;
        selectRowsOptions.Timestamp = timestamp;

        NProfiling::TWallTimer selectTimer;
        auto rowset = WaitFor(Meta_.Context->Client->SelectRows(query, selectRowsOptions)).ValueOrThrow().Rowset;
        Meta_.Metrics.SelectTime.Record(selectTimer.GetElapsedTime());

        const TMetaFields fields(*rowset->GetSchema());
        std::vector<i64> sizes;
        for (auto row : rowset->GetRows()) {
            sizes.push_back(GetDataWeight(row));
            lastKey = fields.ReadKey(row);
            if (fields.IsGhost(row)) {
                const auto& [computationId, familyId, kind, begin] = *lastKey;
                YT_TLOG_WARNING("Skipping a timer page metadata row without a stream")
                    .With("ComputationId", computationId)
                    .With("FamilyId", familyId)
                    .With("Kind", kind)
                    .With("BeginTimestamp", begin);
                continue;
            }
            pages.push_back(fields.ReadPage(row));
        }
        Meta_.Context->LoadThroughputThrottler->RegisterKeys(Meta_.Tag, sizes);
        Meta_.Metrics.SelectRows.Increment(std::ssize(sizes));
        Meta_.Metrics.SelectBytes.Increment(SumSizes(sizes));

        if (std::ssize(rowset->GetRows()) < limit) {
            break;
        }
    }
    return pages;
}

void TTimerPages::WritePage(
    NApi::IDynamicTableTransactionPtr transaction,
    const TTimerPageMeta& page)
{
    TRowsBuilder builder;
    builder.Begin();
    AddPageKey(&builder, page.ComputationId, page.FamilyId, page.Kind, page.Begin);
    builder.AddString("stream_id", page.StreamId.Underlying());
    builder.AddString("key_begin", KeyToYson(page.KeyRange.Lower).AsStringBuf());
    builder.AddString("key_end", KeyToYson(page.KeyRange.Upper).AsStringBuf());
    builder.AddOptionalUint64("end_timestamp", page.End ? std::optional(page.End->Underlying()) : std::nullopt);
    builder.AddBoolean("once_shared", page.Flags.OnceShared);
    builder.AddBoolean("dirty_minima", page.Flags.DirtyMinima);
    builder.AddBoolean("event_done", page.Flags.EventDone);
    builder.AddBoolean("dedup_slots", page.Flags.DedupSlots);
    builder.AddInt64("count", page.Count);
    builder.AddUint64("min_event", page.MinEvent.Underlying());
    builder.AddUint64("min_system", page.MinSystem.Underlying());
    builder.AddOptionalUint64("max_timestamp", page.MaxTimestamp ? std::optional(page.MaxTimestamp->Underlying()) : std::nullopt);
    builder.AddInt64("format_version", page.FormatVersion);
    Meta_.Metrics.WriteBytes.Increment(builder.EndWrite());
    Meta_.Metrics.WriteRows.Increment();
    builder.ModifyRows(transaction, Meta_.Path);
}

void TTimerPages::UpdatePage(
    NApi::IDynamicTableTransactionPtr transaction,
    const TPageUpdate& update)
{
    if (!update.End && update.Flags.IsEmpty() && !update.Minima && update.CountDelta == 0 &&
        !update.MaxTimestamp)
    {
        return;
    }

    TRowsBuilder builder;
    builder.Begin();
    AddPageKey(&builder, update.ComputationId, update.FamilyId, update.Kind, update.Begin);
    if (update.End) {
        builder.AddOptionalUint64("end_timestamp", *update.End ? std::optional((*update.End)->Underlying()) : std::nullopt);
    }
    builder.AddOptionalBoolean("once_shared", update.Flags.OnceShared);
    builder.AddOptionalBoolean("dirty_minima", update.Flags.DirtyMinima);
    builder.AddOptionalBoolean("event_done", update.Flags.EventDone);
    builder.AddOptionalBoolean("dedup_slots", update.Flags.DedupSlots);
    if (update.Minima) {
        builder.AddUint64("min_event", update.Minima->first.Underlying());
        builder.AddUint64("min_system", update.Minima->second.Underlying());
    }
    if (update.CountDelta != 0) {
        builder.AddInt64("count", update.CountDelta, EValueFlags::Aggregate);
    }
    if (update.MaxTimestamp) {
        builder.AddUint64("max_timestamp", update.MaxTimestamp->Underlying(), EValueFlags::Aggregate);
    }
    Meta_.Metrics.WriteBytes.Increment(builder.EndWrite());
    Meta_.Metrics.WriteRows.Increment();

    auto structuralFlags = update.Flags;
    structuralFlags.OnceShared.reset();
    // Blind counter deltas, maxima and monotonic sharing flags commute across page readers.
    bool blind = !update.End && !update.Minima && structuralFlags.IsEmpty() && update.Flags.OnceShared.value_or(true);
    transaction->WriteRows(
        Meta_.Path,
        builder.GetNameTable(),
        MakeSharedRange(builder.TakeRows(), builder.GetRowBuffer()),
        /*options*/ {},
        blind ? ELockType::SharedWrite : ELockType::Exclusive);
}

void TTimerPages::ErasePage(
    NApi::IDynamicTableTransactionPtr transaction,
    const TComputationId& computationId,
    TTimerFamilyId familyId,
    ETimerPageKind kind,
    TSystemTimestamp begin)
{
    TRowsBuilder builder;
    builder.Begin();
    AddPageKey(&builder, computationId, familyId, kind, begin);
    builder.EndDelete();
    Meta_.Metrics.EraseRows.Increment();
    builder.ModifyRows(transaction, Meta_.Path);
}

////////////////////////////////////////////////////////////////////////////////

void TTimerPages::WriteJournal(
    NApi::IDynamicTableTransactionPtr transaction,
    const std::vector<TTimerJournalEntry>& entries)
{
    if (entries.empty()) {
        return;
    }

    TRowsBuilder builder;
    i64 totalBytes = 0;
    for (const auto& entry : entries) {
        builder.Begin();
        // The journal has one tablet; a chaos ordered table rejects rows without the index.
        builder.AddInt64(TabletIndexColumnName, JournalTabletIndex);
        AddPageKey(&builder, entry.ComputationId, entry.FamilyId, entry.Kind, entry.Begin);
        builder.AddString("op", FormatEnum(entry.Op));
        builder.AddString("stream_id", entry.StreamId.Underlying());
        builder.AddString("key_begin", KeyToYson(entry.KeyRange.Lower).AsStringBuf());
        builder.AddString("key_end", KeyToYson(entry.KeyRange.Upper).AsStringBuf());
        builder.AddOptionalBoolean("once_shared", entry.Flags.OnceShared);
        builder.AddOptionalBoolean("event_done", entry.Flags.EventDone);
        builder.AddOptionalBoolean("dedup_slots", entry.Flags.DedupSlots);
        totalBytes += builder.EndWrite();
    }
    builder.ModifyRows(transaction, Journal_.Path);

    Journal_.Metrics.WriteRows.Increment(std::ssize(entries));
    Journal_.Metrics.WriteBytes.Increment(totalBytes);
}

TFuture<ITimerPages::TJournalRowRange> TTimerPages::GetJournalRowRange()
{
    return Journal_.Context->Client->GetTabletInfos(Journal_.Path, std::vector<int>{JournalTabletIndex})
        .Apply(BIND([] (const std::vector<TTabletInfo>& tabletInfos) {
            YT_VERIFY(tabletInfos.size() == 1);
            return TJournalRowRange{
                .TrimmedRowCount = tabletInfos[0].TrimmedRowCount,
                .TotalRowCount = tabletInfos[0].TotalRowCount,
            };
        }));
}

TFuture<ITimerPages::TJournalReadResult> TTimerPages::ReadJournal(i64 fromRowIndex, i64 limit)
{
    return BIND([this, this_ = MakeStrong(this), fromRowIndex, limit] {
        return DoReadJournal(fromRowIndex, limit);
    })
        .AsyncVia(GetCurrentInvoker())
        .Run();
}

ITimerPages::TJournalReadResult TTimerPages::DoReadJournal(i64 fromRowIndex, i64 limit)
{
    WaitFor(Journal_.Context->LoadThroughputThrottler->ThrottleKeys(Journal_.Tag, limit)).ThrowOnError();

    auto query = Format("[$row_index], computation_id, family_id, begin_timestamp, op, stream_id, key_begin, key_end, kind, "
        "once_shared, event_done, dedup_slots "
        "from [%v] where [$tablet_index] = %v and [$row_index] >= %v limit %v",
        Journal_.Path,
        JournalTabletIndex,
        fromRowIndex,
        limit);
    TSelectRowsOptions selectRowsOptions;
    selectRowsOptions.Timestamp = NTransactionClient::SyncLastCommittedTimestamp;

    NProfiling::TWallTimer selectTimer;
    auto rowset = WaitFor(Journal_.Context->Client->SelectRows(query, selectRowsOptions)).ValueOrThrow().Rowset;
    Journal_.Metrics.SelectTime.Record(selectTimer.GetElapsedTime());

    const auto schema = rowset->GetSchema();
    const int rowIndexField = schema->GetColumnIndexOrThrow("$row_index");
    const int computationIdField = schema->GetColumnIndexOrThrow("computation_id");
    const int familyIdField = schema->GetColumnIndexOrThrow("family_id");
    const int beginField = schema->GetColumnIndexOrThrow("begin_timestamp");
    const int opField = schema->GetColumnIndexOrThrow("op");
    const int streamIdField = schema->GetColumnIndexOrThrow("stream_id");
    const int keyBeginField = schema->GetColumnIndexOrThrow("key_begin");
    const int keyEndField = schema->GetColumnIndexOrThrow("key_end");
    const int kindField = schema->GetColumnIndexOrThrow("kind");
    const int onceSharedField = schema->GetColumnIndexOrThrow("once_shared");
    const int eventDoneField = schema->GetColumnIndexOrThrow("event_done");
    const int dedupSlotsField = schema->GetColumnIndexOrThrow("dedup_slots");

    TJournalReadResult result;
    result.NextRowIndex = fromRowIndex;
    std::vector<i64> sizes;
    for (auto row : rowset->GetRows()) {
        result.Entries.push_back(TTimerJournalEntry{
            .Op = ParseEnum<ETimerJournalOp>(ValueOrThrow<TStringBuf>(row[opField], "op")),
            .ComputationId = FromUnversionedValue<TComputationId>(row[computationIdField]),
            .FamilyId = FromUnversionedValue<TTimerFamilyId>(row[familyIdField]),
            .StreamId = ValueOrThrow<TStreamId>(row[streamIdField], "stream_id"),
            .KeyRange = TKeyRange{
                .Lower = KeyFromYson(ValueOrThrow<TStringBuf>(row[keyBeginField], "key_begin")),
                .Upper = KeyFromYson(ValueOrThrow<TStringBuf>(row[keyEndField], "key_end")),
            },
            .Kind = ParseEnum<ETimerPageKind>(ValueOrThrow<TStringBuf>(row[kindField], "kind")),
            .Begin = FromUnversionedValue<TSystemTimestamp>(row[beginField]),
            .Flags = TTimerPageFlagsUpdate{
                .OnceShared = OptionalFromValue<bool>(row[onceSharedField]),
                .EventDone = OptionalFromValue<bool>(row[eventDoneField]),
                .DedupSlots = OptionalFromValue<bool>(row[dedupSlotsField]),
            },
            .RowIndex = FromUnversionedValue<i64>(row[rowIndexField]),
        });
        sizes.push_back(GetDataWeight(row));
    }
    Journal_.Context->LoadThroughputThrottler->RegisterKeys(Journal_.Tag, sizes);
    Journal_.Metrics.SelectRows.Increment(std::ssize(sizes));
    Journal_.Metrics.SelectBytes.Increment(SumSizes(sizes));

    // A single tablet is scanned in row order, but the contract does not promise it.
    std::sort(result.Entries.begin(), result.Entries.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.RowIndex < rhs.RowIndex;
    });
    if (!result.Entries.empty()) {
        result.NextRowIndex = result.Entries.back().RowIndex + 1;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables

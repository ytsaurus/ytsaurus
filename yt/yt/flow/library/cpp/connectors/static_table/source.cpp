#include "source_impl.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/schema.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/queue_client.h>
#include <yt/yt/client/api/queue_transaction.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/transaction_client.h>

#include <yt/yt/client/queue_client/consumer_client.h>

#include <yt/yt/client/complex_types/yson_format_conversion.h>

#include <yt/yt/client/table_client/config.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/library/re2/re2.h>

#include <library/cpp/iterator/zip.h>
#include <library/cpp/timezone_conversion/convert.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>

#include <algorithm>
#include <cmath>
#include <optional>

namespace NYT::NFlow::NStaticTableConnector {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Appended to a path so GetNode returns the node itself without redirecting through a final symlink
// to its target (see Cypress link redirects).
constexpr TStringBuf NoFollowSymlinkSuffix = "&";
const re2::RE2 ExplicitTimezoneSuffixPattern(
    R"([Tt ].*(?:[Zz]|[+-][0-9]{2}:?(?:[0-9]{2})?)$)");

template <class TContextPtr>
IClientPtr CreateClient(const TContextPtr& context, const TRichYPath& path)
{
    return context->ClientsCache->GetClient(*path.GetCluster());
}

bool HasExplicitTimezone(TStringBuf timestamp)
{
    return re2::RE2::PartialMatch(timestamp, ExplicitTimezoneSuffixPattern);
}

TInstant InterpretTimestampInTimezone(TInstant instant, const std::string& timezone)
{
    const auto civilTime = NDatetime::ToCivilTime(instant, NDatetime::GetUtcTimeZone());
    const auto result = NDatetime::ToAbsoluteTime(civilTime, NDatetime::GetTimeZone(timezone));
    THROW_ERROR_EXCEPTION_IF(
        result == TInstant::Max(),
        "Timestamp is out of range in timezone %Qv",
        timezone);
    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TKey GenerateRangeKey(TRangeId rangeId)
{
    return MakeKey(rangeId.Underlying());
}

TRangeId ExtractRangeId(const TKey& key)
{
    if (key.Underlying().GetCount() != 1) {
        THROW_ERROR_EXCEPTION("Static table partition key should have exactly one field, got: %v", key.Underlying().GetCount())
            .With("static_table_partition_key", key);
    }
    return FromUnversionedValue<TRangeId>(key.Underlying()[0]);
}

////////////////////////////////////////////////////////////////////////////////

std::pair<i64, i64> GetRowIndexRange(const TRichYPath& path)
{
    auto ranges = path.GetRanges();
    THROW_ERROR_EXCEPTION_UNLESS(ranges.size() == 1, "Table rich path %v doesn't have exactly one range", path);
    THROW_ERROR_EXCEPTION_UNLESS(ranges[0].LowerLimit().HasRowIndex(), "Table rich path %v doesn't have lower row index", path);
    THROW_ERROR_EXCEPTION_UNLESS(ranges[0].UpperLimit().HasRowIndex(), "Table rich path %v doesn't have upper row index", path);
    return {ranges[0].LowerLimit().GetRowIndex(), ranges[0].UpperLimit().GetRowIndex()};
}

void SetRowIndexRange(TRichYPath& path, i64 lower, i64 upper)
{
    NYT::NChunkClient::TReadRange remainedRange;
    remainedRange.LowerLimit().SetRowIndex(lower);
    remainedRange.UpperLimit().SetRowIndex(upper);
    path.SetRanges({remainedRange});
}

TSystemTimestamp ComputePlannedTimestamp(
    i64 rowIndex,
    std::pair<i64, i64> rowIndexRange,
    TSystemTimestamp plannedTimestamp,
    TDuration plannedReadDuration)
{
    auto [begin, end] = rowIndexRange;
    if (begin == end) {
        return TSystemTimestamp(plannedTimestamp.Underlying() + plannedReadDuration.Seconds());
    }

    auto offset = std::clamp(rowIndex, begin, end) - begin;
    auto offsetSeconds = static_cast<ui64>(
        static_cast<__int128>(plannedReadDuration.Seconds()) * offset / (end - begin));
    return TSystemTimestamp(plannedTimestamp.Underlying() + offsetSeconds);
}

bool IsOneTable(TRichYPath& a, TRichYPath& b)
{
    return a.GetCluster() == b.GetCluster() && a.GetPath() == b.GetPath();
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("committed_offset_exclusive", &TThis::CommittedOffsetExclusive)
        .Default(-1);
}

////////////////////////////////////////////////////////////////////////////////

TThroughputThrottlerConfigPtr TSource::CreateThrottlerConfig(double rowsPerSecond, TDuration throttlerPeriod)
{
    YT_VERIFY(rowsPerSecond > 0);
    auto throttlerConfig = New<TThroughputThrottlerConfig>();
    throttlerConfig->Limit = rowsPerSecond;
    // If period of throttler is so short, that less than 1 message can be processed within it, then no message will be processed ever.
    // We ensure that at least 2 messages can be processed per period to guarantee forward progress.
    auto minAdequatePeriod = TDuration::Seconds(2 / rowsPerSecond);
    throttlerConfig->Period = std::max(minAdequatePeriod, throttlerPeriod);
    return throttlerConfig;
}

TSource::TSource(
    TSourceContextPtr context,
    TDynamicSourceContextPtr dynamicContext)
    : TIntegerOffsetOrderedSourceBase(std::move(context), std::move(dynamicContext))
    , Logger(TOrderedSourceBase::Logger
            .WithTag("Tables", GetParameters()->Tables)
            .WithTag("TablesPath", GetParameters()->TablesPath))
    , Throttler_(CreateReconfigurableThroughputThrottler(
        CreateThrottlerConfig(
            GetDynamicPartitionSpec()->RowsPerSecond,
            GetDynamicParameters()->ThrottlerPeriod),
        Logger,
        GetContext()->Profiler.WithPrefix("/throttler")))
{
    TDynamicTableSourcePartitionSpecPtr previousDynamicPartitionSpec = nullptr;
    SubscribeReconfigured(
        BIND([=, this] (const TDynamicSourceContextPtr& /*dynamicContext*/) mutable {
            // Replica failback may update the plan before this partition observes its removal.
            auto newDynamicPartitionSpec = GetDynamicPartitionSpec();
            YT_TLOG_INFO("Source got new dynamic source partition spec")
                .With("newDynamicPartitionSpec", ConvertToYsonString(newDynamicPartitionSpec, EYsonFormat::Text));
            if (previousDynamicPartitionSpec) {
                YT_VERIFY(newDynamicPartitionSpec->Table.GetPath() == previousDynamicPartitionSpec->Table.GetPath());
                if (!newDynamicPartitionSpec->UsePlannedTimestamps && !previousDynamicPartitionSpec->UsePlannedTimestamps) {
                    YT_VERIFY(newDynamicPartitionSpec->EventTimestamp == previousDynamicPartitionSpec->EventTimestamp);
                    YT_VERIFY(newDynamicPartitionSpec->SystemTimestamp == previousDynamicPartitionSpec->SystemTimestamp);
                }
            }
            previousDynamicPartitionSpec = newDynamicPartitionSpec;
            auto throttlerConfig = CreateThrottlerConfig(
                newDynamicPartitionSpec->RowsPerSecond,
                GetDynamicParameters()->ThrottlerPeriod);
            Throttler_->Reconfigure(throttlerConfig);
        }));
}

void TSource::DoInit()
{
    ReadErrorState_ = CreateAvailabilityErrorState("/read");
    auto cluster = *GetDynamicPartitionSpec()->Table.GetCluster();
    Client_ = GetContext()->ClientsCache->GetClient(cluster);
}

void TSource::DoTerminate()
{
    CancelReader(TError(NYT::EErrorCode::Canceled, "Source is terminated, so the table reader is no longer needed"));
}

void TSource::DoReportPersistedOffset(TOffset offsetExclusive)
{
    PersistedOffsetExclusive_.store(OffsetToInt(offsetExclusive));
    EraseIf(PendingPlannedReads_, [&] (const auto& pending) {
        return pending.EndOffset <= OffsetToInt(offsetExclusive);
    });
    auto [minOffsetInclusive, maxOffsetExclusive] = GetRowIndexRange(GetDynamicPartitionSpec()->Table);
    UpdatePartitionInfo(
        TPartitionInfoUpdate{
            // There is no external consumer to commit, so mark offset as committed instantly.
            .CommittedOffsetExclusive = IntToOffset(std::max(PersistedOffsetExclusive_.load(), minOffsetInclusive)),
            .MaxOffsetExclusive = IntToOffset(maxOffsetExclusive),
        });
}

void TSource::AdjustInflight(const TInflightStreamTraverseDataPtr& inflight)
{
    const auto& spec = GetDynamicPartitionSpec();
    auto [begin, end] = GetRowIndexRange(spec->Table);
    auto persistedOffset = std::clamp(PersistedOffsetExclusive_.load(), begin, end);
    if (spec->UsePlannedTimestamps) {
        std::optional<TSystemTimestamp> timestamp = ComputePlannedTimestamp(
            persistedOffset,
            {begin, end},
            spec->PlannedTimestamp,
            spec->PlannedReadDuration);
        // Already read rows retain their plan across a live replan until persisted.
        for (const auto& pending : PendingPlannedReads_) {
            auto pendingTimestamp = ComputePlannedTimestamp(
                std::max(PersistedOffsetExclusive_.load(), pending.BeginOffset),
                GetRowIndexRange(pending.Spec->Table),
                pending.Spec->PlannedTimestamp,
                pending.Spec->PlannedReadDuration);
            timestamp = std::min(*timestamp, pendingTimestamp);
        }
        inflight->MinSystemTimestamp = inflight->Empty ? std::nullopt : timestamp;
        inflight->MinEventTimestamp = inflight->Empty ? std::nullopt : timestamp;
    }
}

IMapNodePtr TSource::GetPartitionStatus()
{
    auto status = New<TPartitionStatus>();
    status->CommittedOffsetExclusive = PersistedOffsetExclusive_.load();
    return ConvertTo<IMapNodePtr>(status);
}

NComplexTypes::TYsonServerToClientConverter TSource::MakeAnyColumnConverter()
{
    // V1 any columns may carry any wire type depending on how the table was written.
    // String cells contain pre-serialized raw YSON bytes (for example, written via the Python
    // unstructured writer). Other native types are serialized with UnversionedValueToYson.
    return [] (TUnversionedValue cell, NYson::IYsonConsumer* consumer) {
        if (cell.Type == EValueType::String) {
            consumer->OnRaw(cell.AsStringBuf(), EYsonType::Node);
        } else {
            UnversionedValueToYson(cell, consumer);
        }
    };
}

THashMap<int, NComplexTypes::TYsonServerToClientConverter> TSource::InferTableSchema(
    const NTableClient::TNameTablePtr& nameTable,
    const NTableClient::TTableSchemaPtr& tableSchema,
    std::vector<EValueType>& wireTypes)
{
    THashMap<int, NComplexTypes::TYsonServerToClientConverter> anyConverters;
    if (!tableSchema) {
        return anyConverters;
    }
    NComplexTypes::TYsonConverterConfig config;
    for (int id = 0; id < nameTable->GetSize(); ++id) {
        // NameTable may contain system columns not present in the schema; null check required.
        const auto* col = tableSchema->FindColumn(nameTable->GetName(id));
        if (!col) {
            continue;
        }
        if (col->IsOfV1Type()) {
            wireTypes[id] = col->GetWireType();
            if (col->IsOfV1Type(ESimpleLogicalValueType::Any)) {
                anyConverters.emplace(id, MakeAnyColumnConverter());
            }
        } else {
            wireTypes[id] = EValueType::Any;
            anyConverters.emplace(
                id,
                NComplexTypes::CreateYsonServerToClientConverter(
                    TComplexTypeFieldDescriptor(*col),
                    config));
        }
    }
    return anyConverters;
}

std::pair<NTableClient::TTableSchemaPtr, std::vector<int>> TSource::GetSchemaAndMappingIndex(
    const TSharedRange<TUnversionedRow>& unversionedRowRange,
    const NTableClient::TNameTablePtr& nameTable,
    const std::vector<EValueType>& wireTypes,
    const THashMap<int, NComplexTypes::TYsonServerToClientConverter>& anyConverters)
{
    std::vector<int> idToColumnIndex(nameTable->GetSize());
    // observedTypes tracks the most specific type seen for each column:
    //   EValueType::Min  = never encountered in any row
    //   EValueType::Null = encountered only as null (concrete type still unknown)
    //   other            = concrete wire type confirmed
    // Starts as a copy of wireTypes so schema-declared columns begin with their known type.
    std::vector<EValueType> observedTypes(wireTypes);
    std::vector<int> columnIdOrder;

    // Schema-declared columns must appear in the output schema even if all their cells in this
    // batch are null — the row scan skips null cells, so they would never be registered from rows alone.
    for (int id = 0; id < std::ssize(wireTypes); ++id) {
        if (wireTypes[id] != EValueType::Min) {
            columnIdOrder.push_back(id);
        }
    }

    // Phase 1: scan rows to infer types (weak schema) or verify consistency (strict schema).
    for (auto row : unversionedRowRange) {
        for (const auto& cell : row) {
            auto& observed = observedTypes[cell.Id];
            // All cells for Any-schema columns (anyConverters) are normalized to Any regardless
            // of their physical wire type. This covers both V3 complex types (Composite cells)
            // and V1 any columns (String, Int64, or other native types depending on the writer).
            const EValueType cellType = anyConverters.contains(cell.Id) ? EValueType::Any : cell.Type;
            if (observed == EValueType::Min) {
                // First time this column is seen: register it and record its position.
                columnIdOrder.push_back(cell.Id);
                observed = cellType;
            }
            if (cellType == EValueType::Null) {
                continue; // Null is compatible with any type; no further action needed.
            }
            if (observed == EValueType::Null) {
                observed = cellType; // First non-null cell: upgrade from null-only.
            } else {
                THROW_ERROR_EXCEPTION_UNLESS(
                    observed == cellType,
                    "Inconsistent types in batch: %v vs %v",
                    observed,
                    cellType);
            }
        }
    }

    // Phase 2: build the output schema in first-appearance column order.
    std::vector<TColumnSchema> columns;
    for (int id : columnIdOrder) {
        // All-null columns fall back to Any since their concrete type could not be inferred.
        const EValueType columnType = (observedTypes[id] == EValueType::Null) ? EValueType::Any : observedTypes[id];
        idToColumnIndex[id] = std::ssize(columns);
        columns.push_back(TColumnSchema(std::string(nameTable->GetName(id)), columnType));
    }

    columns.push_back(TColumnSchema(SequenceNumberColumnName, EValueType::Int64));
    return {New<NTableClient::TTableSchema>(columns), idToColumnIndex};
}

NTableClient::TUnversionedValue TSource::ConvertCellToAny(
    const NTableClient::TUnversionedValue& cell,
    const NComplexTypes::TYsonServerToClientConverter* converter,
    TString& ysonBuffer)
{
    if (cell.Type == EValueType::Null) {
        return cell; // Null is compatible with any schema type; preserve it as-is.
    }
    if (converter) {
        TStringOutput out(ysonBuffer);
        TYsonWriter writer(&out);
        (*converter)(cell, &writer);
        writer.Flush();
        return MakeUnversionedAnyValue(TStringBuf(ysonBuffer), cell.Id);
    } else {
        auto result = cell;
        result.Type = EValueType::Any;
        return result;
    }
}

TError TSource::ClassifyPendingRead(
    const TFuture<void>& pendingRead,
    TDuration timeSinceReadProgress,
    TDuration readTimeout)
{
    auto pendingReadError = pendingRead.TryGet();
    if (!pendingReadError) {
        // Neither the reader creation nor the read stream has a deadline of its own, so an
        // indefinitely pending one is only ever caught here.
        if (timeSinceReadProgress > readTimeout) {
            return NYT::TError(NYT::EErrorCode::Timeout, "Timeout of table reader; no rows read for too long")
                .With("read_timeout", readTimeout);
        }
        return {};
    }
    if (!pendingReadError->IsOK()) {
        return NYT::TError("Table reader failed").With(*pendingReadError);
    }
    return {};
}

void TSource::DropReader(const TError& error)
{
    ReadErrorState_->SetError(error);
    YT_TLOG_WARNING("Dropping table reader")
        .With(error);
    CancelReader(error);
}

void TSource::CancelReader(const TError& error)
{
    if (!ReaderFuture_) {
        return;
    }

    // The creation is completed by another thread, so cancelling it first makes the order race-free:
    // on an already set future the cancellation is a no-op, and a creation that lands right after it
    // is still seen by the check below.
    auto readerFuture = std::exchange(ReaderFuture_, {});
    readerFuture.Cancel(error);
    if (readerFuture.IsSet() && readerFuture.GetOrCrash().IsOK()) {
        readerFuture.GetOrCrash().Value()->GetReadyEvent().Cancel(error);
    }
}

TFuture<std::vector<TSource::TRecord>> TSource::DoReadNextBatch(const TMessageBatcherSettingsPtr& settings, TOffset nextOffsetAsKey, std::optional<TOffset> offsetLimitAsKey)
{
    i64 nextOffset = OffsetToInt(nextOffsetAsKey);
    std::optional<i64> offsetLimit = offsetLimitAsKey ? std::optional(OffsetToInt(*offsetLimitAsKey)) : std::nullopt;

    auto dynamicSourcePartitionSpec = GetDynamicPartitionSpec();
    auto [minOffsetInclusive, maxOffsetExclusive] = GetRowIndexRange(dynamicSourcePartitionSpec->Table);

    // The reader is bound to the row range it was created with, so a trimmed range needs a new one.
    // Clamping before the end check keeps a range trimmed to nothing from creating a reader over it.
    if (nextOffset < minOffsetInclusive) {
        CancelReader(TError(NYT::EErrorCode::Canceled, "Partition row range was trimmed under the table reader")
                .With("reader_offset", CurrentOffset_)
                .With("next_offset", nextOffset)
                .With("min_offset_inclusive", minOffsetInclusive));
        nextOffset = minOffsetInclusive;
    }

    // If partition is "trimmed" by controller logic, nextOffset jumps to maxOffsetExclusive.
    // And may differ from CurrentOffset_.
    if (nextOffset == maxOffsetExclusive) {
        CancelReader(TError(NYT::EErrorCode::Canceled, "Partition row range is fully read")
                .With("reader_offset", CurrentOffset_)
                .With("max_offset_exclusive", maxOffsetExclusive));
        // Nothing is left to read, so a read error left by a previous poll is stale.
        ReadErrorState_->ClearError();
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    if (!ReaderFuture_) {
        auto table = dynamicSourcePartitionSpec->Table;
        SetRowIndexRange(table, nextOffset, maxOffsetExclusive);

        YT_TLOG_DEBUG("Create table reader")
            .With("RichPath", table);
        auto readerConfig = NYT::New<NYT::NTableClient::TTableReaderConfig>();
        ReaderFuture_ = Client_->CreateTableReader(
            table,
            NYT::NApi::TTableReaderOptions{
                .Unordered = false,
                .EnableTableIndex = false,
                .EnableRowIndex = false,
                .EnableRangeIndex = false,
                .Config = readerConfig});
        CurrentOffset_ = nextOffset;
        LastReadProgressInstant_ = TInstant::Now();
    }

    YT_VERIFY(CurrentOffset_ == nextOffset);

    if (!ReaderFuture_.IsSet()) {
        auto error = ClassifyPendingRead(
            ReaderFuture_.AsVoid(),
            TInstant::Now() - LastReadProgressInstant_,
            GetDynamicParameters()->ReadTimeout);
        if (!error.IsOK()) {
            DropReader(error);
        }
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    if (!ReaderFuture_.GetOrCrash().IsOK()) {
        DropReader(ReaderFuture_.GetOrCrash());
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    auto reader = ReaderFuture_.GetOrCrash().ValueOrThrow();

    ReadErrorState_->ClearError();

    // Rows may only be taken once the ready event is set; reading before that yields an empty batch
    // that is indistinguishable from a dead read stream.
    auto readyEvent = reader->GetReadyEvent();
    auto error = ClassifyPendingRead(
        readyEvent,
        TInstant::Now() - LastReadProgressInstant_,
        GetDynamicParameters()->ReadTimeout);
    if (!error.IsOK()) {
        DropReader(error);
        return MakeFuture(std::vector<TSource::TRecord>{});
    }
    if (!readyEvent.IsSet()) {
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    i64 maxRowsPerRead = settings->MaxRowsPerBatch;
    if (offsetLimit.has_value()) {
        maxRowsPerRead = std::min(maxRowsPerRead, *offsetLimit - CurrentOffset_);
    }
    auto acquired = Throttler_->TryAcquireAvailable(maxRowsPerRead);
    if (!acquired) {
        // Idling on purpose while the reader is ready, so it must not be timed out for that.
        LastReadProgressInstant_ = TInstant::Now();
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    auto getPlannedTimestamp = [&] (i64 rowOffset) {
        return ComputePlannedTimestamp(
            rowOffset,
            {minOffsetInclusive, maxOffsetExclusive},
            dynamicSourcePartitionSpec->PlannedTimestamp,
            dynamicSourcePartitionSpec->PlannedReadDuration);
    };
    auto plannedTimestamp = getPlannedTimestamp(CurrentOffset_).Underlying();
    auto now = TInstant::Now().Seconds();
    if (dynamicSourcePartitionSpec->PlannedReadDuration != TDuration::Zero() &&
        now > plannedTimestamp &&
        now - plannedTimestamp > GetDynamicParameters()->CatchupLagThreshold.Seconds())
    {
        acquired = static_cast<i64>(std::min<double>(maxRowsPerRead, acquired * GetDynamicParameters()->CatchupSpeedupMultiplier));
    }

    maxRowsPerRead = acquired;

    auto unversionedRowBatch = reader->Read(TRowBatchReadOptions{
        .MaxRowsPerRead = maxRowsPerRead,
        .MaxDataWeightPerRead = settings->MaxBytesPerBatch});

    if (!unversionedRowBatch) {
        DropReader(NYT::TError("Got null batch from table reader, but more rows are expected")
                .With("current_offset", CurrentOffset_)
                .With("max_offset_exclusive", maxOffsetExclusive));
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    if (unversionedRowBatch->IsEmpty()) {
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    auto unversionedRowRange = unversionedRowBatch->MaterializeRows();
    YT_VERIFY(!unversionedRowRange.empty());

    LastReadProgressInstant_ = TInstant::Now();

    const auto& nameTable = reader->GetNameTable();
    std::vector<EValueType> wireTypes(nameTable->GetSize(), EValueType::Min);
    const auto anyConverters = InferTableSchema(nameTable, reader->GetTableSchema(), wireTypes);

    const auto [payloadSchema, idToColumnIndex] = GetSchemaAndMappingIndex(unversionedRowRange, nameTable, wireTypes, anyConverters);
    const int sequenceNumberId = payloadSchema->GetColumnIndexOrThrow(SequenceNumberColumnName);

    std::vector<TRecord> records;
    records.reserve(unversionedRowRange.size());

    for (auto row : unversionedRowRange) {
        i64 rowOffset = CurrentOffset_++;

        std::vector<TString> ysonBuffers(row.GetCount());
        std::vector<TUnversionedValue> adjustedCells;
        adjustedCells.reserve(row.GetCount());
        size_t stringDataSize = 0;
        for (ui32 i = 0; i < row.GetCount(); ++i) {
            const auto& cell = row[i];
            auto adjustedCell = anyConverters.contains(cell.Id)
                ? ConvertCellToAny(cell, anyConverters.FindPtr(cell.Id), ysonBuffers[i])
                : cell;
            auto columnId = idToColumnIndex[cell.Id];
            adjustedCell.Id = columnId;
            ValidateValueType(
                adjustedCell,
                payloadSchema->Columns()[columnId],
                /*typeAnyAcceptsAllValues*/ false,
                /*ignoreRequired*/ true,
                /*validateAnyIsValidYson*/ true);
            if (IsStringLikeType(adjustedCell.Type)) {
                stringDataSize += adjustedCell.Length;
            }
            adjustedCells.push_back(adjustedCell);
        }

        auto payload = TPayload(TPayload::TUnderlying(
            payloadSchema->GetColumnCount(),
            stringDataSize,
            [&] (TMutableUnversionedRow payloadRow) {
                for (int i = 0; i < payloadSchema->GetColumnCount(); ++i) {
                    payloadRow[i] = MakeUnversionedNullValue(i);
                }
                for (const auto& cell : adjustedCells) {
                    payloadRow[cell.Id] = cell;
                }
                payloadRow[sequenceNumberId] = MakeUnversionedInt64Value(rowOffset, sequenceNumberId);
            }));

        TRecord record = {
            .Offset = IntToOffset(rowOffset),
            .WriteTimestamp = dynamicSourcePartitionSpec->SystemTimestamp,
            .CreateTimestamp = dynamicSourcePartitionSpec->EventTimestamp,
            .Payloads = {std::move(payload)},
            .PayloadSchema = payloadSchema};
        if (dynamicSourcePartitionSpec->UsePlannedTimestamps) {
            auto timestamp = getPlannedTimestamp(rowOffset);
            record.WriteTimestamp = timestamp;
            record.CreateTimestamp = timestamp;
        }

        records.push_back(std::move(record));
    }

    if (dynamicSourcePartitionSpec->UsePlannedTimestamps) {
        if (!PendingPlannedReads_.empty() && PendingPlannedReads_.back().Spec == dynamicSourcePartitionSpec) {
            PendingPlannedReads_.back().EndOffset = CurrentOffset_;
        } else {
            PendingPlannedReads_.push_back({nextOffset, CurrentOffset_, dynamicSourcePartitionSpec});
        }
    }
    return MakeFuture(std::move(records));
}

bool TSource::IsFinite()
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

void TSourceControllerTable::SetLimit(i64 limit, TSystemTimestamp now)
{
    YT_VERIFY(limit >= 0);
    const auto freeSlotCount = ReadState->NextRowIndex < RowCount
        ? std::max<i64>(0, limit - std::ssize(ReadState->Ranges))
        : 0;
    while (std::ssize(ReadState->FreeSlotTimestamps) > freeSlotCount) {
        ReadState->FreeSlotTimestamps.erase(std::prev(ReadState->FreeSlotTimestamps.end()));
    }
    while (std::ssize(ReadState->FreeSlotTimestamps) < freeSlotCount) {
        ReadState->FreeSlotTimestamps.insert(now);
    }
}

bool TSourceControllerTable::CanSchedule() const
{
    return !ReadState->FreeSlotTimestamps.empty();
}

void TSourceControllerTable::ScheduleRange(TRangeId rangeId, i64 maxRowCount, double plannedRowsPerSecond)
{
    YT_VERIFY(maxRowCount > 0 && plannedRowsPerSecond > 0);
    YT_VERIFY(ReadState->NextRowIndex < RowCount);
    YT_VERIFY(!ReadState->FreeSlotTimestamps.empty());
    YT_VERIFY(!ReadState->Ranges.contains(rangeId));

    auto slotIt = ReadState->FreeSlotTimestamps.begin();
    auto rowCount = std::min(maxRowCount, RowCount - ReadState->NextRowIndex);
    TReadRange range;
    range.BeginRowIndex = ReadState->NextRowIndex;
    range.EndRowIndex = ReadState->NextRowIndex + rowCount;
    range.PlannedTimestamp = *slotIt;
    range.PlannedReadDuration = TDuration::Seconds(std::ceil(rowCount / plannedRowsPerSecond));
    ReadState->Ranges.emplace(rangeId, range);
    ReadState->NextRowIndex = range.EndRowIndex;
    LegacyDistributedRows = range.EndRowIndex;
    LegacyDistributingRanges.emplace(rangeId, std::pair(range.BeginRowIndex, range.EndRowIndex));
    ReadState->FreeSlotTimestamps.erase(slotIt);
    if (ReadState->NextRowIndex == RowCount) {
        ReadState->FreeSlotTimestamps.clear();
    }
}

bool TSourceControllerTable::FinishRange(TRangeId rangeId)
{
    auto rangeIt = ReadState->Ranges.find(rangeId);
    if (rangeIt == ReadState->Ranges.end()) {
        return false;
    }
    auto nextTimestamp = rangeIt->second.GetPlannedEndTimestamp();
    ReadState->Ranges.erase(rangeIt);
    LegacyDistributingRanges.erase(rangeId);
    if (ReadState->NextRowIndex < RowCount) {
        ReadState->FreeSlotTimestamps.insert(nextTimestamp);
    }
    return true;
}

void TSourceControllerTable::SkipRemainingRows()
{
    ReadState->NextRowIndex = RowCount;
    LegacyDistributedRows = RowCount;
    ReadState->FreeSlotTimestamps.clear();
    for (auto& [rangeId, range] : ReadState->Ranges) {
        range.BeginRowIndex = range.EndRowIndex;
        GetOrCrash(LegacyDistributingRanges, rangeId).first = range.EndRowIndex;
    }
}

i64 TSourceControllerTable::GetNotDistributedRows() const
{
    return RowCount - ReadState->NextRowIndex;
}

std::tuple<i64, TSystemTimestamp, i64> TSourceControllerTable::GetOrderingKey() const
{
    return {Era, EventTimestamp, EventOrdinal};
}

std::tuple<i64, TSystemTimestamp, TSystemTimestamp, std::string> TSourceControllerTable::GetV1OrderingKey() const
{
    return {Era, EventTimestamp, SystemTimestamp, Path.GetPath()};
}

std::string TSourceControllerTable::GetName() const
{
    auto path = Path.Attributes().Find<NYPath::TYPath>("original_path").value_or(Path.GetPath());
    return NYPath::DirNameAndBaseName(path).second;
}

void TSourceControllerTable::Register(TRegistrar registrar)
{
    registrar.Parameter("era", &TThis::Era)
        .Default(0);

    registrar.Parameter("path", &TThis::Path)
        .Default();

    registrar.Parameter("row_count", &TThis::RowCount)
        .Default(0);
    registrar.Parameter("byte_size", &TThis::ByteSize)
        .Default(0);

    registrar.Parameter("event_timestamp", &TThis::EventTimestamp)
        .Alias("create_timestamp")
        .Default(ZeroSystemTimestamp);
    registrar.Parameter("system_timestamp", &TThis::SystemTimestamp)
        .Alias("write_timestamp")
        .Default(ZeroSystemTimestamp);

    registrar.Parameter("event_ordinal", &TThis::EventOrdinal)
        .Default(0);

    registrar.Parameter("read_state", &TThis::ReadState)
        .DefaultNew();

    registrar.Parameter("distributed_rows", &TThis::LegacyDistributedRows)
        .Default(0);
    registrar.Parameter("distributing_ranges", &TThis::LegacyDistributingRanges)
        .Default();

    registrar.Postprocessor([] (TThis* table) {
        // COMPAT(mikari): trunk has no per-row plan; preserve its progress.
        if (!table->IsSet("read_state")) {
            table->ReadState = New<TTableReadState>();
            table->ReadState->NextRowIndex = table->LegacyDistributedRows;
            auto timestamp = table->SystemTimestamp != ZeroSystemTimestamp
                ? table->SystemTimestamp
                : TSystemTimestamp(TInstant::Now().Seconds());
            for (const auto& [rangeId, bounds] : table->LegacyDistributingRanges) {
                TReadRange range;
                range.BeginRowIndex = bounds.first;
                range.EndRowIndex = bounds.second;
                range.PlannedTimestamp = timestamp;
                table->ReadState->Ranges.emplace(rangeId, range);
            }
        }

        table->LegacyDistributedRows = table->ReadState->NextRowIndex;
        table->LegacyDistributingRanges.clear();
        for (const auto& [rangeId, range] : table->ReadState->Ranges) {
            table->LegacyDistributingRanges.emplace(rangeId, std::pair(range.BeginRowIndex, range.EndRowIndex));
        }
    });
}

i64 CalculateRemainingRowCount(
    const TSourceControllerTable& table,
    const THashMap<TRangeId, i64>& committedOffsets)
{
    auto rows = table.GetNotDistributedRows();
    for (const auto& [rangeId, range] : table.ReadState->Ranges) {
        auto offset = std::max(range.BeginRowIndex, GetOrDefault(committedOffsets, rangeId, i64(-1)));
        rows += std::max<i64>(0, range.EndRowIndex - offset);
    }
    return rows;
}

bool IsReadFinished(const TSourceControllerTable& table)
{
    return table.GetNotDistributedRows() == 0 && table.ReadState->Ranges.empty();
}

bool HasUnreportedRanges(
    const TTableReadState& state,
    const THashMap<TRangeId, i64>& committedOffsets)
{
    return AnyOf(state.Ranges, [&] (const auto& range) {
        return !committedOffsets.contains(range.first);
    });
}

TSystemTimestamp CalculateFutureTimestamp(
    const TSourceControllerTable& table,
    const THashMap<TRangeId, i64>& committedOffsets,
    TSystemTimestamp now)
{
    auto timestamp = now;
    for (const auto& [rangeId, range] : table.ReadState->Ranges) {
        if (!committedOffsets.contains(rangeId)) {
            timestamp = std::min(timestamp, range.PlannedTimestamp);
        }
        if (table.GetNotDistributedRows() > 0) {
            timestamp = std::min(timestamp, range.GetPlannedEndTimestamp());
        }
    }
    if (!table.ReadState->FreeSlotTimestamps.empty()) {
        timestamp = std::min(timestamp, *table.ReadState->FreeSlotTimestamps.begin());
    }
    return timestamp;
}

void TClusterProgress::Register(TRegistrar registrar)
{
    registrar.Parameter("by_cluster", &TThis::ByCluster)
        .Default();
    registrar.Parameter("era", &TThis::Era)
        .Default(0);
    registrar.Parameter("event_timestamp", &TThis::EventTimestamp)
        .Default();
    registrar.Parameter("event_ordinal", &TThis::EventOrdinal)
        .Default(0);
}

void TEventNameOrder::Register(TRegistrar registrar)
{
    registrar.Parameter("event_timestamp", &TThis::EventTimestamp)
        .Default();
    registrar.Parameter("names", &TThis::Names)
        .Default();
}

void TSourceControllerState::Register(TRegistrar registrar)
{
    registrar.Parameter("inited", &TThis::Inited)
        .Default(false);

    registrar.Parameter("distributing_table", &TThis::DistributingTable)
        .DefaultNew();
    registrar.Parameter("distribution_finished", &TThis::DistributionFinished)
        .Default(false);

    registrar.Parameter("era", &TThis::Era)
        .Default(0);
    registrar.Parameter("era_start_instant", &TThis::EraStartInstant)
        .Default(TInstant::Zero());

    registrar.Parameter("pending_count", &TThis::PendingCount)
        .Default(0);
    registrar.Parameter("pending_bytes", &TThis::PendingBytes)
        .Default(0);

    registrar.Parameter("processed_tables", &TThis::ProcessedTables)
        .Default(0);
    registrar.Parameter("lost_tables", &TThis::LostTables)
        .Default(0);

    registrar.Parameter("event_name_order", &TThis::EventNameOrder)
        .Default();

    registrar.Parameter("cluster_progress", &TThis::ClusterProgress)
        .Default();
    registrar.Parameter("pending_cleanup_tables", &TThis::PendingCleanupTables)
        .Default();
    registrar.Parameter("active_cluster_unavailable_since", &TThis::ActiveClusterUnavailableSince)
        .Default(TInstant::Zero());

    registrar.Parameter("mode", &TThis::Mode)
        .Default();
    registrar.Parameter("cutover_era", &TThis::CutoverEra)
        .Default();
    registrar.Parameter("cutover_event_timestamp", &TThis::CutoverEventTimestamp)
        .Default();
    registrar.Parameter("cutover_processed_table_names", &TThis::CutoverProcessedTableNames)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TSourceController::TMetrics::TMetrics(NProfiling::TProfiler profiler)
    : ProcessedTables(profiler.WithDefaultDisabled().Gauge("/processed_tables"))
    , LostTables(profiler.WithDefaultDisabled().Gauge("/lost_tables"))
{ }

TSourceController::TSourceController(
    TSourceControllerContextPtr context,
    TDynamicSourceControllerContextPtr dynamicContext)
    : TSourceControllerBase(std::move(context), std::move(dynamicContext))
    , CheckDistributingTableErrorState_(GetContext()->StatusProfiler->ErrorState("/check_distributing_table"))
    , Metrics_(GetContext()->Profiler.WithPrefix("/static_table"))
{ }

void TSourceController::Init(IInitContextPtr initContext)
{
    initContext->InitClient<TSourceControllerState>(State_, "v0");
    const bool isNativeV2Source = GetContext()->SourceSpec->SourceClassName != TypeName<TSource>();
    InitializeMigrationState(State_.Get(), isNativeV2Source);
    for (const auto& table : State_->PendingCleanupTables) {
        table->SkipRemainingRows();
    }
}

void TSourceController::Sync()
{ }

void TSourceController::Commit()
{ }

void TSourceController::InitializeMigrationState(
    TSourceControllerState* state,
    bool isNativeV2Source)
{
    if (!state->Mode) {
        const bool hasV2State = state->EventNameOrder || state->ClusterProgress;
        const bool hasV1Progress =
            state->Inited ||
            !state->DistributingTable->Path.GetPath().empty() ||
            state->DistributionFinished ||
            state->Era != 0 ||
            state->EraStartInstant != TInstant::Zero() ||
            state->PendingCount != 0 ||
            state->PendingBytes != 0 ||
            state->ProcessedTables != 0 ||
            state->LostTables != 0;
        state->Mode = hasV2State || (!hasV1Progress && isNativeV2Source)
            ? EMigrationMode::V2
            : EMigrationMode::V1;
    }

    if (!state->EventNameOrder) {
        state->EventNameOrder = New<TEventNameOrder>();
    }
    if (!state->ClusterProgress) {
        state->ClusterProgress = New<TClusterProgress>();
    }
}

void TSourceController::UpdateMigrationState(
    TSourceControllerState* state,
    bool allowV1Migration,
    const std::vector<TSourceControllerTablePtr>& tables,
    const TLogger& publicLogger)
{
    if (*state->Mode == EMigrationMode::V1 && allowV1Migration) {
        if (!state->Inited || state->DistributingTable->Path.GetPath().empty()) {
            state->Mode = EMigrationMode::V2;
        } else {
            state->Mode = EMigrationMode::Draining;
            state->CutoverEra = state->Era;
            state->CutoverEventTimestamp = state->DistributingTable->EventTimestamp;
            state->CutoverProcessedTableNames.clear();
            YT_TLOG_EVENT(publicLogger, ELogLevel::Info, "Static table source V1-to-V2 migration started draining the current timestamp")
                .With("CutoverEra", *state->CutoverEra)
                .With("CutoverEventTimestamp", *state->CutoverEventTimestamp);
        }
    }

    if (*state->Mode != EMigrationMode::Draining) {
        return;
    }

    YT_VERIFY(state->CutoverEra.has_value());
    YT_VERIFY(state->CutoverEventTimestamp.has_value());
    const auto& current = state->DistributingTable;
    for (const auto& table : tables) {
        if (table->RowCount > 0 &&
            table->Era == *state->CutoverEra &&
            table->EventTimestamp == *state->CutoverEventTimestamp &&
            table->GetV1OrderingKey() <= current->GetV1OrderingKey() &&
            !Contains(state->CutoverProcessedTableNames, table->GetName()))
        {
            state->CutoverProcessedTableNames.push_back(table->GetName());
        }
    }

    if (!state->DistributionFinished) {
        return;
    }

    const bool hasRemainingV1CutoverTable = AnyOf(tables, [&] (const TSourceControllerTablePtr& table) {
        return table->RowCount > 0 &&
            table->Era == *state->CutoverEra &&
            table->EventTimestamp == *state->CutoverEventTimestamp &&
            current->GetV1OrderingKey() < table->GetV1OrderingKey();
    });
    if (!hasRemainingV1CutoverTable) {
        state->Mode = EMigrationMode::V2;
        YT_TLOG_EVENT(publicLogger, ELogLevel::Info, "Static table source V1-to-V2 migration switched to V2 ordering")
            .With("CutoverEra", *state->CutoverEra)
            .With("CutoverEventTimestamp", *state->CutoverEventTimestamp);
    }
}

TSystemTimestamp TSourceController::ExtractTimestamp(
    const INodePtr& node,
    const TTableTimestampLocatorSpecPtr& locator)
{
    auto timestamp = node->Attributes().GetYson(locator->Attribute);
    auto timestampNode = ConvertTo<INodePtr>(timestamp);

    TInstant instant;
    switch (locator->Format) {
        case ETimestampFormat::Iso8601: {
            THROW_ERROR_EXCEPTION_UNLESS(timestampNode->GetType() == ENodeType::String, "Expected string for iso8601 timestamp, got %v", timestampNode->GetType());
            const auto& timestampString = timestampNode->AsString()->GetValue();
            if (TInstant::TryParseIso8601(timestampString, instant)) {
                if (!HasExplicitTimezone(timestampString) && locator->Timezone) {
                    instant = InterpretTimestampInTimezone(instant, *locator->Timezone);
                }
                return TSystemTimestamp(instant.Seconds());
            }
            THROW_ERROR_EXCEPTION("Cannot parse timestamp string %Qv as iso8601", timestampString);
        }
        case ETimestampFormat::Seconds:
            THROW_ERROR_EXCEPTION_UNLESS(timestampNode->GetType() == ENodeType::Uint64, "Expected ui64 for seconds timestamp, got %v", timestampNode->GetType());
            instant = TInstant::Seconds(timestampNode->AsUint64()->GetValue());
            return TSystemTimestamp(instant.Seconds());
        case ETimestampFormat::MilliSeconds:
            THROW_ERROR_EXCEPTION_UNLESS(timestampNode->GetType() == ENodeType::Uint64, "Expected ui64 for milliseconds timestamp, got %v", timestampNode->GetType());
            instant = TInstant::MilliSeconds(timestampNode->AsUint64()->GetValue());
            return TSystemTimestamp(instant.Seconds());
    }
}

std::vector<std::string> TSourceController::GetRequiredTableAttributes(const TTableSourceParametersPtr& sourceParameters)
{
    return std::vector<std::string>{
        "id",
        "type",
        "row_count",
        "uncompressed_data_size",
        sourceParameters->EventTimestampLocator->Attribute,
        sourceParameters->SystemTimestampLocator->Attribute,
    };
}

std::pair<TRichYPath, INodePtr> TSourceController::ResolveTable(
    const IClientPtr& client,
    const TRichYPath& table,
    const std::vector<std::string>& attributes)
{
    TGetNodeOptions getOptions;
    getOptions.Attributes = attributes;
    // Keep a final symlink unresolved, so an explicitly listed entry is never silently followed to
    // its target; a symlink (or any other non-table node) is rejected below.
    auto node = ConvertToNode(WaitFor(client->GetNode(table.GetPath() + NoFollowSymlinkSuffix, getOptions)).ValueOrThrow());
    THROW_ERROR_EXCEPTION_UNLESS(
        node->Attributes().Get<EObjectType>("type") == EObjectType::Table,
        "Node %v listed in parameter \"tables\" must be a table, symlinks are not allowed",
        table);
    return {table, node};
}

std::vector<TSourceControllerTablePtr> TSourceController::MakeTables(
    const std::vector<std::pair<TRichYPath, INodePtr>>& tablesInfo,
    const TTableSourceParametersPtr& sourceParameters,
    const TDynamicTableSourceParametersPtr& dynamicSourceSpec,
    const TSourceControllerTablePtr& lastProcessingTable,
    i64 era,
    const TEventNameOrderPtr& nameOrder,
    EMigrationMode mode,
    std::optional<i64> cutoverEra,
    std::optional<TSystemTimestamp> cutoverEventTimestamp,
    const std::vector<std::string>& cutoverProcessedTableNames)
{
    std::vector<TSourceControllerTablePtr> result;
    for (const auto& tableInfo : tablesInfo) {
        const auto& [path, node] = tableInfo;
        const auto nodeType = node->Attributes().Get<EObjectType>("type");
        if (sourceParameters->IgnoreSymlinks && EObjectType::Link == nodeType) {
            continue;
        } else if (sourceParameters->SkipNonTableNodes && EObjectType::Table != nodeType) {
            continue;
        } else {
            THROW_ERROR_EXCEPTION_UNLESS(EObjectType::Table == nodeType, "Node %v under \"tables_path\" must be a table, symlinks are not allowed", path);
        }

        if (sourceParameters->TableNameFilter &&
            !NRe2::TRe2::FullMatch(DirNameAndBaseName(path.GetPath()).second, *sourceParameters->TableNameFilter))
        {
            continue;
        }

        auto distributingTable = New<TSourceControllerTable>();

        distributingTable->Era = era;

        distributingTable->Path = path;
        distributingTable->Path.SetPath(Format("#%v", node->Attributes().Get<std::string>("id")));
        distributingTable->Path.Attributes().Set("original_path", path.GetPath());

        distributingTable->RowCount = node->Attributes().Get<i64>("row_count");
        distributingTable->ByteSize = node->Attributes().Get<i64>("uncompressed_data_size");

        distributingTable->SystemTimestamp = ExtractTimestamp(node, sourceParameters->SystemTimestampLocator);
        distributingTable->EventTimestamp = ExtractTimestamp(node, sourceParameters->EventTimestampLocator);

        result.push_back(distributingTable);
    }

    FilterTables(
        result,
        dynamicSourceSpec,
        lastProcessingTable,
        nameOrder,
        mode,
        cutoverEra,
        cutoverEventTimestamp,
        cutoverProcessedTableNames);
    SortTables(result, mode);

    return result;
}

namespace {

std::optional<i64> FindKnownEventOrdinal(
    const TEventNameOrderPtr& nameOrder,
    TSystemTimestamp eventTimestamp,
    const std::string& name)
{
    if (!nameOrder || nameOrder->EventTimestamp != eventTimestamp) {
        return std::nullopt;
    }
    auto index = Find(nameOrder->Names, name);
    if (index == nameOrder->Names.end()) {
        return std::nullopt;
    }
    return std::distance(nameOrder->Names.begin(), index);
}

std::vector<std::pair<TRichYPath, INodePtr>> ListDirTables(
    const IClientPtr& client,
    const TRichYPath& dirPath,
    const std::vector<std::string>& attributes)
{
    TListNodeOptions listOptions;
    listOptions.Attributes = attributes;
    auto listNode = ConvertToNode(WaitFor(client->ListNode(dirPath.GetPath(), listOptions)).ValueOrThrow());

    std::vector<std::pair<TRichYPath, INodePtr>> tablesInfo;
    for (const auto& node : listNode->AsList()->GetChildren()) {
        auto path = dirPath;
        path.SetPath(YPathJoin(dirPath.GetPath(), ConvertTo<std::string>(node)));
        tablesInfo.push_back({path, node});
    }
    return tablesInfo;
}

} // namespace

void TSourceController::FilterTables(
    std::vector<TSourceControllerTablePtr>& tables,
    const TDynamicTableSourceParametersPtr& dynamicSourceParameters,
    const TSourceControllerTablePtr& lastProcessingTable,
    const TEventNameOrderPtr& nameOrder,
    EMigrationMode mode,
    std::optional<i64> cutoverEra,
    std::optional<TSystemTimestamp> cutoverEventTimestamp,
    const std::vector<std::string>& cutoverProcessedTableNames)
{
    if (mode != EMigrationMode::V2) {
        EraseIf(tables, [&] (const TSourceControllerTablePtr& table) {
            if (dynamicSourceParameters->MinEventTimestamp.has_value() && table->EventTimestamp.Underlying() < *dynamicSourceParameters->MinEventTimestamp) {
                return true;
            }
            if (dynamicSourceParameters->MaxEventTimestamp.has_value() && table->EventTimestamp.Underlying() > *dynamicSourceParameters->MaxEventTimestamp) {
                return true;
            }
            return false;
        });
        return;
    }

    auto lastCoarseKey = std::tie(lastProcessingTable->Era, lastProcessingTable->EventTimestamp);
    EraseIf(tables, [&] (const TSourceControllerTablePtr& table) {
        if (dynamicSourceParameters->MinEventTimestamp.has_value() && table->EventTimestamp.Underlying() < *dynamicSourceParameters->MinEventTimestamp) {
            return true;
        }
        if (dynamicSourceParameters->MaxEventTimestamp.has_value() && table->EventTimestamp.Underlying() > *dynamicSourceParameters->MaxEventTimestamp) {
            return true;
        }
        if (cutoverEra.has_value() &&
            cutoverEventTimestamp.has_value() &&
            table->Era == *cutoverEra)
        {
            if (table->EventTimestamp < *cutoverEventTimestamp) {
                return true;
            }
            if (table->EventTimestamp == *cutoverEventTimestamp &&
                Contains(cutoverProcessedTableNames, table->GetName()))
            {
                return true;
            }
        }
        auto coarseKey = std::tie(table->Era, table->EventTimestamp);
        if (coarseKey < lastCoarseKey) {
            return true;
        }
        if (coarseKey > lastCoarseKey) {
            return false;
        }
        auto ordinal = FindKnownEventOrdinal(nameOrder, table->EventTimestamp, table->GetName());
        return ordinal.has_value() && *ordinal < lastProcessingTable->EventOrdinal;
    });
}

bool TSourceController::IsTableLess(
    const TSourceControllerTablePtr& lhs,
    const TSourceControllerTablePtr& rhs,
    EMigrationMode mode)
{
    return mode == EMigrationMode::V2
        ? lhs->GetOrderingKey() < rhs->GetOrderingKey()
        : lhs->GetV1OrderingKey() < rhs->GetV1OrderingKey();
}

bool TSourceController::IsSameTableKey(
    const TSourceControllerTablePtr& lhs,
    const TSourceControllerTablePtr& rhs,
    EMigrationMode mode)
{
    return !IsTableLess(lhs, rhs, mode) && !IsTableLess(rhs, lhs, mode);
}

void TSourceController::SortTables(
    std::vector<TSourceControllerTablePtr>& tables,
    EMigrationMode mode)
{
    std::sort(tables.begin(), tables.end(), [&] (const auto& lhs, const auto& rhs) {
        return IsTableLess(lhs, rhs, mode);
    });
}

std::vector<TSourceControllerTablePtr> TSourceController::ListClusterTables(
    const std::string& cluster,
    const TTableSourceParametersPtr& sourceParameters,
    const TDynamicTableSourceParametersPtr& dynamicSourceParameters,
    i64 era,
    const TSourceControllerTablePtr& lastProcessingTable,
    const TEventNameOrderPtr& nameOrder,
    EMigrationMode mode,
    std::optional<i64> cutoverEra,
    std::optional<TSystemTimestamp> cutoverEventTimestamp,
    const std::vector<std::string>& cutoverProcessedTableNames)
{
    auto dirPath = *sourceParameters->TablesPath;
    dirPath.Attributes().Remove("clusters");
    dirPath.SetCluster(cluster);

    auto client = CreateClient(GetContext(), dirPath);
    auto tablesInfo = ListDirTables(client, dirPath, GetRequiredTableAttributes(sourceParameters));

    return MakeTables(
        tablesInfo,
        sourceParameters,
        dynamicSourceParameters,
        lastProcessingTable,
        era,
        nameOrder,
        mode,
        cutoverEra,
        cutoverEventTimestamp,
        cutoverProcessedTableNames);
}

TListedTables TSourceController::GetTables(
    const TTableSourceParametersPtr& sourceParameters,
    const TDynamicTableSourceParametersPtr& dynamicSourceParameters,
    i64 era,
    const TSourceControllerTablePtr& lastProcessingTable,
    const TEventNameOrderPtr& nameOrder,
    EMigrationMode mode,
    std::optional<i64> cutoverEra,
    std::optional<TSystemTimestamp> cutoverEventTimestamp,
    std::vector<std::string> cutoverProcessedTableNames)
{
    THROW_ERROR_EXCEPTION_IF(
        mode != EMigrationMode::V2 && IsMultiCluster(sourceParameters),
        "Multi-cluster input is not supported while static table source is in %Qlv migration mode",
        mode);

    if (IsMultiCluster(sourceParameters)) {
        return GetMultiClusterTables(
            sourceParameters,
            dynamicSourceParameters,
            era,
            lastProcessingTable,
            nameOrder,
            mode,
            cutoverEra,
            cutoverEventTimestamp,
            cutoverProcessedTableNames);
    }
    return GetSingleClusterTables(
        sourceParameters,
        dynamicSourceParameters,
        era,
        lastProcessingTable,
        nameOrder,
        mode,
        cutoverEra,
        cutoverEventTimestamp,
        cutoverProcessedTableNames);
}

TListedTables TSourceController::GetMultiClusterTables(
    const TTableSourceParametersPtr& sourceParameters,
    const TDynamicTableSourceParametersPtr& dynamicSourceParameters,
    i64 era,
    const TSourceControllerTablePtr& lastProcessingTable,
    const TEventNameOrderPtr& nameOrder,
    EMigrationMode mode,
    std::optional<i64> cutoverEra,
    std::optional<TSystemTimestamp> cutoverEventTimestamp,
    const std::vector<std::string>& cutoverProcessedTableNames)
{
    auto clusters = GetPathClusters(*sourceParameters->TablesPath);

    // Active cluster should be first
    if (auto active = lastProcessingTable->Path.GetCluster()) {
        if (auto it = Find(clusters, *active); it != clusters.end()) {
            std::rotate(clusters.begin(), it, it + 1);
        }
    }

    std::vector<std::vector<TSourceControllerTablePtr>> perClusterTables;
    perClusterTables.reserve(clusters.size());
    THashSet<std::string> unavailableClusters;
    for (const auto& cluster : clusters) {
        try {
            perClusterTables.push_back(
                ListClusterTables(
                    cluster,
                    sourceParameters,
                    dynamicSourceParameters,
                    era,
                    lastProcessingTable,
                    nameOrder,
                    mode,
                    cutoverEra,
                    cutoverEventTimestamp,
                    cutoverProcessedTableNames));
        } catch (const std::exception& ex) {
            YT_TLOG_WARNING("Failed to list tables on cluster")
                .With("Cluster", cluster)
                .With(ex);
            perClusterTables.emplace_back();
            unavailableClusters.insert(cluster);
        }
    }
    THROW_ERROR_EXCEPTION_IF(unavailableClusters.size() == clusters.size(), "Failed to list tables from all clusters")
        .With("clusters", clusters);

    return TListedTables{
        .Tables = MergeByName(perClusterTables),
        .UnavailableClusters = std::move(unavailableClusters),
    };
}

TListedTables TSourceController::GetSingleClusterTables(
    const TTableSourceParametersPtr& sourceParameters,
    const TDynamicTableSourceParametersPtr& dynamicSourceParameters,
    i64 era,
    const TSourceControllerTablePtr& lastProcessingTable,
    const TEventNameOrderPtr& nameOrder,
    EMigrationMode mode,
    std::optional<i64> cutoverEra,
    std::optional<TSystemTimestamp> cutoverEventTimestamp,
    const std::vector<std::string>& cutoverProcessedTableNames)
{
    std::vector<std::pair<TRichYPath, INodePtr>> tablesInfo;
    if (sourceParameters->Tables.has_value()) {
        auto attributes = GetRequiredTableAttributes(sourceParameters);
        for (const auto& table : *sourceParameters->Tables) {
            auto client = CreateClient(GetContext(), table);
            tablesInfo.push_back(ResolveTable(client, table, attributes));
        }
    } else {
        auto client = CreateClient(GetContext(), *sourceParameters->TablesPath);
        tablesInfo = ListDirTables(client, *sourceParameters->TablesPath, GetRequiredTableAttributes(sourceParameters));
    }

    return TListedTables{
        .Tables = MakeTables(
            tablesInfo,
            sourceParameters,
            dynamicSourceParameters,
            lastProcessingTable,
            era,
            nameOrder,
            mode,
            cutoverEra,
            cutoverEventTimestamp,
            cutoverProcessedTableNames),
    };
}

void TSourceController::StashRangesForCleanup(TSourceControllerState* state, const TSourceControllerTablePtr& table)
{
    if (table->ReadState->Ranges.empty()) {
        return;
    }
    auto snapshot = NYTree::CloneYsonStruct(table);
    snapshot->SkipRemainingRows();
    state->PendingCleanupTables.push_back(std::move(snapshot));
}

void TSourceController::ResetStashIfTableChanged(TSourceControllerState* state, const TSourceControllerTablePtr& current)
{
    if (!state->ClusterProgress) {
        state->ClusterProgress = New<TClusterProgress>();
    }
    auto& clusterProgress = *state->ClusterProgress;
    auto stashOrderingKey = std::tie(clusterProgress.Era, clusterProgress.EventTimestamp, clusterProgress.EventOrdinal);
    if (stashOrderingKey == current->GetOrderingKey()) {
        return;
    }
    for (const auto& [cluster, table] : clusterProgress.ByCluster) {
        StashRangesForCleanup(state, table);
    }
    clusterProgress.ByCluster.clear();
    stashOrderingKey = current->GetOrderingKey();
}

void TSourceController::EnsureCurrentPresent(
    std::vector<TSourceControllerTablePtr>& tables,
    const TSourceControllerTablePtr& current)
{
    bool present = AnyOf(tables, [&] (const TSourceControllerTablePtr& table) {
        return table->GetOrderingKey() == current->GetOrderingKey();
    });
    if (!present) {
        tables.push_back(current);
        SortBy(tables, [] (const TSourceControllerTablePtr& table) {
            return table->GetOrderingKey();
        });
    }
}

void TSourceController::ReconcileDistributingTable(TListedTables listed)
{
    auto* state = State_.Get();
    UpdateMigrationState(
        state,
        GetDynamicParameters()->AllowV1Migration,
        listed.Tables,
        GetContext()->PublicLogger);

    const auto mode = *state->Mode;
    auto updateState = [&] {
        UpdateControllerState(state, listed.Tables, GetContext()->PublicLogger, mode);
    };
    if (mode != EMigrationMode::V2) {
        if (state->Inited) {
            EraseIf(listed.Tables, [&] (const TSourceControllerTablePtr& table) {
                return table->RowCount == 0 || IsTableLess(table, state->DistributingTable, mode);
            });
        } else {
            EraseIf(listed.Tables, [] (const TSourceControllerTablePtr& table) {
                return table->RowCount == 0;
            });
        }
        SortTables(listed.Tables, mode);
        updateState();
        return;
    }

    FilterTables(
        listed.Tables,
        GetDynamicParameters(),
        state->DistributingTable,
        state->EventNameOrder,
        mode,
        state->CutoverEra,
        state->CutoverEventTimestamp,
        state->CutoverProcessedTableNames);
    EraseIf(listed.Tables, [] (const TSourceControllerTablePtr& table) {
        return table->RowCount == 0;
    });
    AssignEventOrdinals(listed.Tables, state);
    if (state->Inited) {
        EraseIf(listed.Tables, [&] (const TSourceControllerTablePtr& table) {
            return IsTableLess(table, state->DistributingTable, mode);
        });
    }
    SortTables(listed.Tables, mode);

    const auto& current = state->DistributingTable;

    const bool canFailover = IsMultiCluster(GetParameters()) &&
        state->Inited &&
        !state->DistributionFinished &&
        !current->Path.GetPath().empty() &&
        current->Path.GetCluster().has_value();

    if (!canFailover) {
        updateState();
        return;
    }

    auto activeCluster = *current->Path.GetCluster();
    ResetStashIfTableChanged(state, current);

    const bool activeUnavailable = listed.UnavailableClusters.contains(activeCluster);
    if (activeUnavailable) {
        EnsureCurrentPresent(listed.Tables, current);
        if (state->ActiveClusterUnavailableSince == TInstant::Zero()) {
            state->ActiveClusterUnavailableSince = TInstant::Now();
        }
        if (TInstant::Now() - state->ActiveClusterUnavailableSince <= GetParameters()->FailoverDelay) {
            updateState();
            return;
        }
    } else {
        state->ActiveClusterUnavailableSince = TInstant::Zero();
    }

    auto serving = FindReplicaServingCurrentTable(listed.Tables, current->GetName());
    auto now = serving
        ? WaitFor(GetContext()->TimeProvider->GetTimestamp(/*barrier*/ false)).ValueOrThrow()
        : ZeroSystemTimestamp;

    if (serving &&
        serving->Path.GetCluster() == current->Path.GetCluster() &&
        serving->Path.GetPath() != current->Path.GetPath())
    {
        YT_TLOG_EVENT(GetContext()->PublicLogger, ELogLevel::Warning, "Current table was recreated under a new object id; rereading from scratch")
            .With("Cluster", activeCluster)
            .With("EventTimestamp", current->EventTimestamp)
            .With("OldId", current->Path.GetPath())
            .With("NewId", serving->Path.GetPath());
        auto newTable = MakeFailoverTable(current, serving, /*resumeFrom*/ nullptr, now, GetDynamicParameters());
        StashRangesForCleanup(state, current);
        state->DistributingTable = std::move(newTable);
        CommittedOffsetsExclusive_.clear();
    }

    auto decision = DecideFailover(current, serving, state->ClusterProgress->ByCluster, GetContext()->PublicLogger, now, GetDynamicParameters());
    if (!decision) {
        updateState();
        return;
    }

    const auto& newTable = decision->NewTable;
    auto toCluster = *newTable->Path.GetCluster();

    state->ClusterProgress->ByCluster[decision->StashedCluster] = NYTree::CloneYsonStruct(current);
    state->ClusterProgress->ByCluster.erase(toCluster);

    YT_TLOG_EVENT(
        GetContext()->PublicLogger,
        ELogLevel::Info,
        "Failing over current table to another cluster")
        .With("EventTimestamp", current->EventTimestamp)
        .With("FromCluster", decision->StashedCluster)
        .With("ToCluster", toCluster)
        .With("ResumedDistributedRows", newTable->ReadState->NextRowIndex)
        .With("RowCount", newTable->RowCount);

    state->DistributingTable = newTable;
    CommittedOffsetsExclusive_.clear();

    updateState();
}

std::vector<std::string> TSourceController::GetPathClusters(const TRichYPath& path)
{
    if (auto clusters = path.GetClusters()) {
        return *clusters;
    }
    if (auto cluster = path.GetCluster()) {
        return {*cluster};
    }
    return {};
}

bool TSourceController::IsMultiCluster(const TTableSourceParametersPtr& sourceSpec)
{
    return sourceSpec->TablesPath.has_value() && sourceSpec->TablesPath->GetClusters().has_value();
}

std::vector<TSourceControllerTablePtr> TSourceController::MergeByName(
    const std::vector<std::vector<TSourceControllerTablePtr>>& perClusterActiveFirst)
{
    THashMap<std::string, TSourceControllerTablePtr> byName;
    for (const auto& clusterTables : perClusterActiveFirst) {
        for (const auto& table : clusterTables) {
            byName.emplace(table->GetName(), table);
        }
    }

    std::vector<TSourceControllerTablePtr> result;
    result.reserve(byName.size());
    for (auto& [_, table] : byName) {
        result.push_back(std::move(table));
    }
    return result;
}

TSourceControllerTablePtr TSourceController::FindReplicaServingCurrentTable(
    const std::vector<TSourceControllerTablePtr>& tables,
    const std::string& name)
{
    auto* found = FindIfPtr(tables, [&] (const TSourceControllerTablePtr& table) {
        return table->GetName() == name;
    });
    return found ? *found : nullptr;
}

TSourceControllerTablePtr TSourceController::MakeFailoverTable(
    const TSourceControllerTablePtr& current,
    const TSourceControllerTablePtr& serving,
    const TSourceControllerTablePtr& resumeFrom,
    TSystemTimestamp now,
    const TDynamicTableSourceParametersPtr& dynamicParameters)
{
    auto newTable = resumeFrom
        ? NYTree::CloneYsonStruct(resumeFrom)
        : New<TSourceControllerTable>();

    newTable->Path = serving->Path;
    newTable->RowCount = serving->RowCount;
    newTable->ByteSize = serving->ByteSize;

    newTable->Era = current->Era;
    newTable->EventTimestamp = current->EventTimestamp;
    newTable->EventOrdinal = current->EventOrdinal;
    newTable->ReadState->FreeSlotTimestamps.clear();
    if (!newTable->ReadState->Ranges.empty()) {
        auto rangeRate = GetDesiredRangeRowsPerSecond(dynamicParameters, newTable.Get());
        auto totalRate = std::min(
            GetDesiredRowsPerSecond(dynamicParameters, newTable.Get()),
            rangeRate * dynamicParameters->MaxPartitionCount);
        auto rangeCount = std::min<i64>(dynamicParameters->MaxPartitionCount, std::ceil(totalRate / rangeRate));
        auto rowsPerSecond = std::min(rangeRate, totalRate / std::max(rangeCount, std::ssize(newTable->ReadState->Ranges)));
        for (auto& [rangeId, range] : newTable->ReadState->Ranges) {
            range.PlannedTimestamp = now;
            range.PlannedReadDuration = TDuration::Seconds(std::ceil(
                (range.EndRowIndex - range.BeginRowIndex) / rowsPerSecond));
        }
    }

    auto maxSystemTimestamp = std::max({
        current->SystemTimestamp.Underlying(),
        serving->SystemTimestamp.Underlying(),
        newTable->SystemTimestamp.Underlying(),
    });
    newTable->SystemTimestamp = TSystemTimestamp(maxSystemTimestamp);

    return newTable;
}

std::optional<TFailoverDecision> TSourceController::DecideFailover(
    const TSourceControllerTablePtr& current,
    const TSourceControllerTablePtr& servingReplica,
    const THashMap<std::string, TSourceControllerTablePtr>& stash,
    const TLogger& publicLogger,
    TSystemTimestamp now,
    const TDynamicTableSourceParametersPtr& dynamicParameters)
{
    if (!servingReplica ||
        !current->Path.GetCluster().has_value() ||
        !servingReplica->Path.GetCluster().has_value() ||
        *servingReplica->Path.GetCluster() == *current->Path.GetCluster())
    {
        return std::nullopt;
    }

    TSourceControllerTablePtr resumeFrom;
    if (auto it = stash.find(*servingReplica->Path.GetCluster()); it != stash.end()) {
        resumeFrom = it->second;
        if (resumeFrom->Path.GetPath() != servingReplica->Path.GetPath()) {
            YT_TLOG_EVENT(publicLogger, ELogLevel::Warning, "Stashed table was recreated on the target cluster; rereading from scratch")
                .With("Cluster", *servingReplica->Path.GetCluster())
                .With("EventTimestamp", current->EventTimestamp)
                .With("StashedId", resumeFrom->Path.GetPath())
                .With("LiveId", servingReplica->Path.GetPath());
            resumeFrom = nullptr;
        }
    }

    return TFailoverDecision{
        .NewTable = MakeFailoverTable(current, servingReplica, resumeFrom, now, dynamicParameters),
        .StashedCluster = *current->Path.GetCluster(),
    };
}

namespace {

void AssignOrdinalsInGroup(
    std::vector<std::string>& names,
    const std::vector<TSourceControllerTablePtr>& group)
{
    THashMap<std::string, i64> indexByName;
    for (int i = 0; i < std::ssize(names); ++i) {
        indexByName[names[i]] = i;
    }

    std::vector<TSourceControllerTablePtr> fresh;
    for (const auto& table : group) {
        if (auto it = indexByName.find(table->GetName()); it != indexByName.end()) {
            table->EventOrdinal = it->second;
        } else {
            fresh.push_back(table);
        }
    }

    SortBy(fresh, [] (const TSourceControllerTablePtr& table) {
        return std::pair(table->SystemTimestamp, table->GetName());
    });

    for (const auto& table : fresh) {
        table->EventOrdinal = std::ssize(names);
        names.push_back(table->GetName());
    }
}

} // namespace

void TSourceController::AssignEventOrdinals(
    std::vector<TSourceControllerTablePtr>& tables,
    TSourceControllerState* state)
{
    if (!state->EventNameOrder) {
        state->EventNameOrder = New<TEventNameOrder>();
    }
    auto& nameOrder = *state->EventNameOrder;

    std::optional<TSystemTimestamp> currentEventTimestamp;
    if (state->Inited) {
        currentEventTimestamp = state->DistributingTable->EventTimestamp;
    }

    if (currentEventTimestamp && nameOrder.EventTimestamp != *currentEventTimestamp) {
        nameOrder.EventTimestamp = *currentEventTimestamp;
        nameOrder.Names = {state->DistributingTable->GetName()};
    }

    THashMap<TSystemTimestamp, std::vector<TSourceControllerTablePtr>> byEventTs;
    for (const auto& table : tables) {
        byEventTs[table->EventTimestamp].push_back(table);
    }

    for (auto& [eventTimestamp, group] : byEventTs) {
        if (currentEventTimestamp && eventTimestamp == *currentEventTimestamp) {
            AssignOrdinalsInGroup(nameOrder.Names, group);
        } else {
            std::vector<std::string> transientNames;
            AssignOrdinalsInGroup(transientNames, group);
        }
    }
}

void TSourceController::UpdateControllerState(
    TSourceControllerState* state,
    const std::vector<TSourceControllerTablePtr>& tables,
    const TLogger& publicLogger,
    EMigrationMode mode)
{
    if (state->EraStartInstant == TInstant::Zero()) {
        state->EraStartInstant = TInstant::Now();
    }

    YT_VERIFY(std::is_sorted(tables.begin(), tables.end(), [&] (const auto& lhs, const auto& rhs) {
        return IsTableLess(lhs, rhs, mode);
    }));
    for (auto& table : tables) {
        YT_VERIFY(table->RowCount != 0, Format("Table %Qv is empty", table->Path));
        if (mode == EMigrationMode::V2) {
            YT_VERIFY(!IsTableLess(table, state->DistributingTable, mode), Format("Table %Qv has outdated ordering key (Key: %v, ThresholdKey: %v)", table->Path, table->GetOrderingKey(), state->DistributingTable->GetOrderingKey()));
        } else {
            YT_VERIFY(!IsTableLess(table, state->DistributingTable, mode), Format("Table %Qv has outdated ordering key (Key: %v, ThresholdKey: %v)", table->Path, table->GetV1OrderingKey(), state->DistributingTable->GetV1OrderingKey()));
        }
    }

    auto it = tables.begin();

    // Skip/check current.
    if (it != tables.end() && IsSameTableKey(*it, state->DistributingTable, mode)) {
        ++it;
    } else {
        if (state->DistributingTable->GetNotDistributedRows() != 0) {
            YT_TLOG_EVENT(
                publicLogger,
                ELogLevel::Error,
                "Data loss was detected, table was removed before it is fully read")
                .With("Table", state->DistributingTable->Path)
                .With("RowsLost", state->DistributingTable->GetNotDistributedRows())
                .With("RowsTotal", state->DistributingTable->RowCount);
            state->LostTables += 1;
        } else if (!state->DistributionFinished) {
            state->ProcessedTables += 1;
        }
        state->DistributingTable->SkipRemainingRows();
    }

    bool needStartNewTable = (it != tables.end() && state->DistributionFinished);
    if (needStartNewTable) {
        state->DistributingTable = *it;
        state->DistributingTable->Era = state->Era;
        state->DistributionFinished = false;
        ++it;
    }

    state->PendingCount = 0;
    state->PendingBytes = 0;
    for (; it != tables.end(); ++it) {
        state->PendingCount += (*it)->RowCount;
        state->PendingBytes += (*it)->ByteSize;
    }

    if (needStartNewTable) {
        YT_TLOG_EVENT(
            publicLogger,
            ELogLevel::Info,
            "Starting to read new table")
            .With("Table", state->DistributingTable->Path)
            .With("TableRows", state->DistributingTable->RowCount)
            .With("TableBytes", state->DistributingTable->ByteSize)
            .With("TableEventTimestamp", state->DistributingTable->EventTimestamp)
            .With("TableSystemTimestamp", state->DistributingTable->SystemTimestamp)
            .With("QueuedTablesRows", state->PendingCount)
            .With("QueuedTablesBytes", state->PendingBytes);
    }
}

bool TSourceController::ApplyRestartInstantLogic(
    TSourceControllerState* state,
    TInstant restartInstant,
    const TLogger& publicLogger)
{
    auto now = TInstant::Now();
    if (restartInstant > now) {
        YT_TLOG_EVENT(
            publicLogger,
            ELogLevel::Warning,
            "Misconfiguration: restart instant in dynamic parameters is greater than now")
            .With("RestartInstant", restartInstant)
            .With("Now", now);
        return false;
    }
    if (restartInstant > state->EraStartInstant) {
        state->DistributingTable->SkipRemainingRows();

        state->Era += 1;
        YT_TLOG_EVENT(
            publicLogger,
            ELogLevel::Warning,
            "Starting new era")
            .With("Era", state->Era)
            .With("LastEraStartInstant", state->EraStartInstant)
            .With("NewEraStartInstant", now);
        state->EraStartInstant = now;
        return true;
    }
    return false;
}

bool TSourceController::CheckDistributingTable()
{
    if (!TablesFuture_) {
        TablesFuture_ = BIND(
            &TSourceController::GetTables,
            MakeStrong(this),
            GetParameters(),
            GetDynamicParameters(),
            State_->Era,
            State_->DistributingTable,
            NYTree::CloneYsonStruct(State_->EventNameOrder),
            *State_->Mode,
            State_->CutoverEra,
            State_->CutoverEventTimestamp,
            State_->CutoverProcessedTableNames)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }
    if (TablesFuture_.IsSet()) {
        if (TablesFuture_.GetOrCrash().IsOK()) {
            try {
                if (ApplyRestartInstantLogic(State_.Get(), GetDynamicParameters()->RestartInstant, GetContext()->PublicLogger)) {
                    TablesFuture_ = {};
                    return State_->Inited;
                }
                CheckDistributionFinished();
                ReconcileDistributingTable(TablesFuture_.GetOrCrash().ValueOrThrow());
                State_->Inited = true;
                CheckDistributingTableErrorState_->ClearError();
            } catch (const std::exception& ex) {
                static constexpr auto Message = "Failed to update distributing table"_sb;
                auto error = TError(Message)
                    .With(ex);
                YT_TLOG_EVENT(
                    GetContext()->PublicLogger,
                    ELogLevel::Error,
                    Message)
                    .With(ex);
                CheckDistributingTableErrorState_->SetError(error);
            }
        } else {
            static constexpr auto Message = "Failed to get tables"_sb;
            const auto& tablesError = TablesFuture_.GetOrCrash();
            auto error = TError(Message)
                .With(tablesError);
            YT_TLOG_EVENT(
                GetContext()->PublicLogger,
                ELogLevel::Error,
                Message)
                .With(tablesError);
            CheckDistributingTableErrorState_->SetError(error);
        }
        TablesFuture_ = {};
    }
    return State_->Inited;
}

double TSourceController::GetDesiredRangeRowsPerSecond(
    const TDynamicTableSourceParametersPtr& dynamicParameters,
    const TSourceControllerTable* distributingTable)
{
    double estimatedRowByteSize = std::max(1.0, static_cast<double>(distributingTable->ByteSize) / distributingTable->RowCount);
    return std::min<double>(
        dynamicParameters->DesiredPartitionRowsPerSecond,
        dynamicParameters->DesiredPartitionBytesPerSecond / estimatedRowByteSize);
}

void TSourceController::ProcessPartitionStatuses(const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& statuses)
{
    for (const auto& [key, status] : statuses) {
        YT_VERIFY(status->PartitionState != EPartitionState::Interrupting);
        auto rangeId = ExtractRangeId(key);
        if (status->PartitionState == EPartitionState::Completed) {
            CommittedOffsetsExclusive_.erase(rangeId);
        } else {
            CommittedOffsetsExclusive_[rangeId] = ConvertTo<TPartitionStatusPtr>(status->PartitionStatus)->CommittedOffsetExclusive;
        }
    }

    for (const auto& [key, status] : statuses) {
        if (status->PartitionState != EPartitionState::Completed) {
            continue;
        }
        auto rangeId = ExtractRangeId(key);
        State_->DistributingTable->FinishRange(rangeId);
        for (const auto& table : State_->PendingCleanupTables) {
            table->FinishRange(rangeId);
        }
    }

    EraseIf(State_->PendingCleanupTables, [] (const TSourceControllerTablePtr& table) {
        return IsReadFinished(*table);
    });
}

void TSourceController::CheckDistributionFinished()
{
    auto* state = State_.Get();
    const auto& distributingTable = state->DistributingTable;
    if (state->DistributionFinished) {
        return;
    }
    if (distributingTable->Path.GetPath().empty()) {
        state->DistributionFinished = true;
        return;
    }
    if (!IsReadFinished(*distributingTable)) {
        return;
    }

    if (!state->ClusterProgress->ByCluster.empty()) {
        for (const auto& [cluster, table] : state->ClusterProgress->ByCluster) {
            StashRangesForCleanup(state, table);
        }
        state->ClusterProgress->ByCluster.clear();
    }
    if (!state->PendingCleanupTables.empty()) {
        return;
    }

    state->DistributionFinished = true;
    state->ProcessedTables += 1;
    YT_TLOG_EVENT(
        GetContext()->PublicLogger,
        ELogLevel::Info,
        "Table was processed")
        .With("Table", distributingTable->Path);
}

double TSourceController::GetDesiredRowsPerSecond(
    const TDynamicTableSourceParametersPtr& dynamicParameters,
    const TSourceControllerTable* distributingTable)
{
    double estimatedRowByteSize = std::max(1.0, static_cast<double>(distributingTable->ByteSize) / distributingTable->RowCount);
    return std::min<double>({
        // Ideal.
        distributingTable->RowCount / dynamicParameters->DesiredTableProcessTime.SecondsFloat(),
        // Manual limitations.
        dynamicParameters->MaxRowsPerSecond,
        dynamicParameters->MaxBytesPerSecond / estimatedRowByteSize,
    });
}

THashMap<TRangeId, IMapNodePtr> TSourceController::DoDistributing(
    const TDynamicTableSourceParametersPtr& dynamicParameters,
    double desiredRangeRowsPerSecond,
    const THashMap<TRangeId, i64>& committedOffsetsExclusive,
    const TSourceControllerTablePtr& distributingTable,
    std::function<TRangeId()> rangeIdGenerator,
    bool usePlannedTimestamps,
    TSystemTimestamp now)
{
    THROW_ERROR_EXCEPTION_IF(desiredRangeRowsPerSecond == 0, "Can not do partitioning with zero desired partition rows per second");
    if (distributingTable->RowCount == 0) {
        return {};
    }

    double desiredRowsPerSecond = std::min(
        GetDesiredRowsPerSecond(dynamicParameters, distributingTable.Get()),
        desiredRangeRowsPerSecond * dynamicParameters->MaxPartitionCount);
    const i64 desiredRangeCount = std::min<i64>(dynamicParameters->MaxPartitionCount, std::ceil(desiredRowsPerSecond / desiredRangeRowsPerSecond));

    const auto desiredRangeProcessTimeSeconds = std::min(
        dynamicParameters->DesiredPartitionProcessTime.SecondsFloat(),
        CalculateRemainingRowCount(*distributingTable, committedOffsetsExclusive) / desiredRowsPerSecond);

    distributingTable->SetLimit(desiredRangeCount, now);
    const i64 rows = std::max<i64>(1, desiredRangeRowsPerSecond * desiredRangeProcessTimeSeconds);
    auto plannedRowsPerSecond = std::min(desiredRangeRowsPerSecond, desiredRowsPerSecond / desiredRangeCount);
    while (distributingTable->CanSchedule()) {
        distributingTable->ScheduleRange(rangeIdGenerator(), rows, plannedRowsPerSecond);
    }
    return BuildPartitionSpecs(distributingTable, desiredRowsPerSecond, desiredRangeRowsPerSecond, usePlannedTimestamps);
}

THashMap<TRangeId, IMapNodePtr> TSourceController::BuildPartitionSpecs(
    const TSourceControllerTablePtr& table,
    double totalRowsPerSecond,
    double rangeRowsPerSecond,
    bool usePlannedTimestamps)
{
    const auto& ranges = table->ReadState->Ranges;
    THashMap<TRangeId, IMapNodePtr> result;
    for (const auto& [rangeId, range] : ranges) {
        auto spec = New<TDynamicTableSourcePartitionSpec>();
        spec->Table = table->Path;
        SetRowIndexRange(spec->Table, range.BeginRowIndex, range.EndRowIndex);
        spec->EventTimestamp = table->EventTimestamp;
        spec->SystemTimestamp = table->SystemTimestamp;
        spec->PlannedTimestamp = range.PlannedTimestamp;
        spec->PlannedReadDuration = range.PlannedReadDuration;
        spec->UsePlannedTimestamps = usePlannedTimestamps;
        if (usePlannedTimestamps) {
            spec->EventTimestamp = range.PlannedTimestamp;
            spec->SystemTimestamp = range.PlannedTimestamp;
        }
        spec->RowsPerSecond = std::min(totalRowsPerSecond / ranges.size(), rangeRowsPerSecond);
        result[rangeId] = ConvertTo<IMapNodePtr>(spec);
    }
    return result;
}

void TSourceController::UpdateMetrics()
{
    Metrics_.ProcessedTables.Update(State_->ProcessedTables);
    Metrics_.LostTables.Update(State_->LostTables);
}

TSystemTimestamp TSourceController::CalculateEventWatermark(
    TSystemTimestamp tableEventTimestamp,
    TSystemTimestamp now,
    bool isIdle,
    const std::optional<TDuration>& idleWatermarkDelay)
{
    if (!isIdle || !idleWatermarkDelay) {
        return tableEventTimestamp;
    }

    auto idleWatermark = now.Underlying() - std::min(now.Underlying(), idleWatermarkDelay->Seconds());
    return TSystemTimestamp(std::max(tableEventTimestamp.Underlying(), idleWatermark));
}

std::optional<THashMap<TKey, IMapNodePtr>> TSourceController::ListKeys()
{
    if (!CheckDistributingTable()) {
        return std::nullopt;
    }

    UpdateMetrics();

    const auto& distributingTable = State_->DistributingTable;

    auto rangeIdGenerator = [&] {
        return TRangeId(GetContext()->TimeProvider->GenerateSeqNo());
    };

    auto now = WaitFor(GetContext()->TimeProvider->GetTimestamp(/*barrier*/ false))
        .ValueOrThrow();
    THashMap<TKey, IMapNodePtr> result;
    auto emit = [&] (const THashMap<TRangeId, IMapNodePtr>& specs) {
        for (const auto& [rangeId, spec] : specs) {
            result[GenerateRangeKey(rangeId)] = spec;
        }
    };
    emit(DoDistributing(
        GetDynamicParameters(),
        GetDesiredRangeRowsPerSecond(GetDynamicParameters(), distributingTable.Get()),
        CommittedOffsetsExclusive_,
        distributingTable,
        rangeIdGenerator,
        GetParameters()->UsePlannedTimestamps,
        now));

    // Emit abandoned ranges so their partitions Complete and reclaim source key state.
    for (const auto& table : State_->PendingCleanupTables) {
        auto rangeRate = GetDesiredRangeRowsPerSecond(GetDynamicParameters(), table.Get());
        auto totalRate = std::min(
            GetDesiredRowsPerSecond(GetDynamicParameters(), table.Get()),
            rangeRate * GetDynamicParameters()->MaxPartitionCount);
        emit(BuildPartitionSpecs(table, totalRate, rangeRate, GetParameters()->UsePlannedTimestamps));
    }
    return result;
}

std::optional<TStreamTraverseDataPtr> TSourceController::GetFutureKeysStreamTraverseData()
{
    const auto& distributingTable = State_->DistributingTable;
    i64 notDistributedCount = distributingTable->GetNotDistributedRows() + State_->PendingCount;
    auto hasUnreportedRanges = [&] (const TSourceControllerTablePtr& table) {
        return HasUnreportedRanges(*table->ReadState, CommittedOffsetsExclusive_);
    };
    bool hasUnreported = hasUnreportedRanges(distributingTable);
    for (const auto& table : State_->PendingCleanupTables) {
        hasUnreported |= hasUnreportedRanges(table);
    }

    bool noFuturePartitions = GetParameters()->Finite && State_->Inited && notDistributedCount == 0 &&
        !hasUnreported && State_->PendingCleanupTables.empty() && State_->ClusterProgress->ByCluster.empty();
    auto now = WaitFor(GetContext()->TimeProvider->GetTimestamp(/*barrier*/ false))
        .ValueOrThrow();
    bool isIdle = State_->Inited && notDistributedCount == 0 && !hasUnreported;

    auto sourceStream = New<TStreamTraverseData>();
    sourceStream->Epoch = -1; // Will be fixed in universal controller.
    sourceStream->State = noFuturePartitions ? EStreamState::Completed : EStreamState::Drained;
    sourceStream->SystemWatermark = isIdle ? now : distributingTable->SystemTimestamp;
    sourceStream->EventWatermark = CalculateEventWatermark(
        distributingTable->EventTimestamp,
        now,
        isIdle,
        GetParameters()->IdleWatermarkDelay);
    if (GetParameters()->UsePlannedTimestamps) {
        auto plannedTimestamp = CalculateFutureTimestamp(*distributingTable, CommittedOffsetsExclusive_, now);
        for (const auto& table : State_->PendingCleanupTables) {
            plannedTimestamp = std::min(plannedTimestamp, CalculateFutureTimestamp(*table, CommittedOffsetsExclusive_, now));
        }
        sourceStream->SystemWatermark = plannedTimestamp;
        sourceStream->EventWatermark = plannedTimestamp;
    } else {
        for (const auto& table : State_->PendingCleanupTables) {
            if (hasUnreportedRanges(table)) {
                sourceStream->SystemWatermark = std::min(sourceStream->SystemWatermark, table->SystemTimestamp);
                sourceStream->EventWatermark = std::min(sourceStream->EventWatermark, table->EventTimestamp);
            }
        }
    }

    auto inflightMetrics = sourceStream->InflightMetrics;
    inflightMetrics->Count = notDistributedCount;
    double notDistributedRatio = distributingTable->RowCount > 0
        ? static_cast<double>(distributingTable->GetNotDistributedRows()) / distributingTable->RowCount
        : 0;
    inflightMetrics->ByteSize = notDistributedRatio * distributingTable->ByteSize + State_->PendingBytes;

    return sourceStream;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnector

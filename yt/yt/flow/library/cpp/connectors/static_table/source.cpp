#include "source.h"

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

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/library/re2/re2.h>

#include <library/cpp/iterator/zip.h>

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

template <class TContextPtr>
IClientPtr CreateClient(const TContextPtr& context, const TRichYPath& path)
{
    return context->ClientsCache->GetClient(*path.GetCluster());
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
            << TErrorAttribute("static_table_partition_key", key);
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
    , Logger(TOrderedSourceBase::Logger.WithTag("Tables: %v, TablesPath: %v", GetParameters()->Tables, GetParameters()->TablesPath))
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
            // Sanity check. These parameters are not changed during partition life.
            auto newDynamicPartitionSpec = GetDynamicPartitionSpec();
            YT_TLOG_INFO("Source got new dynamic source partition spec")
                .With("newDynamicPartitionSpec", ConvertToYsonString(newDynamicPartitionSpec, EYsonFormat::Text));
            if (previousDynamicPartitionSpec) {
                YT_VERIFY(newDynamicPartitionSpec->Table.GetPath() == previousDynamicPartitionSpec->Table.GetPath());
                YT_VERIFY(newDynamicPartitionSpec->EventTimestamp == previousDynamicPartitionSpec->EventTimestamp);
                YT_VERIFY(newDynamicPartitionSpec->SystemTimestamp == previousDynamicPartitionSpec->SystemTimestamp);
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
    Client_ = CreateClient(GetContext(), GetDynamicPartitionSpec()->Table);
}

void TSource::DoReportPersistedOffset(TOffset offsetExclusive)
{
    PersistedOffsetExclusive_.store(OffsetToInt(offsetExclusive));
    auto [minOffsetInclusive, maxOffsetExclusive] = GetRowIndexRange(GetDynamicPartitionSpec()->Table);
    UpdatePartitionInfo(
        TPartitionInfoUpdate{
            // There is no external consumer to commit, so mark offset as committed instantly.
            .CommittedOffsetExclusive = IntToOffset(std::max(PersistedOffsetExclusive_.load(), minOffsetInclusive)),
            .MaxOffsetExclusive = IntToOffset(maxOffsetExclusive),
        });
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

TFuture<std::vector<TSource::TRecord>> TSource::DoReadNextBatch(const TMessageBatcherSettingsPtr& settings, TOffset nextOffsetAsKey, std::optional<TOffset> offsetLimitAsKey)
{
    i64 nextOffset = OffsetToInt(nextOffsetAsKey);
    std::optional<i64> offsetLimit = offsetLimitAsKey ? std::optional(OffsetToInt(*offsetLimitAsKey)) : std::nullopt;

    auto dynamicSourcePartitionSpec = GetDynamicPartitionSpec();
    auto [minOffsetInclusive, maxOffsetExclusive] = GetRowIndexRange(dynamicSourcePartitionSpec->Table);

    // If partition is "trimmed" by controller logic, nextOffset jumps to maxOffsetExclusive.
    // And may differ from CurrentOffset_.
    if (nextOffset == maxOffsetExclusive) {
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    if (nextOffset < minOffsetInclusive) {
        ReaderFuture_ = {};
        nextOffset = minOffsetInclusive;
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
    }

    YT_VERIFY(CurrentOffset_ == nextOffset);

    if (!ReaderFuture_.IsSet()) {
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    if (!ReaderFuture_.GetOrCrash().IsOK()) {
        GetReadErrorState()->SetError(ReaderFuture_.GetOrCrash());
        ReaderFuture_ = {};
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    auto reader = ReaderFuture_.GetOrCrash().ValueOrThrow();

    GetReadErrorState()->ClearError();

    i64 maxRowsPerRead = settings->MaxRowsPerBatch;
    if (offsetLimit.has_value()) {
        maxRowsPerRead = std::min(maxRowsPerRead, *offsetLimit - CurrentOffset_);
    }
    maxRowsPerRead = Throttler_->TryAcquireAvailable(maxRowsPerRead);

    if (!maxRowsPerRead) {
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    auto unversionedRowBatch = reader->Read(TRowBatchReadOptions{
        .MaxRowsPerRead = maxRowsPerRead,
        .MaxDataWeightPerRead = settings->MaxBytesPerBatch});

    if (!unversionedRowBatch) {
        auto error = NYT::TError("Got null batch from table reader, but more rows are expected")
            << TErrorAttribute("current_offset", CurrentOffset_)
            << TErrorAttribute("max_offset_exclusive", maxOffsetExclusive);
        GetReadErrorState()->SetError(error);
        ReaderFuture_ = {};
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    if (unversionedRowBatch->IsEmpty()) {
        if (LastNonEmptyBatchRead_ != TInstant::Zero() && TInstant::Now() - LastNonEmptyBatchRead_ > GetDynamicParameters()->ReadTimeout) {
            auto error = NYT::TError(NYT::EErrorCode::Timeout, "Timeout of table reader; receiving empty batches for too long")
                << TErrorAttribute("read_timeout", GetDynamicParameters()->ReadTimeout);
            GetReadErrorState()->SetError(error);
            ReaderFuture_ = {};
        }
        return MakeFuture(std::vector<TSource::TRecord>{});
    }

    auto unversionedRowRange = unversionedRowBatch->MaterializeRows();
    YT_VERIFY(!unversionedRowRange.empty());

    LastNonEmptyBatchRead_ = TInstant::Now();

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
        records.push_back(std::move(record));
    }

    return MakeFuture(std::move(records));
}

bool TSource::IsFinite()
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

i64 TSourceControllerTable::GetNotDistributedRows() const
{
    return RowCount - DistributedRows;
}

std::tuple<i64, TSystemTimestamp, TSystemTimestamp, std::string> TSourceControllerTable::GetOrderingKey() const
{
    return {Era, EventTimestamp, SystemTimestamp, Path.GetPath()};
}

void TSourceControllerTable::SkipRemainingRows()
{
    // Mark all rows as distributed.
    DistributedRows = RowCount;
    // Make every range empty. It is imitation of trimming every partition range to nothing.
    for (auto& [rangeId, range] : DistributingRanges) {
        range.first = range.second;
    }
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

    registrar.Parameter("distributed_rows", &TThis::DistributedRows)
        .Default(0);
    registrar.Parameter("distributing_ranges", &TThis::DistributingRanges)
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
}

void TSourceController::Sync()
{ }

void TSourceController::Commit()
{ }

TSystemTimestamp TSourceController::ExtractTimestamp(
    const INodePtr& node,
    const TTableTimestampLocatorSpecPtr& locator)
{
    auto timestamp = node->Attributes().GetYson(locator->Attribute);
    auto timestampNode = ConvertTo<INodePtr>(timestamp);

    TInstant instant;
    switch (locator->Format) {
        case ETimestampFormat::Iso8601:
            THROW_ERROR_EXCEPTION_UNLESS(timestampNode->GetType() == ENodeType::String, "Expected string for iso8601 timestamp, got %v", timestampNode->GetType());
            if (TInstant::TryParseIso8601(timestampNode->AsString()->GetValue(), instant)) {
                return TSystemTimestamp(instant.Seconds());
            }
            THROW_ERROR_EXCEPTION("Cannot parse timestamp string %Qv as iso8601", timestampNode->AsString()->GetValue());
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
    i64 era)
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

    FilterTables(result, dynamicSourceSpec, lastProcessingTable);
    SortBy(result, [] (const TSourceControllerTablePtr& table) {
        return table->GetOrderingKey();
    });

    return result;
}

void TSourceController::FilterTables(
    std::vector<TSourceControllerTablePtr>& tables,
    const TDynamicTableSourceParametersPtr& dynamicSourceParameters,
    const TSourceControllerTablePtr& lastProcessingTable)
{
    EraseIf(tables, [&] (const TSourceControllerTablePtr& table) {
        if (dynamicSourceParameters->MinEventTimestamp.has_value() && table->EventTimestamp.Underlying() < *dynamicSourceParameters->MinEventTimestamp) {
            return true;
        }
        return table->GetOrderingKey() < lastProcessingTable->GetOrderingKey() || table->RowCount == 0;
    });
}

std::vector<TSourceControllerTablePtr> TSourceController::GetTables(
    const TTableSourceParametersPtr& sourceParameters,
    const TDynamicTableSourceParametersPtr& dynamicSourceParameters,
    i64 era,
    const TSourceControllerTablePtr& lastProcessingTable)
{
    std::vector<TSourceControllerTablePtr> result;

    std::vector<std::pair<TRichYPath, INodePtr>> tablesInfo;
    if (sourceParameters->Tables.has_value()) {
        auto attributes = GetRequiredTableAttributes(sourceParameters);
        for (const auto& table : *sourceParameters->Tables) {
            auto client = CreateClient(GetContext(), table);
            tablesInfo.push_back(ResolveTable(client, table, attributes));
        }
    } else {
        auto client = CreateClient(GetContext(), *sourceParameters->TablesPath);

        TListNodeOptions listOptions;
        listOptions.Attributes = GetRequiredTableAttributes(sourceParameters);
        auto listNode = ConvertToNode(WaitFor(client->ListNode(sourceParameters->TablesPath->GetPath(), listOptions)).ValueOrThrow());

        for (const auto& node : listNode->AsList()->GetChildren()) {
            auto path = *sourceParameters->TablesPath;
            path.SetPath(YPathJoin(path.GetPath(), ConvertTo<std::string>(node)));

            tablesInfo.push_back({path, node});
        }
    }

    return MakeTables(tablesInfo, sourceParameters, dynamicSourceParameters, lastProcessingTable, era);
}

void TSourceController::UpdateControllerState(
    TSourceControllerState* state,
    const std::vector<TSourceControllerTablePtr>& tables,
    const TLogger& publicLogger)
{
    if (state->EraStartInstant == TInstant::Zero()) {
        state->EraStartInstant = TInstant::Now();
    }

    YT_VERIFY(IsSortedBy(tables, [] (const TSourceControllerTablePtr& table) {
        return table->GetOrderingKey();
    }));
    for (auto& table : tables) {
        YT_VERIFY(table->RowCount != 0, Format("Table %Qv is empty", table->Path));
        YT_VERIFY(table->GetOrderingKey() >= state->DistributingTable->GetOrderingKey(),
            Format("Table %Qv has outdated ordering key (Key: %v, ThresholdKey: %v)",
            table->Path,
            table->GetOrderingKey(),
            state->DistributingTable->GetOrderingKey()));
    }

    auto it = tables.begin();

    // Skip/check current.
    if (it != tables.end() && (*it)->GetOrderingKey() == state->DistributingTable->GetOrderingKey()) {
        ++it;
    } else {
        if (state->DistributingTable->GetNotDistributedRows() != 0) {
            YT_TLOG_EVENT_FLUENT(
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
        YT_TLOG_EVENT_FLUENT(
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

void TSourceController::ApplyRestartInstantLogic(
    TSourceControllerState* state,
    TInstant restartInstant,
    const TLogger& publicLogger)
{
    auto now = TInstant::Now();
    if (restartInstant > now) {
        YT_TLOG_EVENT_FLUENT(
            publicLogger,
            ELogLevel::Warning,
            "Misconfiguration: restart instant in dynamic parameters is greater than now")
            .With("RestartInstant", restartInstant)
            .With("Now", now);
    }
    if (restartInstant > state->EraStartInstant) {
        state->DistributingTable->SkipRemainingRows();

        state->Era += 1;
        YT_TLOG_EVENT_FLUENT(
            publicLogger,
            ELogLevel::Warning,
            "Starting new era")
            .With("Era", state->Era)
            .With("LastEraStartInstant", state->EraStartInstant)
            .With("NewEraStartInstant", now);
        state->EraStartInstant = now;
    }
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
            State_->DistributingTable)
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }
    if (TablesFuture_.IsSet()) {
        if (TablesFuture_.GetOrCrash().IsOK()) {
            try {
                ApplyRestartInstantLogic(State_.Get(), GetDynamicParameters()->RestartInstant, GetContext()->PublicLogger);
                CheckDistributionFinished();
                UpdateControllerState(State_.Get(), TablesFuture_.GetOrCrash().ValueOrThrow(), GetContext()->PublicLogger);
                State_->Inited = true;
                CheckDistributingTableErrorState_->ClearError();
            } catch (const std::exception& ex) {
                auto error = TError("Failed to update distributing table") << ex;
                YT_TLOG_EVENT_FLUENT(
                    GetContext()->PublicLogger,
                    ELogLevel::Error,
                    "Failed to update distributing table")
                    .With(error);
                CheckDistributingTableErrorState_->SetError(error);
            }
        } else {
            auto error = TError("Failed to get tables") << TablesFuture_.GetOrCrash();
            YT_TLOG_EVENT_FLUENT(
                GetContext()->PublicLogger,
                ELogLevel::Error,
                "Failed to get tables")
                .With(error);
            CheckDistributingTableErrorState_->SetError(error);
        }
        TablesFuture_ = {};
    }
    return State_->Inited;
}

double TSourceController::GetDesiredRangeRowsPerSecond(
    const TDynamicTableSourceParametersPtr& dynamicParameters,
    const TSourceControllerTablePtr& distributingTable)
{
    double estimatedRowByteSize = std::max(1.0, static_cast<double>(distributingTable->ByteSize) / distributingTable->RowCount);
    return std::min<double>(
        dynamicParameters->DesiredPartitionRowsPerSecond,
        dynamicParameters->DesiredPartitionBytesPerSecond / estimatedRowByteSize);
}

void TSourceController::ProcessPartitionStatuses(const THashMap<TKey, TExtendedSourcePartitionStatusPtr>& statuses)
{
    const auto& distributingTable = State_->DistributingTable;
    for (const auto& [key, status] : statuses) {
        YT_VERIFY(status->PartitionState != EPartitionState::Interrupting);
        auto rangeId = ExtractRangeId(key);
        if (status->PartitionState == EPartitionState::Completed) {
            distributingTable->DistributingRanges.erase(rangeId);
            CommittedOffsetsExclusive_.erase(rangeId);
        } else {
            CommittedOffsetsExclusive_[rangeId] = ConvertTo<TPartitionStatusPtr>(status->PartitionStatus)->CommittedOffsetExclusive;
        }
    }
}

void TSourceController::CheckDistributionFinished()
{
    const auto& distributingTable = State_->DistributingTable;
    if (State_->DistributionFinished) {
        return;
    }
    if (distributingTable->Path.GetPath().empty()) {
        State_->DistributionFinished = true;
    } else if (distributingTable->GetNotDistributedRows() == 0 && distributingTable->DistributingRanges.empty()) {
        State_->DistributionFinished = true;
        State_->ProcessedTables += 1;
        YT_TLOG_EVENT_FLUENT(
            GetContext()->PublicLogger,
            ELogLevel::Info,
            "Table was processed")
            .With("Table", distributingTable->Path);
    }
}

double TSourceController::GetDesiredRowsPerSecond(
    const TDynamicTableSourceParametersPtr& dynamicParameters,
    const TSourceControllerTablePtr& distributingTable)
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

i64 TSourceController::GetRemainingTableRows(
    const TSourceControllerTablePtr& distributingTable,
    const THashMap<TRangeId, i64>& committedOffsetsExclusive)
{
    i64 rows = distributingTable->GetNotDistributedRows();
    for (const auto& [rangeId, range] : distributingTable->DistributingRanges) {
        const auto& [rangeBeginInclusive, rangeEndExclusive] = range;
        i64 localRemainingRows = rangeEndExclusive - std::max<i64>(rangeBeginInclusive, GetOrDefault(committedOffsetsExclusive, rangeId, -1));
        rows += std::max<i64>(0, localRemainingRows);
    }
    return rows;
}

THashMap<TRangeId, IMapNodePtr> TSourceController::DoDistributing(
    const TDynamicTableSourceParametersPtr& dynamicParameters,
    double desiredRangeRowsPerSecond,
    const THashMap<TRangeId, i64>& committedOffsetsExclusive,
    const TSourceControllerTablePtr& distributingTable,
    std::function<TRangeId()> rangeIdGenerator)
{
    THROW_ERROR_EXCEPTION_IF(desiredRangeRowsPerSecond == 0, "Can not do partitioning with zero desired partition rows per second");

    double desiredRowsPerSecond = GetDesiredRowsPerSecond(dynamicParameters, distributingTable);
    // Apply indirect limitation.
    desiredRowsPerSecond = std::min(desiredRowsPerSecond, desiredRangeRowsPerSecond * dynamicParameters->MaxPartitionCount);

    const i64 desiredRangeCount = std::min<i64>(dynamicParameters->MaxPartitionCount, std::ceil(desiredRowsPerSecond / desiredRangeRowsPerSecond));

    // Correct desiredRangeProcessTime if table can be processed faster than dynamicParameters->DesiredPartitionProcessTime.
    // Correction can be made once for one table.
    const auto desiredRangeProcessTimeSeconds = std::min(
        dynamicParameters->DesiredPartitionProcessTime.SecondsFloat(),
        GetRemainingTableRows(distributingTable, committedOffsetsExclusive) / desiredRowsPerSecond);

    auto& distributingRanges = distributingTable->DistributingRanges;

    while (distributingTable->GetNotDistributedRows() > 0 && std::ssize(distributingRanges) < desiredRangeCount) {
        const i64 rows = std::max<i64>(1, desiredRangeRowsPerSecond * desiredRangeProcessTimeSeconds);
        const i64 newDistributedRows = std::min(distributingTable->DistributedRows + rows, distributingTable->RowCount);
        distributingRanges[rangeIdGenerator()] = {distributingTable->DistributedRows, newDistributedRows};
        distributingTable->DistributedRows = newDistributedRows;
    }

    if (distributingRanges.empty()) {
        return {};
    }

    double rangeRowsPerSecond = desiredRowsPerSecond / distributingRanges.size();
    THashMap<TRangeId, IMapNodePtr> result;
    for (const auto& [rangeId, range] : distributingRanges) {
        auto spec = New<TDynamicTableSourcePartitionSpec>();
        spec->Table = distributingTable->Path;
        SetRowIndexRange(spec->Table, range.first, range.second);
        spec->EventTimestamp = distributingTable->EventTimestamp;
        spec->SystemTimestamp = distributingTable->SystemTimestamp;
        spec->RowsPerSecond = std::min(rangeRowsPerSecond, desiredRangeRowsPerSecond);
        result[rangeId] = ConvertTo<IMapNodePtr>(spec);
    }
    return result;
}

void TSourceController::UpdateMetrics()
{
    Metrics_.ProcessedTables.Update(State_->ProcessedTables);
    Metrics_.LostTables.Update(State_->LostTables);
}

std::optional<THashMap<TKey, IMapNodePtr>> TSourceController::ListKeys()
{
    if (!CheckDistributingTable()) {
        return std::nullopt;
    }

    UpdateMetrics();

    const auto& distributingTable = State_->DistributingTable;

    double desiredRangeRowsPerSecond = GetDesiredRangeRowsPerSecond(GetDynamicParameters(), distributingTable);

    auto rangeIdGenerator = [&] {
        return TRangeId(GetContext()->TimeProvider->GenerateSeqNo());
    };
    auto rangeDynamicSourcePartitionSpecs = DoDistributing(
        GetDynamicParameters(),
        desiredRangeRowsPerSecond,
        CommittedOffsetsExclusive_,
        distributingTable,
        rangeIdGenerator);

    THashMap<TKey, IMapNodePtr> result;
    for (const auto& [rangeId, spec] : rangeDynamicSourcePartitionSpecs) {
        result[GenerateRangeKey(rangeId)] = spec;
    }
    return result;
}

std::optional<TStreamTraverseDataPtr> TSourceController::GetFutureKeysStreamTraverseData()
{
    const auto& distributingTable = State_->DistributingTable;
    i64 notDistributedCount = distributingTable->GetNotDistributedRows() + State_->PendingCount;
    bool noFuturePartitions = GetParameters()->Finite && State_->Inited && notDistributedCount == 0;

    auto now = NConcurrency::WaitFor(GetContext()->TimeProvider->GetTimestamp(/*barrier*/ false)).ValueOrThrow();

    bool isIdle = (State_->Inited && notDistributedCount == 0);
    auto eventWatermark = isIdle ? now : distributingTable->EventTimestamp;
    eventWatermark = TSystemTimestamp(eventWatermark.Underlying() - std::min(eventWatermark.Underlying(), GetParameters()->WatermarkDelay.Seconds()));

    auto sourceStream = New<TStreamTraverseData>();
    sourceStream->Epoch = -1; // Will be fixed in universal controller.
    sourceStream->State = noFuturePartitions ? EStreamState::Completed : EStreamState::Drained;
    sourceStream->SystemWatermark = isIdle ? now : distributingTable->SystemTimestamp;
    sourceStream->EventWatermark = eventWatermark;
    auto infightMetrics = sourceStream->InflightMetrics;
    infightMetrics->Count = notDistributedCount;
    double notDistributedRatio = static_cast<double>(distributingTable->GetNotDistributedRows()) / distributingTable->RowCount;
    infightMetrics->ByteSize = notDistributedRatio * distributingTable->ByteSize + State_->PendingBytes;

    return sourceStream;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnector

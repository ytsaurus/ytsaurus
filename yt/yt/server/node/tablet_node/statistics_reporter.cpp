#include "statistics_reporter.h"

#include "bootstrap.h"
#include "config.h"
#include "tablet.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/tablet_node/performance_counters.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/table_client/performance_counters.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/iterator/enumerate.h>
#include <library/cpp/iterator/zip.h>

namespace NYT::NTabletNode {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NTableClient;
using namespace NTracing;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

const auto PerformanceCounterStruct = StructLogicalType({
    {"count", "count", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
    {"rate", "rate", SimpleLogicalType(ESimpleLogicalValueType::Double)},
    {"rate_10m", "rate_10m", SimpleLogicalType(ESimpleLogicalValueType::Double)},
    {"rate_1h", "rate_1h", SimpleLogicalType(ESimpleLogicalValueType::Double)}
}, /*removedFieldStableNames*/ {});

const auto NameTable = TNameTable::FromSchema(TTableSchema({
    {"table_id", EValueType::String, ESortOrder::Ascending},
    {"tablet_id", EValueType::String, ESortOrder::Ascending},
    #define XX(name, Name) {#name, PerformanceCounterStruct},
    ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
    ITERATE_NODE_TABLET_PERFORMANCE_COUNTERS(XX)
    #undef XX
    {"uncompressed_data_size", EValueType::Int64},
    {"compressed_data_size", EValueType::Int64},
}));

const int ColumnCount = NameTable->GetSize();
const int KeyColumnCount = 2;

////////////////////////////////////////////////////////////////////////////////

} // namespace

TStatisticsReporter::TStatisticsReporter(IBootstrap* const bootstrap)
    : Bootstrap_(bootstrap)
    , ActionQueue_(New<TActionQueue>("TabStatReporter"))
    , Executor_(New<TPeriodicExecutor>(
        ActionQueue_->GetInvoker(),
        BIND(&TStatisticsReporter::ProcessStatistics, MakeWeak(this))))
    , Profiler_(TabletNodeProfiler().WithPrefix("/statistics_reporter"))
    , ReportCount_(Profiler_.Counter("/report_count"))
    , ReportErrorCount_(Profiler_.Counter("/report_error_count"))
    , ReportedTabletCount_(Profiler_.Counter("/reported_tablet_count"))
    , ReportTime_(Profiler_.Timer("/report_time"))
{
    Reconfigure(Bootstrap_->GetDynamicConfigManager()->GetConfig());
}

void TStatisticsReporter::Start()
{
    Executor_->Start();
    Started_ = true;
}

void TStatisticsReporter::Reconfigure(const NClusterNode::TClusterNodeDynamicConfigPtr& config)
{
    const auto& statisticsReporterConfig = config->TabletNode->StatisticsReporter;

    bool enableChanged = Enable_ != statisticsReporterConfig->Enable;

    auto guard = Guard(Spinlock_);
    Enable_ = statisticsReporterConfig->Enable;
    MaxTabletsPerTransaction_ = statisticsReporterConfig->MaxTabletsPerTransaction;
    ReportBackoffTime_ = statisticsReporterConfig->ReportBackoffTime;
    TablePath_ = statisticsReporterConfig->TablePath;
    guard.Release();

    Executor_->SetOptions(statisticsReporterConfig->PeriodicOptions);

    if (Started_ && enableChanged) {
        if (Enable_) {
            Executor_->Start();
        } else {
            YT_UNUSED_FUTURE(Executor_->Stop());
        }
    }
}

std::pair<i64, i64> TStatisticsReporter::GetDataSizes(const TTabletSnapshotPtr& tabletSnapshot)
{
    i64 uncompressedDataSize = 0;
    i64 compressedDataSize = 0;

    auto handleStores = [&] <class TContainer> (const TContainer& container) {
        for (const auto& store : container) {
            if (!store->IsDynamic()) {
                uncompressedDataSize += store->GetUncompressedDataSize();
                compressedDataSize += store->GetCompressedDataSize();
            }
        }
    };

    if (tabletSnapshot->PhysicalSchema->IsSorted()) {
        for (const auto& partition : tabletSnapshot->PartitionList) {
            handleStores(partition->Stores);
        }
        handleStores(tabletSnapshot->Eden->Stores);
    } else {
        handleStores(tabletSnapshot->OrderedStores);
    }

    return {uncompressedDataSize, compressedDataSize};
}

TUnversionedRow TStatisticsReporter::MakeUnversionedRow(
    const TTabletSnapshotPtr& tabletSnapshot,
    const TRowBufferPtr& rowBuffer,
    bool keyColumnsOnly)
{
    auto row = rowBuffer->AllocateUnversioned(
        keyColumnsOnly
            ? KeyColumnCount
            : ColumnCount);

    int index = 0;

    auto addValue = [&] (const TUnversionedValue& value) {
        row[index++] = value;
    };

    addValue(rowBuffer->CaptureValue(MakeUnversionedStringValue(ToString(tabletSnapshot->TableId), index)));
    addValue(rowBuffer->CaptureValue(MakeUnversionedStringValue(ToString(tabletSnapshot->TabletId), index)));

    if (keyColumnsOnly) {
        YT_VERIFY(index == KeyColumnCount);
        return row;
    }

    const auto& performanceCounters = tabletSnapshot->PerformanceCounters;

    #define XX(name, Name) \
    performanceCounters->Name.UpdateEma(); \
    addValue(rowBuffer->CaptureValue(MakeUnversionedCompositeValue( \
        BuildYsonStringFluently().BeginList() \
            .Item().Value(performanceCounters->Name.Ema.Count) \
            .Item().Value(performanceCounters->Name.Ema.ImmediateRate) \
            .Item().Value(performanceCounters->Name.Ema.WindowRates[0]) \
            .Item().Value(performanceCounters->Name.Ema.WindowRates[1]) \
        .EndList().AsStringBuf(), \
        index)));
    ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
    ITERATE_NODE_TABLET_PERFORMANCE_COUNTERS(XX)
    #undef XX

    auto [uncompressedDataSize, compressedDataSize] = GetDataSizes(tabletSnapshot);
    addValue(MakeUnversionedInt64Value(uncompressedDataSize, index));
    addValue(MakeUnversionedInt64Value(compressedDataSize, index));

    YT_VERIFY(index == ColumnCount);

    return row;
}

void TStatisticsReporter::WriteRows(
    const TYPath& tablePath,
    TRange<TUnversionedRow> rows,
    TRowBufferPtr&& rowBuffer,
    const std::vector<TTabletSnapshotPtr>& /*tabletSnapshots*/)
{
    int reportedTabletCount = rows.size();

    YT_LOG_DEBUG("Started reporting tablet statistics batch (TabletCount: %v)",
        reportedTabletCount);

    NProfiling::TWallTimer timer;

    auto transaction = WaitFor(Bootstrap_->GetClient()->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();

    transaction->WriteRows(
        tablePath,
        NameTable,
        MakeSharedRange(rows, std::move(rowBuffer)));

    WaitFor(transaction->Commit())
        .ThrowOnError();

    auto elapsedTime = timer.GetElapsedTime();
    ReportedTabletCount_.Increment(reportedTabletCount);
    ReportTime_.Record(elapsedTime);

    YT_LOG_DEBUG("Finished reporting tablet statistics batch (TabletCount: %v, ElapsedTime: %v)",
        reportedTabletCount,
        elapsedTime);
}

TIntrusivePtr<NApi::IUnversionedRowset> TStatisticsReporter::LookupRows(
    const TYPath& tablePath,
    TRange<TUnversionedRow> keys,
    NTableClient::TRowBufferPtr&& rowBuffer)
{
    NApi::TLookupRowsOptions lookupOptions;
    lookupOptions.VersionedReadOptions.ReadMode = EVersionedIOMode::LatestTimestamp;
    lookupOptions.KeepMissingRows = true;

    auto lookupFuture = Bootstrap_->GetClient()->LookupRows(
        tablePath,
        NameTable,
        MakeSharedRange(keys, std::move(rowBuffer)),
        lookupOptions);

    return WaitFor(lookupFuture)
        .ValueOrThrow()
        .Rowset;
}

void TStatisticsReporter::LoadStatistics(
    const TYPath& tablePath,
    TRange<TUnversionedRow> keys,
    NTableClient::TRowBufferPtr&& rowBuffer,
    const std::vector<TTabletSnapshotPtr>& tabletSnapshots)
{
    auto tableStatisticsRowset = LookupRows(
        tablePath,
        keys,
        std::move(rowBuffer));

    const auto& lookupSchema = tableStatisticsRowset->GetSchema();

    auto loadStatistic = [&] (
        std::string name,
        TUnversionedRow row,
        TPerformanceCountersEma* performanceCounter,
        NTabletClient::TTabletId tabletId)
    {
        if (!lookupSchema->FindColumn(name)) {
            THROW_ERROR_EXCEPTION("Table %Qv has no column %Qv", tablePath, name);
        }

        auto timestampColumnIndex = lookupSchema->GetColumnIndexOrThrow("$timestamp:" + name);
        auto measuringTime = TimestampToInstant(row[timestampColumnIndex].Data.Uint64).first;

        auto valueColumnIndex = lookupSchema->GetColumnIndexOrThrow(name);
        auto value = ConvertTo<IListNodePtr>(row[valueColumnIndex]);

        const int CountFieldIndex = 0;
        const int Rate10mFieldIndex = 2;
        const int Rate1hFieldIndex = 3;

        if (!value->FindChildValue<i64>(CountFieldIndex)) {
            THROW_ERROR_EXCEPTION("Column %Qv does not contain value", name)
                << TErrorAttribute("table_path", tablePath)
                << TErrorAttribute("tablet_id", tabletId);
        }

        TEmaCounter<i64> oldCounter(performanceCounter->Ema.WindowDurations);
        oldCounter.Count = value->GetChildValueOrThrow<i64>(CountFieldIndex);
        oldCounter.LastTimestamp = measuringTime;
        oldCounter.WindowRates = TEmaCounterWindowRates<TypicalWindowCount>{
            /*rate_10m*/ value->GetChildValueOrThrow<double>(Rate10mFieldIndex),
            /*rate_1h*/ value->GetChildValueOrThrow<double>(Rate1hFieldIndex),
        };

        performanceCounter->Merge(oldCounter);
    };

    for (auto&& [tabletSnapshot, row]: Zip(tabletSnapshots, tableStatisticsRowset->GetRows())) {
        tabletSnapshot->PerformanceCounters->Initialized = true;
        if (!row) {
            continue;
        }

        auto& performanceCounters = tabletSnapshot->PerformanceCounters;

        #define XX(name, Name) \
            loadStatistic(#name, row, &performanceCounters->Name, tabletSnapshot->TabletId);
        ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
        ITERATE_NODE_TABLET_PERFORMANCE_COUNTERS(XX)
        #undef XX
    }

    YT_LOG_DEBUG("Finished loading tablet statistics batch (TabletCount: %v)",
        tableStatisticsRowset->GetRows().size());
}

THashMap<TTabletId, TTabletSnapshotPtr> TStatisticsReporter::GetLatestTabletSnapshots()
{
    THashMap<TTabletId, TTabletSnapshotPtr> latestTabletSnapshots;
    for (auto&& tabletSnapshot : Bootstrap_->GetTabletSnapshotStore()->GetTabletSnapshots()) {
        THashMap<TTabletId, TTabletSnapshotPtr>::insert_ctx context;
        if (auto it = latestTabletSnapshots.find(tabletSnapshot->TabletId, context)) {
            if (it->second->MountRevision < tabletSnapshot->MountRevision) {
                it->second = std::move(tabletSnapshot);
            }
        } else {
            latestTabletSnapshots.emplace_direct(context, tabletSnapshot->TabletId, std::move(tabletSnapshot));
        }
    }

    return latestTabletSnapshots;
}

void TStatisticsReporter::ProcessStatistics()
{
    auto guard = Guard(Spinlock_);
    if (!Enable_) {
        return;
    }

    auto tablePath = TablePath_;
    i64 maxTabletsPerTransaction = MaxTabletsPerTransaction_;
    auto reportBackoffTime = ReportBackoffTime_;
    guard.Release();

    auto latestTabletSnapshots = GetLatestTabletSnapshots();

    std::vector<TTabletSnapshotPtr> tabletsWithStatistics;
    std::vector<TTabletSnapshotPtr> tabletsWithoutStatistics;
    for (auto&& [_, tabletSnapshot]: latestTabletSnapshots) {
        if (tabletSnapshot->PerformanceCounters->Initialized) {
            tabletsWithStatistics.push_back(tabletSnapshot);
        } else {
            tabletsWithoutStatistics.push_back(tabletSnapshot);
        }
    }

    LoadUninitializedStatistics(
        tabletsWithoutStatistics,
        tablePath,
        maxTabletsPerTransaction,
        reportBackoffTime);

    ReportStatistics(
        tabletsWithStatistics,
        tablePath,
        maxTabletsPerTransaction,
        reportBackoffTime);
}

void TStatisticsReporter::ReportStatistics(
    const std::vector<TTabletSnapshotPtr>& tabletsWithStatistics,
    const NYPath::TYPath& tablePath,
    i64 maxTabletsPerTransaction,
    TDuration reportBackoffTime)
{
    auto context = CreateTraceContextFromCurrent("TabletStatisticsReporter");
    TTraceContextGuard contextGuard(context);
    try {
        YT_LOG_DEBUG("Started reporting tablet statistics");
        DoProcessStatistics(
            tablePath,
            maxTabletsPerTransaction,
            /*keyColumnsOnly*/ false,
            tabletsWithStatistics,
            BIND(&TStatisticsReporter::WriteRows, MakeWeak(this)));
        ReportCount_.Increment();
        YT_LOG_DEBUG("Finished reporting tablet statistics");
    } catch (const std::exception& ex) {
        ReportErrorCount_.Increment();
        YT_LOG_ERROR(ex, "Failed to report tablet statistics");

        TDelayedExecutor::WaitForDuration(reportBackoffTime);
    }
}

void TStatisticsReporter::LoadUninitializedStatistics(
    const std::vector<TTabletSnapshotPtr>& tabletsWithoutStatistics,
    const NYPath::TYPath& tablePath,
    i64 maxTabletsPerTransaction,
    TDuration loadBackoffTime)
{
    auto context = CreateTraceContextFromCurrent("TabletStatisticsReporter");
    TTraceContextGuard contextGuard(context);
    try {
        YT_LOG_DEBUG("Started loading tablet statistics");
        DoProcessStatistics(
            tablePath,
            maxTabletsPerTransaction,
            /*keyColumnsOnly*/ true,
            tabletsWithoutStatistics,
            BIND(&TStatisticsReporter::LoadStatistics, MakeWeak(this)));
        YT_LOG_DEBUG("Finished loading tablet statistics");
    } catch (const std::exception& ex) {
        LoadErrorCount_.Increment();
        YT_LOG_ERROR(ex, "Failed to load tablet statistics");

        TDelayedExecutor::WaitForDuration(loadBackoffTime);
    }
}

void TStatisticsReporter::DoProcessStatistics(
    const TYPath& tablePath,
    i64 maxTabletsPerTransaction,
    bool keyColumnsOnly,
    const std::vector<TTabletSnapshotPtr>& tabletSnapshots,
    TOnRowCallback processRows)
{
    TRowBufferPtr rowBuffer;
    i64 rowsSize;
    TUnversionedRow* rows;

    auto resetRows = [&] (i64 snapshotIndex) {
        rowBuffer = New<TRowBuffer>();
        rowsSize = 0;
        rows = rowBuffer->GetPool()->AllocateUninitialized<TUnversionedRow>(
            std::min(ssize(tabletSnapshots) - snapshotIndex, maxTabletsPerTransaction));
    };
    resetRows(0);

    for (const auto& [snapshotIndex, tabletSnapshot] : Enumerate(tabletSnapshots)) {
        rows[rowsSize++] = MakeUnversionedRow(tabletSnapshot, rowBuffer, keyColumnsOnly);

        if (rowsSize == maxTabletsPerTransaction) {
            processRows(
                tablePath,
                TRange(rows, rowsSize),
                std::move(rowBuffer),
                tabletSnapshots);
            resetRows(snapshotIndex);
        }
    }

    if (rowsSize > 0) {
        processRows(
            tablePath,
            TRange(rows, rowsSize),
            std::move(rowBuffer),
            tabletSnapshots);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

#include "statistics_reporter.h"
#include "bootstrap.h"
#include "tablet.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>
#include <yt/yt/server/lib/tablet_node/performance_counters.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/table_client/performance_counters.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NTabletNode {

using namespace NClusterNode;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NTableClient;
using namespace NTracing;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

const auto PerformanceCounterStruct = StructLogicalType({
    {"count", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
    {"rate", SimpleLogicalType(ESimpleLogicalValueType::Double)},
    {"rate_10m", SimpleLogicalType(ESimpleLogicalValueType::Double)},
    {"rate_1h", SimpleLogicalType(ESimpleLogicalValueType::Double)}
});

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStatisticsReporter::TStatisticsReporter(IBootstrap* const bootstrap)
    : Bootstrap_(bootstrap)
    , ActionQueue_(New<TActionQueue>("TabStatReporter"))
    , Executor_(New<TPeriodicExecutor>(
        ActionQueue_->GetInvoker(),
        BIND(&TStatisticsReporter::ReportStatistics, MakeWeak(this))))
    , Profiler_(TabletNodeProfiler.WithPrefix("/statistics_reporter"))
    , ReportCount_(Profiler_.Counter("report_count"))
    , ReportErrorCount_(Profiler_.Counter("report_error_count"))
    , ReportedTabletCount_(Profiler_.Counter("reported_tablet_count"))
    , ReportTime_(Profiler_.Timer("report_time"))
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
    const TRowBufferPtr& rowBuffer)
{
    const auto& performanceCounters = tabletSnapshot->PerformanceCounters;

    auto row = rowBuffer->AllocateUnversioned(ColumnCount);
    int index = 0;

    auto addValue = [&] (const TUnversionedValue& value) {
        row[index++] = value;
    };

    addValue(rowBuffer->CaptureValue(MakeUnversionedStringValue(ToString(tabletSnapshot->TableId), index)));
    addValue(rowBuffer->CaptureValue(MakeUnversionedStringValue(ToString(tabletSnapshot->TabletId), index)));

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
    TRowBufferPtr&& rowBuffer)
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

void TStatisticsReporter::ReportStatistics()
{
    auto guard = Guard(Spinlock_);
    if (!Enable_) {
        return;
    }

    auto tablePath = TablePath_;
    i64 maxTabletsPerTransaction = MaxTabletsPerTransaction_;
    auto reportBackoffTime = ReportBackoffTime_;
    guard.Release();

    auto context = CreateTraceContextFromCurrent("TabletStatisticsReporter");
    TTraceContextGuard contextGuard(context);
    try {
        YT_LOG_DEBUG("Started reporting tablet statistics");
        DoReportStatistics(tablePath, maxTabletsPerTransaction);
        ReportCount_.Increment();
        YT_LOG_DEBUG("Finished reporting tablet statistics");
    } catch (const std::exception& ex) {
        ReportErrorCount_.Increment();
        YT_LOG_ERROR(ex, "Failed to report tablet statistics");

        TDelayedExecutor::WaitForDuration(reportBackoffTime);
    }
}

void TStatisticsReporter::DoReportStatistics(const TYPath& tablePath, i64 maxTabletsPerTransaction)
{
    THashMap<TTabletId, TTabletSnapshotPtr> tabletSnapshots;
    for (auto&& tabletSnapshot : Bootstrap_->GetTabletSnapshotStore()->GetTabletSnapshots()) {
        THashMap<TTabletId, TTabletSnapshotPtr>::insert_ctx context;
        if (auto it = tabletSnapshots.find(tabletSnapshot->TabletId, context)) {
            if (it->second->MountRevision < tabletSnapshot->MountRevision) {
                it->second = std::move(tabletSnapshot);
            }
        } else {
            tabletSnapshots.emplace_direct(context, tabletSnapshot->TabletId, std::move(tabletSnapshot));
        }
    }

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

    for (const auto& [snapshotIndex, it] : Enumerate(tabletSnapshots)) {
        rows[rowsSize++] = MakeUnversionedRow(it.second, rowBuffer);

        if (rowsSize == maxTabletsPerTransaction) {
            WriteRows(tablePath, MakeRange(rows, rowsSize), std::move(rowBuffer));
            resetRows(snapshotIndex);
        }
    }

    if (rowsSize > 0) {
        WriteRows(tablePath, MakeRange(rows, rowsSize), std::move(rowBuffer));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

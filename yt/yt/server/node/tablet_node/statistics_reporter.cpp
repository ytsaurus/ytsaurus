#include "statistics_reporter.h"
#include "bootstrap.h"
#include "tablet.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/tablet_server/performance_counters.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/table_client/performance_counters.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

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
    #undef XX
    {"uncompressed_data_size", EValueType::Int64},
    {"compressed_data_size", EValueType::Int64},
}));

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStatisticsReporter::TStatisticsReporter(IBootstrap* const bootstrap)
    : Bootstrap_(bootstrap)
    , ActionQueue_(New<TActionQueue>("statistics_reporter"))
    , Executor_(New<TPeriodicExecutor>(
        ActionQueue_->GetInvoker(),
        BIND(&TStatisticsReporter::ReportStatistics, MakeWeak(this))))
{
    Reconfigure(/*oldConfig*/ nullptr, Bootstrap_->GetDynamicConfigManager()->GetConfig());
}

void TStatisticsReporter::Start()
{
    Executor_->Start();
}

void TStatisticsReporter::Reconfigure(
    const NClusterNode::TClusterNodeDynamicConfigPtr& /*oldConfig*/,
    const NClusterNode::TClusterNodeDynamicConfigPtr& newConfig)
{
    const auto& config = newConfig->TabletNode->StatisticsReporter;
    Executor_->SetPeriod(config->Period);

    auto guard = WriterGuard(Spinlock_);
    Enable_ = config->Enable;
    TablePath_ = config->TablePath;
}

void TStatisticsReporter::ReportStatistics()
{
    auto context = CreateTraceContextFromCurrent("TabletStatisticsReporter");
    TTraceContextGuard contextGuard(context);

    try {
        DoReportStatistics();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to report tablet statistics");
    }
}

void TStatisticsReporter::DoReportStatistics()
{
    auto guard = ReaderGuard(Spinlock_);
    if (!Enable_) {
        return;
    }

    TYPath tablePath = TablePath_;
    guard.Release();

    YT_LOG_DEBUG("Started round of reporting tablet statistics");

    NProfiling::TWallTimer timer;

    auto transaction = WaitFor(Bootstrap_->GetClient()->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();

    const auto& tabletSnapshots = Bootstrap_->GetTabletSnapshotStore()->GetTabletSnapshots();

    std::vector<TUnversionedRow> rows;
    rows.reserve(tabletSnapshots.size());
    auto rowBuffer = New<TRowBuffer>();

    for (const auto& tabletSnapshot : tabletSnapshots) {
        auto& performanceCounters = tabletSnapshot->PerformanceCounters;

        #define XX(name, Name) performanceCounters->Name.UpdateEma();
        ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
        #undef XX

        i64 uncompressedDataSize = 0;
        i64 compressedDataSize = 0;

        auto handleStore = [&] (const IStorePtr& store) {
            if (!store->IsDynamic()) {
                uncompressedDataSize += store->GetUncompressedDataSize();
                compressedDataSize += store->GetCompressedDataSize();
            }
        };

        if (tabletSnapshot->PhysicalSchema->IsSorted()) {
            for (const auto& partition : tabletSnapshot->PartitionList) {
                for (const auto& store : partition->Stores) {
                    handleStore(store);
                }
            }
            for (const auto& store : tabletSnapshot->Eden->Stores) {
                handleStore(store);
            }
        } else {
            for (const auto& store : tabletSnapshot->OrderedStores) {
                handleStore(store);
            }
        }

        TUnversionedRowBuilder builder;

        auto tableIdStr = ToString(tabletSnapshot->TableId);
        auto tabletIdStr = ToString(tabletSnapshot->TabletId);
        builder.AddValue(MakeUnversionedStringValue(tableIdStr, NameTable->GetId("table_id")));
        builder.AddValue(MakeUnversionedStringValue(tabletIdStr, NameTable->GetId("tablet_id")));

        #define XX(name, Name) \
        auto yson##Name = BuildYsonStringFluently() \
            .BeginList() \
                .Item().Value(performanceCounters->Name.Ema.Count) \
                .Item().Value(performanceCounters->Name.Ema.ImmediateRate) \
                .Item().Value(performanceCounters->Name.Ema.WindowRates[0]) \
                .Item().Value(performanceCounters->Name.Ema.WindowRates[1]) \
            .EndList(); \
        builder.AddValue(MakeUnversionedCompositeValue(yson##Name.AsStringBuf(), NameTable->GetId(#name)));
        ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
        #undef XX

        builder.AddValue(MakeUnversionedInt64Value(uncompressedDataSize, NameTable->GetId("uncompressed_data_size")));
        builder.AddValue(MakeUnversionedInt64Value(compressedDataSize, NameTable->GetId("compressed_data_size")));

        rows.push_back(rowBuffer->CaptureRow(builder.GetRow()));
    }

    transaction->WriteRows(
        tablePath,
        NameTable,
        MakeSharedRange(std::move(rows), std::move(rowBuffer)));

    // TODO(dave11ar): Profile query rate, duration and errors.
    WaitFor(transaction->Commit()).ThrowOnError();

    YT_LOG_DEBUG("Finished round of reporting tablet statistics (TabletCount: %v, ElapsedTime: %v)",
        tabletSnapshots.size(),
        timer.GetElapsedTime());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

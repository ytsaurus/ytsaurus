#include "statistics_reporter.h"

#include "bootstrap.h"
#include "config.h"
#include "tablet.h"
#include "tablet_snapshot_store.h"

#include <yt/yt/server/lib/tablet_node/performance_counters.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/transaction.h>

#include <yt/yt/ytlib/table_client/performance_counters.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/iterator/enumerate.h>
#include <library/cpp/iterator/zip.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;
using namespace NTableClient;
using namespace NTabletServer;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

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

constexpr int KeyColumnCount = 2;
const int ColumnCount = NameTable->GetSize();

////////////////////////////////////////////////////////////////////////////////

struct TStatisticsReporterRowsBufferTag
{ };

const std::string StatisticsReporterTag = "StatisticsReporter";

////////////////////////////////////////////////////////////////////////////////

TUnversionedRow MakeRowForLookup(TTableId tableId, TTabletId tabletId, const TRowBufferPtr& rowBuffer)
{
    auto row = rowBuffer->AllocateUnversioned(KeyColumnCount);
    row[0] = rowBuffer->CaptureValue(MakeUnversionedStringValue(ToString(tableId), 0));
    row[1] = rowBuffer->CaptureValue(MakeUnversionedStringValue(ToString(tabletId), 1));

    return row;
}

TUnversionedRow MakeRowForWrite(const TTabletSnapshotPtr& tabletSnapshot, const TRowBufferPtr& rowBuffer)
{
    auto row = rowBuffer->AllocateUnversioned(ColumnCount);

    int index = 0;
    auto addValue = [&] (const TUnversionedValue& value) {
        row[index++] = value;
    };

    addValue(rowBuffer->CaptureValue(MakeUnversionedStringValue(ToString(tabletSnapshot->TableId), index)));
    addValue(rowBuffer->CaptureValue(MakeUnversionedStringValue(ToString(tabletSnapshot->TabletId), index)));

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

    addValue(MakeUnversionedInt64Value(tabletSnapshot->TabletSizeMetrics.UncompressedDataSize, index));
    addValue(MakeUnversionedInt64Value(tabletSnapshot->TabletSizeMetrics.CompressedDataSize, index));

    YT_VERIFY(index == ColumnCount);

    return row;
}

////////////////////////////////////////////////////////////////////////////////

class TStatisticsReporterContext
{
private:
    TProfiler Profiler_;

public:
    DEFINE_BYREF_RO_PROPERTY(TEventTimer, IterationTime);
    DEFINE_BYREF_RO_PROPERTY(TCounter, SuccessfulIterationCount);
    DEFINE_BYREF_RO_PROPERTY(TCounter, FailedIterationCount);
    DEFINE_BYREF_RO_PROPERTY(TCounter, ProcessedTabletCount);
    DEFINE_BYREF_RO_PROPERTY(TCounter, FailedTabletCount);

    DEFINE_BYREF_RO_PROPERTY(std::string, TracingSpan);
    DEFINE_BYREF_RO_PROPERTY(std::string, LoggingTag);

    TStatisticsReporterContext(TStringBuf profilingPrefix, std::string loggingTag)
        : Profiler_(TabletNodeProfiler().WithPrefix(profilingPrefix))
        , IterationTime_(Profiler_.Timer("/iteration_time"))
        , SuccessfulIterationCount_(Profiler_.Counter("/successful_iteration_count"))
        , FailedIterationCount_(Profiler_.Counter("/failed_iteration_count"))
        , ProcessedTabletCount_(Profiler_.Counter("/processed_tablet_count"))
        , FailedTabletCount_(Profiler_.Counter("/failed_tablet_count"))
        , TracingSpan_(StatisticsReporterTag + "::" + loggingTag)
        , LoggingTag_(std::move(loggingTag))
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TStatisticsReporter
    : public IStatisticsReporter
{
public:
    explicit TStatisticsReporter(IBootstrap* const bootstrap)
        : Bootstrap_(bootstrap)
        , Logger(TabletNodeLogger().WithTag(StatisticsReporterTag.data()))
        , LoadContext_("/statistics_reporter/load", "Load")
        , ReportContext_("/statistics_reporter/report", "Report")
        , Config_(bootstrap->GetTabletNodeDynamicConfig()->StatisticsReporter)
    { }

    void Start() override
    {
        YT_LOG_DEBUG("Starting tablet statistics reporter");

        auto config = Config_.Acquire();

        Executor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetTabletStatisticsInvoker(),
            BIND(&TStatisticsReporter::DoProcessStatistics, MakeWeak(this)),
            config->PeriodicOptions);

        if (config->Enable) {
            Executor_->Start();
        }
    }

    void Reconfigure(const TTabletNodeDynamicConfigPtr& config) override
    {
        YT_LOG_DEBUG("Reconfiguring tablet statistics reporter");

        const auto& statisticsReporterConfig = config->StatisticsReporter;

        Config_.Store(statisticsReporterConfig);

        // Statistics reported was not started.
        if (!Executor_) {
            return;
        }

        Executor_->SetOptions(statisticsReporterConfig->PeriodicOptions);

        if (statisticsReporterConfig->Enable) {
            Executor_->Start();
        } else {
            YT_UNUSED_FUTURE(Executor_->Stop());
        }
    }

private:
    using TProcessTabletBatch = std::function<void(
        const TStatisticsReporterConfigPtr&,
        TRange<TTabletSnapshotPtr>)>;

    IBootstrap* const Bootstrap_;

    const TLogger Logger;

    const TStatisticsReporterContext LoadContext_;
    const TStatisticsReporterContext ReportContext_;

    TAtomicIntrusivePtr<TStatisticsReporterConfig> Config_;

    TPeriodicExecutorPtr Executor_;

    void DoProcessStatistics()
    {
        YT_ASSERT_INVOKER_AFFINITY(Bootstrap_->GetTabletStatisticsInvoker());

        auto config = Config_.Acquire();
        if (!config->Enable) {
            return;
        }

        YT_LOG_DEBUG("Processing tablet statistics");

        auto tablets = Bootstrap_->GetTabletSnapshotStore()->GetLatestTabletSnapshots();

        std::vector<TTabletSnapshotPtr> uninitializedTablets;
        for (const auto& tablet : tablets) {
            if (!tablet->PerformanceCounters->Initialized) {
                uninitializedTablets.push_back(tablet);
            }
        }

        bool iterationSuccessful = RunContextedIteration(
            config,
            uninitializedTablets,
            LoadContext_,
            std::bind_front(&TStatisticsReporter::LoadStatisticsBatch, this));

        if (iterationSuccessful) {
            iterationSuccessful = RunContextedIteration(
                config,
                tablets,
                ReportContext_,
                std::bind_front(&TStatisticsReporter::ReportStatisticsBatch, this));
        }

        if (!iterationSuccessful) {
            YT_LOG_DEBUG("Failed processing tablet statistics, will wait for duration (Duration: %v)",
                config->ReportBackoffTime);
            TDelayedExecutor::WaitForDuration(config->ReportBackoffTime);

            return;
        }

        YT_LOG_DEBUG("Finished processing tablet statistics");
    }

    bool RunContextedIteration(
        const TStatisticsReporterConfigPtr& config,
        const std::vector<TTabletSnapshotPtr>& tablets,
        const TStatisticsReporterContext& context,
        TProcessTabletBatch processTabletBatch)
    {
        auto traceContext = TTraceContext::NewRoot(context.TracingSpan());
        TTraceContextGuard traceContextGuard(traceContext);

        TWallTimer timer;
        i64 processedTabletCount = 0;

        auto finallyGuard = Finally([&] {
            auto elapsedTime = timer.GetElapsedTime();
            int failedTabletCount = tablets.size() - processedTabletCount;

            if (failedTabletCount > 0) {
                context.FailedIterationCount().Increment();
            } else {
                context.SuccessfulIterationCount().Increment();
            }
            context.IterationTime().Record(timer.GetElapsedTime());
            context.ProcessedTabletCount().Increment(processedTabletCount);
            context.FailedIterationCount().Increment(failedTabletCount);

            YT_LOG_DEBUG("Finished tablet statistics processing iteration "
                "(%v, ElapsedTime: %v, ProcessedTabletCount: %v, FailedTabletCount: %v)",
                context.LoggingTag(),
                elapsedTime,
                processedTabletCount,
                failedTabletCount);
        });

        try {
            YT_LOG_DEBUG("Starting tablet statistics processing iteration (%v)",
                context.LoggingTag());

            for (int batchStartIndex = 0; batchStartIndex < ssize(tablets); batchStartIndex += config->MaxTabletsPerTransaction) {
                int batchSize = std::min<int>(config->MaxTabletsPerTransaction, ssize(tablets) - batchStartIndex);
                TRange<TTabletSnapshotPtr> tabletRange(tablets.begin() + batchStartIndex, batchSize);

                processTabletBatch(config, tabletRange);

                processedTabletCount += batchSize;
            }

            return true;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Tablet statistics processing iteration failed (%v)",
                context.LoggingTag());
            return false;
        }
    }

    void ReportStatisticsBatch(
        const TStatisticsReporterConfigPtr& config,
        TRange<TTabletSnapshotPtr> tablets)
    {
        YT_LOG_DEBUG("Started reporting tablet statistics batch (TabletCount: %v)",
            tablets.Size());

        auto rowBuffer = New<TRowBuffer>(TStatisticsReporterRowsBufferTag());
        auto* rows = rowBuffer->GetPool()->AllocateUninitialized<TUnversionedRow>(tablets.Size());

        for (const auto& [index, tablet] : Enumerate(tablets)) {
            rows[index] = MakeRowForWrite(tablet, rowBuffer);
        }

        auto transaction = WaitFor(Bootstrap_->GetClient()->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();

        transaction->WriteRows(
            config->TablePath,
            NameTable,
            MakeSharedRange(TRange(rows, tablets.Size()), rowBuffer));

        WaitFor(transaction->Commit())
            .ThrowOnError();

        YT_LOG_DEBUG("Finished reporting tablet statistics batch (TabletCount: %v)",
            tablets.Size());
    }

    IUnversionedRowsetPtr LookupStatisticsRowset(
        const TStatisticsReporterConfigPtr& config,
        TRange<TTabletSnapshotPtr> tablets)
    {
        int rowCount = tablets.Size();
        for (const auto& tablet : tablets) {
            rowCount += tablet->OriginatorTablets.size();
        }

        auto rowBuffer = New<TRowBuffer>(TStatisticsReporterRowsBufferTag());
        auto* rows = rowBuffer->GetPool()->AllocateUninitialized<TUnversionedRow>(rowCount);

        int rowIndex = 0;
        for (const auto& tablet : tablets) {
            rows[rowIndex++] = MakeRowForLookup(tablet->TableId, tablet->TabletId, rowBuffer);
            for (const auto& originatorTablet : tablet->OriginatorTablets) {
                rows[rowIndex++] = MakeRowForLookup(tablet->TableId, originatorTablet.TabletId, rowBuffer);
            }
        }

        TLookupRowsOptions lookupOptions;
        lookupOptions.VersionedReadOptions.ReadMode = EVersionedIOMode::LatestTimestamp;
        lookupOptions.KeepMissingRows = true;

        auto lookupFuture = Bootstrap_->GetClient()->LookupRows(
            config->TablePath,
            NameTable,
            MakeSharedRange(TRange(rows, rowCount), rowBuffer),
            lookupOptions);

        return WaitFor(lookupFuture)
            .ValueOrThrow()
            .Rowset;
    }

    template <size_t WindowCount>
    TEmaCounter<i64, WindowCount> LoadEmaCounter(
        const std::string& name,
        TUnversionedRow row,
        const TStatisticsReporterConfigPtr& config,
        const NTableClient::TTableSchemaPtr& schema,
        const TEmaCounterWindowDurations<WindowCount>& windowDurations)
    {
        if (!schema->FindColumn(name)) {
            THROW_ERROR_EXCEPTION("Table %Qv has no column %Qv", config->TablePath, name);
        }

        auto timestampColumnIndex = schema->GetColumnIndexOrThrow(TimestampColumnPrefix + name);
        auto measuringTime = TimestampToInstant(row[timestampColumnIndex].Data.Uint64).first;

        auto valueColumnIndex = schema->GetColumnIndexOrThrow(name);

        auto valueNode = ConvertTo<INodePtr>(row[valueColumnIndex]);
        if (valueNode->GetType() != ENodeType::List) {
            // It is expected behaviour after performance counters table reshard.
            if (valueNode->GetType() == ENodeType::Entity) {
                YT_LOG_DEBUG("Table %Qv column %Qv is empty",
                    config->TablePath,
                    name);

                return TEmaCounter<i64>(windowDurations);
            }

            THROW_ERROR_EXCEPTION("Table %Qv column %Qv is not a list or entity, but %v",
                config->TablePath,
                name,
                valueNode->GetType());
        }

        auto value = valueNode->AsList();

        static constexpr int CountFieldIndex = 0;
        static constexpr int Rate10mFieldIndex = 2;
        static constexpr int Rate1hFieldIndex = 3;

        TEmaCounter<i64> oldCounter(windowDurations);
        oldCounter.Count = value->GetChildValueOrThrow<i64>(CountFieldIndex);
        oldCounter.LastTimestamp = measuringTime;
        oldCounter.WindowRates = TEmaCounterWindowRates<TypicalWindowCount>{
            /*rate_10m*/ value->GetChildValueOrThrow<double>(Rate10mFieldIndex),
            /*rate_1h*/ value->GetChildValueOrThrow<double>(Rate1hFieldIndex),
        };

        return oldCounter;
    };

    template <size_t WindowCount>
    TEmaCounter<i64, WindowCount> CalculateEmaCounterAfterReshard(
        const std::string& name,
        TRange<TUnversionedRow> rows,
        const TStatisticsReporterConfigPtr& config,
        const NTableClient::TTableSchemaPtr& schema,
        const TEmaCounterWindowDurations<WindowCount>& windowDurations,
        const std::vector<TOriginatorTablet>& originatorTablets)
    {
        std::vector<TEmaCounter<i64, WindowCount>> counters;
        counters.reserve(originatorTablets.size());

        for (const auto& [row, originatorTablet] : Zip(rows, originatorTablets)) {
            if (!row) {
                continue;
            }

            auto& counter = counters.emplace_back(LoadEmaCounter(name, row, config, schema, windowDurations));

            counter.Scale(
                originatorTablet.OriginatorCompressedDataSize == 0.0
                    ? 1.0 / ssize(originatorTablets)
                    : static_cast<double>(originatorTablet.InheritedCompressedDataSize) / originatorTablet.OriginatorCompressedDataSize);
        }

        // NB(dave11ar): For correct |Merge|.
        std::sort(
            counters.begin(),
            counters.end(),
            [] (const auto& lhs, const auto& rhs) {
                return lhs.LastTimestamp > rhs.LastTimestamp;
            });

        TEmaCounter<i64> mergedCounter(windowDurations);
        for (const auto& counter : counters) {
            mergedCounter.Merge(counter);
        }

        return mergedCounter;
    }

    void LoadStatisticsBatch(
        const TStatisticsReporterConfigPtr& config,
        TRange<TTabletSnapshotPtr> tablets)
    {
        YT_LOG_DEBUG("Started loading tablet statistics batch (TabletCount: %v)",
            tablets.size());

        auto rowset = LookupStatisticsRowset(config, tablets);
        auto rows = rowset->GetRows();
        int rowIndex = 0;

        const auto& schema = rowset->GetSchema();


        for (const auto& tablet : tablets) {
            const auto& performanceCounters = tablet->PerformanceCounters;

            // Load after move.
            if (rows[rowIndex]) {
                #define XX(name, Name) \
                performanceCounters->Name.Merge( \
                    LoadEmaCounter(#name, rows[rowIndex], config, schema, performanceCounters->Name.Ema.WindowDurations));
                ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
                ITERATE_NODE_TABLET_PERFORMANCE_COUNTERS(XX)
                #undef XX
            } else {
                // Load after reshard.
                auto originatorsBegin = rows.begin() + rowIndex + 1;

                #define XX(name, Name) \
                performanceCounters->Name.Merge( \
                    CalculateEmaCounterAfterReshard( \
                        #name, \
                        TRange(originatorsBegin, tablet->OriginatorTablets.size()), \
                        config, \
                        schema, \
                        performanceCounters->Name.Ema.WindowDurations, \
                        tablet->OriginatorTablets));
                ITERATE_TABLET_PERFORMANCE_COUNTERS(XX)
                ITERATE_NODE_TABLET_PERFORMANCE_COUNTERS(XX)
                #undef XX
            }

            performanceCounters->Initialized = true;
            rowIndex += 1 + tablet->OriginatorTablets.size();
        }

        YT_LOG_DEBUG("Finished loading tablet statistics batch (TabletCount: %v)",
            tablets.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

IStatisticsReporterPtr CreateStatisticsReporter(IBootstrap* const bootstrap)
{
    return New<TStatisticsReporter>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

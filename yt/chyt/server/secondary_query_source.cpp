#include "secondary_query_source.h"

#include "config.h"
#include "helpers.h"
#include "host.h"
#include "query_context.h"
#include "read_plan.h"
#include "subquery_spec.h"
#include "yt_to_ch_block_converter.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>

#include <Interpreters/ExpressionActions.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NLogging;
using namespace NConcurrency;
using namespace NTracing;
using namespace NChunkClient;
using namespace NYTree;
using namespace NStatisticPath;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TSecondaryQuerySource
    : public DB::ISource
{
public:
    TSecondaryQuerySource(
        ISchemalessMultiChunkReaderPtr reader,
        TReadPlanWithFilterPtr readPlan,
        TTraceContextPtr traceContext,
        THost* host,
        TQuerySettingsPtr settings,
        TLogger logger,
        TChunkReaderStatisticsPtr chunkReaderStatistics,
        TCallback<void(const TStatistics&)> statisticsCallback)
        : DB::ISource(
            DeriveHeaderBlockFromReadPlan(readPlan, settings->Composite),
            /*enable_auto_progress*/ false)
        , Reader_(std::move(reader))
        , ReadPlan_(std::move(readPlan))
        , TraceContext_(std::move(traceContext))
        , Host_(host)
        , Settings_(std::move(settings))
        , Logger(std::move(logger))
        , ChunkReaderStatistics_(std::move(chunkReaderStatistics))
        , StatisticsCallback_(std::move(statisticsCallback))
    {
        Initialize();
    }

    DB::String getName() const override
    {
        return "SecondaryQuerySource";
    }

    Status prepare() override
    {
        auto status = DB::ISource::prepare();

        if (status == Status::Finished && !IsFinished_) {
            Finish();
        }

        return status;
    }

    void onUpdatePorts() override
    {
        if (getPort().isFinished()) {
            cancel();
        }
    }

private:
    ISchemalessMultiChunkReaderPtr Reader_;
    const TReadPlanWithFilterPtr ReadPlan_;
    TTraceContextPtr TraceContext_;
    THost* const Host_;
    const TQuerySettingsPtr Settings_;
    const TLogger Logger;

    //! Converters for every step from the read plan.
    //! Every converter converts only additional columns required by corresponding step.
    std::vector<TYTToCHBlockConverter> Converters_;

    TDuration ColumnarConversionCpuTime_;
    TDuration NonColumnarConversionCpuTime_;
    TDuration ConversionSyncWaitTime_;

    TDuration WaitReadyEventTime_;

    TWallTimer IdleTimer_ = TWallTimer(/*start*/ false);

    int ReadCount_ = 0;

    bool IsFinished_ = false;
    TChunkReaderStatisticsPtr ChunkReaderStatistics_;
    TStatistics Statistics_;
    TCallback<void(const TStatistics&)> StatisticsCallback_;

private:
    void Initialize()
    {
        auto nameTable = Reader_->GetNameTable();
        Converters_.reserve(ReadPlan_->Steps.size());
        for (const auto& step : ReadPlan_->Steps) {
            Converters_.emplace_back(step.Columns, nameTable, Settings_->Composite);
        }

        Statistics_.AddSample("/secondary_query_source/step_count"_SP, ReadPlan_->Steps.size());

        IdleTimer_.Start();
    }

    DB::Chunk generate() override
    {
        TCurrentTraceContextGuard guard(TraceContext_);

        TNullTraceContextGuard nullGuard;
        if (Settings_->EnableReaderTracing) {
            nullGuard.Release();
        }

        auto lastIdleDuration = IdleTimer_.GetCurrentDuration();
        IdleTimer_.Stop();
        ++ReadCount_;

        Statistics_.AddSample("/secondary_query_source/idle_time_us"_SP, lastIdleDuration.MicroSeconds());

        TWallTimer totalWallTimer;
        YT_LOG_TRACE("Started reading ClickHouse block");

        i64 readRows = 0;
        DB::Block resultBlock;
        while (resultBlock.rows() == 0) {
            TRowBatchReadOptions options{
                // .MaxRowsPerRead = 100 * 1000,
                // .MaxDataWeightPerRead = 160_MB,
                .Columnar = Settings_->EnableColumnarRead,
            };
            auto batch = Reader_->Read(options);
            if (!batch) {
                return {};
            }
            if (batch->IsEmpty()) {
                NProfiling::TWallTimer wallTimer;
                WaitFor(Reader_->GetReadyEvent())
                    .ThrowOnError();

                auto elapsed = wallTimer.GetElapsedTime();
                WaitReadyEventTime_ += elapsed;
                Statistics_.AddSample("/secondary_query_source/wait_ready_event_time_us"_SP, elapsed.MicroSeconds());

                if (elapsed > TDuration::Seconds(1)) {
                    YT_LOG_DEBUG("Reading took significant time (WallTime: %v)", elapsed);
                }
                continue;
            }

            TBlockWithFilter blockWithFilter(batch->GetRowCount());
            readRows += blockWithFilter.RowCount;

            for (int stepIndex = 0; stepIndex < std::ssize(ReadPlan_->Steps); ++stepIndex) {
                const auto& step = ReadPlan_->Steps[stepIndex];

                auto filterHint = (blockWithFilter.Filter.empty() || blockWithFilter.RowCountAfterFilter == batch->GetRowCount())
                    ? TRange<DB::UInt8>()
                    : TRange(blockWithFilter.Filter.data(), blockWithFilter.Filter.size());

                auto stepBlock = ConvertStepColumns(stepIndex, batch, filterHint);

                for (auto& column : stepBlock) {
                    blockWithFilter.Block.insert(std::move(column));
                }

                if (step.FilterInfo) {
                    step.FilterInfo->Execute(blockWithFilter);
                    if (blockWithFilter.RowCountAfterFilter == 0) {
                        break;
                    }
                }
            }

            // All rows have been filtered out. Abandon current block and retry.
            if (blockWithFilter.RowCountAfterFilter == 0) {
                continue;
            }

            if (ReadPlan_->NeedFilter && blockWithFilter.RowCount != blockWithFilter.RowCountAfterFilter) {
                for (auto& column : blockWithFilter.Block) {
                    column.column = column.column->filter(blockWithFilter.Filter, blockWithFilter.RowCountAfterFilter);
                }
            }

            resultBlock = std::move(blockWithFilter.Block);
        }

        auto totalElapsed = totalWallTimer.GetElapsedTime();
        YT_LOG_TRACE("Finished reading ClickHouse block (WallTime: %v)", totalElapsed);

        // Report the query progress, including rows that were filtered out.
        // The number of bytes read by the reader but later filtered cannot be counted,
        // because this information cannot be directly obtained from RowBatch.
        progress(readRows, resultBlock.bytes());

        Statistics_.AddSample("/secondary_query_source/block_rows"_SP, resultBlock.rows());
        Statistics_.AddSample("/secondary_query_source/block_columns"_SP, resultBlock.columns());
        Statistics_.AddSample("/secondary_query_source/block_bytes"_SP, resultBlock.bytes());

        Statistics_.AddSample("/secondary_query_source/read_impl_us"_SP, totalElapsed.MicroSeconds());

        IdleTimer_.Start();

        if (!resultBlock || isCancelled()) {
            return {};
        }

        return DB::Chunk(resultBlock.getColumns(), resultBlock.rows());
    }

    void Finish()
    {
        TCurrentTraceContextGuard guard(TraceContext_);

        auto lastIdleDuration = IdleTimer_.GetCurrentDuration();
        IdleTimer_.Stop();
        IsFinished_ = true;

        Statistics_.AddSample("/secondary_query_source/idle_time_us"_SP, lastIdleDuration.MicroSeconds());

        if (ChunkReaderStatistics_) {
            Statistics_.AddSample("/secondary_query_source/chunk_reader/data_bytes_read_from_disk"_SP, ChunkReaderStatistics_->DataBytesReadFromDisk);
            Statistics_.AddSample("/secondary_query_source/chunk_reader/data_io_requests"_SP, ChunkReaderStatistics_->DataIORequests);
            Statistics_.AddSample("/secondary_query_source/chunk_reader/data_bytes_transmitted"_SP, ChunkReaderStatistics_->DataBytesTransmitted);
            Statistics_.AddSample("/secondary_query_source/chunk_reader/data_bytes_read_from_cache"_SP, ChunkReaderStatistics_->DataBytesReadFromCache);
            Statistics_.AddSample("/secondary_query_source/chunk_reader/meta_bytes_read_from_disk"_SP, ChunkReaderStatistics_->MetaBytesReadFromDisk);
        }

        if (StatisticsCallback_) {
            StatisticsCallback_(Statistics_);
        }

        YT_LOG_DEBUG(
            "Secondary query source timing statistics (ColumnarConversionCpuTime: %v, NonColumnarConversionCpuTime: %v, "
            "ConversionSyncWaitTime: %v, IdleTime: %v, ReadCount: %v)",
            ColumnarConversionCpuTime_,
            NonColumnarConversionCpuTime_,
            ConversionSyncWaitTime_,
            IdleTimer_.GetElapsedTime(),
            ReadCount_);

        if (TraceContext_ && TraceContext_->IsRecorded()) {
            TraceContext_->AddTag("chyt.reader.data_statistics", Reader_->GetDataStatistics());
            TraceContext_->AddTag("chyt.reader.codec_statistics", Reader_->GetDecompressionStatistics());
            TraceContext_->AddTag("chyt.reader.timing_statistics", Reader_->GetTimingStatistics());
            TraceContext_->AddTag("chyt.reader.idle_time", IdleTimer_.GetElapsedTime());
            if (ColumnarConversionCpuTime_ != TDuration::Zero()) {
                TraceContext_->AddTag("chyt.reader.columnar_conversion_cpu_time", ColumnarConversionCpuTime_);
            }
            if (NonColumnarConversionCpuTime_ != TDuration::Zero()) {
                TraceContext_->AddTag("chyt.reader.non_columnar_conversion_cpu_time", NonColumnarConversionCpuTime_);
            }
            if (ConversionSyncWaitTime_ != TDuration::Zero()) {
                TraceContext_->AddTag("chyt.reader.conversion_sync_wait_time", ConversionSyncWaitTime_);
            }
            // TODO(dakovalkov): YT-14032
            // Delete this statistics when GetTimingStatistics() works properly for TSchemalessMergingMultiChunkReader.
            if (WaitReadyEventTime_ != TDuration::Zero()) {
                TraceContext_->AddTag("chyt.reader.wait_ready_event_time", WaitReadyEventTime_);
            }
            TraceContext_->Finish();
        }
    }

    DB::Block ConvertStepColumns(
        int stepIndex,
        const IUnversionedRowBatchPtr& batch,
        TRange<DB::UInt8> filterHint)
    {
        auto statisticsPrefix = SlashedStatisticPath(Format("/secondary_query_source/steps/%v", stepIndex)).ValueOrThrow();

        DB::Block block;
        if (Settings_->ConvertRowBatchesInWorkerThreadPool) {
            auto start = TInstant::Now();
            block = WaitFor(BIND(
                    &TSecondaryQuerySource::DoConvertStepColumns,
                    this,
                    stepIndex,
                    batch,
                    filterHint)
                .AsyncVia(Host_->GetClickHouseWorkerInvoker())
                .Run())
                .ValueOrThrow();

            auto elapsed = TInstant::Now() - start;
            ConversionSyncWaitTime_ += elapsed;
            Statistics_.AddSample("/secondary_query_source/conversion_sync_wait_time_us"_SP, elapsed.MicroSeconds());
        } else {
            block = DoConvertStepColumns(stepIndex, batch, filterHint);
        }

        Statistics_.AddSample(statisticsPrefix / "block_rows"_L, block.rows());
        Statistics_.AddSample(statisticsPrefix / "block_columns"_L, block.columns());
        Statistics_.AddSample(statisticsPrefix / "block_bytes"_L, block.bytes());

        return block;
    }

    DB::Block DoConvertStepColumns(
        int stepIndex,
        const IUnversionedRowBatchPtr& batch,
        TRange<DB::UInt8> filterHint)
    {
        bool isColumnarBatch = static_cast<bool>(batch->TryAsColumnar());

        TWallTimer timer;
        auto block = Converters_[stepIndex].Convert(batch, filterHint);

        timer.Stop();
        if (isColumnarBatch) {
            ColumnarConversionCpuTime_ += timer.GetElapsedTime();
            Statistics_.AddSample("/secondary_query_source/columnar_conversion_cpu_time_us"_SP, timer.GetElapsedTime().MicroSeconds());
        } else {
            NonColumnarConversionCpuTime_ += timer.GetElapsedTime();
            Statistics_.AddSample("/secondary_query_source/non_columnar_conversion_cpu_time_us"_SP, timer.GetElapsedTime().MicroSeconds());
        }

        return block;
    }
};

////////////////////////////////////////////////////////////////////////////////

TClientChunkReadOptions CreateChunkReadOptions(const TString& user, IGranuleFilterPtr granuleFilter)
{
    TClientChunkReadOptions chunkReadOptions;
    chunkReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserRealtime);
    chunkReadOptions.WorkloadDescriptor.CompressionFairShareTag = user;
    chunkReadOptions.ReadSessionId = NChunkClient::TReadSessionId::Create();
    chunkReadOptions.GranuleFilter = std::move(granuleFilter);
    return chunkReadOptions;
}

ISchemalessMultiChunkReaderPtr CreateSourceReader(
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    TReadPlanWithFilterPtr readPlan,
    const TClientChunkReadOptions& chunkReadOptions,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors)
{
    auto* queryContext = storageContext->QueryContext;

    auto readerMemoryManager = queryContext->Host->GetMultiReaderMemoryManager()->CreateMultiReaderMemoryManager(
        queryContext->Host->GetConfig()->ReaderMemoryRequirement,
        {{"user", queryContext->User}});

    auto tableReaderConfig = CloneYsonStruct(storageContext->Settings->TableReader);
    tableReaderConfig->SamplingMode = subquerySpec.TableReaderConfig->SamplingMode;
    tableReaderConfig->SamplingRate = subquerySpec.TableReaderConfig->SamplingRate;
    tableReaderConfig->SamplingSeed = subquerySpec.TableReaderConfig->SamplingSeed;

    auto chunkReaderHost = TChunkReaderHost::FromClient(queryContext->Client());

    THashMap<int, std::vector<TDataSliceDescriptor>> dataSourceIdToSliceDescriptors;

    for (const auto& descriptor : dataSliceDescriptors) {
        auto& allDataSourceDescriptors = dataSourceIdToSliceDescriptors[descriptor.GetDataSourceIndex()];

        // For SchemalessMergingMultiChunkReader all data source's chunk specs
        // should be stored in single descriptor.
        const auto& dataSource = subquerySpec.DataSourceDirectory->DataSources()[descriptor.GetDataSourceIndex()];
        if (dataSource.GetType() == EDataSourceType::VersionedTable) {
            if (allDataSourceDescriptors.empty()) {
                allDataSourceDescriptors.emplace_back(descriptor);
            } else {
                for (const auto& chunkSpec : descriptor.ChunkSpecs) {
                    allDataSourceDescriptors.front().ChunkSpecs.emplace_back(chunkSpec);
                }
            }

        } else {
            allDataSourceDescriptors.emplace_back(descriptor);
        }
    }

    std::vector<TDataSliceDescriptor> newDataSliceDescriptors;
    for (auto& [_, descriptors] : dataSourceIdToSliceDescriptors) {
        for (auto& descriptor : descriptors) {
            newDataSliceDescriptors.emplace_back(std::move(descriptor));
        }
    }

    auto nameTable = New<TNameTable>();
    for (const auto& step : readPlan->Steps) {
        for (const auto& column : step.Columns) {
            nameTable->RegisterNameOrThrow(column.Name());
        }
    }

    return CreateSchemalessParallelMultiReader(
        std::move(tableReaderConfig),
        New<TTableReaderOptions>(),
        std::move(chunkReaderHost),
        subquerySpec.DataSourceDirectory,
        newDataSliceDescriptors,
        std::nullopt,
        nameTable,
        chunkReadOptions,
        TReaderInterruptionOptions::InterruptibleWithEmptyKey(),
        TColumnFilter(nameTable->GetSize()),
        /*partitionTag*/ std::nullopt,
        /*multiReaderMemoryManager*/ readerMemoryManager);
}

////////////////////////////////////////////////////////////////////////////////

DB::SourcePtr CreateSecondaryQuerySource(
    ISchemalessMultiChunkReaderPtr reader,
    TReadPlanWithFilterPtr readPlan,
    TTraceContextPtr traceContext,
    THost* host,
    TQuerySettingsPtr querySettings,
    TLogger logger,
    TChunkReaderStatisticsPtr chunkReaderStatistics,
    TCallback<void(const TStatistics&)> statisticsCallback)
{
    return std::make_shared<TSecondaryQuerySource>(
        std::move(reader),
        std::move(readPlan),
        std::move(traceContext),
        host,
        std::move(querySettings),
        std::move(logger),
        std::move(chunkReaderStatistics),
        std::move(statisticsCallback));
}

////////////////////////////////////////////////////////////////////////////////

DB::SourcePtr CreateSecondaryQuerySource(
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    TReadPlanWithFilterPtr readPlan,
    const TTraceContextPtr& traceContext,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    IGranuleFilterPtr granuleFilter,
    TCallback<void(const TStatistics&)> statisticsCallback)
{
    auto* queryContext = storageContext->QueryContext;

    TTraceContextPtr sourceTraceContext;
    if (traceContext) {
        sourceTraceContext = traceContext->CreateChild("ClickHouseYt.SecondaryQuerySource");
    }

    TCurrentTraceContextGuard guard(sourceTraceContext);
    // Readers capture context implicitly, so create NullTraceContextGuard if tracing is disabled.
    TNullTraceContextGuard nullGuard;
    if (storageContext->Settings->EnableReaderTracing) {
        nullGuard.Release();
    }

    auto chunkReadOptions = CreateChunkReadOptions(queryContext->User, std::move(granuleFilter));
    auto reader = CreateSourceReader(
        storageContext,
        subquerySpec,
        readPlan,
        chunkReadOptions,
        dataSliceDescriptors);

    TLogger Logger(queryContext->Logger);
    if (auto breakpointFilename = queryContext->Settings->Testing->InputStreamFactoryBreakpoint) {
        HandleBreakpoint(*breakpointFilename, queryContext->Client());
        YT_LOG_DEBUG("Input stream factory handled breakpoint (Breakpoint: %v)", *breakpointFilename);
    }

    return CreateSecondaryQuerySource(
        std::move(reader),
        std::move(readPlan),
        sourceTraceContext,
        queryContext->Host,
        storageContext->Settings,
        queryContext->Logger.WithTag("ReadSessionId: %v", chunkReadOptions.ReadSessionId),
        chunkReadOptions.ChunkReaderStatistics,
        std::move(statisticsCallback));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

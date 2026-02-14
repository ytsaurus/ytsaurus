#include "secondary_query_source.h"

#include "config.h"
#include "columnar_conversion.h"
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
#include <yt/yt/client/table_client/helpers.h>

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

namespace {

TTableReaderConfigPtr MergeTableReaderConfigs(const TTableReaderConfigPtr& storageConfig, const TTableReaderConfigPtr& subqueryConfig)
{
    auto config = CloneYsonStruct(storageConfig);
    config->SamplingMode = subqueryConfig->SamplingMode;
    config->SamplingRate = subqueryConfig->SamplingRate;
    config->SamplingSeed = subqueryConfig->SamplingSeed;
    return config;
}

TNameTablePtr GetPlanNameTable(TReadPlanWithFilterPtr readPlan)
{
    auto nameTable = New<TNameTable>();
    for (const auto& step : readPlan->Steps) {
        for (const auto& column : step.Columns) {
            nameTable->RegisterNameOrThrow(column.Name());
        }
    }
    return nameTable;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TReaderFactory
{
public:
    TReaderFactory(
        TStorageContext* storageContext,
        const TSubquerySpec& subquerySpec,
        TNameTablePtr nameTable,
        const TClientChunkReadOptions& chunkReadOptions)
        : QueryContext_(storageContext->QueryContext)
        , DataSourceDirectory_(subquerySpec.DataSourceDirectory)
        , ChunkReaderHost_(New<TMultiChunkReaderHost>(
            New<TChunkReaderHost>(storageContext->QueryContext->Client())))
        , ChunkReadOptions_(chunkReadOptions)
        , TableReaderConfig_(MergeTableReaderConfigs(
            storageContext->Settings->TableReader,
            subquerySpec.TableReaderConfig))
        , NameTable_(std::move(nameTable))
        , RootReaderMemoryManager_(QueryContext_->Host->GetMultiReaderMemoryManager())
        , GetNextTaskCallback_(QueryContext_->GetOperandReadTaskCallback(subquerySpec.TableIndex))
    { }

    TReaderFactory(
        TStorageContext* storageContext,
        const TSubquerySpec& subquerySpec,
        TNameTablePtr nameTable,
        const TClientChunkReadOptions& chunkReadOptions,
        const TSecondaryQueryReadDescriptors& initialTask)
        : TReaderFactory(storageContext, subquerySpec, std::move(nameTable), chunkReadOptions)
    {
        if (!initialTask.empty()) {
            InitialReader_ = CreateReader(initialTask);
        }
    }

    ISchemalessMultiChunkReaderPtr CreateReader()
    {
        if (InitialReader_) {
            ISchemalessMultiChunkReaderPtr reader;
            InitialReader_.Swap(reader);
            return std::move(reader);
        }

        if (!GetNextTaskCallback_) {
            return nullptr;
        }

        auto nextTask = WaitFor(GetNextTaskCallback_.Run()).ValueOrThrow();
        return nextTask.empty() ? nullptr : CreateReader(nextTask);
    }

    struct TReadingStatistics
    {
        i64 TotalRowCount = 0;
        i64 TotalDataWeight = 0;
    };

    TReadingStatistics GetAndResetStatistics()
    {
        i64 totalRowCount = 0;
        i64 totalDataWeight = 0;
        std::swap(TotalRowCount_, totalRowCount);
        std::swap(TotalDataWeight_, totalDataWeight);
        return {.TotalRowCount = totalRowCount, .TotalDataWeight = totalDataWeight};
    }

private:
    const TQueryContext* QueryContext_;
    const NChunkClient::TDataSourceDirectoryPtr DataSourceDirectory_;
    const TMultiChunkReaderHostPtr ChunkReaderHost_;
    const TClientChunkReadOptions ChunkReadOptions_;
    const TTableReaderConfigPtr TableReaderConfig_;
    const TNameTablePtr NameTable_;
    const NChunkClient::IMultiReaderMemoryManagerPtr RootReaderMemoryManager_;
    const TCallback<TFuture<TSecondaryQueryReadDescriptors>()> GetNextTaskCallback_;
    ISchemalessMultiChunkReaderPtr InitialReader_;

    i64 TotalRowCount_ = 0;
    i64 TotalDataWeight_ = 0;

    ISchemalessMultiChunkReaderPtr CreateReader(const std::vector<TDataSliceDescriptor>& dataSliceDescriptors)
    {
        for (const auto& dataSliceDescriptor : dataSliceDescriptors) {
            for (const auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
                TotalRowCount_ += chunkSpec.row_count_override();
                TotalDataWeight_ += chunkSpec.data_weight_override();
            }
        }

        auto readerMemoryManager = RootReaderMemoryManager_->CreateMultiReaderMemoryManager(
            QueryContext_->Host->GetConfig()->ReaderMemoryRequirement,
            {{"user", QueryContext_->User}});

        return CreateSchemalessParallelMultiReader(
            TableReaderConfig_,
            New<TTableReaderOptions>(),
            ChunkReaderHost_,
            DataSourceDirectory_,
            AdjustDataSlices(dataSliceDescriptors),
            std::nullopt,
            NameTable_,
            ChunkReadOptions_,
            TReaderInterruptionOptions::InterruptibleWithEmptyKey(),
            TColumnFilter(NameTable_->GetSize()),
            /*partitionTags*/ {},
            std::move(readerMemoryManager));
    }

    std::vector<TDataSliceDescriptor> AdjustDataSlices(std::vector<TDataSliceDescriptor> dataSliceDescriptors)
    {
        THashMap<int, std::vector<TDataSliceDescriptor>> dataSourceIdToSliceDescriptors;
        for (auto& descriptor : dataSliceDescriptors) {
            auto& allDataSourceDescriptors = dataSourceIdToSliceDescriptors[descriptor.GetDataSourceIndex()];

            // For SchemalessMergingMultiChunkReader all data source's chunk specs
            // should be stored in single descriptor.
            const auto& dataSource = DataSourceDirectory_->DataSources()[descriptor.GetDataSourceIndex()];
            if (dataSource->GetType() == EDataSourceType::VersionedTable) {
                if (allDataSourceDescriptors.empty()) {
                    allDataSourceDescriptors.emplace_back(std::move(descriptor));
                } else {
                    for (const auto& chunkSpec : descriptor.ChunkSpecs) {
                        allDataSourceDescriptors.front().ChunkSpecs.emplace_back(std::move(chunkSpec));
                    }
                    descriptor.ChunkSpecs.clear();
                }

            } else {
                allDataSourceDescriptors.emplace_back(std::move(descriptor));
            }
        }

        std::vector<TDataSliceDescriptor> newDataSliceDescriptors;
        for (auto& [_, descriptors] : dataSourceIdToSliceDescriptors) {
            for (auto& descriptor : descriptors) {
                newDataSliceDescriptors.emplace_back(std::move(descriptor));
            }
        }

        return newDataSliceDescriptors;
    }
};

using TReaderFactoryPtr = std::unique_ptr<TReaderFactory>;

////////////////////////////////////////////////////////////////////////////////

class TSecondaryQuerySourceBase
    : public DB::ISource
{
public:
    TSecondaryQuerySourceBase(
        TReadPlanWithFilterPtr readPlan,
        TTraceContextPtr traceContext,
        THost* host,
        TQuerySettingsPtr settings,
        TLogger logger,
        TCallback<void(const TStatistics&)> statisticsCallback)
        : DB::ISource(
            DeriveHeaderBlockFromReadPlan(readPlan, settings->Composite),
            /*enable_auto_progress*/ false)
        , ReadPlan_(std::move(readPlan))
        , TraceContext_(std::move(traceContext))
        , Host_(host)
        , Settings_(std::move(settings))
        , Logger(std::move(logger))
        , StatisticsCallback_(std::move(statisticsCallback))
        , NameTable_(GetPlanNameTable(ReadPlan_))
    {
        Initialize();
    }

    virtual bool CanReadBatch() const = 0;

    virtual IUnversionedRowBatchPtr ReadBatch() = 0;

    virtual TFuture<void> GetReadyEvent() const = 0;


    Status prepare() override
    {
        auto status = DB::ISource::prepare();

        if (status == Status::Finished && !Finished_) {
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

    void setStorageLimits(const std::shared_ptr<const DB::StorageLimitsList>& storageLimits) override
    {
        if (!Settings_->Execution->DisableReadTimeEstimation || !storageLimits) {
            DB::ISource::setStorageLimits(storageLimits);
            return;
        }

        auto overridedLimits = std::make_shared<DB::StorageLimitsList>();
        *overridedLimits = *storageLimits;
        for (auto& limit : *overridedLimits) {
            // To avoid false errors, we explicitly remove the limits.
            limit.local_limits.speed_limits.max_estimated_execution_time = 0;
        }
        DB::ISource::setStorageLimits(overridedLimits);
    }

    ~TSecondaryQuerySourceBase()
    {
        if (Finished_) {
            return;
        }
        Finish();
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
        while (CanReadBatch() && resultBlock.rows() == 0) {
            YT_LOG_TRACE("Started reading loop iteration");
            auto batch = ReadBatch();
            if (!batch) {
                continue;
            }
            if (batch->IsEmpty()) {
                NProfiling::TWallTimer wallTimer;
                WaitFor(GetReadyEvent())
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

                bool filterEmpty = blockWithFilter.Filter.empty() || blockWithFilter.RowCountAfterFilter == batch->GetRowCount();
                if (stepIndex == std::ssize(ReadPlan_->Steps) - 1 && Settings_->Execution->EnableOptimizeDistinctRead && !filterEmpty) {
                    YT_VERIFY(step.Columns.size() == 1);
                    if (auto columnarBatch = batch->TryAsColumnar()) {
                        auto batchColumns = columnarBatch->MaterializeColumns();
                        for (const auto* ytColumn : batchColumns) {
                            if (ytColumn->Id == NameTable_->GetIdOrThrow(step.Columns[0].Name())) {
                                ReduceFilterToDistinct(blockWithFilter.Filter, *ytColumn);
                            }
                        }
                    }
                }

                auto filterHint = filterEmpty ? TRange<DB::UInt8>() : TRange(blockWithFilter.Filter.data(), blockWithFilter.Filter.size());

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
            for (const auto& step : ReadPlan_->Steps) {
                if (step.FilterInfo) {
                    // We can't remove filter column from block while running execute,
                    // because block must contain at least one column to preserve row count.
                    step.FilterInfo->RemoveColumnIfNeeded(blockWithFilter);
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

        if (auto breakpointFilename = Settings_->Testing->SourceGenerateCallBreakpoint) {
            HandleBreakpoint(*breakpointFilename, Host_->GetRootClient());
            YT_LOG_DEBUG("Secondary query source generate call handled breakpoint (Breakpoint: %v)", *breakpointFilename);
        }

        auto totalElapsed = totalWallTimer.GetElapsedTime();
        if (totalElapsed > TDuration::Seconds(5)) {
            YT_LOG_DEBUG("Generate call took significant time (WallTime: %v)", totalElapsed);
        }

        TotalGenerateTime_ += totalElapsed;
        YT_LOG_TRACE("Finished reading ClickHouse block (WallTime: %v)", totalElapsed);

        // Report the query progress, including rows that were filtered out.
        // The number of bytes read by the reader but later filtered cannot be counted,
        // because this information cannot be directly obtained from RowBatch.
        progress(readRows, resultBlock.bytes());

        Statistics_.AddSample("/secondary_query_source/total_rows"_SP, readRows);
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

protected:
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
    TDuration TotalGenerateTime_;
    TDuration WaitReadyEventTime_;

    int ReadCount_ = 0;
    int ReadersProcessed_ = 0;

    TWallTimer IdleTimer_ = TWallTimer(/*start*/ false);
    bool Finished_ = false;

    NChunkClient::NProto::TDataStatistics DataStatistics_;
    TCodecStatistics CodecStatistics_;
    TTimingStatistics TimingStatistics_;

    TStatistics Statistics_;
    TCallback<void(const TStatistics&)> StatisticsCallback_;
    TNameTablePtr NameTable_;

    void Initialize()
    {
        Converters_.reserve(ReadPlan_->Steps.size());
        for (int i = 0; i < std::ssize(ReadPlan_->Steps); ++i) {
            bool enableOptimizeDistinctRead = (i == std::ssize(ReadPlan_->Steps) - 1) ? Settings_->Execution->EnableOptimizeDistinctRead : false;
            Converters_.emplace_back(ReadPlan_->Steps[i].Columns, ReadPlan_->Steps[i].ColumnAttributes, NameTable_, Settings_->Composite, enableOptimizeDistinctRead);
        }

        Statistics_.AddSample("/secondary_query_source/step_count"_SP, ReadPlan_->Steps.size());

        YT_LOG_DEBUG("Secondary query source was initialized");

        IdleTimer_.Start();
    }

    virtual void Finish()
    {
        TCurrentTraceContextGuard guard(TraceContext_);

        auto lastIdleDuration = IdleTimer_.GetCurrentDuration();
        IdleTimer_.Stop();
        Finished_ = true;

        Statistics_.AddSample("/secondary_query_source/idle_time_us"_SP, lastIdleDuration.MicroSeconds());

        if (StatisticsCallback_) {
            StatisticsCallback_(Statistics_);
        }

        YT_LOG_DEBUG(
            "Secondary query source timing statistics (TotalGenerateTime: %v, ColumnarConversionCpuTime: %v, NonColumnarConversionCpuTime: %v, "
            "ConversionSyncWaitTime: %v, IdleTime: %v, ReadCount: %v)",
            TotalGenerateTime_,
            ColumnarConversionCpuTime_,
            NonColumnarConversionCpuTime_,
            ConversionSyncWaitTime_,
            IdleTimer_.GetElapsedTime(),
            ReadCount_);

        if (TraceContext_ && TraceContext_->IsRecorded()) {
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
                    &TSecondaryQuerySourceBase::DoConvertStepColumns,
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

class TSecondaryQuerySource
    : public TSecondaryQuerySourceBase
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
        TCallback<void(const TStatistics&)> statisticsCallback,
        TReaderFactoryPtr readerFactory = nullptr)
        : TSecondaryQuerySourceBase(
            readPlan,
            traceContext,
            host,
            settings,
            logger,
            statisticsCallback)
        , ReaderFactory_(std::move(readerFactory))
        , CurrentReader_(std::move(reader))
        , ChunkReaderStatistics_(std::move(chunkReaderStatistics))
    {
        if (!CurrentReader_) {
            OnReaderAbsence();
        }
    }

    DB::String getName() const override
    {
        std::string name = "SecondaryQuerySource";
        if (Settings_->Execution->EnableOptimizeDistinctRead) {
            name += " (Distinct values optimized)";
        }
        return name;
    }

    bool CanReadBatch() const override
    {
        return CurrentReader_ != nullptr;
    }

    IUnversionedRowBatchPtr ReadBatch() override
    {
        auto batch = CurrentReader_->Read(Options_);
        if (!batch) {
            OnReaderFinish();
        }
        return batch;
    }

    TFuture<void> GetReadyEvent() const override
    {
        return CurrentReader_->GetReadyEvent();
    }

private:
    const TReaderFactoryPtr ReaderFactory_;
    ISchemalessMultiChunkReaderPtr CurrentReader_;
    TChunkReaderStatisticsPtr ChunkReaderStatistics_;

    const TRowBatchReadOptions Options_{
            .Columnar = Settings_->EnableColumnarRead,
    };

    void OnReaderFinish()
    {
        DataStatistics_ += CurrentReader_->GetDataStatistics();
        CodecStatistics_ += CurrentReader_->GetDecompressionStatistics();
        TimingStatistics_ += CurrentReader_->GetTimingStatistics();
        ++ReadersProcessed_;

        CurrentReader_ = nullptr;
        OnReaderAbsence();
    }

    void OnReaderAbsence()
    {
        if (!ReaderFactory_) {
            return;
        }

        CurrentReader_ = ReaderFactory_->CreateReader();
        auto stats = ReaderFactory_->GetAndResetStatistics();
        addTotalRowsApprox(stats.TotalRowCount);
        addTotalBytes(stats.TotalDataWeight);
    }

    void Finish() override
    {
        Statistics_.AddSample("/secondary_query_source/read_count"_SP, ReadCount_);
        Statistics_.AddSample("/secondary_query_source/processed_reader_count"_SP, ReadersProcessed_);
        {
            TCurrentTraceContextGuard guard(TraceContext_);
            if (TraceContext_ && TraceContext_->IsRecorded()) {
                TraceContext_->AddTag("chyt.reader.data_statistics", DataStatistics_);
                TraceContext_->AddTag("chyt.reader.codec_statistics", CodecStatistics_);
                TraceContext_->AddTag("chyt.reader.timing_statistics", TimingStatistics_);
            }

            if (ChunkReaderStatistics_) {
                Statistics_.AddSample("/secondary_query_source/chunk_reader/data_bytes_read_from_disk"_SP, ChunkReaderStatistics_->DataBytesReadFromDisk);
                Statistics_.AddSample("/secondary_query_source/chunk_reader/data_io_requests"_SP, ChunkReaderStatistics_->DataIORequests);
                Statistics_.AddSample("/secondary_query_source/chunk_reader/data_bytes_transmitted"_SP, ChunkReaderStatistics_->DataBytesTransmitted);
                Statistics_.AddSample("/secondary_query_source/chunk_reader/data_bytes_read_from_cache"_SP, ChunkReaderStatistics_->DataBytesReadFromCache);
                Statistics_.AddSample("/secondary_query_source/chunk_reader/meta_bytes_read_from_disk"_SP, ChunkReaderStatistics_->MetaBytesReadFromDisk);
            }
        }
        TSecondaryQuerySourceBase::Finish();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSingleBatchSource
    : public TSecondaryQuerySourceBase
{
public:
    TSingleBatchSource(
        TReadPlanWithFilterPtr readPlan,
        TTraceContextPtr traceContext,
        THost* host,
        TQuerySettingsPtr settings,
        TLogger logger,
        TCallback<void(const TStatistics&)> statisticsCallback,
        IUnversionedRowBatchPtr batch,
        std::string batchDescription = "")
        : TSecondaryQuerySourceBase(readPlan, traceContext, host, settings, logger, statisticsCallback)
        , Batch_(std::move(batch))
        , BatchDescription_(std::move(batchDescription))
    {
        addTotalRowsApprox(Batch_->GetRowCount());
    }

    DB::String getName() const override
    {
        return "SingleBatchSource(" + BatchDescription_ + ")";
    }

    bool CanReadBatch() const override
    {
        return Batch_ != nullptr;
    }

    IUnversionedRowBatchPtr ReadBatch() override
    {
        auto batch = std::move(Batch_);
        Batch_ = nullptr;
        return batch;
    }

    TFuture<void> GetReadyEvent() const override
    {
        return OKFuture;
    }

private:
    IUnversionedRowBatchPtr Batch_;
    const std::string BatchDescription_;
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
    TReaderFactory readerFactory(
        storageContext,
        subquerySpec,
        GetPlanNameTable(std::move(readPlan)),
        chunkReadOptions,
        dataSliceDescriptors);
    return readerFactory.CreateReader();
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
    auto nameTable = GetPlanNameTable(readPlan);
    auto readerFactory = std::make_unique<TReaderFactory>(
        storageContext,
        subquerySpec,
        GetPlanNameTable(readPlan),
        chunkReadOptions,
        dataSliceDescriptors);

    TLogger Logger(queryContext->Logger);
    if (auto breakpointFilename = queryContext->SessionSettings->Testing->InputStreamFactoryBreakpoint) {
        HandleBreakpoint(*breakpointFilename, queryContext->Client());
        YT_LOG_DEBUG("Input stream factory handled breakpoint (Breakpoint: %v)", *breakpointFilename);
    }

    // TODO(buyval01): Inject settings from subquerySpec to storageContext settings at tcp handler level.
    storageContext->Settings->Execution->EnableOptimizeDistinctRead = subquerySpec.QuerySettings->Execution->EnableOptimizeDistinctRead;

    return std::make_shared<TSecondaryQuerySource>(
        /*reader*/ nullptr,
        std::move(readPlan),
        sourceTraceContext,
        queryContext->Host,
        storageContext->Settings,
        queryContext->Logger.WithTag("ReadSessionId: %v", chunkReadOptions.ReadSessionId),
        chunkReadOptions.ChunkReaderStatistics,
        std::move(statisticsCallback),
        std::move(readerFactory));
}

DB::SourcePtr CreateSingleBatchSource(
    TStorageContext* storageContext,
    const TTraceContextPtr& traceContext,
    TReadPlanWithFilterPtr readPlan,
    TCallback<void(const TStatistics&)> statisticsCallback,
    IUnversionedRowBatchPtr batch,
    std::string batchDescription)
{
    auto* queryContext = storageContext->QueryContext;
    TTraceContextPtr sourceTraceContext;
    if (traceContext) {
        sourceTraceContext = traceContext->CreateChild("ClickHouseYt.SingleBatchSource");
    }
    TCurrentTraceContextGuard guard(sourceTraceContext);
    return std::make_shared<TSingleBatchSource>(
        std::move(readPlan),
        sourceTraceContext,
        queryContext->Host,
        storageContext->Settings,
        queryContext->Logger,
        std::move(statisticsCallback),
        std::move(batch),
        std::move(batchDescription));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

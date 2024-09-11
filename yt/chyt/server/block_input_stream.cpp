#include "block_input_stream.h"

#include "config.h"
#include "conversion.h"
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

////////////////////////////////////////////////////////////////////////////////

namespace {

TClientChunkReadOptions CreateChunkReadOptions(const TString& user)
{
    TClientChunkReadOptions chunkReadOptions;
    chunkReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserRealtime);
    chunkReadOptions.WorkloadDescriptor.CompressionFairShareTag = user;
    chunkReadOptions.ReadSessionId = NChunkClient::TReadSessionId::Create();
    return chunkReadOptions;
}

struct TBlockWithFilter
{
    DB::Block Block;
    DB::IColumn::Filter Filter;
};

void ExecuteFilterStep(TBlockWithFilter& blockWithFilter, const TFilterInfo& filterInfo)
{
    auto& [block, currentFilter] = blockWithFilter;

    filterInfo.Actions->execute(block);

    auto filterColumnPosition = block.getPositionByName(filterInfo.FilterColumnName);
    auto filterColumn = block.getByPosition(filterColumnPosition).column;

    i64 rowCount = block.rows();

    // Output block should contain at least one column to preserve row count.
    if (filterInfo.RemoveFilterColumn && block.columns() != 1) {
        block.erase(filterColumnPosition);
    }

    if (currentFilter.empty()) {
        currentFilter.resize_fill(rowCount, 1);
    }

    // Combine current filter and filter column.
    // Note that filter column is either UInt8 or Nullable(UInt8).
    if (const auto* nullableFilterColumn = DB::checkAndGetColumn<DB::ColumnNullable>(filterColumn.get())) {
        const auto* nullMapColumn = DB::checkAndGetColumn<DB::ColumnVector<DB::UInt8>>(nullableFilterColumn->getNullMapColumn());
        YT_VERIFY(nullMapColumn);
        const auto& nullMap = nullMapColumn->getData();
        YT_VERIFY(std::ssize(nullMap) == rowCount);

        const auto* nestedColumn = DB::checkAndGetColumn<DB::ColumnVector<DB::UInt8>>(nullableFilterColumn->getNestedColumn());
        YT_VERIFY(nestedColumn);
        const auto& values = nestedColumn->getData();
        YT_VERIFY(std::ssize(values) == rowCount);

        for (i64 index = 0; index < rowCount; ++index) {
            currentFilter[index] &= !nullMap[index] && values[index];
        }
    } else {
        const auto* valueColumn = DB::checkAndGetColumn<DB::ColumnVector<DB::UInt8>>(filterColumn.get());
        YT_VERIFY(valueColumn);
        const auto& values = valueColumn->getData();
        YT_VERIFY(std::ssize(values) == rowCount);

        for (i64 index = 0; index < rowCount; ++index) {
            currentFilter[index] &= static_cast<bool>(values[index]);
        }
    }
}

}  // namespace

TBlockInputStream::TBlockInputStream(
    ISchemalessMultiChunkReaderPtr reader,
    TReadPlanWithFilterPtr readPlan,
    TTraceContextPtr traceContext,
    THost* host,
    TQuerySettingsPtr settings,
    TLogger logger,
    TChunkReaderStatisticsPtr chunkReaderStatistics,
    TCallback<void(const TStatistics&)> statisticsCallback)
    : Reader_(std::move(reader))
    , ReadPlan_(std::move(readPlan))
    , TraceContext_(std::move(traceContext))
    , Host_(host)
    , Settings_(std::move(settings))
    , Logger(std::move(logger))
    , RowBuffer_(New<NTableClient::TRowBuffer>())
    , ChunkReaderStatistics_(std::move(chunkReaderStatistics))
    , StatisticsCallback_(std::move(statisticsCallback))
{
    Prepare();
}

std::string TBlockInputStream::getName() const
{
    return "BlockInputStream";
}

DB::Block TBlockInputStream::getHeader() const
{
    return HeaderBlock_;
}

void TBlockInputStream::readPrefixImpl()
{
    TCurrentTraceContextGuard guard(TraceContext_);
    YT_LOG_DEBUG("readPrefixImpl() is called");

    IdleTimer_.Start();
}

void TBlockInputStream::readSuffixImpl()
{
    TCurrentTraceContextGuard guard(TraceContext_);
    YT_LOG_DEBUG("readSuffixImpl() is called");

    auto lastIdleDuration = IdleTimer_.GetCurrentDuration();
    IdleTimer_.Stop();

    Statistics_.AddSample("/block_input_stream/idle_time_us", lastIdleDuration.MicroSeconds());

    if (ChunkReaderStatistics_) {
        Statistics_.AddSample("/block_input_stream/chunk_reader/data_bytes_read_from_disk", ChunkReaderStatistics_->DataBytesReadFromDisk);
        Statistics_.AddSample("/block_input_stream/chunk_reader/data_io_requests", ChunkReaderStatistics_->DataIORequests);
        Statistics_.AddSample("/block_input_stream/chunk_reader/data_bytes_transmitted", ChunkReaderStatistics_->DataBytesTransmitted);
        Statistics_.AddSample("/block_input_stream/chunk_reader/data_bytes_read_from_cache", ChunkReaderStatistics_->DataBytesReadFromCache);
        Statistics_.AddSample("/block_input_stream/chunk_reader/meta_bytes_read_from_disk", ChunkReaderStatistics_->MetaBytesReadFromDisk);
    }

    if (StatisticsCallback_) {
        StatisticsCallback_(Statistics_);
    }

    YT_LOG_DEBUG(
        "Block input stream timing statistics (ColumnarConversionCpuTime: %v, NonColumnarConversionCpuTime: %v, "
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

DB::Block TBlockInputStream::readImpl()
{
    TCurrentTraceContextGuard guard(TraceContext_);

    TNullTraceContextGuard nullGuard;
    if (Settings_->EnableReaderTracing) {
        nullGuard.Release();
    }

    auto lastIdleDuration = IdleTimer_.GetCurrentDuration();
    IdleTimer_.Stop();
    ++ReadCount_;

    Statistics_.AddSample("/block_input_stream/idle_time_us", lastIdleDuration.MicroSeconds());

    NProfiling::TWallTimer totalWallTimer;
    YT_LOG_TRACE("Started reading ClickHouse block");

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
            Statistics_.AddSample("/block_input_stream/wait_ready_event_time_us", elapsed.MicroSeconds());

            if (elapsed > TDuration::Seconds(1)) {
                YT_LOG_DEBUG("Reading took significant time (WallTime: %v)", elapsed);
            }
            continue;
        }

        TBlockWithFilter blockWithFilter;
        i64 rowCountAfterFilter = batch->GetRowCount();

        for (int stepIndex = 0; stepIndex < std::ssize(ReadPlan_->Steps); ++stepIndex) {
            const auto& step = ReadPlan_->Steps[stepIndex];

            auto filterHint = (blockWithFilter.Filter.empty() || rowCountAfterFilter == batch->GetRowCount())
                ? TRange<DB::UInt8>()
                : TRange(blockWithFilter.Filter.data(), blockWithFilter.Filter.size());

            auto stepBlock = ConvertStepColumns(stepIndex, batch, filterHint);

            for (auto& column : stepBlock) {
                blockWithFilter.Block.insert(std::move(column));
            }

            if (step.FilterInfo) {
                ExecuteFilterStep(blockWithFilter, *step.FilterInfo);
                rowCountAfterFilter = DB::countBytesInFilter(blockWithFilter.Filter);
                if (rowCountAfterFilter == 0) {
                    break;
                }
            }
        }

        // All rows have been filtered out. Abandon current block and retry.
        if (rowCountAfterFilter == 0) {
            continue;
        }

        if (ReadPlan_->NeedFilter && rowCountAfterFilter != static_cast<i64>(blockWithFilter.Block.rows())) {
            for (auto& column : blockWithFilter.Block) {
                column.column = column.column->filter(blockWithFilter.Filter, rowCountAfterFilter);
            }
        }

        resultBlock = std::move(blockWithFilter.Block);

        // NB: ConvertToField copies all strings, so clearing row buffer is safe here.
        RowBuffer_->Clear();
    }

    auto totalElapsed = totalWallTimer.GetElapsedTime();
    YT_LOG_TRACE("Finished reading ClickHouse block (WallTime: %v)", totalElapsed);

    Statistics_.AddSample("/block_input_stream/block_rows", resultBlock.rows());
    Statistics_.AddSample("/block_input_stream/block_columns", resultBlock.columns());
    Statistics_.AddSample("/block_input_stream/block_bytes", resultBlock.bytes());

    Statistics_.AddSample("/block_input_stream/read_impl_us", totalElapsed.MicroSeconds());

    IdleTimer_.Start();

    return resultBlock;
}

void TBlockInputStream::Prepare()
{
    auto nameTable = Reader_->GetNameTable();

    Converters_.reserve(ReadPlan_->Steps.size());

    TBlockWithFilter blockWithFilter;

    for (const auto& step : ReadPlan_->Steps) {
        const auto& converter = Converters_.emplace_back(step.Columns, nameTable, Settings_->Composite);

        for (const auto& column : converter.GetHeaderBlock()) {
            blockWithFilter.Block.insert(column);
        }
        if (step.FilterInfo) {
            ExecuteFilterStep(blockWithFilter, *step.FilterInfo);
        }
    }

    HeaderBlock_ = std::move(blockWithFilter.Block);

    Statistics_.AddSample("/block_input_stream/step_count", ReadPlan_->Steps.size());
}

DB::Block TBlockInputStream::ConvertStepColumns(
    int stepIndex,
    const IUnversionedRowBatchPtr& batch,
    TRange<DB::UInt8> filterHint)
{
    auto statisticsPrefix = Format("/block_input_stream/steps/%v", stepIndex);

    DB::Block block;
    if (Settings_->ConvertRowBatchesInWorkerThreadPool) {
        auto start = TInstant::Now();
        block = WaitFor(BIND(
                &TBlockInputStream::DoConvertStepColumns,
                this,
                stepIndex,
                batch,
                filterHint)
            .AsyncVia(Host_->GetClickHouseWorkerInvoker())
            .Run())
            .ValueOrThrow();

        auto elapsed = TInstant::Now() - start;
        ConversionSyncWaitTime_ += elapsed;
        Statistics_.AddSample("/block_input_stream/conversion_sync_wait_time_us", elapsed.MicroSeconds());
    } else {
        block = DoConvertStepColumns(stepIndex, batch, filterHint);
    }

    Statistics_.AddSample(statisticsPrefix + "/block_rows", block.rows());
    Statistics_.AddSample(statisticsPrefix + "/block_columns", block.columns());
    Statistics_.AddSample(statisticsPrefix + "/block_bytes", block.bytes());

    return block;
}

DB::Block TBlockInputStream::DoConvertStepColumns(
    int stepIndex,
    const IUnversionedRowBatchPtr& batch,
    TRange<DB::UInt8> filterHint)
{
    bool isColumnarBatch = static_cast<bool>(batch->TryAsColumnar());

    NProfiling::TWallTimer timer;
    auto block = Converters_[stepIndex].Convert(batch, filterHint);

    timer.Stop();
    if (isColumnarBatch) {
        ColumnarConversionCpuTime_ += timer.GetElapsedTime();
        Statistics_.AddSample("/block_input_stream/columnar_conversion_cpu_time_us", timer.GetElapsedTime().MicroSeconds());
    } else {
        NonColumnarConversionCpuTime_ += timer.GetElapsedTime();
        Statistics_.AddSample("/block_input_stream/non_columnar_conversion_cpu_time_us", timer.GetElapsedTime().MicroSeconds());
    }

    return block;
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TBlockInputStream> CreateBlockInputStream(
    ISchemalessMultiChunkReaderPtr reader,
    TReadPlanWithFilterPtr readPlan,
    TTraceContextPtr traceContext,
    THost* host,
    TQuerySettingsPtr querySettings,
    TLogger logger,
    TChunkReaderStatisticsPtr chunkReaderStatistics,
    TCallback<void(const TStatistics&)> statisticsCallback)
{
    return std::make_shared<TBlockInputStream>(
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

std::shared_ptr<TBlockInputStream> CreateBlockInputStream(
    TStorageContext* storageContext,
    const TSubquerySpec& subquerySpec,
    TReadPlanWithFilterPtr readPlan,
    const NTracing::TTraceContextPtr& traceContext,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    IGranuleFilterPtr granuleFilter,
    TCallback<void(const TStatistics&)> statisticsCallback)
{
    auto* queryContext = storageContext->QueryContext;
    auto chunkReadOptions = CreateChunkReadOptions(queryContext->User);

    NTracing::TTraceContextPtr blockInputStreamTraceContext;
    if (traceContext) {
        blockInputStreamTraceContext = traceContext->CreateChild("ClickHouseYt.BlockInputStream");
    }

    TCurrentTraceContextGuard guard(blockInputStreamTraceContext);
    // Readers capture context implicitly, so create NullTraceContextGuard if tracing is disabled.
    NTracing::TNullTraceContextGuard nullGuard;
    if (storageContext->Settings->EnableReaderTracing) {
        nullGuard.Release();
    }

    ISchemalessMultiChunkReaderPtr reader;

    auto readerMemoryManager = queryContext->Host->GetMultiReaderMemoryManager()->CreateMultiReaderMemoryManager(
        queryContext->Host->GetConfig()->ReaderMemoryRequirement,
        {{"user", queryContext->User}});

    auto tableReaderConfig = CloneYsonStruct(storageContext->Settings->TableReader);
    tableReaderConfig->SamplingMode = subquerySpec.TableReaderConfig->SamplingMode;
    tableReaderConfig->SamplingRate = subquerySpec.TableReaderConfig->SamplingRate;
    tableReaderConfig->SamplingSeed = subquerySpec.TableReaderConfig->SamplingSeed;

    TLogger Logger(queryContext->Logger);

    if (auto breakpointFilename = queryContext->Settings->Testing->InputStreamFactoryBreakpoint) {
        HandleBreakpoint(*breakpointFilename, queryContext->Client());
        YT_LOG_DEBUG("Input stream factory handled breakpoint (Breakpoint: %v)", *breakpointFilename);
    }

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

    chunkReadOptions.GranuleFilter = granuleFilter;

    reader = CreateSchemalessParallelMultiReader(
        std::move(tableReaderConfig),
        New<NTableClient::TTableReaderOptions>(),
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

    return CreateBlockInputStream(
        std::move(reader),
        std::move(readPlan),
        blockInputStreamTraceContext,
        queryContext->Host,
        storageContext->Settings,
        queryContext->Logger.WithTag("ReadSessionId: %v", chunkReadOptions.ReadSessionId),
        chunkReadOptions.ChunkReaderStatistics,
        std::move(statisticsCallback));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

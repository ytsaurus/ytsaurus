#include "block_input_stream.h"

#include "query_context.h"
#include "host.h"
#include "helpers.h"
#include "config.h"
#include "subquery_spec.h"
#include "batch_conversion.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/client/table_client/unversioned_row_batch.h>
#include <yt/client/table_client/name_table.h>

#include <DataTypes/DataTypeNothing.h>

#include <Interpreters/ExpressionActions.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NLogging;
using namespace NConcurrency;
using namespace NTracing;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

NTableClient::TTableSchemaPtr FilterColumnsInSchema(
    const NTableClient::TTableSchema& schema,
    DB::Names columnNames)
{
    std::vector<TString> columnNamesInString;
    for (const auto& columnName : columnNames) {
        schema.GetColumnOrThrow(columnName);
        columnNamesInString.emplace_back(columnName);
    }
    return schema.Filter(columnNamesInString);
}

TClientBlockReadOptions CreateBlockReadOptions(const TString& user)
{
    TClientBlockReadOptions blockReadOptions;
    blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();
    blockReadOptions.WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserRealtime);
    blockReadOptions.WorkloadDescriptor.CompressionFairShareTag = user;
    blockReadOptions.ReadSessionId = NChunkClient::TReadSessionId::Create();
    return blockReadOptions;
}

TTableReaderConfigPtr CreateTableReaderConfig()
{
    auto config = New<TTableReaderConfig>();
    config->GroupSize = 150_MB;
    config->WindowSize = 200_MB;
    return config;
}

// Analog of the method from MergeTreeBaseSelectBlockInputStream::executePrewhereActions from CH.
void ExecutePrewhereActions(DB::Block& block, const DB::PrewhereInfoPtr& prewhereInfo)
{
    if (prewhereInfo->alias_actions) {
        prewhereInfo->alias_actions->execute(block);
    }
    prewhereInfo->prewhere_actions->execute(block);
    if (!block) {
        block.insert({nullptr, std::make_shared<DB::DataTypeNothing>(), "_nothing"});
    }
}

DB::Block FilterRowsByPrewhereInfo(
    DB::Block&& blockToFilter,
    const IUnversionedRowBatchPtr& batch,
    const DB::PrewhereInfoPtr& prewhereInfo,
    const TTableSchema& schema,
    const std::vector<int>& idToColumnIndex,
    const TRowBufferPtr& rowBuffer,
    const DB::Block& headerBlock,
    const IInvokerPtr& invoker)
{
    // Create prewhere column for filtering.
    ExecutePrewhereActions(blockToFilter, prewhereInfo);
    const auto& prewhereColumn = blockToFilter.getByName(prewhereInfo->prewhere_column_name).column;

    auto rowsToFilter = batch->MaterializeRows();
    std::vector<TUnversionedRow> filteredRows;
    filteredRows.reserve(prewhereColumn->size());
    for (size_t index = 0; index < prewhereColumn->size(); ++index) {
        if (prewhereColumn->getBool(index)) {
            filteredRows.push_back(rowsToFilter[index]);
        }
    }

    auto filteredBatch = CreateBatchFromUnversionedRows(
        MakeSharedRange(std::move(filteredRows), std::move(batch)));
    auto filteredBlock = WaitFor(BIND(
        &ConvertRowBatchToBlock,
        filteredBatch,
        schema,
        idToColumnIndex,
        rowBuffer,
        headerBlock)
        .AsyncVia(invoker)
        .Run())
        .ValueOrThrow();

    // Execute prewhere actions for filtered block.
    ExecutePrewhereActions(filteredBlock, prewhereInfo);

    return filteredBlock;
}

}  // namespace

TBlockInputStream::TBlockInputStream(
    ISchemalessMultiChunkReaderPtr reader,
    TTableSchemaPtr readSchema,
    TTraceContextPtr traceContext,
    THost* host,
    TLogger logger,
    DB::PrewhereInfoPtr prewhereInfo)
    : Reader_(std::move(reader))
    , ReadSchema_(std::move(readSchema))
    , TraceContext_(std::move(traceContext))
    , Host_(host)
    , Logger(std::move(logger))
    , RowBuffer_(New<NTableClient::TRowBuffer>())
    , PrewhereInfo_(std::move(prewhereInfo))
{
    Prepare();
}

std::string TBlockInputStream::getName() const
{
    return "BlockInputStream";
}

DB::Block TBlockInputStream::getHeader() const
{
    return OutputHeaderBlock_;
}

void TBlockInputStream::readPrefixImpl()
{
    TTraceContextGuard guard(TraceContext_);
    TraceContext_ = GetCurrentTraceContext();
    YT_LOG_DEBUG("readPrefixImpl() is called");
}

void TBlockInputStream::readSuffixImpl()
{
    TTraceContextGuard guard(TraceContext_);
    YT_LOG_DEBUG("readSuffixImpl() is called");

    if (TraceContext_) {
        NTracing::GetCurrentTraceContext()->AddTag("chyt.reader.data_statistics", ToString(Reader_->GetDataStatistics()));
        NTracing::GetCurrentTraceContext()->AddTag("chyt.reader.codec_statistics", ToString(Reader_->GetDecompressionStatistics()));
        TraceContext_->Finish();
    }
}

DB::Block TBlockInputStream::readImpl()
{
    TTraceContextGuard guard(TraceContext_);

    NProfiling::TWallTimer totalWallTimer;
    YT_LOG_TRACE("Started reading ClickHouse block");

    DB::Block block;
    while (block.rows() == 0) {
        TRowBatchReadOptions options{
            .Columnar = Host_->GetConfig()->EnableColumnarRead
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
            if (elapsed > TDuration::Seconds(1)) {
                YT_LOG_DEBUG("Reading took significant time (WallTime: %v)", elapsed);
            }
            continue;
        }

        block = WaitFor(BIND(
            &ConvertRowBatchToBlock,
            batch,
            *ReadSchema_,
            IdToColumnIndex_,
            RowBuffer_,
            InputHeaderBlock_)
            .AsyncVia(Host_->GetClickHouseWorkerInvoker())
            .Run())
            .ValueOrThrow();

        if (PrewhereInfo_) {
            block = FilterRowsByPrewhereInfo(
                std::move(block),
                batch,
                PrewhereInfo_,
                *ReadSchema_,
                IdToColumnIndex_,
                RowBuffer_,
                InputHeaderBlock_,
                Host_->GetClickHouseWorkerInvoker());
        }

        // NB: ConvertToField copies all strings, so clearing row buffer is safe here.
        RowBuffer_->Clear();
    }

    auto totalElapsed = totalWallTimer.GetElapsedTime();
    YT_LOG_TRACE("Finished reading ClickHouse block (WallTime: %v)", totalElapsed);

    return block;
}

void TBlockInputStream::Prepare()
{
    InputHeaderBlock_ = ToHeaderBlock(*ReadSchema_);
    OutputHeaderBlock_ = ToHeaderBlock(*ReadSchema_);
    
    if (PrewhereInfo_) {
        // Create header with executed prewhere actions.
        ExecutePrewhereActions(OutputHeaderBlock_, PrewhereInfo_);
    }

    for (int index = 0; index < static_cast<int>(ReadSchema_->Columns().size()); ++index) {
        const auto& columnSchema = ReadSchema_->Columns()[index];
        auto id = Reader_->GetNameTable()->GetIdOrRegisterName(columnSchema.Name());
        if (static_cast<int>(IdToColumnIndex_.size()) <= id) {
            IdToColumnIndex_.resize(id + 1, -1);
        }
        IdToColumnIndex_[id] = index;
    }
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TBlockInputStream> CreateBlockInputStream(
    ISchemalessMultiChunkReaderPtr reader,
    TTableSchemaPtr readSchema,
    TTraceContextPtr traceContext,
    THost* host,
    TLogger logger,
    DB::PrewhereInfoPtr prewhereInfo)
{
    return std::make_shared<TBlockInputStream>(
        std::move(reader),
        std::move(readSchema),
        std::move(traceContext),
        host,
        logger,
        std::move(prewhereInfo));
}

////////////////////////////////////////////////////////////////////////////////

class TBlockInputStreamLoggingAdapter
    : public DB::IBlockInputStream
{
public:
    TBlockInputStreamLoggingAdapter(
        DB::BlockInputStreamPtr stream,
        TLogger logger)
        : UnderlyingStream_(std::move(stream))
        , Logger(TLogger(std::move(logger)
            .AddTag("UnderlyingStream: %v", UnderlyingStream_.get())))
    {
        YT_LOG_DEBUG("Stream created");
        addChild(UnderlyingStream_);
    }

    virtual void readPrefix() override
    {
        YT_LOG_DEBUG("readPrefix() is called");
        UnderlyingStream_->readPrefix();
    }

    virtual void readSuffix() override
    {
        YT_LOG_DEBUG("readSuffix() is called");
        UnderlyingStream_->readSuffix();
    }

    virtual DB::Block readImpl() override
    {
        NProfiling::TWallTimer wallTimer;
        auto result = UnderlyingStream_->read();
        auto elapsed = wallTimer.GetElapsedTime();
        if (elapsed > TDuration::Seconds(1)) {
            YT_LOG_DEBUG("Read took significant time (WallTime: %v)", elapsed);
        }

        YT_LOG_TRACE("Block read (Block: %v)", result);

        return result;
    }

    virtual DB::String getName() const override
    {
        return "TBlockInputStreamLoggingAdapter";
    }

    virtual DB::Block getHeader() const override
    {
        YT_LOG_DEBUG("Started getting header from the underlying stream");
        auto result = UnderlyingStream_->getHeader();
        YT_LOG_DEBUG("Finished getting header from the underlying stream");
        return result;
    }

    virtual const DB::BlockMissingValues& getMissingValues() const override
    {
        return UnderlyingStream_->getMissingValues();
    }

    virtual bool isSortedOutput() const override
    {
        return UnderlyingStream_->isSortedOutput();
    }

    virtual const DB::SortDescription& getSortDescription() const override
    {
        return UnderlyingStream_->getSortDescription();
    }

    virtual DB::Block getTotals() override
    {
        return UnderlyingStream_->getTotals();
    }

    virtual void progress(const DB::Progress& value) override
    {
        UnderlyingStream_->progress(value);
    }

    virtual void cancel(bool kill) override
    {
        UnderlyingStream_->cancel(kill);
    }

private:
    const DB::BlockInputStreamPtr UnderlyingStream_;
    const TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

DB::BlockInputStreamPtr CreateBlockInputStreamLoggingAdapter(
    DB::BlockInputStreamPtr stream,
    TLogger logger)
{
    return std::make_shared<TBlockInputStreamLoggingAdapter>(
        std::move(stream),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TBlockInputStream> CreateBlockInputStream(
    TQueryContext* queryContext,
    const TSubquerySpec& subquerySpec,
    const DB::Names& columnNames,
    const NTracing::TTraceContextPtr& traceContext,
    const std::vector<TDataSliceDescriptor>& dataSliceDescriptors,
    DB::PrewhereInfoPtr prewhereInfo)
{
    auto schema = FilterColumnsInSchema(*subquerySpec.ReadSchema, columnNames);
    auto blockReadOptions = CreateBlockReadOptions(queryContext->User);

    auto blockInputStreamTraceContext = NTracing::CreateChildTraceContext(
        traceContext,
        "ClickHouseYt.BlockInputStream");
    NTracing::TTraceContextGuard guard(blockInputStreamTraceContext);

    auto readerMemoryManager = queryContext->Host->GetMultiReaderMemoryManager()->CreateMultiReaderMemoryManager(
        queryContext->Host->GetConfig()->ReaderMemoryRequirement,
        {queryContext->UserTagId});

    ISchemalessMultiChunkReaderPtr reader;

    if (!subquerySpec.DataSourceDirectory->DataSources().empty() &&
        subquerySpec.DataSourceDirectory->DataSources()[0].GetType() == EDataSourceType::VersionedTable)
    {
        std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;
        for (const auto& dataSliceDescriptor : dataSliceDescriptors) {
            for (auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
                chunkSpecs.emplace_back(std::move(chunkSpec));
            }
        }
        TDataSliceDescriptor dataSliceDescriptor(std::move(chunkSpecs));

        reader = CreateSchemalessMergingMultiChunkReader(
            CreateTableReaderConfig(),
            New<NTableClient::TTableReaderOptions>(),
            queryContext->Client(),
            /* localDescriptor */ {},
            /* partitionTag */ std::nullopt,
            queryContext->Client()->GetNativeConnection()->GetBlockCache(),
            queryContext->Client()->GetNativeConnection()->GetNodeDirectory(),
            subquerySpec.DataSourceDirectory,
            dataSliceDescriptor,
            TNameTable::FromSchema(*schema),
            blockReadOptions,
            TColumnFilter(schema->Columns().size()),
            /* trafficMeter */ nullptr,
            GetUnlimitedThrottler(),
            GetUnlimitedThrottler());
    } else {
        reader = CreateSchemalessParallelMultiReader(
            CreateTableReaderConfig(),
            New<NTableClient::TTableReaderOptions>(),
            queryContext->Client(),
            /* localDescriptor =*/{},
            std::nullopt,
            queryContext->Client()->GetNativeConnection()->GetBlockCache(),
            queryContext->Client()->GetNativeConnection()->GetNodeDirectory(),
            subquerySpec.DataSourceDirectory,
            dataSliceDescriptors,
            TNameTable::FromSchema(*schema),
            blockReadOptions,
            TColumnFilter(schema->Columns().size()),
            /* keyColumns =*/{},
            /* partitionTag =*/std::nullopt,
            /* trafficMeter =*/nullptr,
            /* bandwidthThrottler =*/GetUnlimitedThrottler(),
            /* rpsThrottler =*/GetUnlimitedThrottler(),
            /* multiReaderMemoryManager =*/readerMemoryManager);
    }

    return CreateBlockInputStream(
        reader,
        schema,
        blockInputStreamTraceContext,
        queryContext->Host,
        TLogger(queryContext->Logger)
            .AddTag("ReadSessionId: %v", blockReadOptions.ReadSessionId),
        prewhereInfo);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

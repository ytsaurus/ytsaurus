#include "block_input_stream.h"

#include "query_context.h"
#include "host.h"
#include "helpers.h"
#include "config.h"
#include "subquery_spec.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/client/table_client/schemaless_reader.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/helpers.h>

#include <DataTypes/DataTypeNothing.h>

#include <Storages/MergeTree/MergeTreeBaseSelectBlockInputStream.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NLogging;
using namespace NConcurrency;
using namespace NTracing;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchema FilterColumnsInSchema(
    const NTableClient::TTableSchema& schema,
    DB::Names columnNames)
{
    std::vector<TString> columnNamesInString;
    for (const auto& columnName : columnNames) {
        if (!schema.FindColumn(columnName)) {
            THROW_ERROR_EXCEPTION("Column not found")
                << TErrorAttribute("column", columnName);
        }
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

DB::Block ConvertRowsToBlock(
    const std::vector<TUnversionedRow>& rows,
    const TTableSchema& readSchema,
    const std::vector<int>& idToColumnIndex,
    TRowBufferPtr rowBuffer,
    DB::Block block)
{
    // NB(max42): CHYT-256.
    // If chunk schema contains not all of the requested columns (which may happen
    // when a non-required column was introduced after chunk creation), we are not
    // going to receive some of the unversioned values with nulls. We still need
    // to provide them to CH, though, so we keep track of present columns for each
    // row we get and add nulls for all unpresent columns.
    std::vector<bool> presentValueMask;

    for (const auto& row : rows) {
        presentValueMask.assign(readSchema.GetColumnCount(), false);
        for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
            auto value = row[index];
            auto id = value.Id;
            int columnIndex = (id < idToColumnIndex.size()) ? idToColumnIndex[id] : -1;
            YT_VERIFY(columnIndex != -1);
            presentValueMask[columnIndex] = true;
            switch (value.Type) {
                case EValueType::Null:
                    // TODO(max42): consider transforming to Y_ASSERT.
                    YT_VERIFY(!readSchema.Columns()[columnIndex].Required());
                    block.getByPosition(columnIndex).column->assumeMutable()->insertDefault();
                    break;

                // NB(max42): When rewriting this properly, remember that Int64 may
                // correspond to shorter integer columns.
                case EValueType::String:
                case EValueType::Any:
                case EValueType::Composite:
                case EValueType::Int64:
                case EValueType::Uint64:
                case EValueType::Double:
                case EValueType::Boolean: {
                    if (readSchema.Columns()[columnIndex].GetPhysicalType() == EValueType::Any) {
                        ToAny(rowBuffer.Get(), &value, &value);
                    }
                    auto field = ConvertToField(value);
                    block.getByPosition(columnIndex).column->assumeMutable()->insert(field);
                    break;
                }
                default:
                    Y_UNREACHABLE();
            }
        }
        for (int columnIndex = 0; columnIndex < readSchema.GetColumnCount(); ++columnIndex) {
            if (!presentValueMask[columnIndex]) {
                YT_VERIFY(!readSchema.Columns()[columnIndex].Required());
                block.getByPosition(columnIndex).column->assumeMutable()->insertDefault();
            }
        }
    }

    return block;
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
    std::vector<TUnversionedRow>&& rowsToFilter,
    const DB::PrewhereInfoPtr& prewhereInfo,
    const TTableSchema& schema,
    const std::vector<int>& idToColumnIndex,
    const TRowBufferPtr& rowBuffer,
    const DB::Block& headerBlock,
    IInvokerPtr invoker)
{
    // Create prewhere column for filtering.
    ExecutePrewhereActions(blockToFilter, prewhereInfo);
    const auto& prewhereColumn = blockToFilter.getByName(prewhereInfo->prewhere_column_name).column;

    std::vector<TUnversionedRow> filteredRows;
    for (size_t index = 0; index < prewhereColumn->size(); ++index) {
        if (prewhereColumn->getBool(index)) {
            filteredRows.emplace_back(rowsToFilter[index]);
        }
    }

    auto filteredBlock = WaitFor(BIND(
        &NDetail::ConvertRowsToBlock,
        filteredRows,
        schema,
        idToColumnIndex,
        rowBuffer,
        headerBlock.cloneEmpty())
        .AsyncVia(invoker)
        .Run())
        .ValueOrThrow();

    // Execute prewhere actions for filtered block.
    ExecutePrewhereActions(filteredBlock, prewhereInfo);

    return filteredBlock;
}

////////////////////////////////////////////////////////////////////////////////

}  // namespace NDetail

TBlockInputStream::TBlockInputStream(
    ISchemalessMultiChunkReaderPtr reader,
    TTableSchema readSchema,
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
    YT_LOG_TRACE("Started reading one CH block");

    DB::Block block;
    while (block.rows() == 0) {
        // TODO(max42): consult with psushin@ about contract here.
        std::vector<TUnversionedRow> rows;
        // TODO(max42): make customizable.
        constexpr int rowsPerRead = 10 * 1024;
        rows.reserve(rowsPerRead);
        while (true) {
            if (!Reader_->Read(&rows)) {
                return {};
            } else if (rows.empty()) {
                NProfiling::TWallTimer wallTimer;
                WaitFor(Reader_->GetReadyEvent())
                    .ThrowOnError();
                auto elapsed = wallTimer.GetElapsedTime();
                if (elapsed > TDuration::Seconds(1)) {
                    YT_LOG_DEBUG("Reading took significant time (WallTime: %v)", elapsed);
                }
            } else {
                break;
            }
        }

        block = WaitFor(BIND(
            &NDetail::ConvertRowsToBlock,
            rows,
            ReadSchema_,
            IdToColumnIndex_,
            RowBuffer_,
            InputHeaderBlock_.cloneEmpty())
            .AsyncVia(Host_->GetWorkerInvoker())
            .Run())
            .ValueOrThrow();

        if (PrewhereInfo_) {
            block = NDetail::FilterRowsByPrewhereInfo(
                std::move(block),
                std::move(rows),
                PrewhereInfo_,
                ReadSchema_,
                IdToColumnIndex_,
                RowBuffer_,
                InputHeaderBlock_,
                Host_->GetWorkerInvoker());
        }

        // NB: ConvertToField copies all strings, so clearing row buffer is safe here.
        RowBuffer_->Clear();
    }

    auto totalElapsed = totalWallTimer.GetElapsedTime();
    YT_LOG_TRACE("Finished reading one CH block (WallTime: %v)", totalElapsed);

    return block;
}

void TBlockInputStream::Prepare()
{
    InputHeaderBlock_ = ToHeaderBlock(ReadSchema_);
    OutputHeaderBlock_ = ToHeaderBlock(ReadSchema_);
    if (PrewhereInfo_) {
        // Create header with executed prewhere actions.
        NDetail::ExecutePrewhereActions(OutputHeaderBlock_, PrewhereInfo_);
    }

    for (int index = 0; index < static_cast<int>(ReadSchema_.Columns().size()); ++index) {
        const auto& columnSchema = ReadSchema_.Columns()[index];
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
    TTableSchema readSchema,
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
    TBlockInputStreamLoggingAdapter(DB::BlockInputStreamPtr stream, TLogger logger)
        : UnderlyingStream_(std::move(stream))
        , Logger(logger)
    {
        Logger.AddTag("UnderlyingStream: %v", static_cast<void*>(UnderlyingStream_.get()));
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
            YT_LOG_DEBUG("Reading took significant time (WallTime: %v)", elapsed);
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
    DB::BlockInputStreamPtr UnderlyingStream_;
    TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

DB::BlockInputStreamPtr CreateBlockInputStreamLoggingAdapter(DB::BlockInputStreamPtr stream, TLogger logger)
{
    return std::make_shared<TBlockInputStreamLoggingAdapter>(std::move(stream), logger);
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
    auto schema = NDetail::FilterColumnsInSchema(subquerySpec.ReadSchema, columnNames);
    auto blockReadOptions = NDetail::CreateBlockReadOptions(queryContext->User);

    auto blockInputStreamTraceContext = NTracing::CreateChildTraceContext(
        traceContext,
        "ClickHouseYt.BlockInputStream");
    NTracing::TTraceContextGuard guard(blockInputStreamTraceContext);

    auto readerMemoryManager = queryContext->Host->GetMultiReaderMemoryManager()->CreateMultiReaderMemoryManager(
        queryContext->Host->GetConfig()->ReaderMemoryRequirement,
        {queryContext->UserTagId});
    auto reader = CreateSchemalessParallelMultiReader(
        NDetail::CreateTableReaderConfig(),
        New<NTableClient::TTableReaderOptions>(),
        queryContext->Client(),
        /* localDescriptor =*/{},
        std::nullopt,
        queryContext->Client()->GetNativeConnection()->GetBlockCache(),
        queryContext->Client()->GetNativeConnection()->GetNodeDirectory(),
        subquerySpec.DataSourceDirectory,
        dataSliceDescriptors,
        TNameTable::FromSchema(schema),
        blockReadOptions,
        TColumnFilter(schema.Columns().size()),
        /* keyColumns =*/{},
        /* partitionTag =*/std::nullopt,
        /* trafficMeter =*/nullptr,
        /* bandwidthThrottler =*/GetUnlimitedThrottler(),
        /* rpsThrottler =*/GetUnlimitedThrottler(),
        /* multiReaderMemoryManager =*/readerMemoryManager);

    return CreateBlockInputStream(
        reader,
        schema,
        blockInputStreamTraceContext,
        queryContext->Host,
        TLogger(queryContext->Logger).AddTag("ReadSessionId: %v", blockReadOptions.ReadSessionId),
        prewhereInfo);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

#include "block_input_stream.h"

#include "bootstrap.h"
#include "helpers.h"

#include <yt/client/table_client/schemaless_reader.h>
#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/helpers.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>

#include <Storages/MergeTree/MergeTreeBaseSelectBlockInputStream.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NLogging;
using namespace NConcurrency;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

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

}  //  namespace NDetail

class TBlockInputStream
    : public DB::IBlockInputStream
{
public:
    TBlockInputStream(
        ISchemalessReaderPtr reader,
        TTableSchema readSchema,
        TTraceContextPtr traceContext,
        TBootstrap* bootstrap,
        TLogger logger,
        DB::PrewhereInfoPtr prewhereInfo)
        : Reader_(std::move(reader))
        , ReadSchema_(std::move(readSchema))
        , TraceContext_(std::move(traceContext))
        , Bootstrap_(bootstrap)
        , Logger(std::move(logger))
        , PrewhereInfo_(std::move(prewhereInfo))
    {
        Prepare();
    }

    virtual std::string getName() const override
    {
        return "BlockInputStream";
    }

    virtual DB::Block getHeader() const override
    {
        return OutputHeaderBlock_;
    }

    virtual void readPrefixImpl() override
    {
        TTraceContextGuard guard(TraceContext_);
        TraceContext_ = GetCurrentTraceContext();
        YT_LOG_DEBUG("readPrefixImpl() is called");
    }

    virtual void readSuffixImpl() override
    {
        TTraceContextGuard guard(TraceContext_);
        YT_LOG_DEBUG("readSuffixImpl() is called");

        if (TraceContext_) {
            NTracing::GetCurrentTraceContext()->AddTag("chyt.reader.data_statistics", ToString(Reader_->GetDataStatistics()));
            NTracing::GetCurrentTraceContext()->AddTag("chyt.reader.codec_statistics", ToString(Reader_->GetDecompressionStatistics()));
            TraceContext_->Finish();
        }
    }

private:
    ISchemalessReaderPtr Reader_;
    TTableSchema ReadSchema_;
    TTraceContextPtr TraceContext_;

    TBootstrap* Bootstrap_;
    TLogger Logger;
    DB::Block InputHeaderBlock_;
    DB::Block OutputHeaderBlock_;
    std::vector<int> IdToColumnIndex_;
    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    DB::PrewhereInfoPtr PrewhereInfo_;

    DB::Block readImpl() override
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
                .AsyncVia(Bootstrap_->GetWorkerInvoker())
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
                    Bootstrap_->GetWorkerInvoker());
            }

            // NB: ConvertToField copies all strings, so clearing row buffer is safe here.
            RowBuffer_->Clear();
        }

        auto totalElapsed = totalWallTimer.GetElapsedTime();
        YT_LOG_TRACE("Finished reading one CH block (WallTime: %v)", totalElapsed);

        return block;
    }

    void Prepare()
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
};

////////////////////////////////////////////////////////////////////////////////

DB::BlockInputStreamPtr CreateBlockInputStream(
    ISchemalessReaderPtr reader,
    TTableSchema readSchema,
    TTraceContextPtr traceContext,
    TBootstrap* bootstrap,
    TLogger logger,
    DB::PrewhereInfoPtr prewhereInfo)
{
    return std::make_shared<TBlockInputStream>(
        std::move(reader),
        std::move(readSchema),
        std::move(traceContext),
        bootstrap,
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

} // namespace NYT::NClickHouseServer

#include "versioned_chunk_reader.h"
#include "segment_readers.h"
#include "read_span_refiner.h"
#include "rowset_builder.h"

#include <yt/yt/ytlib/table_chunk_format/helpers.h>

#include <yt/yt/ytlib/chunk_client/block.h>

#include <yt/yt/ytlib/chunk_client/block_fetcher.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/ytlib/query_client/coordination_helpers.h>

#include <yt/yt/client/table_client/config.h>

#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>
#include <yt/yt/ytlib/table_client/private.h>

#include <yt/yt/core/misc/tls_guard.h>

namespace NYT::NNewTableClient {

using NTableChunkFormat::NProto::TColumnMeta;

using NProfiling::TValueIncrementingTimingGuard;
using NProfiling::TWallTimer;

using NChunkClient::TBlock;
using NChunkClient::TBlockFetcher;
using NChunkClient::IBlockCache;
using NChunkClient::IBlockCachePtr;
using NChunkClient::TBlockFetcherPtr;
using NChunkClient::IChunkReaderPtr;

using NChunkClient::TChunkReaderMemoryManager;
using NChunkClient::TChunkReaderMemoryManagerOptions;
using NChunkClient::TClientChunkReadOptions;

using NTableClient::TCachedVersionedChunkMetaPtr;
using NTableClient::TRefCountedBlockMetaPtr;
using NTableClient::TChunkReaderConfigPtr;
using NTableClient::TColumnIdMapping;
using NTableClient::TColumnFilter;
using NTableClient::ETableChunkFormat;

using NTableClient::IVersionedReader;
using NTableClient::IVersionedReaderPtr;
using NTableClient::TVersionedRow;
using NTableClient::TChunkReaderPerformanceCountersPtr;

using NChunkClient::TCodecStatistics;
using NChunkClient::TChunkId;
using NChunkClient::NProto::TDataStatistics;

using NTableClient::IVersionedRowBatchPtr;
using NTableClient::TRowBatchReadOptions;
using NTableClient::CreateEmptyVersionedRowBatch;

////////////////////////////////////////////////////////////////////////////////

std::vector<EValueType> GetKeyTypes(const TCachedVersionedChunkMetaPtr& chunkMeta)
{
    std::vector<EValueType> keyTypes;

    auto keyColumnCount = chunkMeta->GetKeyColumnCount();
    const auto& schemaColumns = chunkMeta->GetSchema()->Columns();
    for (int keyColumnIndex = 0; keyColumnIndex < keyColumnCount; ++keyColumnIndex) {
        auto type = schemaColumns[keyColumnIndex].GetPhysicalType();
        keyTypes.push_back(type);
    }

    return keyTypes;
}

std::vector<TValueSchema> GetValueTypes(
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    TRange<TColumnIdMapping> valueIdMapping)
{
    std::vector<TValueSchema> valueSchema;

    const auto& schemaColumns = chunkMeta->GetChunkSchema()->Columns();
    for (const auto& idMapping : valueIdMapping) {
        auto type = schemaColumns[idMapping.ChunkSchemaIndex].GetPhysicalType();
        valueSchema.push_back(TValueSchema{
            type,
            ui16(idMapping.ReaderSchemaIndex),
            schemaColumns[idMapping.ChunkSchemaIndex].Aggregate().has_value()});
    }

    return valueSchema;
}

///////////////////////////////////////////////////////////////////////////////

template <class T>
struct IColumnRefiner
{
    virtual ~IColumnRefiner() = default;

    virtual void Refine(
        TKeysSlice<T>* keys,
        const TColumnSlice& columnSlice,
        const std::vector<TSpanMatching>& matchings,
        std::vector<TSpanMatching>* nextMatchings) = 0;
};

template <class T, EValueType Type>
struct TColumnRefiner
    : public IColumnRefiner<T>
    , public TColumnIterator<Type>
{
    using TBase = TColumnIterator<Type>;

    virtual void Refine(
        TKeysSlice<T>* keys,
        const TColumnSlice& columnSlice,
        const std::vector<TSpanMatching>& matchings,
        std::vector<TSpanMatching>* nextMatchings) override
    {
        if (columnSlice.Block) {
            // Blocks are not set for null columns.
            TBase::SetBlock(columnSlice.Block, columnSlice.SegmentsMeta);
        }

        for (auto [chunk, control] : matchings) {
            if (IsEmpty(control)) {
                nextMatchings->push_back({chunk, control});
                continue;
            }

            TBase::Init(chunk);
            keys->Init(control);

            BuildReadRowRanges(this, keys, nextMatchings);
        }
    }
};

template <class TItem, class TPredicate>
std::vector<TSpanMatching> DoBuildReadWindows(
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    TRange<TItem> items,
    TPredicate pred)
{
    const auto& blockLastKeys = chunkMeta->LegacyBlockLastKeys();
    const auto& blockMeta = chunkMeta->BlockMeta();

    std::vector<TSpanMatching> readWindows;

    using NQueryClient::GroupItemsByShards;

    // Block last keys may repeat.

    // TODO(lukyan): Fix repeated block last keys other way.
    ui32 checkLastLimit = 0;

    GroupItemsByShards(items, blockLastKeys, pred, [&] (
        const TLegacyKey* shardIt,
        const TItem* itemIt,
        const TItem* itemItEnd)
    {
        if (shardIt == blockLastKeys.end()) {
            auto chunkRowCount = chunkMeta->Misc().row_count();
            TReadSpan controlSpan(itemIt - items.begin(), itemItEnd - items.begin());
            // Add window for ranges after chunk end bound. It will be used go generate sentinel rows for lookup.
            readWindows.emplace_back(TReadSpan(chunkRowCount, chunkRowCount), controlSpan);
            return;
        }

        auto shardIndex = shardIt - blockLastKeys.begin();
        auto upperRowLimit = blockMeta->blocks(shardIndex).chunk_row_count();

        // TODO(lukyan): Rewrite calculation of start index.
        while (shardIndex > 0 && blockMeta->blocks(shardIndex - 1).chunk_row_count() == upperRowLimit) {
            --shardIndex;
        }

        ui32 startIndex = shardIndex > 0 ? blockMeta->blocks(shardIndex - 1).chunk_row_count() : 0;

        if (startIndex < checkLastLimit) {
            return;
        }

        YT_VERIFY(itemIt != itemItEnd);

        TReadSpan controlSpan(itemIt - items.begin(), itemItEnd - items.begin());
        readWindows.emplace_back(TReadSpan(startIndex, upperRowLimit), controlSpan);

        checkLastLimit = upperRowLimit;
    });

    return readWindows;
}

struct IRefiner
{
    virtual ~IRefiner() = default;

    virtual std::vector<TReadSpan> BuildReadListForWindow(
        TRange<TColumnSlice> columnSlices,
        TSpanMatching window) const = 0;

    virtual std::vector<TSpanMatching> BuildReadWindows(const TCachedVersionedChunkMetaPtr& chunkMeta) const = 0;
};

class TRangeRefiner
    : public IRefiner
{
public:
    template <EValueType Type>
    struct TCreateRefiner
    {
        static std::unique_ptr<IColumnRefiner<TRowRange>> Do()
        {
            return std::make_unique<TColumnRefiner<TRowRange, Type>>();
        }
    };

    TRangeRefiner(TSharedRange<TRowRange> keyRanges, TRange<EValueType> keyTypes)
        : KeyRanges_(keyRanges)
    {
        for (auto type : keyTypes) {
            ColumnRefiners_.push_back(DispatchByDataType<TCreateRefiner>(type));
        }
    }

    virtual std::vector<TSpanMatching> BuildReadWindows(const TCachedVersionedChunkMetaPtr& chunkMeta) const override
    {
        struct TPredicate
        {
            bool operator() (const TRowRange* itemIt, const TLegacyKey* shardIt) const
            {
                return itemIt->second <= *shardIt;
            }

            bool operator() (const TLegacyKey* shardIt, const TRowRange* itemIt) const
            {
                return *shardIt < itemIt->first;
            }
        };

        return DoBuildReadWindows(chunkMeta, KeyRanges_, TPredicate{});
    }

    std::vector<TReadSpan> BuildReadListForWindow(
        TRange<TColumnSlice> columnSlices,
        TSpanMatching initialWindow) const
    {
        std::vector<TSpanMatching> matchings;
        std::vector<TSpanMatching> nextMatchings;

        // Each range consists of two bounds.
        initialWindow.Control.Lower *= 2;
        initialWindow.Control.Upper *= 2;

        TRangeSliceAdapter keys;
        keys.Ranges = KeyRanges_;

        // All values must be accessible in column refiner.
        if (!keys.GetBound(initialWindow.Control.Lower).GetCount()) {
            // Bound is empty skip it.
            ++initialWindow.Control.Lower;
        }

        matchings.push_back(initialWindow);

        for (ui32 columnId = 0; columnId < ColumnRefiners_.size(); ++columnId) {
            keys.ColumnId = columnId;
            keys.LastColumn = columnId + 1 == ColumnRefiners_.size();
            auto columnSlice = columnId < columnSlices.size() ? columnSlices[columnId] : TColumnSlice();
            ColumnRefiners_[columnId]->Refine(&keys, columnSlice, matchings, &nextMatchings);
            matchings.clear();
            nextMatchings.swap(matchings);
        }

        std::vector<TReadSpan> result;

        // TODO(lukyan): Concat adjacent spans for scan mode.
        for (const auto& [chunkSpan, controlSpan] : matchings) {
            YT_VERIFY(controlSpan.Lower == controlSpan.Upper ||
                controlSpan.Lower + 1 == controlSpan.Upper);

            result.push_back(chunkSpan);
        }

        return result;
    }

private:
    const TSharedRange<TRowRange> KeyRanges_;
    std::vector<std::unique_ptr<IColumnRefiner<TRowRange>>> ColumnRefiners_;
};

class TLookupRefiner
    : public IRefiner
{
public:
    template <EValueType Type>
    struct TCreateRefiner
    {
        static std::unique_ptr<IColumnRefiner<TLegacyKey>> Do()
        {
            return std::make_unique<TColumnRefiner<TLegacyKey, Type>>();
        }
    };

    TLookupRefiner(TSharedRange<TLegacyKey> keys, TRange<EValueType> keyTypes)
        : Keys_(keys)
    {
        for (auto type : keyTypes) {
            ColumnRefiners_.push_back(DispatchByDataType<TCreateRefiner>(type));
        }
    }

    virtual std::vector<TSpanMatching> BuildReadWindows(const TCachedVersionedChunkMetaPtr& chunkMeta) const override
    {
        // Strong typedef.
        struct TItem
            : public TLegacyKey
        {
            using TLegacyKey::TLegacyKey;
        };

        struct TPredicate
        {
            bool operator() (const TItem* itemIt, const TLegacyKey* shardIt) const
            {
                return *itemIt <= *shardIt;
            }

            bool operator() (const TLegacyKey* shardIt, const TItem* itemIt) const
            {
                return *shardIt < *itemIt;
            }
        };

        return DoBuildReadWindows(
            chunkMeta,
            MakeRange(static_cast<const TItem*>(Keys_.begin()), Keys_.size()),
            TPredicate{});
    }

    std::vector<TReadSpan> BuildReadListForWindow(
        TRange<TColumnSlice> columnSlices,
        TSpanMatching initialWindow) const
    {
        // TODO(lukyan): Reuse vectors.
        std::vector<TSpanMatching> matchings;
        std::vector<TSpanMatching> nextMatchings;

        TKeysSlice<TLegacyKey> keys;
        keys.Keys = Keys_;

        matchings.push_back(initialWindow);

        for (ui32 columnId = 0; columnId < ColumnRefiners_.size(); ++columnId) {
            keys.ColumnId = columnId;
            auto columnSlice = columnId < columnSlices.size() ? columnSlices[columnId] : TColumnSlice();
            ColumnRefiners_[columnId]->Refine(&keys, columnSlice, matchings, &nextMatchings);

            matchings.clear();
            nextMatchings.swap(matchings);
        }

        std::vector<TReadSpan> result;

        // Encode non existent keys in chunk as read span (0, 0).
        auto offset = initialWindow.Control.Lower;
        for (const auto& [chunk, control] : matchings) {
            YT_VERIFY(control.Lower + 1 == control.Upper);

            while (offset < control.Lower) {
                result.push_back(TReadSpan{0, 0});
                ++offset;
            }

            result.push_back(chunk);
            offset = control.Upper;
        }

        while (offset < initialWindow.Control.Upper) {
            result.push_back(TReadSpan{0, 0});
            ++offset;
        }

        return result;
    }

private:
    const TSharedRange<TLegacyKey> Keys_;
    std::vector<std::unique_ptr<IColumnRefiner<TLegacyKey>>> ColumnRefiners_;
};

std::unique_ptr<IRefiner> MakeRefiner(TSharedRange<TRowRange> keyRanges, TRange<EValueType> keyTypes)
{
    return std::make_unique<TRangeRefiner>(keyRanges, keyTypes);
}

std::unique_ptr<IRefiner> MakeRefiner(TSharedRange<TLegacyKey> keys, TRange<EValueType> keyTypes)
{
    return std::make_unique<TLookupRefiner>(keys, keyTypes);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<ui32> BuildColumnBlockSequence(const TColumnMeta& meta)
{
    std::vector<ui32> blockSequence;
    ui32 segmentIndex = 0;

    while (segmentIndex < meta.segments_size()) {
        auto blockIndex = meta.segments(segmentIndex).block_index();
        blockSequence.push_back(blockIndex);
        do {
            ++segmentIndex;
        } while (segmentIndex < meta.segments_size() &&
             meta.segments(segmentIndex).block_index() == blockIndex);
    }
    return blockSequence;
}

std::vector<TBlockFetcher::TBlockInfo> BuildBlockInfos(
    const std::vector<TRange<ui32>>& columnsBlockIndexes,
    TRange<TSpanMatching> windows,
    const TCachedVersionedChunkMetaPtr& chunkMeta)
{
    const auto& blockMetas = chunkMeta->BlockMeta();

    std::vector<TBlockFetcher::TBlockInfo> blockInfos;
    for (const auto& blockIds : columnsBlockIndexes) {
        auto windowIt = windows.begin();
        auto blockIdsIt = blockIds.begin();

        while (windowIt != windows.end() && blockIdsIt != blockIds.end()) {
            blockIdsIt = ExponentialSearch(blockIdsIt, blockIds.end(), [&] (auto blockIt) {
                const auto& blockMeta = blockMetas->blocks(*blockIt);
                return blockMeta.chunk_row_count() < windowIt->Chunk.Upper;
            });

            YT_VERIFY(blockIdsIt != blockIds.end());

            auto blockIndex = *blockIdsIt;
            const auto& blockMeta = blockMetas->blocks(blockIndex);

            TBlockFetcher::TBlockInfo blockInfo;
            blockInfo.Index = blockIndex;
            blockInfo.Priority = blockMeta.chunk_row_count() - blockMeta.row_count();
            blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();

            blockInfos.push_back(blockInfo);

            windowIt = ExponentialSearch(windowIt, windows.end(), [&] (auto it) {
                return it->Chunk.Upper <= blockMeta.chunk_row_count();
            });
        }
    }

    return blockInfos;
}

class TColumnBlockHolder
{
public:
    TColumnBlockHolder(const TColumnBlockHolder&) = delete;
    TColumnBlockHolder(TColumnBlockHolder&&) = default;

    explicit TColumnBlockHolder(const TColumnMeta& meta)
        : Meta(meta)
        , BlockIds(BuildColumnBlockSequence(meta))
    { }

    // TODO(lukyan): Return segmentIt. Get data = Block.Begin() + segmentIt->offset() outside.
    // Or return InitSegmentFlag and create accessor to CurrentSegment?
    void SkipToSegment(ui32 rowIndex, IColumnBase* column, TTmpBuffers* tmpBuffers)
    {
        if (rowIndex < SegmentRowLimit_) {
            return;
        }

        auto segmentIt = BinarySearch(
            Meta.segments().begin() + SegmentStart_,
            Meta.segments().begin() + SegmentEnd_,
            [&] (auto segmentMetaIt) {
                return segmentMetaIt->chunk_row_count() <= rowIndex;
            });

        if (segmentIt != Meta.segments().begin() + SegmentEnd_) {
            YT_VERIFY(Block_);
            column->SetSegmentData(*segmentIt, Block_.Begin() + segmentIt->offset(), tmpBuffers);
            SegmentRowLimit_ = segmentIt->chunk_row_count();
        }
    }

    TColumnSlice GetColumnSlice()
    {
        // TODO(lukyan): Use TRange instead of vector but it is incompatible with protobuf.
        std::vector<NProto::TSegmentMeta> segmentMetas;
        for (size_t index = SegmentStart_; index != SegmentEnd_; ++index) {
            segmentMetas.push_back(Meta.segments(index));
        }

        return TColumnSlice{Block_, MakeSharedRange(segmentMetas)};
    }

    bool NeedUpdateBlock(ui32 rowIndex) const
    {
        return rowIndex >= BlockRowLimit_ && BlockIdIndex_ < BlockIds.size();
    }

    void SetBlock(TSharedRef data, const TRefCountedBlockMetaPtr& blockMeta)
    {
        YT_VERIFY(BlockIdIndex_ < BlockIds.size());
        auto blockId = BlockIds[BlockIdIndex_];

        Block_ = data;

        auto segmentIt = BinarySearch(
            Meta.segments().begin(),
            Meta.segments().end(),
            [&] (auto segmentMetaIt) {
                return segmentMetaIt->block_index() < blockId;
            });

        auto segmentItEnd = BinarySearch(
            segmentIt,
            Meta.segments().end(),
            [&] (auto segmentMetaIt) {
                return segmentMetaIt->block_index() <= blockId;
            });

        SegmentStart_ = segmentIt - Meta.segments().begin();
        SegmentEnd_ = segmentItEnd - Meta.segments().begin();

        BlockRowLimit_ = blockMeta->blocks(blockId).chunk_row_count();
        YT_VERIFY(segmentIt != segmentItEnd);

        auto limitBySegment = (segmentItEnd - 1)->chunk_row_count();
        YT_VERIFY(limitBySegment == BlockRowLimit_);
    }

    // TODO(lukyan): Use block row limits vector instead of blockMeta.
    // Returns block id.
    std::optional<ui32> SkipToBlock(ui32 rowIndex, const TRefCountedBlockMetaPtr& blockMeta)
    {
        if (!NeedUpdateBlock(rowIndex)) {
            return std::nullopt;
        }

        // Need to find block with rowIndex.
        while (BlockIdIndex_ < BlockIds.size() &&
            blockMeta->blocks(BlockIds[BlockIdIndex_]).chunk_row_count() <= rowIndex)
        {
            ++BlockIdIndex_;
        }

        // It is used for generating sentinel rows in lookup (for keys after end of chunk).
        if (BlockIdIndex_ == BlockIds.size()) {
            return std::nullopt;
        }

        YT_VERIFY(BlockIdIndex_ < BlockIds.size());
        return BlockIds[BlockIdIndex_];
    }

    TRange<ui32> GetBlockIds() const
    {
        return BlockIds;
    }

private:
    const TColumnMeta Meta;
    const std::vector<ui32> BlockIds;

    // Blob value data is stored in blocks without capturing in TRowBuffer.
    // Therefore block must not change during from the current call of ReadRows till the next one.
    TSharedRef Block_;

    ui32 SegmentStart_ = 0;
    ui32 SegmentEnd_ = 0;

    ui32 BlockIdIndex_ = 0;
    ui32 BlockRowLimit_ = 0;

    // TODO(lukyan): Do not keep SegmentRowLimit_ here. Use value from IColumnBase.
    ui32 SegmentRowLimit_ = 0;
};

std::vector<TColumnBlockHolder> CreateColumnBlockHolders(
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    TRange<TColumnIdMapping> valueIdMapping)
{
    std::vector<TColumnBlockHolder> columns;
    const auto& columnMetas = chunkMeta->ColumnMeta();

    // Init columns.
    {
        int timestampReaderIndex = chunkMeta->ColumnMeta()->columns().size() - 1;
        auto columnMeta = columnMetas->columns(timestampReaderIndex);
        columns.emplace_back(columnMeta);
    }

    for (size_t index = 0; index < chunkMeta->GetChunkKeyColumnCount(); ++index) {
        auto columnMeta = columnMetas->columns(index);
        columns.emplace_back(columnMeta);
    }

    for (size_t index = 0; index < valueIdMapping.size(); ++index) {
        auto columnMeta = columnMetas->columns(valueIdMapping[index].ChunkSchemaIndex);
        columns.emplace_back(columnMeta);
    }

    return columns;
}

// Rows (and theirs indexes) in chunk are partitioned by block borders.
// Block borders are block last keys and corresponding block last indexes (block.chunk_row_count).
// Window is the range of row indexes between block borders.
// TBlockWindowManager updates blocks in column block holders for each window (range of row indexes between block).
class TBlockWindowManager
{
public:
    TBlockWindowManager(
        std::vector<TColumnBlockHolder> columns,
        TRefCountedBlockMetaPtr blockMeta,
        TBlockFetcherPtr blockFetcher,
        TReaderTimeStatisticsPtr timeStatistics)
        : Columns_(std::move(columns))
        , BlockFetcher_(std::move(blockFetcher))
        , TimeStatistics_(timeStatistics)
        , BlockMeta_(std::move(blockMeta))
    { }

    // Returns true if need wait for ready event.
    bool TryUpdateWindow(ui32 rowIndex)
    {
        TValueIncrementingTimingGuard<TWallTimer> timingGuard(&TimeStatistics_->FetchBlockTime);
        if (FetchedBlocks_) {
            if (!FetchedBlocks_.IsSet()) {
                return true;
            }

            YT_VERIFY(FetchedBlocks_.IsSet());

            const auto& loadedBlocks = FetchedBlocks_.Get().ValueOrThrow();

            size_t index = 0;
            for (auto& column : Columns_) {
                if (column.NeedUpdateBlock(rowIndex)) {
                    YT_VERIFY(index < loadedBlocks.size());
                    column.SetBlock(loadedBlocks[index++].Data, BlockMeta_);
                }
            }
            YT_VERIFY(index == loadedBlocks.size());

            FetchedBlocks_.Reset();
            return false;
        }

        // Skip to window.
        std::vector<TFuture<TBlock>> pendingBlocks;
        for (auto& column : Columns_) {
            if (auto blockId = column.SkipToBlock(rowIndex, BlockMeta_)) {
                YT_VERIFY(column.NeedUpdateBlock(rowIndex));
                pendingBlocks.push_back(BlockFetcher_->FetchBlock(*blockId));
            }
        }

        // Not every window switch causes block updates.
        // Read windows are built by all block last keys but here only reading block set is considered.
        if (pendingBlocks.empty()) {
            return false;
        }

        FetchedBlocks_ = AllSucceeded(std::move(pendingBlocks));
        ReadyEvent_ = FetchedBlocks_.As<void>();

        return true;
    }

protected:
    std::vector<TColumnBlockHolder> Columns_;
    TBlockFetcherPtr BlockFetcher_;
    TFuture<void> ReadyEvent_ = VoidFuture;
    TReaderTimeStatisticsPtr TimeStatistics_;

private:
    const TRefCountedBlockMetaPtr BlockMeta_;
    TFuture<std::vector<TBlock>> FetchedBlocks_;
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedChunkReader
    : protected TBlockWindowManager
{
public:
    TVersionedChunkReader(
        TCachedVersionedChunkMetaPtr chunkMeta,
        std::vector<TColumnBlockHolder> columnBlockHolders,
        TBlockFetcherPtr blockFetcher,
        std::unique_ptr<IRefiner> refiner,
        std::unique_ptr<TVersionedRowsetBuilder> rowsetBuilder,
        std::vector<TSpanMatching>&& windowsList,
        TReaderTimeStatisticsPtr timeStatistics)
        : TBlockWindowManager(
            std::move(columnBlockHolders),
            chunkMeta->BlockMeta(),
            std::move(blockFetcher),
            timeStatistics)
        , ChunkMeta_(std::move(chunkMeta))
        , Refiner_(std::move(refiner))
        , RowsetBuilder_(std::move(rowsetBuilder))
        , ChunkKeyColumnCount_(ChunkMeta_->GetChunkKeyColumnCount())
        , WindowsList_(std::move(windowsList))
    {
        std::reverse(WindowsList_.begin(), WindowsList_.end());
    }

    TFuture<void> GetReadyEvent() const
    {
        return ReadyEvent_;
    }

    bool ReadRows(std::vector<TMutableVersionedRow>* rows, ui32 readCount, TDataWeightStatistics* statistics)
    {
        YT_VERIFY(rows->empty());
        rows->assign(readCount, TMutableVersionedRow());

        RowsetBuilder_->ClearBuffer();
        ui32 offset = 0;

        while (offset < readCount) {
            if (!SpanList_.empty()) {
                // Read rows within window.
                offset += ReadRowsByList(rows->data() + offset, readCount - offset, statistics);
            } else if (WindowsList_.empty()) {
                rows->resize(offset);
                return false;
            } else if (UpdateWindow()) {
                break;
            }
        }

        rows->resize(offset);
        return true;
    }

protected:
    const TCachedVersionedChunkMetaPtr ChunkMeta_;
    const std::unique_ptr<IRefiner> Refiner_;
    const std::unique_ptr<TVersionedRowsetBuilder> RowsetBuilder_;
    const ui32 ChunkKeyColumnCount_;

    TTmpBuffers LocalBuffers_;

    // Vector is used as a stack. No need to keep ReadRangeIndex and RowIndex.
    // Also use stack for WindowsList and do not keep NextWindowIndex.
    std::vector<TSpanMatching> WindowsList_;
    std::vector<TReadSpan> SpanList_;

    // Returns true if need wait for ready event.
    bool UpdateWindow()
    {
        YT_VERIFY(!WindowsList_.empty());
        if (TryUpdateWindow(WindowsList_.back().Chunk.Lower)) {
            return true;
        }

        {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&TimeStatistics_->BuildRangesTime);
            YT_VERIFY(SpanList_.empty());

            // Update read list.
            std::vector<TColumnSlice> columnSlices;
            for (size_t index = 0; index < ChunkKeyColumnCount_; ++index) {
                // Skip timestamp column.
                columnSlices.push_back(Columns_[index + 1].GetColumnSlice());
            }

            SpanList_ = Refiner_->BuildReadListForWindow(columnSlices, WindowsList_.back());
            std::reverse(SpanList_.begin(), SpanList_.end());

            WindowsList_.pop_back();
        }

        return false;
    }

    void UpdateSegments(ui32 startRowIndex)
    {
        // TODO(lukyan): Return limit from SkipToSegment?
        TValueIncrementingTimingGuard<TWallTimer> timingGuard(&TimeStatistics_->DecodeSegmentTime);

        // Adjust segments for column readers within blocks.

        // Init timestamp column.
        Columns_[0].SkipToSegment(startRowIndex, RowsetBuilder_.get(), &LocalBuffers_);

        YT_VERIFY(ChunkKeyColumnCount_ <= RowsetBuilder_->GetKeyColumnCount());

        // Key columns after ChunkKeyColumnCount_ are left nullable.
        auto keyColumns = RowsetBuilder_->GetKeyColumns();
        for (size_t index = 0; index < ChunkKeyColumnCount_; ++index) {
            // TODO(lukyan): Return true if segment updated. Reset position in this case.
            Columns_[1 + index].SkipToSegment(
                startRowIndex,
                keyColumns[index].get(),
                &LocalBuffers_);
        }

        auto valueColumns = RowsetBuilder_->GetValueColumns();
        for (size_t index = 0; index < valueColumns.size(); ++index) {
            Columns_[1 + ChunkKeyColumnCount_ + index].SkipToSegment(
                startRowIndex,
                valueColumns[index].get(),
                &LocalBuffers_);
        }
    }

    // Returns read row count.
    ui32 ReadRowsByList(TMutableVersionedRow* rows, ui32 readCount, TDataWeightStatistics* statistics)
    {
        UpdateSegments(SpanList_.back().Lower);

        TValueIncrementingTimingGuard<TWallTimer> timingGuard(&TimeStatistics_->DoReadTime);

        ui32 segmentRowLimit = RowsetBuilder_->GetSegmentRowLimit(); // Timestamp column segment limit.
        segmentRowLimit = RowsetBuilder_->GetKeySegmentsRowLimit(segmentRowLimit);
        segmentRowLimit = RowsetBuilder_->GetValueSegmentsRowLimit(segmentRowLimit);

        ui32 leftCount = readCount;

        auto rangesIt = SpanList_.rbegin();

        // Ranges are limited by segmentRowLimit and readCountLimit

        // Last range [.........]
        //                ^       ^
        //                |       readCountLimit = leftCount + lower
        //                segmentRowLimit

        // Last range [.........]
        //               ^          ^
        //  readCountLimit          |
        //            segmentRowLimit

        // Last range [.........]
        //                ^   ^
        //                |   readCountLimit
        //                segmentRowLimit

        while (rangesIt != SpanList_.rend() && rangesIt->Upper <= segmentRowLimit) {
            auto [lower, upper] = *rangesIt;
            // One for sentinel row.
            auto spanLength = lower != upper ? upper - lower : 1;

            if (spanLength > leftCount) {
                break;
            }

            leftCount -= spanLength;
            ++rangesIt;
        }

        // TODO(lukyan): Do not copy spans. Split them inplace.
        auto spanCount = rangesIt - SpanList_.rbegin();
        auto spanBatch = RowsetBuilder_->Allocate<TReadSpan>(spanCount + 1);

        // Pop back items from rangesIt till end.
        std::copy(SpanList_.rbegin(), rangesIt, spanBatch);
        SpanList_.resize(SpanList_.size() - spanCount);

        if (rangesIt != SpanList_.rend() && rangesIt->Lower < segmentRowLimit && leftCount > 0) {
            auto& [lower, upper] = *rangesIt;
            auto split = std::min(lower + leftCount, segmentRowLimit);
            YT_VERIFY(split < upper);

            leftCount -= split - lower;
            spanBatch[spanCount++] = {lower, split};
            lower = split;
        }

        auto gaps = RowsetBuilder_->Allocate<ui32>(spanCount);

        auto spanEnd = BuildGapIndexes(spanBatch, spanBatch + spanCount, gaps);
        auto gapCount = spanBatch + spanCount - spanEnd;

        // Now all spans are not empty.
        RowsetBuilder_->ReadRows(rows + gapCount, MakeRange(spanBatch, spanEnd), statistics);

        // Insert sentinel rows.
        InsertGaps(MakeRange(gaps, gapCount), rows, gapCount);

        return readCount - leftCount;
    }

    TReadSpan* BuildGapIndexes(TReadSpan* it, TReadSpan* end, ui32* gaps)
    {
        ui32 offset = 0;
        auto* spanDest = it;

        for (; it != end; ++it) {
            auto batchSize = it->Upper - it->Lower;

            if (batchSize == 0) {
                *gaps++ = offset;
            } else {
                *spanDest++ = *it;
                offset += batchSize;
            }
        }

        return spanDest;
    }

    void InsertGaps(TRange<ui32> gaps, TMutableVersionedRow* rows, ui32 sourceOffset)
    {
        auto destRows = rows;
        rows = rows + sourceOffset;
        sourceOffset = 0;

        for (auto gap : gaps) {
            if (sourceOffset < gap) {
                destRows = std::move(rows + sourceOffset, rows + gap, destRows);
                sourceOffset = gap;
            }
            *destRows++ = TMutableVersionedRow();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// TReaderWrapper implements IVersionedReader interface.
// It calculates performance counters and provides data and decompression statistics.
class TReaderWrapper
    : public IVersionedReader
    , public TVersionedChunkReader
{
public:
    TReaderWrapper(
        TCachedVersionedChunkMetaPtr chunkMeta,
        std::vector<TColumnBlockHolder> columnBlockHolders,
        TBlockFetcherPtr blockFetcher,
        std::unique_ptr<IRefiner> refiner,
        std::unique_ptr<TVersionedRowsetBuilder> rowsetBuilder,
        std::vector<TSpanMatching>&& windowsList,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        bool lookup,
        TReaderTimeStatisticsPtr timeStatistics)
        : TVersionedChunkReader(
            std::move(chunkMeta),
            std::move(columnBlockHolders),
            std::move(blockFetcher),
            std::move(refiner),
            std::move(rowsetBuilder),
            std::move(windowsList),
            std::move(timeStatistics))
        , PerformanceCounters_(std::move(performanceCounters))
        , Lookup_(lookup)
        , HasHunkColumns_(ChunkMeta_->GetSchema()->HasHunkColumns())
    { }

    virtual TFuture<void> Open() override
    {
        return VoidFuture;
    }

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        YT_VERIFY(options.MaxRowsPerRead > 0);

        std::vector<TMutableVersionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        bool hasMore = Read(&rows);
        if (!hasMore) {
            YT_VERIFY(rows.empty());
            return nullptr;
        }

        if (rows.empty()) {
            return CreateEmptyVersionedRowBatch();
        }

        auto* rowsData = rows.data();
        auto rowsSize = rows.size();
        return CreateBatchFromVersionedRows(MakeSharedRange(
            TRange<TVersionedRow>(rowsData, rowsSize),
            std::move(rows),
            MakeStrong(this)));
    }

    virtual TFuture<void> GetReadyEvent() const
    {
        return TVersionedChunkReader::GetReadyEvent();
    }

    // TODO(lukyan): Provide statistics object to BlockFetcher.
    virtual TDataStatistics GetDataStatistics() const override
    {
        if (!BlockFetcher_) {
            return TDataStatistics();
        }

        TDataStatistics dataStatistics;
        dataStatistics.set_chunk_count(1);
        dataStatistics.set_uncompressed_data_size(BlockFetcher_->GetUncompressedDataSize());
        dataStatistics.set_compressed_data_size(BlockFetcher_->GetCompressedDataSize());
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return BlockFetcher_
            ? TCodecStatistics().Append(BlockFetcher_->GetDecompressionTime())
            : TCodecStatistics();
    }

    virtual bool IsFetchingCompleted() const override
    {
        if (!BlockFetcher_) {
            return true;
        }

        return BlockFetcher_->IsFetchingCompleted();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

private:
    const TChunkReaderPerformanceCountersPtr PerformanceCounters_;
    // TODO(lukyan): Use performance counters adapter and increment counters uniformly and remove this flag.
    const bool Lookup_;
    const bool HasHunkColumns_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;


    bool Read(std::vector<TMutableVersionedRow>* rows)
    {
        TDataWeightStatistics dataWeightStatistics;

        rows->clear();
        bool hasMore = ReadRows(
            rows,
            rows->capacity(),
            &dataWeightStatistics);

        if (HasHunkColumns_) {
            auto* pool = RowsetBuilder_->GetPool();
            for (auto row : *rows) {
                GlobalizeHunkValues(pool, ChunkMeta_, row);
            }
        }

        i64 rowCount = static_cast<i64>(rows->size());

        RowCount_ += rowCount;
        DataWeight_ += dataWeightStatistics.Count;

        if (Lookup_) {
            PerformanceCounters_->StaticChunkRowLookupCount += rowCount;
            PerformanceCounters_->StaticChunkRowLookupDataWeightCount += dataWeightStatistics.Count;
        } else {
            PerformanceCounters_->StaticChunkRowReadCount += rowCount;
            PerformanceCounters_->StaticChunkRowReadDataWeightCount += dataWeightStatistics.Count;
        }

        if (hasMore) {
            return true;
        }

        // Actually does not have more but caller expect true if rows are not empty.
        return !rows->empty();
    }
};

bool IsKeys(const TSharedRange<TRowRange>&)
{
    return false;
}

bool IsKeys(const TSharedRange<TLegacyKey>&)
{
    return true;
}

template <class TItem>
IVersionedReaderPtr CreateVersionedChunkReader(
    TSharedRange<TItem> readItems,
    TTimestamp timestamp,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TColumnFilter& columnFilter,
    IBlockCachePtr blockCache,
    const TChunkReaderConfigPtr config,
    IChunkReaderPtr underlyingReader,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    const TClientChunkReadOptions& chunkReadOptions,
    bool produceAll,
    TReaderTimeStatisticsPtr timeStatistics)
{
    if (!timeStatistics) {
        timeStatistics = New<TReaderTimeStatistics>();
    }

    auto keyTypes = GetKeyTypes(chunkMeta);
    auto refiner = MakeRefiner(readItems, keyTypes);
    auto windowsList = refiner->BuildReadWindows(chunkMeta);
    auto schemaIdMapping = BuildVersionedSimpleSchemaIdMapping(columnFilter, chunkMeta);
    auto columnBlockHolders = CreateColumnBlockHolders(chunkMeta, schemaIdMapping);

    TBlockFetcherPtr blockFetcher;
    if (!windowsList.empty()) {
        std::vector<TRange<ui32>> columnsBlockIds;
        for (const auto& column : columnBlockHolders) {
            columnsBlockIds.push_back(column.GetBlockIds());
        }

        auto blockInfos = BuildBlockInfos(columnsBlockIds, windowsList, chunkMeta);

        auto memoryManager = New<TChunkReaderMemoryManager>(TChunkReaderMemoryManagerOptions(config->WindowSize));

        blockFetcher = New<TBlockFetcher>(
            config,
            std::move(blockInfos),
            memoryManager,
            underlyingReader,
            blockCache,
            CheckedEnumCast<NCompression::ECodec>(chunkMeta->Misc().compression_codec()),
            static_cast<double>(chunkMeta->Misc().compressed_data_size()) / chunkMeta->Misc().uncompressed_data_size(),
            chunkReadOptions);
    }

    auto valueSchema = GetValueTypes(chunkMeta, schemaIdMapping);

    const auto& Logger = NTableClient::TableClientLogger;

    YT_LOG_DEBUG("Creating rowset builder (KeyTypes: %v, ValueTypes: %v)",
        MakeFormattableView(keyTypes, [] (TStringBuilderBase* builder, EValueType type) {
            builder->AppendFormat("%v", type);
        }),
        MakeFormattableView(valueSchema, [] (TStringBuilderBase* builder, TValueSchema valueSchema) {
            builder->AppendFormat("%v", valueSchema.Type);
        }));

    auto rowBuilder = CreateVersionedRowsetBuilder(keyTypes, valueSchema, timestamp, produceAll);

    return New<TReaderWrapper>(
        std::move(chunkMeta),
        std::move(columnBlockHolders),
        std::move(blockFetcher),
        std::move(refiner),
        std::move(rowBuilder),
        std::move(windowsList),
        std::move(performanceCounters),
        IsKeys(readItems),
        timeStatistics);
}

template
IVersionedReaderPtr CreateVersionedChunkReader<TRowRange>(
    TSharedRange<TRowRange> readItems,
    TTimestamp timestamp,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TColumnFilter& columnFilter,
    IBlockCachePtr blockCache,
    const TChunkReaderConfigPtr config,
    IChunkReaderPtr underlyingReader,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    const TClientChunkReadOptions& chunkReadOptions,
    bool produceAll,
    TReaderTimeStatisticsPtr timeStatistics);

template
IVersionedReaderPtr CreateVersionedChunkReader<TLegacyKey>(
    TSharedRange<TLegacyKey> readItems,
    TTimestamp timestamp,
    TCachedVersionedChunkMetaPtr chunkMeta,
    const TColumnFilter& columnFilter,
    IBlockCachePtr blockCache,
    const TChunkReaderConfigPtr config,
    IChunkReaderPtr underlyingReader,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    const TClientChunkReadOptions& chunkReadOptions,
    bool produceAll,
    TReaderTimeStatisticsPtr timeStatistics);

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TRowRange> ClipRanges(
    TSharedRange<TRowRange> ranges,
    TUnversionedRow lower,
    TUnversionedRow upper,
    THolderPtr holder)
{
    auto startIt = ranges.begin();
    auto endIt = ranges.end();

    if (lower) {
        startIt = BinarySearch(startIt, endIt, [&] (auto it) {
            return it->second <= lower;
        });
    }

    if (upper) {
        endIt = BinarySearch(startIt, endIt, [&] (auto it) {
            return it->first < upper;
        });
    }

    if (startIt != endIt) {
        std::vector<TRowRange> items(startIt, endIt);
        if (lower && lower > items.front().first) {
            items.front().first = lower;
        }

        if (upper && upper < items.back().second) {
            items.back().second = upper;
        }

        return MakeSharedRange(std::move(items), ranges.GetHolder(), holder);
    } else {
        return TSharedRange<TRowRange>(); // Empty ranges.
    }
}

TSharedRange<TRowRange> ConvertLegacyRanges(
    NTableClient::TLegacyOwningKey lowerLimit,
    NTableClient::TLegacyOwningKey upperLimit)
{
    // Workaround for case with invalid read limits (lower is greater than upper: [0#1, 1#<Min>] .. [0#1])
    // Test: test_traverse_table_with_alter_and_ranges_stress
    return MakeSingletonRowRange(lowerLimit, std::max(lowerLimit, upperLimit));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

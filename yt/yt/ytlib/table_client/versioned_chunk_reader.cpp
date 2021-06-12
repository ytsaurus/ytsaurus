#include "cached_versioned_chunk_meta.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader_base.h"
#include "chunk_state.h"
#include "columnar_chunk_reader_base.h"
#include "config.h"
#include "private.h"
#include "schemaless_multi_chunk_reader.h"
#include "versioned_block_reader.h"
#include "versioned_chunk_reader.h"
#include "versioned_reader_adapter.h"
#include "hunks.h"

#include <yt/yt/ytlib/chunk_client/block_cache.h>
#include <yt/yt/ytlib/chunk_client/block_id.h>
#include <yt/yt/ytlib/chunk_client/cache_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/block_fetcher.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/data_statistics.pb.h>
#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_reader.h>
#include <yt/yt/client/table_client/schemaful_reader_adapter.h>

#include <yt/yt/ytlib/table_chunk_format/column_reader.h>
#include <yt/yt/ytlib/table_chunk_format/timestamp_reader.h>
#include <yt/yt/ytlib/table_chunk_format/null_column_reader.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/compression/codec.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableChunkFormat;
using namespace NTableChunkFormat::NProto;

using NChunkClient::TReadLimit;
using NChunkClient::TReadRange;
using NChunkClient::TDataSliceDescriptor;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 CacheSize = 32_KBs;
static constexpr i64 MinRowsPerRead = 32;

////////////////////////////////////////////////////////////////////////////////

std::vector<TColumnIdMapping> BuildVersionedSimpleSchemaIdMapping(
    const TColumnFilter& columnFilter,
    const TCachedVersionedChunkMetaPtr& chunkMeta)
{
    if (columnFilter.IsUniversal()) {
        return chunkMeta->SchemaIdMapping();
    }

    std::vector<TColumnIdMapping> schemaIdMapping;
    schemaIdMapping.reserve(chunkMeta->SchemaIdMapping().size());
    for (auto index : columnFilter.GetIndexes()) {
        if (index < chunkMeta->GetKeyColumnCount()) {
            continue;
        }

        for (const auto& mapping : chunkMeta->SchemaIdMapping()) {
            if (mapping.ReaderSchemaIndex == index) {
                schemaIdMapping.push_back(mapping);
                break;
            }
        }
    }

    return schemaIdMapping;
}

std::vector<TColumnIdMapping> BuildSchemalessHorizontalSchemaIdMapping(
    const TColumnFilter& columnFilter,
    const TCachedVersionedChunkMetaPtr& chunkMeta)
{
    int keyColumnCount = chunkMeta->GetChunkKeyColumnCount();
    std::vector<TColumnIdMapping> idMapping;
    idMapping.resize(
        keyColumnCount + chunkMeta->SchemaIdMapping().size(),
        TColumnIdMapping{-1,-1});

    for (int index = 0; index < keyColumnCount; ++index) {
        idMapping[index].ReaderSchemaIndex = index;
    }

    if (columnFilter.IsUniversal()) {
        for (const auto& mapping : chunkMeta->SchemaIdMapping()) {
            YT_VERIFY(mapping.ChunkSchemaIndex < std::ssize(idMapping));
            YT_VERIFY(mapping.ChunkSchemaIndex >= keyColumnCount);
            idMapping[mapping.ChunkSchemaIndex].ReaderSchemaIndex = mapping.ReaderSchemaIndex;
        }
    } else {
        for (auto index : columnFilter.GetIndexes()) {
            for (const auto& mapping : chunkMeta->SchemaIdMapping()) {
                if (mapping.ReaderSchemaIndex == index) {
                    YT_VERIFY(mapping.ChunkSchemaIndex < std::ssize(idMapping));
                    YT_VERIFY(mapping.ChunkSchemaIndex >= keyColumnCount);
                    idMapping[mapping.ChunkSchemaIndex].ReaderSchemaIndex = mapping.ReaderSchemaIndex;
                    break;
                }
            }
        }
    }

    return idMapping;
}

////////////////////////////////////////////////////////////////////////////////

struct TVersionedChunkReaderPoolTag { };

class TSimpleVersionedChunkReaderBase
    : public IVersionedReader
    , public TChunkReaderBase
{
public:
    TSimpleVersionedChunkReaderBase(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        const TClientChunkReadOptions& chunkReadOptions,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TChunkReaderMemoryManagerPtr& memoryManager,
        TKeyComparer keyComparer = [] (auto lhs, auto rhs) {
            return CompareRows(lhs, rhs);
        });

    virtual TFuture<void> Open() override
    {
        return ReadyEvent();
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = TChunkReaderBase::GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

protected:
    const TCachedVersionedChunkMetaPtr ChunkMeta_;
    const TTimestamp Timestamp_;
    const bool ProduceAllVersions_;
    const TKeyComparer KeyComparer_;

    const std::vector<TColumnIdMapping> SchemaIdMapping_;

    std::unique_ptr<TSimpleVersionedBlockReader> BlockReader_;

    TChunkedMemoryPool MemoryPool_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    TChunkReaderPerformanceCountersPtr PerformanceCounters_;
};

TSimpleVersionedChunkReaderBase::TSimpleVersionedChunkReaderBase(
    TChunkReaderConfigPtr config,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr blockCache,
    const TClientChunkReadOptions& chunkReadOptions,
    const TColumnFilter& columnFilter,
    TChunkReaderPerformanceCountersPtr performanceCounters,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TChunkReaderMemoryManagerPtr& memoryManager,
    TKeyComparer keyComparer)
    : TChunkReaderBase(
        std::move(config),
        std::move(underlyingReader),
        std::move(blockCache),
        chunkReadOptions,
        memoryManager)
    , ChunkMeta_(std::move(chunkMeta))
    , Timestamp_(timestamp)
    , ProduceAllVersions_(produceAllVersions)
    , KeyComparer_(std::move(keyComparer))
    , SchemaIdMapping_(BuildVersionedSimpleSchemaIdMapping(columnFilter, ChunkMeta_))
    , MemoryPool_(TVersionedChunkReaderPoolTag())
    , PerformanceCounters_(std::move(performanceCounters))
{
    YT_VERIFY(ChunkMeta_->Misc().sorted());
    YT_VERIFY(ChunkMeta_->GetChunkType() == EChunkType::Table);
    YT_VERIFY(ChunkMeta_->GetChunkFormat() == EChunkFormat::TableVersionedSimple);
    YT_VERIFY(Timestamp_ != AllCommittedTimestamp || columnFilter.IsUniversal());
    YT_VERIFY(PerformanceCounters_);
}

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedRangeChunkReader
    : public TSimpleVersionedChunkReaderBase
{
public:
    TSimpleVersionedRangeChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        const TClientChunkReadOptions& chunkReadOptions,
        TSharedRange<TRowRange> ranges,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TSharedRange<TRowRange>& singletonClippingRange,
        const TChunkReaderMemoryManagerPtr& memoryManager)
        : TSimpleVersionedChunkReaderBase(
            std::move(config),
            std::move(chunkMeta),
            std::move(underlyingReader),
            std::move(blockCache),
            chunkReadOptions,
            columnFilter,
            std::move(performanceCounters),
            timestamp,
            produceAllVersions,
            memoryManager)
        , Ranges_(std::move(ranges))
        , ClippingRange_(singletonClippingRange)
    {
        SetReadyEvent(DoOpen(GetBlockSequence(), ChunkMeta_->Misc()));
    }

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        YT_VERIFY(options.MaxRowsPerRead > 0);

        std::vector<TVersionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        MemoryPool_.Clear();

        if (RangeIndex_ >= Ranges_.Size()) {
            return nullptr;
        }

        if (!BeginRead()) {
            // Not ready yet.
            return CreateEmptyVersionedRowBatch();
        }

        if (!BlockReader_) {
            // Nothing to read from chunk.
            return nullptr;
        }

        if (BlockEnded_) {
            BlockReader_.reset();
            return OnBlockEnded()
                ? CreateEmptyVersionedRowBatch()
                : nullptr;
        }

        i64 rowCount = 0;
        i64 dataWeight = 0;

        auto hasHunkColumns = ChunkMeta_->GetSchema()->HasHunkColumns();

        while (rows.size() < rows.capacity()) {
            if (CheckKeyLimit_ && KeyComparer_(
                BlockReader_->GetKey(), GetCurrentRangeUpperKey()) >= 0)
            {
                if (++RangeIndex_ < Ranges_.Size()) {
                    if (!BlockReader_->SkipToKey(GetCurrentRangeLowerKey())) {
                        BlockEnded_ = true;
                        break;
                    } else {
                        continue;
                    }
                } else {
                    // TODO(lukyan): return false and fix usages of method Read
                    break;
                }
            }

            auto row = BlockReader_->GetRow(&MemoryPool_);
            if (row) {
                YT_ASSERT(
                    rows.empty() ||
                    !rows.back() ||
                    CompareRows(
                        rows.back().BeginKeys(), rows.back().EndKeys(),
                        row.BeginKeys(), row.EndKeys()) < 0);
                if (hasHunkColumns) {
                    GlobalizeHunkValues(&MemoryPool_, ChunkMeta_, row);
                }
            }
            rows.push_back(row);
            ++rowCount;
            dataWeight += GetDataWeight(row);

            if (!BlockReader_->NextRow()) {
                BlockEnded_ = true;
                break;
            }
        }

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;
        PerformanceCounters_->StaticChunkRowReadCount += rowCount;
        PerformanceCounters_->StaticChunkRowReadDataWeightCount += dataWeight;

        return CreateBatchFromVersionedRows(MakeSharedRange(rows, MakeStrong(this)));
    }

private:
    std::vector<size_t> BlockIndexes_;
    size_t NextBlockIndex_ = 0;
    TSharedRange<TRowRange> Ranges_;
    size_t RangeIndex_ = 0;
    TSharedRange<TRowRange> ClippingRange_;

    const TLegacyKey& GetCurrentRangeLowerKey() const
    {
        return GetRangeLowerKey(RangeIndex_);
    }

    const TLegacyKey& GetCurrentRangeUpperKey() const
    {
        return GetRangeUpperKey(RangeIndex_);
    }

    const TLegacyKey& GetRangeLowerKey(int rangeIndex) const
    {
        if (rangeIndex == 0 && ClippingRange_) {
            if (const auto& clippingLowerBound = ClippingRange_.Front().first) {
                return std::max(clippingLowerBound, Ranges_[rangeIndex].first);
            }
        }
        return Ranges_[rangeIndex].first;
    }

    const TLegacyKey& GetRangeUpperKey(int rangeIndex) const
    {
        if (rangeIndex + 1 == std::ssize(Ranges_) && ClippingRange_) {
            if (const auto& clippingUpperBound = ClippingRange_.Front().second) {
                return std::min(clippingUpperBound, Ranges_[rangeIndex].second);
            }
        }
        return Ranges_[rangeIndex].second;
    }

    std::vector<TBlockFetcher::TBlockInfo> GetBlockSequence()
    {
        const auto& blockMetaExt = ChunkMeta_->BlockMeta();
        const auto& blockIndexKeys = ChunkMeta_->LegacyBlockLastKeys();

        std::vector<TBlockFetcher::TBlockInfo> blocks;

        int rangeIndex = 0;
        auto blocksIt = blockIndexKeys.begin();

        while (rangeIndex != std::ssize(Ranges_)) {
            blocksIt = std::lower_bound(
                blocksIt,
                blockIndexKeys.end(),
                GetRangeLowerKey(rangeIndex));

            auto blockKeysEnd = std::lower_bound(
                blocksIt,
                blockIndexKeys.end(),
                GetRangeUpperKey(rangeIndex));

            if (blockKeysEnd != blockIndexKeys.end()) {
                auto saved = rangeIndex;
                rangeIndex = BinarySearch(
                    rangeIndex,
                    static_cast<int>(Ranges_.size()),
                    [&] (int index) {
                        return GetRangeUpperKey(index) <= *blockKeysEnd;
                    });

                ++blockKeysEnd;
                YT_VERIFY(rangeIndex > saved);
            } else {
                ++rangeIndex;
            }

            for (auto it = blocksIt; it < blockKeysEnd; ++it) {
                int blockIndex = std::distance(blockIndexKeys.begin(), it);
                BlockIndexes_.push_back(blockIndex);
                auto& blockMeta = blockMetaExt->blocks(blockIndex);
                int priority = blocks.size();
                blocks.push_back(TBlockFetcher::TBlockInfo{blockIndex, blockMeta.uncompressed_size(), priority});
            }

            blocksIt = blockKeysEnd;
        }

        return blocks;
    }

    void LoadBlock()
    {
        int chunkBlockIndex = BlockIndexes_[NextBlockIndex_];
        CheckBlockUpperKeyLimit(
            ChunkMeta_->LegacyBlockLastKeys()[chunkBlockIndex],
            GetCurrentRangeUpperKey(),
            ChunkMeta_->GetKeyColumnCount());

        YT_VERIFY(CurrentBlock_ && CurrentBlock_.IsSet());
        BlockReader_.reset(new TSimpleVersionedBlockReader(
            CurrentBlock_.Get().ValueOrThrow().Data,
            ChunkMeta_->BlockMeta()->blocks(chunkBlockIndex),
            ChunkMeta_->GetChunkSchema(),
            ChunkMeta_->GetChunkKeyColumnCount(),
            ChunkMeta_->GetKeyColumnCount(),
            SchemaIdMapping_,
            KeyComparer_,
            Timestamp_,
            ProduceAllVersions_,
            true));
    }

    virtual void InitFirstBlock() override
    {
        InitNextBlock();
    }

    virtual void InitNextBlock() override
    {
        LoadBlock();
        YT_VERIFY(BlockReader_->SkipToKey(GetCurrentRangeLowerKey()));
        ++NextBlockIndex_;
    }

};

////////////////////////////////////////////////////////////////////////////////

class TSimpleVersionedLookupChunkReader
    : public TSimpleVersionedChunkReaderBase
{
public:
    TSimpleVersionedLookupChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr blockCache,
        const TClientChunkReadOptions& chunkReadOptions,
        const TSharedRange<TLegacyKey>& keys,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TKeyComparer keyComparer,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TChunkReaderMemoryManagerPtr& memoryManager)
        : TSimpleVersionedChunkReaderBase(
            std::move(config),
            std::move(chunkMeta),
            std::move(underlyingReader),
            std::move(blockCache),
            chunkReadOptions,
            columnFilter,
            std::move(performanceCounters),
            timestamp,
            produceAllVersions,
            memoryManager,
            std::move(keyComparer))
        , Keys_(keys)
        , KeyFilterTest_(Keys_.Size(), true)
    {
        SetReadyEvent(DoOpen(GetBlockSequence(), ChunkMeta_->Misc()));
    }

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        auto readGuard = AcquireReadGuard();

        YT_VERIFY(options.MaxRowsPerRead > 0);

        std::vector<TVersionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        MemoryPool_.Clear();

        if (!BeginRead()) {
            // Not ready yet.
            return CreateEmptyVersionedRowBatch();
        }

        i64 rowCount = 0;
        i64 dataWeight = 0;

        if (!BlockReader_) {
            // Nothing to read from chunk.
            if (KeyIndex_ == std::ssize(Keys_)) {
                return nullptr;
            }

            while (rows.size() < rows.capacity() && KeyIndex_ < std::ssize(Keys_)) {
                rows.push_back(TVersionedRow());
                ++KeyIndex_;
            }

            PerformanceCounters_->StaticChunkRowLookupCount += rowCount;
            PerformanceCounters_->StaticChunkRowLookupDataWeightCount += dataWeight;

            return CreateBatchFromVersionedRows(MakeSharedRange(rows, MakeStrong(this)));
        }

        if (BlockEnded_) {
            BlockReader_.reset();
            OnBlockEnded();
            return CreateBatchFromVersionedRows(MakeSharedRange(rows, MakeStrong(this)));
        }

        auto hasHunkColumns = ChunkMeta_->GetSchema()->HasHunkColumns();

        while (rows.size() < rows.capacity()) {
            if (KeyIndex_ == std::ssize(Keys_)) {
                BlockEnded_ = true;
                break;
            }

            if (!KeyFilterTest_[KeyIndex_]) {
                rows.push_back(TVersionedRow());
                ++PerformanceCounters_->StaticChunkRowLookupTrueNegativeCount;
            } else {
                const auto& key = Keys_[KeyIndex_];
                if (!BlockReader_->SkipToKey(key)) {
                    BlockEnded_ = true;
                    break;
                }

                if (key == BlockReader_->GetKey()) {
                    auto row = BlockReader_->GetRow(&MemoryPool_);
                    if (hasHunkColumns) {
                        GlobalizeHunkValues(&MemoryPool_, ChunkMeta_, row);
                    }
                    rows.push_back(row);
                    ++KeyIndex_;
                    ++rowCount;
                    dataWeight += GetDataWeight(rows.back());
                } else if (BlockReader_->GetKey() > key) {
                    auto nextKeyIt = std::lower_bound(
                        Keys_.begin() + KeyIndex_,
                        Keys_.end(),
                        BlockReader_->GetKey());

                    size_t skippedKeys = std::distance(Keys_.begin() + KeyIndex_, nextKeyIt);
                    skippedKeys = std::min(skippedKeys, rows.capacity() - rows.size());

                    rows.insert(rows.end(), skippedKeys, TVersionedRow());
                    KeyIndex_ += skippedKeys;
                    dataWeight += skippedKeys * GetDataWeight(TVersionedRow());
                } else {
                    YT_ABORT();
                }
            }
        }

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;
        PerformanceCounters_->StaticChunkRowLookupCount += rowCount;
        PerformanceCounters_->StaticChunkRowLookupDataWeightCount += dataWeight;

        return CreateBatchFromVersionedRows(MakeSharedRange(rows, MakeStrong(this)));
    }

private:
    const TSharedRange<TLegacyKey> Keys_;

    std::vector<bool> KeyFilterTest_;
    std::vector<int> BlockIndexes_;

    int NextBlockIndex_ = 0;
    i64 KeyIndex_ = 0;

    std::vector<TBlockFetcher::TBlockInfo> GetBlockSequence()
    {
        const auto& blockMetaExt = ChunkMeta_->BlockMeta();
        const auto& blockIndexKeys = ChunkMeta_->LegacyBlockLastKeys();

        std::vector<TBlockFetcher::TBlockInfo> blocks;

        auto keyIt = Keys_.begin();
        auto blocksIt = blockIndexKeys.begin();

        while (keyIt != Keys_.end()) {
            blocksIt = std::lower_bound(
                blocksIt,
                blockIndexKeys.end(),
                *keyIt);

            if (blocksIt != blockIndexKeys.end()) {
                auto saved = keyIt;
                keyIt = std::upper_bound(keyIt, Keys_.end(), *blocksIt);
                YT_VERIFY(keyIt > saved);

                int blockIndex = std::distance(blockIndexKeys.begin(), blocksIt);
                BlockIndexes_.push_back(blockIndex);
                auto& blockMeta = blockMetaExt->blocks(blockIndex);
                int priority = blocks.size();
                blocks.push_back(TBlockFetcher::TBlockInfo{blockIndex, blockMeta.uncompressed_size(), priority});

                ++blocksIt;
            } else {
                break;
            }
        }

        return blocks;
    }

    virtual void InitFirstBlock() override
    {
        InitNextBlock();
    }

    virtual void InitNextBlock() override
    {
        int chunkBlockIndex = BlockIndexes_[NextBlockIndex_];
        BlockReader_.reset(new TSimpleVersionedBlockReader(
            CurrentBlock_.Get().ValueOrThrow().Data,
            ChunkMeta_->BlockMeta()->blocks(chunkBlockIndex),
            ChunkMeta_->GetChunkSchema(),
            ChunkMeta_->GetChunkKeyColumnCount(),
            ChunkMeta_->GetKeyColumnCount(),
            SchemaIdMapping_,
            KeyComparer_,
            Timestamp_,
            ProduceAllVersions_,
            true));
        ++NextBlockIndex_;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TBase>
class TColumnarVersionedChunkReaderBase
    : public TBase
    , public IVersionedReader
{
public:
    TColumnarVersionedChunkReaderBase(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        const TSortColumns& sortColumns,
        IBlockCachePtr blockCache,
        const TClientChunkReadOptions& chunkReadOptions,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp,
        const TChunkReaderMemoryManagerPtr& memoryManager)
        : TBase(
            chunkMeta,
            std::move(config),
            std::move(underlyingReader),
            sortColumns,
            std::move(blockCache),
            chunkReadOptions,
            [] (int) { YT_ABORT(); }, // Rows should not be skipped in versioned reader.
            memoryManager)
        , ChunkMeta_(std::move(chunkMeta))
        , Timestamp_(timestamp)
        , SchemaIdMapping_(BuildVersionedSimpleSchemaIdMapping(columnFilter, ChunkMeta_))
        , PerformanceCounters_(std::move(performanceCounters))
    {
        YT_VERIFY(ChunkMeta_->Misc().sorted());
        YT_VERIFY(ChunkMeta_->GetChunkType() == EChunkType::Table);
        YT_VERIFY(ChunkMeta_->GetChunkFormat() == EChunkFormat::TableVersionedColumnar);
        YT_VERIFY(Timestamp_ != AllCommittedTimestamp || columnFilter.IsUniversal());
        YT_VERIFY(PerformanceCounters_);

        KeyColumnReaders_.resize(ChunkMeta_->GetKeyColumnCount());
        for (int keyColumnIndex = 0;
             keyColumnIndex < ChunkMeta_->GetChunkKeyColumnCount();
             ++keyColumnIndex)
        {
            auto columnReader = CreateUnversionedColumnReader(
                ChunkMeta_->GetChunkSchema()->Columns()[keyColumnIndex],
                ChunkMeta_->ColumnMeta()->columns(keyColumnIndex),
                keyColumnIndex,
                keyColumnIndex,
                ESortOrder::Ascending);
            KeyColumnReaders_[keyColumnIndex] = columnReader.get();
            TBase::Columns_.emplace_back(std::move(columnReader), keyColumnIndex);
        }

        // Null readers for wider keys.
        for (int keyColumnIndex = ChunkMeta_->GetChunkKeyColumnCount();
             keyColumnIndex < std::ssize(KeyColumnReaders_);
             ++keyColumnIndex)
        {
            auto columnReader = CreateBlocklessUnversionedNullColumnReader(
                keyColumnIndex,
                keyColumnIndex,
                ESortOrder::Ascending);
            KeyColumnReaders_[keyColumnIndex] = columnReader.get();
            TBase::Columns_.emplace_back(std::move(columnReader));
        }

        for (const auto& idMapping : SchemaIdMapping_) {
            auto columnReader = CreateVersionedColumnReader(
                ChunkMeta_->GetChunkSchema()->Columns()[idMapping.ChunkSchemaIndex],
                ChunkMeta_->ColumnMeta()->columns(idMapping.ChunkSchemaIndex),
                idMapping.ReaderSchemaIndex);

            ValueColumnReaders_.push_back(columnReader.get());
            TBase::Columns_.emplace_back(std::move(columnReader), idMapping.ChunkSchemaIndex);
        }
    }

    virtual TDataStatistics GetDataStatistics() const override
    {
        auto dataStatistics = TBase::GetDataStatistics();
        dataStatistics.set_row_count(RowCount_);
        dataStatistics.set_data_weight(DataWeight_);
        return dataStatistics;
    }

    virtual TFuture<void> Open() override
    {
        return VoidFuture;
    }

protected:
    const TCachedVersionedChunkMetaPtr ChunkMeta_;
    const TTimestamp Timestamp_;
    const std::vector<TColumnIdMapping> SchemaIdMapping_;
    const TChunkReaderPerformanceCountersPtr PerformanceCounters_;

    i64 RowCount_ = 0;
    i64 DataWeight_ = 0;

    std::vector<IUnversionedColumnReader*> KeyColumnReaders_;
    std::vector<IVersionedColumnReader*> ValueColumnReaders_;
};

////////////////////////////////////////////////////////////////////////////////

class TRowBuilderBase
{
public:
    TRowBuilderBase(
        TCachedVersionedChunkMetaPtr chunkMeta,
        const std::vector<IVersionedColumnReader*>& valueColumnReaders,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        TTimestamp timestamp)
        : ChunkMeta_(std::move(chunkMeta))
        , ValueColumnReaders_(valueColumnReaders)
        , SchemaIdMapping_(schemaIdMapping)
        , Timestamp_(timestamp)
        , ColumnHunkFlags_(new bool[ChunkMeta_->GetSchema()->GetColumnCount()])
    {
        for (const auto& idMapping : SchemaIdMapping_) {
            const auto& columnSchema = ChunkMeta_->GetChunkSchema()->Columns()[idMapping.ChunkSchemaIndex];
            ColumnHunkFlags_[idMapping.ReaderSchemaIndex] = columnSchema.MaxInlineHunkSize().has_value();
        }
    }

    void Clear()
    {
        Pool_.Clear();
    }

    TChunkedMemoryPool* GetMemoryPool()
    {
        return &Pool_;
    }

protected:
    const TCachedVersionedChunkMetaPtr ChunkMeta_;
    const std::vector<IVersionedColumnReader*>& ValueColumnReaders_;
    const std::vector<TColumnIdMapping>& SchemaIdMapping_;
    const TTimestamp Timestamp_;

    const std::unique_ptr<bool[]> ColumnHunkFlags_;

    TChunkedMemoryPool Pool_{TVersionedChunkReaderPoolTag()};
};

////////////////////////////////////////////////////////////////////////////////

class TLookupRowBuilderBase
    : public TRowBuilderBase
{
public:
    TLookupRowBuilderBase(
        TCachedVersionedChunkMetaPtr chunkMeta,
        const std::vector<IUnversionedColumnReader*>& keyColumnReaders,
        const std::vector<IVersionedColumnReader*>& valueColumnReaders,
        const std::vector<TColumnIdMapping>& schemaIdMapping,
        TTimestamp timestamp)
        : TRowBuilderBase(
            std::move(chunkMeta),
            valueColumnReaders,
            schemaIdMapping,
            timestamp)
        , KeyColumnReaders_(keyColumnReaders)
    { }

protected:
    const std::vector<IUnversionedColumnReader*>& KeyColumnReaders_;
};

////////////////////////////////////////////////////////////////////////////////

class TScanColumnarRowBuilder
    : public TRowBuilderBase
{
public:
    using TRowBuilderBase::TRowBuilderBase;

    // We pass away the ownership of the reader.
    // All column reader are owned by the chunk reader.
    std::unique_ptr<IColumnReaderBase> CreateTimestampReader()
    {
        YT_VERIFY(!TimestampReader_);

        int timestampReaderIndex = ChunkMeta_->ColumnMeta()->columns().size() - 1;
        TimestampReader_ = new TScanTransactionTimestampReader(
            ChunkMeta_->ColumnMeta()->columns(timestampReaderIndex),
            Timestamp_);
        return std::unique_ptr<IColumnReaderBase>(TimestampReader_);
    }

    TMutableRange<TMutableVersionedRow> AllocateRows(
        std::vector<TVersionedRow>* rows,
        i64 rowLimit,
        i64 currentRowIndex,
        i64 safeUpperRowIndex)
    {
        TimestampReader_->PrepareRows(rowLimit);
        auto timestampIndexRanges = TimestampReader_->GetTimestampIndexRanges(rowLimit);

        std::vector<ui32> valueCountPerRow(rowLimit, 0);
        std::vector<ui32> columnValueCount(rowLimit, 0);
        for (int valueColumnIndex = 0; valueColumnIndex < std::ssize(SchemaIdMapping_); ++valueColumnIndex) {
            const auto& idMapping = SchemaIdMapping_[valueColumnIndex];
            const auto& columnSchema = ChunkMeta_->GetChunkSchema()->Columns()[idMapping.ChunkSchemaIndex];
            if (columnSchema.Aggregate()) {
                // Possibly multiple values per column for aggregate columns.
                ValueColumnReaders_[valueColumnIndex]->ReadValueCounts(TMutableRange<ui32>(columnValueCount));
            } else {
                // No more than one value per column for aggregate columns.
                columnValueCount.resize(0);
                columnValueCount.resize(rowLimit, 1);
            }

            for (int index = 0; index < rowLimit; ++index) {
                valueCountPerRow[index] += columnValueCount[index];
            }
        }

        i64 rangeBegin = rows->size();
        for (i64 index = 0; index < rowLimit; ++index) {
            i64 rowIndex = currentRowIndex + index;

            auto deleteTimestamp = TimestampReader_->GetDeleteTimestamp(rowIndex);
            auto timestampIndexRange = timestampIndexRanges[index];

            bool hasWriteTimestamp = timestampIndexRange.first < timestampIndexRange.second;
            bool hasDeleteTimestamp = deleteTimestamp != NullTimestamp;
            if (!hasWriteTimestamp && !hasDeleteTimestamp) {
                if (rowIndex < safeUpperRowIndex) {
                    rows->push_back(TMutableVersionedRow());
                } else {
                    // Reserve space for key, to compare with #UpperLimit_.
                    rows->push_back(TMutableVersionedRow::Allocate(
                        &Pool_,
                        ChunkMeta_->GetKeyColumnCount(),
                        0,
                        0,
                        0));
                }
            } else {
                // Allocate according to schema.
                auto row = TMutableVersionedRow::Allocate(
                    &Pool_,
                    ChunkMeta_->GetKeyColumnCount(),
                    hasWriteTimestamp ? valueCountPerRow[index] : 0,
                    hasWriteTimestamp ? 1 : 0,
                    hasDeleteTimestamp ? 1 : 0);
                rows->push_back(row);

                if (hasDeleteTimestamp) {
                    *row.BeginDeleteTimestamps() = deleteTimestamp;
                }

                if (hasWriteTimestamp) {
                    *row.BeginWriteTimestamps() = TimestampReader_->GetWriteTimestamp(rowIndex);

                    // Value count is increased inside value column readers.
                    row.SetValueCount(0);
                }
            }
        }

        return TMutableRange<TMutableVersionedRow>(
            static_cast<TMutableVersionedRow*>(rows->data() + rangeBegin),
            static_cast<TMutableVersionedRow*>(rows->data() + rangeBegin + rowLimit));
    }

    void ReadValues(TMutableRange<TMutableVersionedRow> range, i64 currentRowIndex)
    {
        // Read timestamp indexes.
        auto timestampIndexRanges = TimestampReader_->GetTimestampIndexRanges(range.Size());

        for (auto* valueColumnReader : ValueColumnReaders_) {
            valueColumnReader->ReadValues(range, timestampIndexRanges, false);
        }

        // Read timestamps.
        for (i64 index = 0; index < std::ssize(range); ++index) {
            auto& row = range[index];
            if (!row) {
                continue;
            }

            if (row.GetWriteTimestampCount() == 0 && row.GetDeleteTimestampCount() == 0) {
                // This row was created in order to compare with UpperLimit.
                row = TMutableVersionedRow();
                continue;
            }

            for (auto* value = row.BeginValues(); value != row.EndValues(); ++value) {
                value->Timestamp = TimestampReader_->GetValueTimestamp(
                    currentRowIndex + index,
                    static_cast<ui32>(value->Timestamp));
                if (ColumnHunkFlags_[value->Id]) {
                    value->Flags |= EValueFlags::Hunk;
                }
            }
        }

        TimestampReader_->SkipPreparedRows();
    }

private:
    TScanTransactionTimestampReader* TimestampReader_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

class TCompactionColumnarRowBuilder
    : public TRowBuilderBase
{
public:
    using TRowBuilderBase::TRowBuilderBase;

    std::unique_ptr<IColumnReaderBase> CreateTimestampReader()
    {
        YT_VERIFY(!TimestampReader_);
        int timestampReaderIndex = ChunkMeta_->ColumnMeta()->columns().size() - 1;
        TimestampReader_ = new TCompactionTimestampReader(
            ChunkMeta_->ColumnMeta()->columns(timestampReaderIndex));

        return std::unique_ptr<IColumnReaderBase>(TimestampReader_);
    }

    TMutableRange<TMutableVersionedRow> AllocateRows(
        std::vector<TVersionedRow>* rows,
        i64 rowLimit,
        i64 currentRowIndex,
        i64 /* safeUpperRowIndex */)
    {
        TimestampReader_->PrepareRows(rowLimit);
        i64 rangeBegin = rows->size();

        std::vector<ui32> valueCountPerRow(rowLimit, 0);
        std::vector<ui32> columnValueCount(rowLimit, 0);
        for (const auto& valueColumnReader : ValueColumnReaders_) {
            valueColumnReader->ReadValueCounts(TMutableRange<ui32>(columnValueCount));
            for (int index = 0; index < rowLimit; ++index) {
                valueCountPerRow[index] += columnValueCount[index];
            }
        }

        for (i64 index = 0; index < rowLimit; ++index) {
            i64 rowIndex = currentRowIndex + index;

            auto row = TMutableVersionedRow::Allocate(
                &Pool_,
                ChunkMeta_->GetKeyColumnCount(),
                valueCountPerRow[index],
                TimestampReader_->GetWriteTimestampCount(rowIndex),
                TimestampReader_->GetDeleteTimestampCount(rowIndex));
            rows->push_back(row);

            row.SetValueCount(0);

            for (
                ui32 timestampIndex = 0;
                timestampIndex < TimestampReader_->GetWriteTimestampCount(rowIndex);
                ++timestampIndex)
            {
                row.BeginWriteTimestamps()[timestampIndex] = TimestampReader_->GetValueTimestamp(rowIndex, timestampIndex);
            }

            for (
                ui32 timestampIndex = 0;
                timestampIndex < TimestampReader_->GetDeleteTimestampCount(rowIndex);
                ++timestampIndex)
            {
                row.BeginDeleteTimestamps()[timestampIndex] = TimestampReader_->GetDeleteTimestamp(rowIndex, timestampIndex);
            }
        }

        return TMutableRange<TMutableVersionedRow>(
            static_cast<TMutableVersionedRow*>(rows->data() + rangeBegin),
            static_cast<TMutableVersionedRow*>(rows->data() + rangeBegin + rowLimit));
    }

    void ReadValues(TMutableRange<TMutableVersionedRow> range, i64 currentRowIndex)
    {
        for (auto& valueColumnReader : ValueColumnReaders_) {
            valueColumnReader->ReadAllValues(range);
        }

        // Read timestamps.
        for (i64 index = 0; index < std::ssize(range); ++index) {
            if (!range[index]) {
                continue;
            }

            for (auto* value = range[index].BeginValues(); value != range[index].EndValues(); ++value) {
                value->Timestamp = TimestampReader_->GetValueTimestamp(
                    currentRowIndex + index,
                    static_cast<ui32>(value->Timestamp));
            }
        }

        TimestampReader_->SkipPreparedRows();
    }

private:
    TCompactionTimestampReader* TimestampReader_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <class TRowBuilder>
class TColumnarVersionedRangeChunkReader
    : public TColumnarVersionedChunkReaderBase<TColumnarRangeChunkReaderBase>
{
public:
    TColumnarVersionedRangeChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        const TSortColumns& sortColumns,
        IBlockCachePtr blockCache,
        const TClientChunkReadOptions& chunkReadOptions,
        TSharedRange<TRowRange> ranges,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp,
        const TChunkReaderMemoryManagerPtr& memoryManager)
        : TColumnarVersionedChunkReaderBase(
            std::move(config),
            chunkMeta,
            std::move(underlyingReader),
            sortColumns,
            std::move(blockCache),
            chunkReadOptions,
            columnFilter,
            std::move(performanceCounters),
            timestamp,
            memoryManager)
        , RowBuilder_(
            chunkMeta,
            ValueColumnReaders_,
            SchemaIdMapping_,
            timestamp)
    {
        YT_VERIFY(ranges.Size() == 1);
        LegacyLowerLimit_.SetLegacyKey(TLegacyOwningKey(ranges[0].first));
        LegacyUpperLimit_.SetLegacyKey(TLegacyOwningKey(ranges[0].second));

        LowerLimit_ = ReadLimitFromLegacyReadLimit(LegacyLowerLimit_, /* isUpper */ false, Comparator_.GetLength());
        UpperLimit_ = ReadLimitFromLegacyReadLimit(LegacyUpperLimit_, /* isUpper */ true, Comparator_.GetLength());

        int timestampReaderIndex = ChunkMeta_->ColumnMeta()->columns().size() - 1;
        Columns_.emplace_back(RowBuilder_.CreateTimestampReader(), timestampReaderIndex);

        // Empirical formula to determine max rows per read for better cache friendliness.
        MaxRowsPerRead_ = CacheSize / (KeyColumnReaders_.size() * sizeof(TUnversionedValue) +
            ValueColumnReaders_.size() * sizeof(TVersionedValue));
        MaxRowsPerRead_ = std::max(MaxRowsPerRead_, MinRowsPerRead);

        InitLowerRowIndex();
        InitUpperRowIndex();

        if (LowerRowIndex_ < HardUpperRowIndex_) {
            InitBlockFetcher();
            SetReadyEvent(RequestFirstBlocks());
        } else {
            Initialized_ = true;
            Completed_ = true;
        }
    }

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        auto readGuard = AcquireReadGuard();

        YT_VERIFY(options.MaxRowsPerRead > 0);
        std::vector<TVersionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        RowBuilder_.Clear();

        if (!ReadyEvent().IsSet() || !ReadyEvent().Get().IsOK()) {
            return CreateEmptyVersionedRowBatch();
        }

        if (!Initialized_) {
            FeedBlocksToReaders();
            Initialize(MakeRange(KeyColumnReaders_.data(), KeyColumnReaders_.size()));
            Initialized_ = true;
            RowIndex_ = LowerRowIndex_;
        }

        if (Completed_) {
            return nullptr;
        }

        auto hasHunkColumns = ChunkMeta_->GetSchema()->HasHunkColumns();

        while (rows.size() < rows.capacity()) {
            FeedBlocksToReaders();
            ArmColumnReaders();

            // Compute the number rows to read.
            i64 rowLimit = static_cast<i64>(rows.capacity() - rows.size());
            rowLimit = std::min(rowLimit, HardUpperRowIndex_ - RowIndex_);
            rowLimit = std::min(rowLimit, GetReadyRowCount());
            rowLimit = std::min(rowLimit, MaxRowsPerRead_);
            YT_VERIFY(rowLimit > 0);

            auto range = RowBuilder_.AllocateRows(&rows, rowLimit, RowIndex_, SafeUpperRowIndex_);

            // Read key values.
            for (const auto& keyColumnReader : KeyColumnReaders_) {
                keyColumnReader->ReadValues(range);
            }

            YT_VERIFY(RowIndex_ + rowLimit <= HardUpperRowIndex_);
            if (RowIndex_ + rowLimit > SafeUpperRowIndex_ && LegacyUpperLimit_.HasLegacyKey()) {
                i64 index = std::max(SafeUpperRowIndex_ - RowIndex_, i64(0));
                for (; index < rowLimit; ++index) {
                    if (CompareRows(
                        range[index].BeginKeys(),
                        range[index].EndKeys(),
                        LegacyUpperLimit_.GetLegacyKey().Begin(),
                        LegacyUpperLimit_.GetLegacyKey().End()) >= 0)
                    {
                        Completed_ = true;
                        range = range.Slice(range.Begin(), range.Begin() + index);
                        rows.resize(rows.size() - rowLimit + index);
                        break;
                    }
                }
            }

            if (RowIndex_ + rowLimit == HardUpperRowIndex_) {
                Completed_ = true;
            }

            RowBuilder_.ReadValues(range, RowIndex_);

            if (hasHunkColumns) {
                for (auto row : range) {
                    GlobalizeHunkValues(RowBuilder_.GetMemoryPool(), ChunkMeta_, row);
                }
            }

            RowIndex_ += range.Size();
            if (Completed_ || !TryFetchNextRow()) {
                break;
            }
        }

        i64 rowCount = 0;
        i64 dataWeight = 0;
        for (auto row : rows) {
            if (row) {
                ++rowCount;
            }
            dataWeight += GetDataWeight(row);
        }

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;
        PerformanceCounters_->StaticChunkRowReadCount += rowCount;
        PerformanceCounters_->StaticChunkRowReadDataWeightCount += dataWeight;

        return CreateBatchFromVersionedRows(MakeSharedRange(rows, MakeStrong(this)));
    }

private:
    bool Initialized_ = false;
    bool Completed_ = false;

    i64 MaxRowsPerRead_;
    i64 RowIndex_ = 0;

    TRowBuilder RowBuilder_;

    TLegacyReadLimit LegacyLowerLimit_;
    TLegacyReadLimit LegacyUpperLimit_;
};

////////////////////////////////////////////////////////////////////////////////

class TLookupSingleVersionColumnarRowBuilder
    : public TLookupRowBuilderBase
{
public:
    using TLookupRowBuilderBase::TLookupRowBuilderBase;

    std::unique_ptr<IColumnReaderBase> CreateTimestampReader()
    {
        YT_VERIFY(!TimestampReader_);

        int timestampReaderIndex = ChunkMeta_->ColumnMeta()->columns().size() - 1;
        TimestampReader_ = new TLookupTransactionTimestampReader(
            ChunkMeta_->ColumnMeta()->columns(timestampReaderIndex),
            Timestamp_);
        return std::unique_ptr<IColumnReaderBase>(TimestampReader_);
    }

    TMutableVersionedRow ReadRow()
    {
        auto deleteTimestamp = TimestampReader_->GetDeleteTimestamp();
        auto timestampIndexRange = TimestampReader_->GetTimestampIndexRange();

        bool hasWriteTimestamp = timestampIndexRange.first < timestampIndexRange.second;
        bool hasDeleteTimestamp = deleteTimestamp != NullTimestamp;
        if (!hasWriteTimestamp && !hasDeleteTimestamp) {
            // No record of this key at this point of time.
            return TMutableVersionedRow();
        }

        i64 valueCount = 0;
        for (int valueColumnIndex = 0; valueColumnIndex < std::ssize(SchemaIdMapping_); ++valueColumnIndex) {
            const auto& idMapping = SchemaIdMapping_[valueColumnIndex];
            const auto& columnSchema = ChunkMeta_->GetChunkSchema()->Columns()[idMapping.ChunkSchemaIndex];
            ui32 columnValueCount = 1;
            if (columnSchema.Aggregate()) {
                // Possibly multiple values per column for aggregate columns.
                ValueColumnReaders_[valueColumnIndex]->ReadValueCounts(TMutableRange<ui32>(&columnValueCount, 1));
            }
            valueCount += columnValueCount;
        }

        // Allocate according to schema.
        auto row = TMutableVersionedRow::Allocate(
            &Pool_,
            ChunkMeta_->GetKeyColumnCount(),
            hasWriteTimestamp ? valueCount : 0,
            hasWriteTimestamp ? 1 : 0,
            hasDeleteTimestamp ? 1 : 0);

        // Read key values.
        for (auto* keyColumnReader : KeyColumnReaders_) {
            keyColumnReader->ReadValues(TMutableRange<TMutableVersionedRow>(&row, 1));
        }

        if (hasDeleteTimestamp) {
            *row.BeginDeleteTimestamps() = deleteTimestamp;
        }

        if (!hasWriteTimestamp) {
            return row;
        }

        // Value count is increased inside value column readers.
        row.SetValueCount(0);

        // Read non-key values.
        for (auto* valueColumnReader : ValueColumnReaders_) {
            valueColumnReader->ReadValues(
                TMutableRange<TMutableVersionedRow>(&row, 1),
                MakeRange(&timestampIndexRange, 1),
                false);
        }

        for (auto* value = row.BeginValues(); value != row.EndValues(); ++value) {
            value->Timestamp = TimestampReader_->GetTimestamp(static_cast<ui32>(value->Timestamp));
            if (ColumnHunkFlags_[value->Id]) {
                value->Flags |= EValueFlags::Hunk;
            }
        }

        *row.BeginWriteTimestamps() = TimestampReader_->GetWriteTimestamp();

        return row;
    }

private:
    TLookupTransactionTimestampReader* TimestampReader_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

class TLookupAllVersionsColumnarRowBuilder
    : public TLookupRowBuilderBase
{
public:
    using TLookupRowBuilderBase::TLookupRowBuilderBase;

    std::unique_ptr<IColumnReaderBase> CreateTimestampReader()
    {
        YT_VERIFY(!TimestampReader_);

        int timestampReaderIndex = ChunkMeta_->ColumnMeta()->columns().size() - 1;
        TimestampReader_ = new TLookupTransactionAllVersionsTimestampReader(
            ChunkMeta_->ColumnMeta()->columns(timestampReaderIndex),
            Timestamp_);
        return std::unique_ptr<IColumnReaderBase>(TimestampReader_);
    }

    TMutableVersionedRow ReadRow()
    {
        ui32 writeTimestampCount = TimestampReader_->GetWriteTimestampCount();
        ui32 deleteTimestampCount = TimestampReader_->GetDeleteTimestampCount();

        if (writeTimestampCount == 0 && deleteTimestampCount == 0) {
            return {};
        }

        size_t valueCount = 0;
        for (int valueColumnIndex = 0; valueColumnIndex < std::ssize(SchemaIdMapping_); ++valueColumnIndex) {
            ui32 columnValueCount;
            ValueColumnReaders_[valueColumnIndex]->ReadValueCounts(TMutableRange<ui32>(&columnValueCount, 1));
            valueCount += columnValueCount;
        }

        // Allocate according to schema.
        auto row = TMutableVersionedRow::Allocate(
            &Pool_,
            ChunkMeta_->GetKeyColumnCount(),
            writeTimestampCount > 0 ? valueCount : 0,
            writeTimestampCount,
            deleteTimestampCount);

        // Read key values.
        for (auto* keyColumnReader : KeyColumnReaders_) {
            keyColumnReader->ReadValues(TMutableRange<TMutableVersionedRow>(&row, 1));
        }

        // Read delete timestamps.
        for (ui32 timestampIndex = 0; timestampIndex < deleteTimestampCount; ++timestampIndex) {
            row.BeginDeleteTimestamps()[timestampIndex] = TimestampReader_->GetDeleteTimestamp(timestampIndex);
        }

        if (writeTimestampCount == 0) {
            return row;
        }

        // Read write timestamps.
        for (ui32 timestampIndex = 0; timestampIndex < writeTimestampCount; ++timestampIndex) {
            row.BeginWriteTimestamps()[timestampIndex] = TimestampReader_->GetValueTimestamp(timestampIndex);
        }

        // Value count is increased inside value column readers.
        row.SetValueCount(0);

        // Read non-key values.
        auto writeTimestampIndexRange = TimestampReader_->GetWriteTimestampIndexRange();
        auto* memoryTo = const_cast<char*>(row.GetMemoryEnd());
        for (auto* valueColumnReader : ValueColumnReaders_) {
            valueColumnReader->ReadValues(
                TMutableRange<TMutableVersionedRow>(&row, 1),
                MakeRange(&writeTimestampIndexRange, 1),
                true);
        }

        for (int i = 0; i < row.GetValueCount(); ++i) {
            row.BeginValues()[i].Timestamp = TimestampReader_->GetValueTimestamp(
                static_cast<i32>(row.BeginValues()[i].Timestamp) - writeTimestampIndexRange.first);
        }

        auto* memoryFrom = const_cast<char*>(row.GetMemoryEnd());
        Pool_.Free(memoryFrom, memoryTo);

        return row;
    }

private:
    TLookupTransactionAllVersionsTimestampReader* TimestampReader_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <class TRowBuilder>
class TColumnarVersionedLookupChunkReader
    : public TColumnarVersionedChunkReaderBase<TColumnarLookupChunkReaderBase>
{
public:
    TColumnarVersionedLookupChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        const TSortColumns& sortColumns,
        IBlockCachePtr blockCache,
        const TClientChunkReadOptions& chunkReadOptions,
        const TSharedRange<TLegacyKey>& keys,
        const TColumnFilter& columnFilter,
        TChunkReaderPerformanceCountersPtr performanceCounters,
        TTimestamp timestamp,
        const TChunkReaderMemoryManagerPtr& memoryManager)
        : TColumnarVersionedChunkReaderBase(
            std::move(config),
            chunkMeta,
            std::move(underlyingReader),
            sortColumns,
            std::move(blockCache),
            chunkReadOptions,
            columnFilter,
            std::move(performanceCounters),
            timestamp,
            memoryManager)
        , HasHunkColumns_(ChunkMeta_->GetSchema()->HasHunkColumns())
        , RowBuilder_(
            chunkMeta,
            KeyColumnReaders_,
            ValueColumnReaders_,
            SchemaIdMapping_,
            timestamp)
    {
        Keys_ = keys;

        int timestampReaderIndex = ChunkMeta_->ColumnMeta()->columns().size() - 1;
        Columns_.emplace_back(RowBuilder_.CreateTimestampReader(), timestampReaderIndex);

        Initialize();

        SetReadyEvent(RequestFirstBlocks());
    }

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        auto readGuard = AcquireReadGuard();

        std::vector<TVersionedRow> rows;
        rows.reserve(options.MaxRowsPerRead);

        RowBuilder_.Clear();

        if (!ReadyEvent().IsSet() || !ReadyEvent().Get().IsOK()) {
            return CreateEmptyVersionedRowBatch();
        }

        if (NextKeyIndex_ == std::ssize(Keys_)) {
            return nullptr;
        }

        while (rows.size() < rows.capacity()) {
            FeedBlocksToReaders();

            bool found = false;
            if (RowIndexes_[NextKeyIndex_] < ChunkMeta_->Misc().row_count()) {
                auto key = Keys_[NextKeyIndex_];
                YT_VERIFY(static_cast<int>(key.GetCount()) == ChunkMeta_->GetKeyColumnCount());

                // Reading row.
                i64 lowerRowIndex = KeyColumnReaders_[0]->GetCurrentRowIndex();
                i64 upperRowIndex = KeyColumnReaders_[0]->GetBlockUpperRowIndex();
                for (int i = 0; i < ChunkMeta_->GetKeyColumnCount(); ++i) {
                    std::tie(lowerRowIndex, upperRowIndex) = KeyColumnReaders_[i]->GetEqualRange(
                        key[i],
                        lowerRowIndex,
                        upperRowIndex);
                }

                if (upperRowIndex != lowerRowIndex) {
                    // Key must be present in exactly one row.
                    YT_VERIFY(upperRowIndex == lowerRowIndex + 1);
                    i64 rowIndex = lowerRowIndex;
                    rows.push_back(ReadRow(rowIndex));
                    found = true;
                }
            }

            if (!found) {
                rows.push_back({});
            }

            if (++NextKeyIndex_ == std::ssize(Keys_) || !TryFetchNextRow()) {
                break;
            }
        }

        i64 rowCount = 0;
        i64 dataWeight = 0;
        for (auto row : rows) {
            if (row) {
                ++rowCount;
            }
            dataWeight += GetDataWeight(row);
        }

        RowCount_ += rowCount;
        DataWeight_ += dataWeight;
        PerformanceCounters_->StaticChunkRowLookupCount += rowCount;
        PerformanceCounters_->StaticChunkRowLookupDataWeightCount += dataWeight;

        return CreateBatchFromVersionedRows(MakeSharedRange(rows, MakeStrong(this)));
    }

private:
    const bool HasHunkColumns_;

    TRowBuilder RowBuilder_;

    TMutableVersionedRow ReadRow(i64 rowIndex)
    {
        for (const auto& column : Columns_) {
            column.ColumnReader->SkipToRowIndex(rowIndex);
        }
        auto row = RowBuilder_.ReadRow();
        if (HasHunkColumns_) {
            GlobalizeHunkValues(RowBuilder_.GetMemoryPool(), ChunkMeta_, row);
        }
        return row;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFilteringReader
    : public IVersionedReader
{
public:
    TFilteringReader(
        IVersionedReaderPtr underlyingReader,
        TSharedRange<TRowRange> ranges)
        : UnderlyingReader_(std::move(underlyingReader))
        , Ranges_(std::move(ranges))
    { }

    virtual TDataStatistics GetDataStatistics() const override
    {
        return UnderlyingReader_->GetDataStatistics();
    }

    virtual TCodecStatistics GetDecompressionStatistics() const override
    {
        return UnderlyingReader_->GetDecompressionStatistics();
    }

    virtual TFuture<void> Open() override
    {
        return UnderlyingReader_->Open();
    }

    virtual TFuture<void> GetReadyEvent() const override
    {
        return UnderlyingReader_->GetReadyEvent();
    }

    virtual bool IsFetchingCompleted() const override
    {
        return UnderlyingReader_->IsFetchingCompleted();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return UnderlyingReader_->GetFailedChunkIds();
    }

    virtual IVersionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        auto comparator = [] (const TVersionedRow& lhs, const TUnversionedRow& rhs) {
            return CompareRows(lhs.BeginKeys(), lhs.EndKeys(), rhs.Begin(), rhs.End()) < 0;
        };

        std::vector<TVersionedRow> rows;

        bool hasMoreData = true;
        while (rows.empty() && hasMoreData) {
            auto batch = UnderlyingReader_->Read(options);
            hasMoreData = static_cast<bool>(batch);
            if (!batch || batch->IsEmpty()) {
                break;
            }

            auto range = batch->MaterializeRows();
            rows = std::vector<TVersionedRow>(range.begin(), range.end());

            rows.erase(
                std::remove_if(
                    rows.begin(),
                    rows.end(),
                    [] (TVersionedRow row) {
                        return !row;
                    }),
                rows.end());

            auto finish = rows.begin();
            for (auto start = rows.begin(); start != rows.end() && RangeIndex_ < Ranges_.Size();) {
                start = std::lower_bound(start, rows.end(), Ranges_[RangeIndex_].first, comparator);

                if (start != rows.end() && !comparator(*start, Ranges_[RangeIndex_].second)) {
                    auto nextBoundIt = std::upper_bound(
                        Ranges_.begin() + RangeIndex_,
                        Ranges_.end(),
                        *start,
                        [&] (const TVersionedRow& lhs, const TRowRange& rhs) {
                            return comparator(lhs, rhs.second);
                        });
                    auto newNextBoundIndex = std::distance(Ranges_.begin(), nextBoundIt);

                    YT_VERIFY(newNextBoundIndex > static_cast<ssize_t>(RangeIndex_));

                    RangeIndex_ = std::distance(Ranges_.begin(), nextBoundIt);
                    continue;
                }

                auto end = std::lower_bound(start, rows.end(), Ranges_[RangeIndex_].second, comparator);

                finish = std::move(start, end, finish);

                if (end != rows.end()) {
                    ++RangeIndex_;
                }

                start = end;
            }
            size_t newSize = std::distance(rows.begin(), finish);

            YT_VERIFY(newSize <= rows.size());
            rows.resize(newSize);

            if (RangeIndex_ == Ranges_.Size()) {
                hasMoreData = false;
            }
        }

        if (!rows.empty() || hasMoreData) {
            return CreateBatchFromVersionedRows(MakeSharedRange(rows, MakeStrong(this)));
        } else {
            return nullptr;
        }
    }

private:
    const IVersionedReaderPtr UnderlyingReader_;
    const TSharedRange<TRowRange> Ranges_;
    size_t RangeIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TClientChunkReadOptions& chunkReadOptions,
    TSharedRange<TRowRange> ranges,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TSharedRange<TRowRange>& singletonClippingRange,
    const TChunkReaderMemoryManagerPtr& memoryManager)
{
    const auto& blockCache = chunkState->BlockCache;
    const auto& performanceCounters = chunkState->PerformanceCounters;

    auto getCappedBounds = [&ranges, &singletonClippingRange] {
        TLegacyKey lowerBound = ranges.Front().first;
        TLegacyKey upperBound = ranges.Back().second;

        YT_VERIFY(singletonClippingRange.Size() <= 1);
        if (singletonClippingRange.Size() > 0) {
            TLegacyKey clippedLowerBound = singletonClippingRange.Front().first;
            TLegacyKey clippedUpperBound = singletonClippingRange.Front().second;
            if (clippedLowerBound && clippedLowerBound > lowerBound) {
                lowerBound = clippedLowerBound;
            }
            if (clippedUpperBound && clippedUpperBound < upperBound) {
                upperBound = clippedUpperBound;
            }
        }

        SmallVector<TRowRange, 1> cappedBounds(1, TRowRange(lowerBound, upperBound));
        return MakeSharedRange(std::move(cappedBounds), ranges.GetHolder(), singletonClippingRange.GetHolder());
    };

    if (chunkState->ChunkTimestamp && timestamp < chunkState->ChunkTimestamp) {
        return CreateEmptyVersionedReader();
    }

    IVersionedReaderPtr reader;

    switch (chunkMeta->GetChunkFormat()) {
        case EChunkFormat::TableVersionedSimple:
            reader = New<TSimpleVersionedRangeChunkReader>(
                std::move(config),
                chunkMeta,
                std::move(chunkReader),
                blockCache,
                chunkReadOptions,
                std::move(ranges),
                columnFilter,
                performanceCounters,
                timestamp,
                produceAllVersions,
                singletonClippingRange,
                memoryManager);
            break;

        case EChunkFormat::TableVersionedColumnar: {
            YT_VERIFY(!ranges.Empty());
            IVersionedReaderPtr unwrappedReader;

            if (timestamp == AllCommittedTimestamp) {
                unwrappedReader = New<TColumnarVersionedRangeChunkReader<TCompactionColumnarRowBuilder>>(
                    std::move(config),
                    chunkMeta,
                    std::move(chunkReader),
                    chunkMeta->GetSchema()->GetSortColumns(),
                    blockCache,
                    chunkReadOptions,
                    getCappedBounds(),
                    columnFilter,
                    performanceCounters,
                    timestamp,
                    memoryManager);
            } else {
                unwrappedReader  = New<TColumnarVersionedRangeChunkReader<TScanColumnarRowBuilder>>(
                    std::move(config),
                    chunkMeta,
                    std::move(chunkReader),
                    chunkMeta->GetSchema()->GetSortColumns(),
                    blockCache,
                    chunkReadOptions,
                    getCappedBounds(),
                    columnFilter,
                    performanceCounters,
                    timestamp,
                    memoryManager);
            }
            reader = New<TFilteringReader>(unwrappedReader, ranges);
            break;
        }

        case EChunkFormat::TableUnversionedColumnar:
        case EChunkFormat::TableSchemalessHorizontal: {
            // COMPAT(sandello): Fix me.
            if (produceAllVersions && timestamp != AllCommittedTimestamp) {
                THROW_ERROR_EXCEPTION("Reading all value versions is not supported with a particular timestamp");
            }

            auto chunkTimestamp = chunkState->ChunkTimestamp
                ? chunkState->ChunkTimestamp
                : static_cast<TTimestamp>(chunkMeta->Misc().min_timestamp());
            if (timestamp < chunkTimestamp) {
                return CreateEmptyVersionedReader();
            }

            YT_VERIFY(!ranges.Empty());

            auto schemalessReaderFactory = [&] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
                auto options = New<TTableReaderOptions>();
                options->DynamicTable = true;

                auto cappedBounds = getCappedBounds();

                int keyColumnCount = chunkMeta->GetSchema()->GetKeyColumnCount();
                auto lowerKeyBound = KeyBoundFromLegacyRow(cappedBounds.Front().first, /* isUpper */ false, keyColumnCount);
                auto upperKeyBound = KeyBoundFromLegacyRow(cappedBounds.Front().second, /* isUpper */ true, keyColumnCount);

                TReadRange readRange(TReadLimit{lowerKeyBound}, TReadLimit{upperKeyBound});

                return CreateSchemalessRangeChunkReader(
                    chunkState,
                    chunkMeta,
                    config,
                    options,
                    chunkReader,
                    nameTable,
                    chunkReadOptions,
                    chunkMeta->GetSchema()->GetSortColumns(),
                    /* omittedInaccessibleColumns */ {},
                    columnFilter,
                    readRange,
                    std::nullopt,
                    memoryManager);
            };
            auto schemafulReaderFactory = [&] (const TTableSchemaPtr& schema, const TColumnFilter& columnFilter) {
                return CreateSchemafulReaderAdapter(schemalessReaderFactory, schema, columnFilter, /*ignoreRequired*/ true);
            };

            auto unwrappedReader = CreateVersionedReaderAdapter(
                schemafulReaderFactory,
                chunkMeta->GetSchema(),
                columnFilter,
                chunkTimestamp);

            return New<TFilteringReader>(unwrappedReader, ranges);
        }
        default:
            YT_ABORT();
    }

    if (chunkState->ChunkTimestamp) {
        return CreateTimestampResettingAdapter(
            reader,
            chunkState->ChunkTimestamp);
    } else {
        return reader;
    }
}

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TClientChunkReadOptions& chunkReadOptions,
    TLegacyOwningKey lowerLimit,
    TLegacyOwningKey upperLimit,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TChunkReaderMemoryManagerPtr& memoryManager)
{
    return CreateVersionedChunkReader(
        config,
        chunkReader,
        chunkState,
        chunkMeta,
        chunkReadOptions,
        MakeSingletonRowRange(lowerLimit, upperLimit),
        columnFilter,
        timestamp,
        produceAllVersions,
        {},
        memoryManager);
}

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TClientChunkReadOptions& chunkReadOptions,
    const TSharedRange<TLegacyKey>& keys,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions,
    const TChunkReaderMemoryManagerPtr& memoryManager)
{
    const auto& blockCache = chunkState->BlockCache;
    const auto& performanceCounters = chunkState->PerformanceCounters;
    const auto& keyComparer = chunkState->KeyComparer;

    if (chunkState->ChunkTimestamp && timestamp < chunkState->ChunkTimestamp) {
        return CreateEmptyVersionedReader(keys.Size());
    }

    IVersionedReaderPtr reader;

    switch (chunkMeta->GetChunkFormat()) {
        case EChunkFormat::TableVersionedSimple:
            reader = New<TSimpleVersionedLookupChunkReader>(
                std::move(config),
                chunkMeta,
                std::move(chunkReader),
                blockCache,
                chunkReadOptions,
                keys,
                columnFilter,
                performanceCounters,
                keyComparer,
                timestamp,
                produceAllVersions,
                memoryManager);
            break;

        case EChunkFormat::TableVersionedColumnar: {
            if (produceAllVersions) {
                YT_VERIFY(columnFilter.IsUniversal());
            }
            if (produceAllVersions) {
                reader = New<TColumnarVersionedLookupChunkReader<TLookupAllVersionsColumnarRowBuilder>>(
                    std::move(config),
                    chunkMeta,
                    std::move(chunkReader),
                    chunkMeta->GetSchema()->GetSortColumns(),
                    blockCache,
                    chunkReadOptions,
                    keys,
                    columnFilter,
                    performanceCounters,
                    timestamp,
                    memoryManager);
            } else {
                reader = New<TColumnarVersionedLookupChunkReader<TLookupSingleVersionColumnarRowBuilder>>(
                    std::move(config),
                    chunkMeta,
                    std::move(chunkReader),
                    chunkMeta->GetSchema()->GetSortColumns(),
                    blockCache,
                    chunkReadOptions,
                    keys,
                    columnFilter,
                    performanceCounters,
                    timestamp,
                    memoryManager);
            }
            break;
        }

        case EChunkFormat::TableUnversionedColumnar:
        case EChunkFormat::TableSchemalessHorizontal: {
            if (produceAllVersions && !columnFilter.IsUniversal()) {
                THROW_ERROR_EXCEPTION("Reading all value versions is not supported with non-universal column filter");
            }

            auto chunkTimestamp = chunkState->ChunkTimestamp
                ? chunkState->ChunkTimestamp
                : static_cast<TTimestamp>(chunkMeta->Misc().min_timestamp());
            if (timestamp < chunkTimestamp) {
                return CreateEmptyVersionedReader(keys.Size());
            }

            auto schemalessReaderFactory = [&] (TNameTablePtr nameTable, const TColumnFilter& columnFilter) {
                auto options = New<TTableReaderOptions>();
                options->DynamicTable = true;

                return CreateSchemalessLookupChunkReader(
                    chunkState,
                    chunkMeta,
                    config,
                    options,
                    chunkReader,
                    nameTable,
                    chunkReadOptions,
                    chunkMeta->GetSchema()->GetSortColumns(),
                    /* omittedInaccessibleColumns */ {},
                    columnFilter,
                    keys,
                    nullptr,
                    std::nullopt,
                    memoryManager);
            };
            auto schemafulReaderFactory = [&] (const TTableSchemaPtr& schema, const TColumnFilter& columnFilter) {
                return CreateSchemafulReaderAdapter(schemalessReaderFactory, schema, columnFilter, /*ignoreRequired*/ true);
            };

            return CreateVersionedReaderAdapter(
                std::move(schemafulReaderFactory),
                chunkMeta->GetSchema(),
                columnFilter,
                chunkTimestamp);
        }

        default:
            YT_ABORT();
    }

    if (chunkState->ChunkTimestamp) {
        return CreateTimestampResettingAdapter(
            reader,
            chunkState->ChunkTimestamp);
    } else {
        return reader;
    }
}

////////////////////////////////////////////////////////////////////////////////

TRowReaderAdapter::TRowReaderAdapter(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr chunkReader,
    const TChunkStatePtr& chunkState,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TClientChunkReadOptions& chunkReadOptions,
    const TSharedRange<TLegacyKey>& keys,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp,
    bool produceAllVersions)
    : KeyCount_(keys.Size())
    , UnderlyingReader_(
        CreateVersionedChunkReader(
            config,
            chunkReader,
            chunkState,
            chunkMeta,
            chunkReadOptions,
            keys,
            columnFilter,
            timestamp,
            produceAllVersions))
{ }

void TRowReaderAdapter::ReadRowset(const std::function<void(TVersionedRow)>& onRow)
{
    for (int i = 0; i < KeyCount_; ++i) {
        onRow(FetchRow());
    }
}

TVersionedRow TRowReaderAdapter::FetchRow()
{
    ++RowIndex_;
    if (!RowBatch_ || RowIndex_ >= RowBatch_->GetRowCount()) {
        RowIndex_ = 0;
        while (true) {
            TRowBatchReadOptions options{
                .MaxRowsPerRead = RowBufferCapacity
            };
            RowBatch_ = UnderlyingReader_->Read(options);
            YT_VERIFY(RowBatch_);
            if (!RowBatch_->IsEmpty()) {
                break;
            }
            NConcurrency::WaitFor(UnderlyingReader_->GetReadyEvent())
                .ThrowOnError();
        }
    }
    return RowBatch_->MaterializeRows()[RowIndex_];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

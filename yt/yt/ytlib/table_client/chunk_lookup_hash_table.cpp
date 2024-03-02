#include "cached_versioned_chunk_meta.h"
#include "chunk_lookup_hash_table.h"
#include "private.h"
#include "schemaless_block_reader.h"
#include "versioned_block_reader.h"
#include "versioned_chunk_reader.h"
#include "chunk_column_mapping.h"

#include <yt/yt/ytlib/table_chunk_format/slim_versioned_block_reader.h>

#include <yt/yt/ytlib/columnar_chunk_format/versioned_chunk_reader.h>

#include <yt/yt/ytlib/chunk_client/block_fetcher.h>
#include <yt/yt/ytlib/chunk_client/preloaded_block_cache.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NTableChunkFormat;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

// We put 16-bit block index and 32-bit row index into 48-bit value entry in LinearProbeHashTable.

static constexpr i64 MaxBlockIndex = std::numeric_limits<ui16>::max();

////////////////////////////////////////////////////////////////////////////////

class TSimpleBlockCache
    : public IBlockCache
{
public:
    TSimpleBlockCache(
        int startBlockIndex,
        const std::vector<TBlock>& blocks)
        : StartBlockIndex_(startBlockIndex)
        , Blocks_(blocks)
    { }

    void PutBlock(
        const TBlockId& /*id*/,
        EBlockType /*type*/,
        const TBlock& /*block*/) override
    {
        YT_ABORT();
    }

    TCachedBlock FindBlock(
        const TBlockId& id,
        EBlockType type) override
    {
        YT_VERIFY(type == EBlockType::UncompressedData);
        return id.BlockIndex >= StartBlockIndex_ && id.BlockIndex < StartBlockIndex_ + static_cast<int>(Blocks_.size())
            ? TCachedBlock(Blocks_[id.BlockIndex - StartBlockIndex_])
            : TCachedBlock();
    }

    std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const TBlockId& id,
        EBlockType type) override
    {
        YT_VERIFY(type == EBlockType::UncompressedData);
        return id.BlockIndex >= StartBlockIndex_ && id.BlockIndex < StartBlockIndex_ + static_cast<int>(Blocks_.size())
            ? CreatePresetCachedBlockCookie(TCachedBlock(Blocks_[id.BlockIndex - StartBlockIndex_]))
            : CreateActiveCachedBlockCookie();
    }

    EBlockType GetSupportedBlockTypes() const override
    {
        return EBlockType::UncompressedData;
    }

    bool IsBlockTypeActive(EBlockType blockType) const override
    {
        return blockType == EBlockType::UncompressedData;
    }

private:
    const int StartBlockIndex_;
    const std::vector<TBlock>& Blocks_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TReader, class TRow>
bool ReadRows(const TReader& reader, std::vector<TRow>* rows)
{
    TRowBatchReadOptions readOptions{
        .MaxRowsPerRead = i64(rows->capacity())
    };

    auto batch = reader->Read(readOptions);

    if (!batch) {
        return false;
    }

    // Materialize rows here.
    // Drop null rows.
    auto batchRows = batch->MaterializeRows();
    rows->reserve(batchRows.size());
    rows->clear();
    for (auto row : batchRows) {
        rows->push_back(row);
    }

    return true;
}

void FormatValue(TStringBuilderBase* builder, TRange<TUnversionedValue> row, TStringBuf format)
{
    if (row) {
        builder->AppendChar('[');
        JoinToString(
            builder,
            row.Begin(),
            row.End(),
            [&] (TStringBuilderBase* builder, const TUnversionedValue& value) {
                FormatValue(builder, value, format);
            });
        builder->AppendChar(']');
    } else {
        builder->AppendString("<null>");
    }
}

TChunkLookupHashTablePtr CreateChunkLookupHashTableForColumnarFormat(
    IVersionedReaderPtr reader,
    size_t chunkRowCount)
{
    auto hashTable = New<TChunkLookupHashTable>(chunkRowCount);

    std::vector<TVersionedRow> rows;
    rows.reserve(1024);

    int rowIndex = 0;
    while (ReadRows(reader, &rows)) {
        if (rows.empty()) {
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
        } else {
            for (const auto& row : rows) {
                YT_VERIFY(hashTable->Insert(GetFarmFingerprint(row.Keys()), rowIndex));
                ++rowIndex;
            }
        }
    }

    return hashTable;
}

TChunkLookupHashTablePtr CreateChunkLookupHashTable(
    NChunkClient::TChunkId chunkId,
    int startBlockIndex,
    int endBlockIndex,
    IBlockCachePtr blockCache,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TTableSchemaPtr& tableSchema,
    const TKeyComparer& keyComparer)
{
    auto chunkFormat = chunkMeta->GetChunkFormat();
    const auto& chunkBlockMeta = chunkMeta->DataBlockMeta();

    if (chunkFormat == EChunkFormat::TableVersionedColumnar) {
        auto chunkRowCount = chunkMeta->Misc().row_count();

        auto blockManagerFactory = NColumnarChunkFormat::CreateSyncBlockWindowManagerFactory(
            blockCache,
            chunkMeta,
            chunkId);

        auto keysReader = NColumnarChunkFormat::CreateVersionedChunkReader(
            MakeSingletonRowRange(MinKey(), MaxKey()),
            NTransactionClient::AllCommittedTimestamp,
            chunkMeta,
            tableSchema,
            TColumnFilter(tableSchema->GetKeyColumnCount()),
            nullptr,
            blockManagerFactory,
            true);

        return CreateChunkLookupHashTableForColumnarFormat(keysReader, chunkRowCount);
    }

    if (chunkFormat != EChunkFormat::TableVersionedSimple &&
        chunkFormat != EChunkFormat::TableVersionedIndexed &&
        chunkFormat != EChunkFormat::TableUnversionedSchemalessHorizontal)
    {
        YT_LOG_INFO("Cannot create lookup hash table for improper chunk format "
            "(ChunkId: %v, ChunkFormat: %v)",
            chunkId,
            chunkFormat);
        return nullptr;
    }

    int lastBlockIndex = endBlockIndex - 1;
    if (lastBlockIndex > MaxBlockIndex) {
        YT_LOG_INFO("Cannot create lookup hash table because chunk has too many blocks "
            "(ChunkId: %v, LastBlockIndex: %v)",
            chunkId,
            lastBlockIndex);
        return nullptr;
    }

    auto chunkSize = chunkBlockMeta->data_blocks(lastBlockIndex).chunk_row_count() -
        (startBlockIndex > 0 ? chunkBlockMeta->data_blocks(startBlockIndex - 1).chunk_row_count() : 0);
    auto hashTable = New<TChunkLookupHashTable>(chunkSize);

    const std::vector<ESortOrder> sortOrders(tableSchema->GetKeyColumnCount(), ESortOrder::Ascending);

    for (int blockIndex = startBlockIndex; blockIndex <= lastBlockIndex; ++blockIndex) {
        auto blockId = TBlockId(chunkId, blockIndex);
        auto uncompressedBlock = blockCache->FindBlock(blockId, EBlockType::UncompressedData);
        if (!uncompressedBlock) {
            YT_LOG_INFO("Cannot create lookup hash table because chunk data is missing in the cache "
                "(ChunkId: %v, BlockIndex: %v)",
                chunkId,
                blockIndex);
            return nullptr;
        }

        const auto& blockMeta = chunkBlockMeta->data_blocks(blockIndex);

        auto fillHashTableFromReader = [&] (auto& blockReader) {
            // Verify that row index fits into 32 bits.
            YT_VERIFY(sizeof(blockMeta.row_count()) <= sizeof(ui32));

            YT_VERIFY(blockReader.SkipToRowIndex(0));

            for (int rowIndex = 0; rowIndex < blockMeta.row_count(); ++rowIndex) {
                auto key = blockReader.GetKey();
                YT_VERIFY(hashTable->Insert(GetFarmFingerprint(MakeRange(key.Begin(), key.End())), PackBlockAndRowIndexes(blockIndex, rowIndex)));
                blockReader.NextRow();
            }
        };

        auto fillHashTable = [&] <class TReader> {
            TReader blockReader(
                uncompressedBlock.Data,
                blockMeta,
                chunkMeta->Misc().block_format_version(),
                chunkMeta->ChunkSchema(),
                tableSchema->GetKeyColumnCount(),
                TChunkColumnMapping(tableSchema, chunkMeta->ChunkSchema())
                    .BuildVersionedSimpleSchemaIdMapping(TColumnFilter()),
                keyComparer,
                AllCommittedTimestamp,
                /*produceAllVersions*/ true);
            fillHashTableFromReader(blockReader);
        };

        switch (chunkFormat) {
            case EChunkFormat::TableVersionedSimple:
                fillHashTable.operator()<TSimpleVersionedBlockReader>();
                break;

            case EChunkFormat::TableVersionedIndexed:
                fillHashTable.operator()<TIndexedVersionedBlockReader>();
                break;

            case EChunkFormat::TableVersionedSlim:
                fillHashTable.operator()<TSlimVersionedBlockReader>();
                break;

            case EChunkFormat::TableUnversionedSchemalessHorizontal: {
                THorizontalSchemalessVersionedBlockReader blockReader(
                    uncompressedBlock.Data,
                    blockMeta,
                    GetCompositeColumnFlags(chunkMeta->ChunkSchema()),
                    GetHunkColumnFlags(chunkMeta->GetChunkFormat(), chunkMeta->GetChunkFeatures(), chunkMeta->ChunkSchema()),
                    chunkMeta->HunkChunkMetas(),
                    chunkMeta->HunkChunkRefs(),
                    TChunkColumnMapping(tableSchema, chunkMeta->ChunkSchema())
                        .BuildSchemalessHorizontalSchemaIdMapping(TColumnFilter()),
                    sortOrders,
                    chunkMeta->GetChunkKeyColumnCount(),
                    tableSchema,
                    chunkMeta->Misc().min_timestamp());
                fillHashTableFromReader(blockReader);
                break;
            }

            default:
                YT_ABORT();
        }
    }

    return hashTable;
}

TChunkLookupHashTablePtr CreateChunkLookupHashTable(
    NChunkClient::TChunkId chunkId,
    int startBlockIndex,
    const std::vector<TBlock>& blocks,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TTableSchemaPtr& tableSchema,
    const TKeyComparer& keyComparer)
{
    auto blockCache = New<TSimpleBlockCache>(startBlockIndex, blocks);

    return CreateChunkLookupHashTable(chunkId, startBlockIndex, startBlockIndex + blocks.size(), blockCache, chunkMeta, tableSchema, keyComparer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

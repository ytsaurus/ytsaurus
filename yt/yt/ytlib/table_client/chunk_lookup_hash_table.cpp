#include "cached_versioned_chunk_meta.h"
#include "chunk_lookup_hash_table.h"
#include "private.h"
#include "schemaless_block_reader.h"
#include "versioned_block_reader.h"
#include "versioned_chunk_reader.h"
#include "chunk_column_mapping.h"

#include <yt/yt/ytlib/table_chunk_format/slim_versioned_block_reader.h>

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NTableChunkFormat;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkLookupHashTable
    : public IChunkLookupHashTable
{
public:
    explicit TChunkLookupHashTable(size_t size);
    void Insert(TLegacyKey key, std::pair<ui16, ui32> index) override;
    TCompactVector<std::pair<ui16, ui32>, 1> Find(TLegacyKey key) const override;
    size_t GetByteSize() const override;

private:
    TLinearProbeHashTable HashTable_;
};

////////////////////////////////////////////////////////////////////////////////

// We put 16-bit block index and 32-bit row index into 48-bit value entry in LinearProbeHashTable.

static constexpr i64 MaxBlockIndex = std::numeric_limits<ui16>::max();

TChunkLookupHashTable::TChunkLookupHashTable(size_t size)
    : HashTable_(size)
{ }

void TChunkLookupHashTable::Insert(TLegacyKey key, std::pair<ui16, ui32> index)
{
    YT_VERIFY(HashTable_.Insert(GetFarmFingerprint(key), (static_cast<ui64>(index.first) << 32) | index.second));
}

TCompactVector<std::pair<ui16, ui32>, 1> TChunkLookupHashTable::Find(TLegacyKey key) const
{
    TCompactVector<std::pair<ui16, ui32>, 1> result;
    TCompactVector<ui64, 1> items;
    HashTable_.Find(GetFarmFingerprint(key), &items);
    for (const auto& value : items) {
        result.emplace_back(value >> 32, static_cast<ui32>(value));
    }
    return result;
}

size_t TChunkLookupHashTable::GetByteSize() const
{
    return HashTable_.GetByteSize();
}

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

private:
    const int StartBlockIndex_;
    const std::vector<TBlock>& Blocks_;
};

////////////////////////////////////////////////////////////////////////////////

IChunkLookupHashTablePtr CreateChunkLookupHashTable(
    NChunkClient::TChunkId chunkId,
    int startBlockIndex,
    const std::vector<TBlock>& blocks,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TTableSchemaPtr& tableSchema,
    const TKeyComparer& keyComparer)
{
    auto chunkFormat = chunkMeta->GetChunkFormat();
    const auto& chunkBlockMeta = chunkMeta->DataBlockMeta();

    if (chunkFormat != EChunkFormat::TableVersionedSimple &&
        chunkFormat != EChunkFormat::TableVersionedIndexed &&
        chunkFormat != EChunkFormat::TableUnversionedSchemalessHorizontal)
    {
        YT_LOG_INFO("Cannot create lookup hash table for improper chunk format (ChunkId: %v, ChunkFormat: %v)",
            chunkId,
            chunkFormat);
        return nullptr;
    }

    int lastBlockIndex = startBlockIndex + static_cast<int>(blocks.size()) - 1;
    if (lastBlockIndex > MaxBlockIndex) {
        YT_LOG_INFO("Cannot create lookup hash table because chunk has too many blocks (ChunkId: %v, LastBlockIndex: %v)",
            chunkId,
            lastBlockIndex);
        return nullptr;
    }

    auto blockCache = New<TSimpleBlockCache>(startBlockIndex, blocks);

    auto chunkSize = chunkBlockMeta->data_blocks(lastBlockIndex).chunk_row_count() -
        (startBlockIndex > 0 ? chunkBlockMeta->data_blocks(startBlockIndex - 1).chunk_row_count() : 0);
    auto hashTable = New<TChunkLookupHashTable>(chunkSize);

    const std::vector<ESortOrder> sortOrders(tableSchema->GetKeyColumnCount(), ESortOrder::Ascending);

    for (int blockIndex = startBlockIndex; blockIndex <= lastBlockIndex; ++blockIndex) {
        auto blockId = TBlockId(chunkId, blockIndex);
        auto uncompressedBlock = blockCache->FindBlock(blockId, EBlockType::UncompressedData).Block;
        if (!uncompressedBlock) {
            YT_LOG_INFO("Cannot create lookup hash table because chunk data is missing in the cache (ChunkId: %v, BlockIndex: %v)",
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
                hashTable->Insert(key, std::make_pair<ui16, ui32>(blockIndex, rowIndex));
                blockReader.NextRow();
            }
        };

        auto fillHashTable = [&] <class TReader> {
            TReader blockReader(
                uncompressedBlock.Data,
                blockMeta,
                chunkMeta->Misc().block_format_version(),
                chunkMeta->GetChunkSchema(),
                tableSchema->GetKeyColumnCount(),
                TChunkColumnMapping(tableSchema, chunkMeta->GetChunkSchema())
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
                    GetCompositeColumnFlags(chunkMeta->GetChunkSchema()),
                    TChunkColumnMapping(tableSchema, chunkMeta->GetChunkSchema())
                        .BuildSchemalessHorizontalSchemaIdMapping(TColumnFilter()),
                    sortOrders,
                    chunkMeta->GetChunkKeyColumnCount(),
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

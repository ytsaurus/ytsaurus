#include "cached_versioned_chunk_meta.h"
#include "chunk_lookup_hash_table.h"
#include "private.h"
#include "schemaless_block_reader.h"
#include "versioned_block_reader.h"
#include "versioned_chunk_reader.h"

namespace NYT::NTableClient {

using namespace NConcurrency;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkLookupHashTable
    : public IChunkLookupHashTable
{
public:
    explicit TChunkLookupHashTable(size_t size);
    virtual void Insert(TKey key, std::pair<ui16, ui32> index) override;
    virtual SmallVector<std::pair<ui16, ui32>, 1> Find(TKey key) const override;
    virtual size_t GetByteSize() const override;

private:
    TLinearProbeHashTable HashTable_;
};

////////////////////////////////////////////////////////////////////////////////

// We put 16-bit block index and 32-bit row index into 48-bit value entry in LinearProbeHashTable.

static constexpr i64 MaxBlockIndex = std::numeric_limits<ui16>::max();

TChunkLookupHashTable::TChunkLookupHashTable(size_t size)
    : HashTable_(size)
{ }

void TChunkLookupHashTable::Insert(TKey key, std::pair<ui16, ui32> index)
{
    YT_VERIFY(HashTable_.Insert(GetFarmFingerprint(key), (static_cast<ui64>(index.first) << 32) | index.second));
}

SmallVector<std::pair<ui16, ui32>, 1> TChunkLookupHashTable::Find(TKey key) const
{
    SmallVector<std::pair<ui16, ui32>, 1> result;
    SmallVector<ui64, 1> items;
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

    virtual void Put(
        const TBlockId& /*id*/,
        EBlockType /*type*/,
        const TBlock& /*block*/,
        const std::optional<NNodeTrackerClient::TNodeDescriptor>& /*source*/) override
    {
        YT_ABORT();
    }

    virtual TBlock Find(
        const TBlockId& id,
        EBlockType type) override
    {
        YT_ASSERT(type == EBlockType::UncompressedData);
        return id.BlockIndex >= StartBlockIndex_ && id.BlockIndex < StartBlockIndex_ + static_cast<int>(Blocks_.size())
            ? Blocks_[id.BlockIndex - StartBlockIndex_]
            : TBlock();
    }

    virtual EBlockType GetSupportedBlockTypes() const override
    {
        return EBlockType::UncompressedData;
    }

private:
    const int StartBlockIndex_;
    const std::vector<TBlock>& Blocks_;
};

////////////////////////////////////////////////////////////////////////////////

IChunkLookupHashTablePtr CreateChunkLookupHashTable(
    int startBlockIndex,
    const std::vector<TBlock>& blocks,
    const TCachedVersionedChunkMetaPtr& chunkMeta,
    const TKeyComparer& keyComparer)
{
    if (chunkMeta->GetChunkFormat() != ETableChunkFormat::VersionedSimple &&
        chunkMeta->GetChunkFormat() != ETableChunkFormat::SchemalessHorizontal)
    {
        YT_LOG_INFO("Cannot create lookup hash table for improper chunk format (ChunkId: %v, ChunkFormat: %v)",
            chunkMeta->GetChunkId(),
            chunkMeta->GetChunkFormat());
        return nullptr;
    }

    int lastBlockIndex = startBlockIndex + static_cast<int>(blocks.size()) - 1;
    if (lastBlockIndex > MaxBlockIndex) {
        YT_LOG_INFO("Cannot create lookup hash table because chunk has too many blocks (ChunkId: %v, LastBlockIndex: %v)",
            chunkMeta->GetChunkId(),
            lastBlockIndex);
        return nullptr;
    }

    auto blockCache = New<TSimpleBlockCache>(startBlockIndex, blocks);

    auto chunkSize = chunkMeta->BlockMeta()->blocks(lastBlockIndex).chunk_row_count() -
        (startBlockIndex > 0 ? chunkMeta->BlockMeta()->blocks(startBlockIndex - 1).chunk_row_count() : 0);
    auto hashTable = New<TChunkLookupHashTable>(chunkSize);

    for (int blockIndex = startBlockIndex; blockIndex <= lastBlockIndex; ++blockIndex) {
        auto blockId = TBlockId(chunkMeta->GetChunkId(), blockIndex);
        auto uncompressedBlock = blockCache->Find(blockId, EBlockType::UncompressedData);
        if (!uncompressedBlock) {
            YT_LOG_INFO("Cannot create lookup hash table because chunk data is missing in the cache (ChunkId: %v, BlockIndex: %v)",
                chunkMeta->GetChunkId(),
                blockIndex);
            return nullptr;
        }

        const auto& blockMeta = chunkMeta->BlockMeta()->blocks(blockIndex);

        std::unique_ptr<IVersionedBlockReader> blockReader;
        switch (chunkMeta->GetChunkFormat()) {
            case ETableChunkFormat::VersionedSimple:
                blockReader = std::make_unique<TSimpleVersionedBlockReader>(
                    uncompressedBlock.Data,
                    blockMeta,
                    chunkMeta->ChunkSchema(),
                    chunkMeta->GetChunkKeyColumnCount(),
                    chunkMeta->GetKeyColumnCount(),
                    BuildVersionedSimpleSchemaIdMapping(TColumnFilter(), chunkMeta),
                    keyComparer,
                    AllCommittedTimestamp,
                    true,
                    true);
                break;

            case ETableChunkFormat::SchemalessHorizontal:
                blockReader = std::make_unique<THorizontalSchemalessVersionedBlockReader>(
                    uncompressedBlock.Data,
                    blockMeta,
                    chunkMeta->ChunkSchema(),
                    BuildSchemalessHorizontalSchemaIdMapping(TColumnFilter(), chunkMeta),
                    chunkMeta->GetChunkKeyColumnCount(),
                    chunkMeta->GetKeyColumnCount(),
                    chunkMeta->Misc().min_timestamp());
                break;

            default:
                YT_ABORT();
        }

        // Verify that row index fits into 32 bits.
        YT_VERIFY(sizeof(blockMeta.row_count()) <= sizeof(ui32));

        for (int rowIndex = 0; rowIndex < blockMeta.row_count(); ++rowIndex) {
            auto key = blockReader->GetKey();
            hashTable->Insert(key, std::make_pair<ui16, ui32>(blockIndex, rowIndex));
            blockReader->NextRow();
        }
    }

    return hashTable;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#include "stdafx.h"
#include "versioned_chunk_reader.h"
#include "cached_versioned_chunk_meta.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader_base.h"
#include "config.h"
#include "schema.h"
#include "versioned_block_reader.h"
#include "versioned_reader.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/chunk_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/sequential_reader.h>

#include <core/compression/public.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient::NProto;

using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

struct TVersionedChunkReaderPoolTag { };

class TVersionedChunkReader
    : public IVersionedReader
    , public TChunkReaderBase
{
public:
    TVersionedChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IChunkReaderPtr underlyingReader,
        IBlockCachePtr uncompressedBlockCache,
        TReadLimit lowerLimit,
        TReadLimit upperLimit,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp);

    virtual bool Read(std::vector<TVersionedRow>* rows) override;

private:
    const TTimestamp Timestamp_;

    TCachedVersionedChunkMetaPtr CachedChunkMeta_;
    std::vector<TColumnIdMapping> SchemaIdMapping_;

    std::unique_ptr<TSimpleVersionedBlockReader> BlockReader_;

    TChunkedMemoryPool MemoryPool_;

    int CurrentBlockIndex_ = 0;
    i64 CurrentRowIndex_ = 0;

    i64 RowCount_ = 0;


    virtual std::vector<TSequentialReader::TBlockInfo> GetBlockSequence() override;

    virtual void InitFirstBlock() override;
    virtual void InitNextBlock() override;

};

////////////////////////////////////////////////////////////////////////////////

TVersionedChunkReader::TVersionedChunkReader(
    TChunkReaderConfigPtr config,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IChunkReaderPtr underlyingReader,
    IBlockCachePtr uncompressedBlockCache,
    TReadLimit lowerLimit,
    TReadLimit upperLimit,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp)
    : TChunkReaderBase(
        std::move(config),
        std::move(lowerLimit),
        std::move(upperLimit),
        std::move(underlyingReader),
        chunkMeta->Misc(),
        std::move(uncompressedBlockCache))
    , Timestamp_(timestamp)
    , CachedChunkMeta_(std::move(chunkMeta))
    , MemoryPool_(TVersionedChunkReaderPoolTag())
{
    YCHECK(CachedChunkMeta_->Misc().sorted());
    YCHECK(EChunkType(CachedChunkMeta_->ChunkMeta().type()) == EChunkType::Table);
    YCHECK(ETableChunkFormat(CachedChunkMeta_->ChunkMeta().version()) == ETableChunkFormat::VersionedSimple);
    YCHECK(Timestamp_ != AsyncAllCommittedTimestamp || columnFilter.All);

    if (columnFilter.All) {
        SchemaIdMapping_ = CachedChunkMeta_->SchemaIdMapping();
    } else {
        SchemaIdMapping_.reserve(CachedChunkMeta_->SchemaIdMapping().size());
        int keyColumnCount = static_cast<int>(CachedChunkMeta_->KeyColumns().size());
        for (auto index : columnFilter.Indexes) {
            if (index >= keyColumnCount) {
                auto mappingIndex = index - keyColumnCount;
                SchemaIdMapping_.push_back(CachedChunkMeta_->SchemaIdMapping()[mappingIndex]);
            }
        }
    }
}

bool TVersionedChunkReader::Read(std::vector<TVersionedRow>* rows)
{
    YCHECK(rows->capacity() > 0);

    MemoryPool_.Clear();
    rows->clear();

    if (!ReadyEvent_.IsSet()) {
        // Waiting for the next block.
        return true;
    }

    if (!BlockReader_) {
        // Nothing to read from chunk.
        return false;
    }

    if (BlockEnded_) {
        BlockReader_.reset();
        return OnBlockEnded();
    }

    while (rows->size() < rows->capacity()) {
        ++CurrentRowIndex_;
        if (UpperLimit_.HasRowIndex() && CurrentRowIndex_ == UpperLimit_.GetRowIndex()) {
            return false;
        }

        if (UpperLimit_.HasKey() && CompareRows(BlockReader_->GetKey(), UpperLimit_.GetKey().Get()) >= 0) {
            return false;
        }

        auto row = BlockReader_->GetRow(&MemoryPool_);
        if (row) {
            YASSERT(
                rows->empty() ||
                CompareRows(
                    rows->back().BeginKeys(), rows->back().EndKeys(),
                    row.BeginKeys(), row.EndKeys()) < 0);
            rows->push_back(row);
            ++RowCount_;
        }

        if (!BlockReader_->NextRow()) {
            BlockEnded_ = true;
            return true;
        }
    }

    return true;
}

std::vector<TSequentialReader::TBlockInfo> TVersionedChunkReader::GetBlockSequence()
{
    const auto& blockMetaExt = CachedChunkMeta_->BlockMeta();

    int CurrentBlockIndex_ = std::max(ApplyLowerRowLimit(blockMetaExt), ApplyLowerKeyLimit(blockMetaExt));
    int endBlockIndex = std::min(ApplyUpperRowLimit(blockMetaExt), ApplyUpperKeyLimit(blockMetaExt));

    std::vector<TSequentialReader::TBlockInfo> blocks;

    if (CurrentBlockIndex_ >= blockMetaExt.blocks_size()) {
        return blocks;
    }

    auto& blockMeta = blockMetaExt.blocks(CurrentBlockIndex_);
    CurrentRowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();

    for (int index = CurrentBlockIndex_; index < endBlockIndex; ++index) {
        auto& blockMeta = blockMetaExt.blocks(index);
        TSequentialReader::TBlockInfo blockInfo;
        blockInfo.Index = blockMeta.block_index();
        blockInfo.UncompressedDataSize = blockMeta.uncompressed_size();
        blocks.push_back(blockInfo);
    }

    return blocks;
}

void TVersionedChunkReader::InitFirstBlock()
{
    BlockReader_.reset(new TSimpleVersionedBlockReader(
        SequentialReader_->GetCurrentBlock(),
        CachedChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
        CachedChunkMeta_->ChunkSchema(),
        CachedChunkMeta_->KeyColumns(),
        SchemaIdMapping_,
        Timestamp_));

    if (LowerLimit_.HasRowIndex()) {
        YCHECK(BlockReader_->SkipToRowIndex(LowerLimit_.GetRowIndex() - CurrentRowIndex_));
        CurrentRowIndex_ = LowerLimit_.GetRowIndex();
    }

    if (LowerLimit_.HasKey()) {
        auto blockRowIndex = BlockReader_->GetRowIndex();
        YCHECK(BlockReader_->SkipToKey(LowerLimit_.GetKey().Get()));
        CurrentRowIndex_ += BlockReader_->GetRowIndex() - blockRowIndex;
    }
}

void TVersionedChunkReader::InitNextBlock()
{
    ++CurrentBlockIndex_;
    BlockReader_.reset(new TSimpleVersionedBlockReader(
        SequentialReader_->GetCurrentBlock(),
        CachedChunkMeta_->BlockMeta().blocks(CurrentBlockIndex_),
        CachedChunkMeta_->ChunkSchema(),
        CachedChunkMeta_->KeyColumns(),
        SchemaIdMapping_,
        Timestamp_));
}

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    IChunkReaderPtr chunkReader,
    IBlockCachePtr uncompressedBlockCache,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TReadLimit lowerLimit,
    TReadLimit upperLimit,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp)
{
    auto formatVersion = ETableChunkFormat(chunkMeta->ChunkMeta().version());
    switch (formatVersion) {
        case ETableChunkFormat::VersionedSimple:
            return New<TVersionedChunkReader>(
                std::move(config),
                std::move(chunkMeta),
                std::move(chunkReader),
                std::move(uncompressedBlockCache),
                std::move(lowerLimit),
                std::move(upperLimit),
                columnFilter,
                timestamp);

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

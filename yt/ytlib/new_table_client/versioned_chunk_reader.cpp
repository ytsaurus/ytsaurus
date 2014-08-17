#include "stdafx.h"

#include "versioned_chunk_reader.h"

#include "cached_versioned_chunk_meta.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "schema.h"
#include "versioned_block_reader.h"
#include "versioned_reader.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/reader.h>
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

template <class TBlockReader>
class TVersionedChunkReader
    : public IVersionedReader
{
public:
    TVersionedChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IReaderPtr chunkReader,
        TReadLimit lowerLimit,
        TReadLimit upperLimit,
        const TColumnFilter& columnFilter,
        TTimestamp timestamp);

    virtual TAsyncError Open() override;
    virtual bool Read(std::vector<TVersionedRow>* rows) override;
    virtual TAsyncError GetReadyEvent() override;

private:
    const TChunkReaderConfigPtr Config_;
    TCachedVersionedChunkMetaPtr CachedChunkMeta_;
    IReaderPtr ChunkReader_;
    TReadLimit LowerLimit_;
    TReadLimit UpperLimit_;

    const TTimestamp Timestamp_;

    std::vector<TColumnIdMapping> SchemaIdMapping_;

    std::unique_ptr<TBlockReader> BlockReader_;
    std::unique_ptr<TBlockReader> PreviousBlockReader_;

    TSequentialReaderPtr SequentialReader_;

    TChunkedMemoryPool MemoryPool_;

    int CurrentBlockIndex_ = 0;
    i64 CurrentRowIndex_ = 0;

    i64 RowCount_ = 0;

    TAsyncErrorPromise ReadyEvent_ = MakePromise(TError());


    int GetBeginBlockIndex() const;
    int GetEndBlockIndex() const;

    void DoOpen();
    void DoSwitchBlock();
    TBlockReader* NewBlockReader();

};

////////////////////////////////////////////////////////////////////////////////

template <class TBlockReader>
TVersionedChunkReader<TBlockReader>::TVersionedChunkReader(
    TChunkReaderConfigPtr config,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IReaderPtr chunkReader,
    TReadLimit lowerLimit,
    TReadLimit upperLimit,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp)
    : Config_(std::move(config))
    , CachedChunkMeta_(std::move(chunkMeta))
    , ChunkReader_(std::move(chunkReader))
    , LowerLimit_(std::move(lowerLimit))
    , UpperLimit_(std::move(upperLimit))
    , Timestamp_(timestamp)
    , MemoryPool_(TVersionedChunkReaderPoolTag())
{
    YCHECK(CachedChunkMeta_->Misc().sorted());
    YCHECK(CachedChunkMeta_->ChunkMeta().type() == EChunkType::Table);
    YCHECK(CachedChunkMeta_->ChunkMeta().version() == TBlockReader::FormatVersion);
    YCHECK(Timestamp_ != AllCommittedTimestamp || columnFilter.All);

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

template <class TBlockReader>
TAsyncError TVersionedChunkReader<TBlockReader>::Open()
{
    return BIND(&TVersionedChunkReader<TBlockReader>::DoOpen, MakeStrong(this))
        .Guarded()
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

template <class TBlockReader>
bool TVersionedChunkReader<TBlockReader>::Read(std::vector<TVersionedRow>* rows)
{
    YCHECK(rows->capacity() > 0);

    MemoryPool_.Clear();
    rows->clear();

    if (PreviousBlockReader_) {
        PreviousBlockReader_.reset();
    }

    if (!ReadyEvent_.IsSet()) {
        // Waiting for the next block.
        return true;
    }

    if (!BlockReader_) {
        // Nothing to read from chunk.
        return false;
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
            PreviousBlockReader_.swap(BlockReader_);
            if (SequentialReader_->HasNext()) {
                ReadyEvent_ = NewPromise<TError>();
                BIND(&TVersionedChunkReader<TBlockReader>::DoSwitchBlock, MakeWeak(this))
                    .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
                    .Run();
                return true;
            } else {
                return false;
            }
        }
    }

    return true;
}

template <class TBlockReader>
TAsyncError TVersionedChunkReader<TBlockReader>::GetReadyEvent()
{
    return ReadyEvent_.ToFuture();
}

template <class TBlockReader>
int TVersionedChunkReader<TBlockReader>::GetBeginBlockIndex() const
{
    auto& blockMetaEntries = CachedChunkMeta_->BlockMeta().entries();
    auto& blockIndexKeys = CachedChunkMeta_->BlockIndexKeys();

    int beginBlockIndex = 0;
    if (LowerLimit_.HasRowIndex()) {
        // To make search symmetrical with blockIndex we ignore last block.
        typedef decltype(blockMetaEntries.end()) TIter;
        auto rbegin = std::reverse_iterator<TIter>(blockMetaEntries.end() - 1);
        auto rend = std::reverse_iterator<TIter>(blockMetaEntries.begin());
        auto it = std::upper_bound(
            rbegin,
            rend,
            LowerLimit_.GetRowIndex(),
            [] (int index, const TBlockMeta& blockMeta) {
                // Global (chunkwide) index of last row in block.
                auto maxRowIndex = blockMeta.chunk_row_count() - 1;
                return index > maxRowIndex;
            });

        if (it != rend) {
            beginBlockIndex = std::max(
                beginBlockIndex,
                static_cast<int>(std::distance(it, rend)));
        }
    }

    if (LowerLimit_.HasKey()) {
        typedef decltype(blockIndexKeys.end()) TIter;
        auto rbegin = std::reverse_iterator<TIter>(blockIndexKeys.end());
        auto rend = std::reverse_iterator<TIter>(blockIndexKeys.begin());
        auto it = std::upper_bound(
            rbegin,
            rend,
            LowerLimit_.GetKey(),
            [] (const TOwningKey& pivot, const TOwningKey& indexKey) {
                return pivot > indexKey;
            });

        if (it != rend) {
            beginBlockIndex = std::max(
                beginBlockIndex,
                static_cast<int>(std::distance(it, rend)));
        }
    }

    return beginBlockIndex;
}

template <class TBlockReader>
int TVersionedChunkReader<TBlockReader>::GetEndBlockIndex() const
{
    auto& blockMetaEntries = CachedChunkMeta_->BlockMeta().entries();
    auto& blockIndexKeys = CachedChunkMeta_->BlockIndexKeys();

    int endBlockIndex = blockMetaEntries.size();
    if (UpperLimit_.HasRowIndex()) {
        auto begin = blockMetaEntries.begin();
        auto end = blockMetaEntries.end() - 1;
        auto it = std::lower_bound(
            begin,
            end,
            UpperLimit_.GetRowIndex(),
            [] (const TBlockMeta& blockMeta, int index) {
                auto maxRowIndex = blockMeta.chunk_row_count() - 1;
                return maxRowIndex < index;
            });

        if (it != end) {
            endBlockIndex = std::min(
                endBlockIndex,
                static_cast<int>(std::distance(blockMetaEntries.begin(), it)) + 1);
        }
    }

    if (UpperLimit_.HasKey()) {
        auto it = std::lower_bound(
            blockIndexKeys.begin(),
            blockIndexKeys.end(),
            UpperLimit_.GetKey(),
            [] (const TOwningKey& indexKey, const TOwningKey& pivot) {
                return indexKey < pivot;
            });

        if (it != blockIndexKeys.end()) {
            endBlockIndex = std::min(
                endBlockIndex,
                static_cast<int>(std::distance(blockIndexKeys.begin(), it)) + 1);
        }
    }

    return endBlockIndex;
}

template <class TBlockReader>
void TVersionedChunkReader<TBlockReader>::DoOpen()
{
    // Check sensible lower limit.
    if (LowerLimit_.HasKey() && LowerLimit_.GetKey() > CachedChunkMeta_->GetMaxKey())
        return;
    if (LowerLimit_.HasRowIndex() &&
        LowerLimit_.GetRowIndex() >= CachedChunkMeta_->Misc().row_count())
        return;

    CurrentBlockIndex_ = GetBeginBlockIndex();
    auto endBlockIndex = GetEndBlockIndex();

    auto& blockMeta = CachedChunkMeta_->BlockMeta().entries(CurrentBlockIndex_);
    CurrentRowIndex_ = blockMeta.chunk_row_count() - blockMeta.row_count();

    std::vector<TSequentialReader::TBlockInfo> blocks;
    for (int index = CurrentBlockIndex_; index < endBlockIndex; ++index) {
        TSequentialReader::TBlockInfo blockInfo;
        blockInfo.Index = index;
        blockInfo.Size = CachedChunkMeta_->BlockMeta().entries(index).block_size();
        blocks.push_back(blockInfo);
    }

    if (blocks.empty())
        return;

    SequentialReader_ = New<TSequentialReader>(
        Config_,
        std::move(blocks),
        ChunkReader_,
        NCompression::ECodec(CachedChunkMeta_->Misc().compression_codec()));

    {
        auto error = WaitFor(SequentialReader_->AsyncNextBlock());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }

    BlockReader_.reset(NewBlockReader());

    if (LowerLimit_.HasRowIndex()) {
        YCHECK(BlockReader_->SkipToRowIndex(LowerLimit_.GetRowIndex() - CurrentRowIndex_));
    }

    if (LowerLimit_.HasKey()) {
        YCHECK(BlockReader_->SkipToKey(LowerLimit_.GetKey().Get()));
    }
}

template <class TBlockReader>
TBlockReader* TVersionedChunkReader<TBlockReader>::NewBlockReader()
{
    return new TBlockReader(
        SequentialReader_->GetBlock(),
        CachedChunkMeta_->BlockMeta().entries(CurrentBlockIndex_),
        CachedChunkMeta_->ChunkSchema(),
        CachedChunkMeta_->KeyColumns(),
        SchemaIdMapping_,
        Timestamp_);
}

template <class TBlockReader>
void TVersionedChunkReader<TBlockReader>::DoSwitchBlock()
{
    auto error = WaitFor(SequentialReader_->AsyncNextBlock());
    ++CurrentBlockIndex_;
    if (error.IsOK()) {
        BlockReader_.reset(NewBlockReader());
    }

    ReadyEvent_.Set(error);
}

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(TChunkReaderConfigPtr config,
    IReaderPtr chunkReader,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TReadLimit lowerLimit,
    TReadLimit upperLimit,
    const TColumnFilter& columnFilter,
    TTimestamp timestamp)
{
    switch (chunkMeta->ChunkMeta().version()) {
        case ETableChunkFormat::VersionedSimple:
            return New<TVersionedChunkReader<TSimpleVersionedBlockReader>>(
                config,
                chunkMeta,
                chunkReader,
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

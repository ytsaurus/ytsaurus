#include "stdafx.h"
#include "versioned_chunk_reader.h"

#include "chunk_meta_extensions.h"
#include "config.h"
#include "schema.h"
#include "versioned_block_reader.h"
#include "versioned_reader.h"
#include "unversioned_row.h"

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/sequential_reader.h>

#include <core/compression/public.h>
#include <core/concurrency/fiber.h>
#include <core/misc/property.h>
#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NProto;
using namespace NConcurrency;
using namespace NChunkClient;

using NCompression::ECodec;

using NChunkClient::NProto::TChunkMeta;
using NChunkClient::NProto::TMiscExt;

////////////////////////////////////////////////////////////////////////////////

TCachedVersionedChunkMeta::TCachedVersionedChunkMeta(
    IAsyncReaderPtr asyncReader,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns)
    : KeyColumns_(keyColumns)
    , AsyncReader_(asyncReader)
    , ReaderSchema_(schema)
{ }

TAsyncError TCachedVersionedChunkMeta::Load()
{
    TAsyncError asyncError;

    auto error = ValidateKeyColumns(ReaderSchema_, KeyColumns_);
    if (!error.IsOK()) {
        asyncError = MakeFuture(error);
    } else {
        asyncError = BIND(&TCachedVersionedChunkMeta::DoLoad, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    asyncError.Subscribe(BIND(
        &TCachedVersionedChunkMeta::ReleaseReader,
        MakeWeak(this)));

    return asyncError;
}

TError TCachedVersionedChunkMeta::ValidateSchema()
{
    auto keyColumns = GetProtoExtension<TKeyColumnsExt>(ChunkMeta_.extensions());
    if (keyColumns.names_size() != KeyColumns_.size()) {
        auto protoColumns = NYT::FromProto<Stroka>(keyColumns.names());
        return TError("Incorrect key columns: actual [%s], expected [%s]",
            ~JoinToString(KeyColumns_),
            ~JoinToString(protoColumns));
    }

    if (!std::equal(
        KeyColumns_.begin(),
        KeyColumns_.end(),
        keyColumns.names().begin()))
    {
        auto protoColumns = NYT::FromProto<Stroka>(keyColumns.names());
        return TError("Incorrect key columns: actual [%s], expected [%s]",
            ~JoinToString(KeyColumns_),
            ~JoinToString(protoColumns));
    }

    auto protoSchema = GetProtoExtension<TTableSchemaExt>(ChunkMeta_.extensions());
    FromProto(&ChunkSchema_, protoSchema);

    SchemaIdMapping_.reserve(ReaderSchema_.Columns().size());
    for (int readerIndex = 0; readerIndex < ReaderSchema_.Columns().size(); ++readerIndex) {
        auto& column = ReaderSchema_.Columns()[readerIndex];
        auto* chunkColumn = ChunkSchema_.FindColumn(column.Name);
        if (!chunkColumn) {
            return TError("Incompatible schema: column %s is absent in chunk schema",
                ~column.Name.Quote());
        }

        if (chunkColumn->Type != column.Type) {
            return TError("Incompatible type for column %s: actual: %s, expected %s",
                ~FormatEnum(chunkColumn->Type).Quote(),
                ~FormatEnum(column.Type).Quote());
        }

        int index = ChunkSchema_.GetColumnIndex(*chunkColumn);
        SchemaIdMapping_.push_back(index);
    }

    return TError();
}

TError TCachedVersionedChunkMeta::DoLoad()
{
    auto getMetaResult = WaitFor(AsyncReader_->AsyncGetChunkMeta());
    RETURN_IF_ERROR(getMetaResult)

    ChunkMeta_ = getMetaResult.GetValue();
    if (ChunkMeta_.type() != EChunkType::Table) {
        return TError("Incorrect chunk type: actual %s, expected %s",
            ~FormatEnum(EChunkType(ChunkMeta_.type())).Quote(),
            ~FormatEnum(EChunkType(EChunkType::Table)).Quote());
    }

    if (ChunkMeta_.version() != ETableChunkFormat::SimpleVersioned) {
        return TError("Incorrect chunk format version: actual %s, expected: %s",
            ~FormatEnum(ETableChunkFormat(ChunkMeta_.version())).Quote(),
            ~FormatEnum(ETableChunkFormat(ETableChunkFormat::SimpleVersioned)).Quote());
    }

    auto error = ValidateSchema();
    RETURN_IF_ERROR(error)

    Misc_ = GetProtoExtension<TMiscExt>(ChunkMeta_.extensions());
    BlockMeta_ = GetProtoExtension<TBlockMetaExt>(ChunkMeta_.extensions());
    BlockIndex_ = GetProtoExtension<TBlockIndexExt>(ChunkMeta_.extensions());
    BoundaryKeys_ = GetProtoExtension<TBoundaryKeysExt>(ChunkMeta_.extensions());

    return TError();
}

void TCachedVersionedChunkMeta::ReleaseReader(TError /* error */)
{
    AsyncReader_.Reset();
}

////////////////////////////////////////////////////////////////////////////////

template <class TBlockReader>
class TVersionedChunkReader
    : public IVersionedReader
{
public:
    TVersionedChunkReader(
        TChunkReaderConfigPtr config,
        TCachedVersionedChunkMetaPtr chunkMeta,
        IAsyncReaderPtr asyncReader,
        TReadLimit lowerLimit,
        TReadLimit upperLimit,
        TTimestamp timestamp);

    virtual TAsyncError Open() override;
    virtual bool Read(std::vector<TVersionedRow>* rows) override;
    virtual TAsyncError GetReadyEvent() override;

private:
    const TChunkReaderConfigPtr Config_;
    TCachedVersionedChunkMetaPtr CachedChunkMeta_;
    IAsyncReaderPtr AsyncReader_;
    TReadLimit LowerLimit_;
    TReadLimit UpperLimit_;

    const TTimestamp Timestamp_;

    std::unique_ptr<TBlockReader> BlockReader_;
    std::unique_ptr<TBlockReader> PreviousBlockReader_;

    TSequentialReaderPtr SequentialReader_;

    TChunkedMemoryPool MemoryPool_;

    int CurrentBlockIndex_;
    i64 CurrentRowIndex_;

    i64 RowCount_;

    TAsyncError NextBlockFuture_;

    int GetBeginBlockIndex() const;
    int GetEndBlockIndex() const;

    TError DoOpen();
    void DoSwitchBlock();
    TBlockReader* NewBlockReader();

};

////////////////////////////////////////////////////////////////////////////////

template <class TBlockReader>
TVersionedChunkReader<TBlockReader>::TVersionedChunkReader(
    TChunkReaderConfigPtr config,
    TCachedVersionedChunkMetaPtr chunkMeta,
    IAsyncReaderPtr asyncReader,
    TReadLimit lowerLimit,
    TReadLimit upperLimit,
    TTimestamp timestamp)
    : Config_(config)
    , CachedChunkMeta_(chunkMeta)
    , AsyncReader_(asyncReader)
    , LowerLimit_(std::move(lowerLimit))
    , UpperLimit_(std::move(upperLimit))
    , Timestamp_(timestamp)
    , CurrentBlockIndex_(0)
    , CurrentRowIndex_(0)
    , RowCount_(0)
    , NextBlockFuture_(MakeFuture(TError()))
{
    YCHECK(CachedChunkMeta_->Misc().sorted());
    YCHECK(CachedChunkMeta_->ChunkMeta().type() == EChunkType::Table);
    YCHECK(CachedChunkMeta_->ChunkMeta().version() == TBlockReader::FormatVersion);
}

template <class TBlockReader>
TAsyncError TVersionedChunkReader<TBlockReader>::Open()
{
    return BIND(&TVersionedChunkReader<TBlockReader>::DoOpen, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

template <class TBlockReader>
bool TVersionedChunkReader<TBlockReader>::Read(std::vector<TVersionedRow>* rows)
{
    YCHECK(rows->capacity() > 0);
    YCHECK(NextBlockFuture_.IsSet());

    MemoryPool_.Clear();
    rows->clear();

    if (PreviousBlockReader_) {
        PreviousBlockReader_.reset();
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

        if (UpperLimit_.HasKey() && CompareRows(BlockReader_->GetKey(), UpperLimit_.GetKey()) >= 0) {
            return false;
        }

        auto row = BlockReader_->GetRow(&MemoryPool_);
        if (row) {
            rows->push_back(row);
            ++RowCount_;
        }

        if (!BlockReader_->NextRow()) {
            PreviousBlockReader_.swap(BlockReader_);
            if (SequentialReader_->HasNext()) {
                NextBlockFuture_ = SequentialReader_->AsyncNextBlock();
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
    return NextBlockFuture_;
}

template <class TBlockReader>
int TVersionedChunkReader<TBlockReader>::GetBeginBlockIndex() const
{
    auto& blockMetaEntries = CachedChunkMeta_->BlockMeta().entries();
    auto& blockIndexEntries = CachedChunkMeta_->BlockIndex().entries();

    int beginBlockIndex = 0;
    if (LowerLimit_.HasRowIndex() && LowerLimit_.GetRowIndex() > 0) {
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
                auto lastRowIndex = blockMeta.chunk_row_count() - 1;
                return index > lastRowIndex;
            });

        if (it != rend) {
            beginBlockIndex = std::max(
                beginBlockIndex,
                static_cast<int>(std::distance(it, rend)));
        }
    }

    if (LowerLimit_.HasKey()) {
        typedef decltype(blockIndexEntries.end()) TIter;
        auto rbegin = std::reverse_iterator<TIter>(blockIndexEntries.end());
        auto rend = std::reverse_iterator<TIter>(blockIndexEntries.begin());
        auto it = std::upper_bound(
            rbegin,
            rend,
            LowerLimit_.GetKey(),
            [] (const TOwningKey& pivot, const TProtoStringType& protoKey) {
                TOwningKey key;
                FromProto(&key, protoKey);
                return pivot > key;
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
    auto& blockIndexEntries = CachedChunkMeta_->BlockIndex().entries();

    int endBlockIndex = blockMetaEntries.size();
    if (UpperLimit_.HasRowIndex() && UpperLimit_.GetRowIndex() > 0) {
        auto begin = blockMetaEntries.begin();
        auto end = blockMetaEntries.end() - 1;
        auto it = std::lower_bound(
            begin,
            end,
            UpperLimit_.GetRowIndex(),
            [] (const TBlockMeta& blockMeta, int index) {
                auto lastRowIndex = blockMeta.chunk_row_count() - 1;
                return index < lastRowIndex;
            });

        if (it != end) {
            endBlockIndex = std::min(
                endBlockIndex,
                static_cast<int>(std::distance(blockMetaEntries.begin(), it)) + 1);
        }
    }

    if (UpperLimit_.HasKey()) {
        auto it = std::lower_bound(
            blockIndexEntries.begin(),
            blockIndexEntries.end(),
            UpperLimit_.GetKey(),
            [] (const TProtoStringType& protoKey, const TOwningKey& pivot) {
                TOwningKey key;
                FromProto(&key, protoKey);
                return pivot < key;
            });

        if (it != blockIndexEntries.end()) {
            endBlockIndex = std::min(
                endBlockIndex,
                static_cast<int>(std::distance(blockIndexEntries.begin(), it)) + 1);
        }
    }

    return endBlockIndex;
}

template <class TBlockReader>
TError TVersionedChunkReader<TBlockReader>::DoOpen()
{
    // Check sensible lower limit.
    if (LowerLimit_.HasKey()) {
        TOwningKey lastKey;
        FromProto(&lastKey, CachedChunkMeta_->BoundaryKeys().last());
        if (LowerLimit_.GetKey() > lastKey) {
            return TError();
        }
    }

    if (LowerLimit_.HasRowIndex() &&
        LowerLimit_.GetRowIndex() >= CachedChunkMeta_->Misc().row_count())
    {
        return TError();
    }

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

    if (blocks.empty()) {
        return TError();
    }

    SequentialReader_ = New<TSequentialReader>(
        Config_,
        std::move(blocks),
        AsyncReader_,
        ECodec(CachedChunkMeta_->Misc().compression_codec()));

    auto error = WaitFor(SequentialReader_->AsyncNextBlock());
    RETURN_IF_ERROR(error);

    BlockReader_.reset(NewBlockReader());

    if (LowerLimit_.HasRowIndex()) {
        YCHECK(BlockReader_->SkipToRowIndex(LowerLimit_.GetRowIndex() - CurrentRowIndex_));
    }

    if (LowerLimit_.HasKey()) {
        YCHECK(BlockReader_->SkipToKey(LowerLimit_.GetKey()));
    }

    return TError();
}

template <class TBlockReader>
TBlockReader* TVersionedChunkReader<TBlockReader>::NewBlockReader()
{
    return new TBlockReader(
        SequentialReader_->GetBlock(),
        CachedChunkMeta_->BlockMeta().entries(CurrentBlockIndex_),
        CachedChunkMeta_->ChunkSchema(),
        CachedChunkMeta_->KeyColumns(),
        CachedChunkMeta_->SchemaIdMapping(),
        Timestamp_);
}

template <class TBlockReader>
void TVersionedChunkReader<TBlockReader>::DoSwitchBlock()
{
    auto error = WaitFor(NextBlockFuture_);
    ++CurrentBlockIndex_;
    if (!error.IsOK()) {
        return;
    }

    BlockReader_.reset(NewBlockReader());
}

////////////////////////////////////////////////////////////////////////////////

IVersionedReaderPtr CreateVersionedChunkReader(
    TChunkReaderConfigPtr config,
    IAsyncReaderPtr asyncReader,
    TCachedVersionedChunkMetaPtr chunkMeta,
    TReadLimit lowerLimit,
    TReadLimit upperLimit,
    TTimestamp timestamp)
{
    switch (chunkMeta->ChunkMeta().version()) {
        case ETableChunkFormat::SimpleVersioned:
            return New<TVersionedChunkReader<TSimpleVersionedBlockReader>>(
                config,
                chunkMeta,
                asyncReader,
                std::move(lowerLimit),
                std::move(upperLimit),
                timestamp);

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

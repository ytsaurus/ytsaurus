#include "stdafx.h"

#include "chunk_reader_base.h"

#include "config.h"

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <core/compression/codec.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NCompression;
using namespace NConcurrency;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TChunkReaderBase::TChunkReaderBase(
    TChunkReaderConfigPtr config,
    const NChunkClient::TReadLimit& lowerLimit,
    const NChunkClient::TReadLimit& upperLimit,
    NChunkClient::IAsyncReaderPtr underlyingReader,
    const NChunkClient::NProto::TMiscExt& misc)
    : Config_(config)
    , LowerLimit_(lowerLimit)
    , UpperLimit_(upperLimit)
    , UnderlyingReader_(underlyingReader)
    , Misc_(misc)
    , BlockEnded_(false)
{ }

TAsyncError TChunkReaderBase::Open()
{
    ReadyEvent_ = BIND(&TChunkReaderBase::DoOpen, MakeStrong(this))
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
    return ReadyEvent_;
}

TAsyncError TChunkReaderBase::GetReadyEvent()
{
    return ReadyEvent_;
}

TError TChunkReaderBase::DoOpen()
{
    try {
        auto blocks = GetBlockSequence();

        if (blocks.empty()) {
            return TError();
        }

        SequentialReader_ = New<TSequentialReader>(
            Config_,
            std::move(blocks),
            UnderlyingReader_,
            ECodec(Misc_.compression_codec()));

        YCHECK(SequentialReader_->HasNext());

        auto error = WaitFor(SequentialReader_->AsyncNextBlock());
        RETURN_IF_ERROR(error);

        InitFirstBlock();

        return TError();
    } catch (const std::exception& ex) {
        return TError(ex);
    }
}

TError TChunkReaderBase::DoSwitchBlock()
{
    auto error = WaitFor(SequentialReader_->AsyncNextBlock());

    if (error.IsOK()) {
        InitNextBlock();
    }

    return error;
}

bool TChunkReaderBase::OnBlockEnded()
{
    BlockEnded_ = false;
    if (SequentialReader_->HasNext()) {
        ReadyEvent_ = BIND(&TChunkReaderBase::DoSwitchBlock, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
        return true;
    } else {
        return false;
    }
}

int TChunkReaderBase::GetBeginBlockIndex(const TBlockMetaExt& blockMeta) const
{
    auto& blockMetaEntries = blockMeta.entries();
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

    return beginBlockIndex;
}

int TChunkReaderBase::GetBeginBlockIndex(const TBlockIndexExt& blockIndex) const
{
    auto& blockIndexEntries = blockIndex.entries();
    int beginBlockIndex = 0;

    if (LowerLimit_.HasKey()) {
        typedef decltype(blockIndexEntries.end()) TIter;
        auto rbegin = std::reverse_iterator<TIter>(blockIndexEntries.end() - 1);
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

int TChunkReaderBase::GetEndBlockIndex(const TBlockMetaExt& blockMeta) const
{
    auto& blockMetaEntries = blockMeta.entries();
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
                static_cast<int>(std::distance(begin, it)) + 1);
        }
    }

    return endBlockIndex;
}

int TChunkReaderBase::GetEndBlockIndex(const TBlockIndexExt& blockIndex) const
{
    auto& blockIndexEntries = blockIndex.entries();
    int endBlockIndex = blockIndexEntries.size();

    if (UpperLimit_.HasKey()) {
        auto begin = blockIndexEntries.begin();
        auto end = blockIndexEntries.end() - 1;
        auto it = std::lower_bound(
            begin,
            end,
            UpperLimit_.GetKey(),
            [] (const TProtoStringType& protoKey, const TOwningKey& pivot) {
                TOwningKey key;
                FromProto(&key, protoKey);
                return key < pivot;
            });

        if (it != end) {
            endBlockIndex = std::min(
                endBlockIndex,
                static_cast<int>(std::distance(begin, it)) + 1);
        }
    }
    return endBlockIndex;
}

TDataStatistics TChunkReaderBase::GetDataStatistics() const
{
    TDataStatistics dataStatistics;
    dataStatistics.set_chunk_count(1);
    dataStatistics.set_uncompressed_data_size(SequentialReader_->GetUncompressedDataSize());
    dataStatistics.set_compressed_data_size(SequentialReader_->GetCompressedDataSize());

    return dataStatistics;
}

TFuture<void> TChunkReaderBase::GetFetchingCompletedEvent()
{
    return SequentialReader_->GetFetchingCompletedEvent();
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

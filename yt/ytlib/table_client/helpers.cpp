#include "stdafx.h"
#include "helpers.h"
#include "key.h"
#include "chunk_meta_extensions.h"

#include <ytlib/chunk_client/chunk_meta_extensions.h>

namespace NYT {
namespace NTableClient {

using namespace NTableClient::NProto;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TRefCountedInputChunk::TRefCountedInputChunk(const TInputChunk& other, int tableIndex)
    : TableIndex(tableIndex)
{
    CopyFrom(other);
}

TRefCountedInputChunk::TRefCountedInputChunk(TInputChunk&& other, int tableIndex)
    : TableIndex(tableIndex)
{
    Swap(&other);
}

TRefCountedInputChunk::TRefCountedInputChunk(const TRefCountedInputChunk& other)
    : TableIndex(other.TableIndex)
{
    CopyFrom(other);
}

////////////////////////////////////////////////////////////////////////////////

void GetStatistics(
    const TInputChunk& inputChunk,
    i64* dataSize,
    i64* rowCount,
    i64* valueCount)
{
    auto miscExt = GetProtoExtension<TMiscExt>(inputChunk.extensions());
    auto sizeOverrideExt = FindProtoExtension<NTableClient::NProto::TSizeOverrideExt>(inputChunk.extensions());
    if (sizeOverrideExt) {
        if (dataSize) {
            *dataSize = sizeOverrideExt->uncompressed_data_size();
        }
        if (rowCount) {
            *rowCount = sizeOverrideExt->row_count();
        }
    } else {
        if (dataSize) {
            *dataSize = miscExt.uncompressed_data_size();
        }
        if (rowCount) {
            *rowCount = miscExt.row_count();
        }
    }
    if (valueCount) {
        *valueCount = miscExt.value_count();
    }
}

TRefCountedInputChunkPtr SliceChunk(
    TRefCountedInputChunkPtr inputChunk,
    const TNullable<NProto::TKey>& startKey /*= Null*/,
    const TNullable<NProto::TKey>& endKey /*= Null*/)
{
    auto result = New<TRefCountedInputChunk>(*inputChunk);

    const auto& slice = inputChunk->slice();

    if (startKey && (!slice.start_limit().has_key() || slice.start_limit().key() < startKey.Get())) {
        *result->mutable_slice()->mutable_start_limit()->mutable_key() = startKey.Get();
    }

    if (endKey && (!slice.end_limit().has_key() || slice.end_limit().key() > endKey.Get())) {
        *result->mutable_slice()->mutable_end_limit()->mutable_key() = endKey.Get();
    }

    return result;
}

std::vector<TRefCountedInputChunkPtr> SliceChunkEvenly(TRefCountedInputChunkPtr inputChunk, int count)
{
    YCHECK(count > 0);

    std::vector<TRefCountedInputChunkPtr> result;

    i64 dataSize;
    i64 rowCount;
    GetStatistics(*inputChunk, &dataSize, &rowCount);

    const auto& startLimit = inputChunk->slice().start_limit();
    // Inclusive.
    i64 startRowIndex = startLimit.has_row_index() ? startLimit.row_index() : 0;

    const auto& endLimit = inputChunk->slice().end_limit();
    // Exclusive.
    i64 endRowIndex = endLimit.has_row_index() ? endLimit.row_index() : rowCount;

    rowCount = endRowIndex - startRowIndex;

    for (int i = 0; i < count; ++i) {
        i64 sliceStartRowIndex = startRowIndex + rowCount * i / count;
        i64 sliceEndRowIndex = startRowIndex + rowCount * (i + 1) / count;
        if (sliceStartRowIndex < sliceEndRowIndex) {
            auto slicedChunk = New<TRefCountedInputChunk>(*inputChunk);
            slicedChunk->mutable_slice()->mutable_start_limit()->set_row_index(sliceStartRowIndex);
            slicedChunk->mutable_slice()->mutable_end_limit()->set_row_index(sliceEndRowIndex);

            // This is merely an approximation.
            NTableClient::NProto::TSizeOverrideExt sizeOverride;
            sizeOverride.set_row_count(sliceEndRowIndex - sliceStartRowIndex);
            sizeOverride.set_uncompressed_data_size(dataSize / count + 1);
            UpdateProtoExtension(slicedChunk->mutable_extensions(), sizeOverride);

            result.push_back(slicedChunk);
        }
    }

    return result;
}

TRefCountedInputChunkPtr CreateCompleteChunk(TRefCountedInputChunkPtr inputChunk)
{
    auto chunk = New<TRefCountedInputChunk>(*inputChunk);
    chunk->mutable_slice()->mutable_start_limit()->clear_key();
    chunk->mutable_slice()->mutable_start_limit()->clear_row_index();
    chunk->mutable_slice()->mutable_end_limit()->clear_key();
    chunk->mutable_slice()->mutable_end_limit()->clear_row_index();

    RemoveProtoExtension<NTableClient::NProto::TSizeOverrideExt>(chunk->mutable_extensions());
    return chunk;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

#include "stdafx.h"
#include "input_chunk.h"
#include "key.h"
#include "chunk_meta_extensions.h"

#include <ytlib/erasure/codec.h>

#include <ytlib/ytree/attribute_helpers.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TRefCountedInputChunk::TRefCountedInputChunk(const TInputChunk& other, int tableIndex)
{
    CopyFrom(other);
    set_table_index(tableIndex);
}

TRefCountedInputChunk::TRefCountedInputChunk(TInputChunk&& other, int tableIndex)
{
    Swap(&other);
    set_table_index(tableIndex);
}

TRefCountedInputChunk::TRefCountedInputChunk(const TRefCountedInputChunk& other)
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
    auto sizeOverrideExt = FindProtoExtension<TSizeOverrideExt>(inputChunk.extensions());

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

i64 GetLocality(const NProto::TInputChunk& inputChunk)
{
    auto miscExt = GetProtoExtension<TMiscExt>(inputChunk.extensions());
    auto sizeOverrideExt = FindProtoExtension<TSizeOverrideExt>(inputChunk.extensions());

    i64 result = sizeOverrideExt
        ? sizeOverrideExt->uncompressed_data_size()
        : miscExt.uncompressed_data_size();

    // For erasure chunks data size is assumed to be split evenly between data parts.
    auto codecId = NErasure::ECodec(inputChunk.erasure_codec());
    if (codecId != NErasure::ECodec::None) {
        auto* codec = NErasure::GetCodec(codecId);
        int dataPartCount = codec->GetDataPartCount();
        result = (result + dataPartCount - 1) / dataPartCount;
    }

    return result;
}

TRefCountedInputChunkPtr SliceChunk(
    TRefCountedInputChunkPtr inputChunk,
    const TNullable<NProto::TKey>& startKey /*= Null*/,
    const TNullable<NProto::TKey>& endKey /*= Null*/)
{
    auto result = New<TRefCountedInputChunk>(*inputChunk);

    if (startKey && (!inputChunk->start_limit().has_key() || inputChunk->start_limit().key() < startKey.Get())) {
        *result->mutable_start_limit()->mutable_key() = startKey.Get();
    }

    if (endKey && (!inputChunk->end_limit().has_key() || inputChunk->end_limit().key() > endKey.Get())) {
        *result->mutable_end_limit()->mutable_key() = endKey.Get();
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

    const auto& startLimit = inputChunk->start_limit();
    // Inclusive.
    i64 startRowIndex = startLimit.has_row_index() ? startLimit.row_index() : 0;

    const auto& endLimit = inputChunk->end_limit();
    // Exclusive.
    i64 endRowIndex = endLimit.has_row_index() ? endLimit.row_index() : rowCount;

    rowCount = endRowIndex - startRowIndex;

    for (int i = 0; i < count; ++i) {
        i64 sliceStartRowIndex = startRowIndex + rowCount * i / count;
        i64 sliceEndRowIndex = startRowIndex + rowCount * (i + 1) / count;
        if (sliceStartRowIndex < sliceEndRowIndex) {
            auto slicedChunk = New<TRefCountedInputChunk>(*inputChunk);
            slicedChunk->mutable_start_limit()->set_row_index(sliceStartRowIndex);
            slicedChunk->mutable_end_limit()->set_row_index(sliceEndRowIndex);

            // This is merely an approximation.
            TSizeOverrideExt sizeOverride;
            sizeOverride.set_row_count(sliceEndRowIndex - sliceStartRowIndex);
            sizeOverride.set_uncompressed_data_size(dataSize / count + 1);
            SetProtoExtension(slicedChunk->mutable_extensions(), sizeOverride);

            result.push_back(slicedChunk);
        }
    }

    return result;
}

TRefCountedInputChunkPtr CreateCompleteChunk(TRefCountedInputChunkPtr inputChunk)
{
    auto chunk = New<TRefCountedInputChunk>(*inputChunk);
    chunk->mutable_start_limit()->clear_key();
    chunk->mutable_start_limit()->clear_row_index();
    chunk->mutable_end_limit()->clear_key();
    chunk->mutable_end_limit()->clear_row_index();

    RemoveProtoExtension<TSizeOverrideExt>(chunk->mutable_extensions());
    return chunk;
}

////////////////////////////////////////////////////////////////////////////////

bool ExtractOverwriteFlag(const NYTree::IAttributeDictionary& attributes)
{
    return !attributes.Get<bool>("append", false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

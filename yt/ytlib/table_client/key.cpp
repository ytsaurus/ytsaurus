#include "stdafx.h"
#include "key.h"

#include <ytlib/misc/string.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace {

int CompareKeyParts(const NProto::TKeyPart& lhs, const NProto::TKeyPart& rhs)
{
    if (lhs.type() != rhs.type()) {
        return lhs.type() - rhs.type();
    }

    if (lhs.has_double_value()) {
        if (lhs.double_value() > rhs.double_value())
            return 1;
        if (lhs.double_value() < rhs.double_value())
            return -1;
        return 0;
    }

    if (lhs.has_int_value()) {
        if (lhs.int_value() > rhs.int_value())
            return 1;
        if (lhs.int_value() < rhs.int_value())
            return -1;
        return 0;
    }

    if (lhs.has_str_value()) {
        return lhs.str_value().compare(rhs.str_value());
    }

    return 0;
}

} // namespace

namespace NProto {

Stroka ToString(const NProto::TKey& key)
{
    return ToString(TNonOwningKey::FromProto(key));
}

int CompareKeys(const NProto::TKey& lhs, const NProto::TKey& rhs, int prefixLength)
{
    int lhsSize = std::min(lhs.parts_size(), prefixLength);
    int rhsSize = std::min(rhs.parts_size(), prefixLength);
    int minSize = std::min(lhsSize, rhsSize);
    for (int index = 0; index < minSize; ++index) {
        int result = CompareKeyParts(lhs.parts(index), rhs.parts(index));
        if (result != 0) {
            return result;
        }
    }
    return lhsSize - rhsSize;
}

bool operator>(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareKeys(lhs, rhs) > 0;
}

bool operator>=(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareKeys(lhs, rhs) >= 0;
}

bool operator<(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareKeys(lhs, rhs) < 0;
}

bool operator<=(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareKeys(lhs, rhs) <= 0;
}

bool operator==(const NProto::TKey& lhs, const NProto::TKey& rhs)
{
    return CompareKeys(lhs, rhs) == 0;
}

NProto::TKey GetSuccessorKey(const NProto::TKey& key)
{
    NProto::TKey result;
    result.CopyFrom(key);
    auto* sentinelPart = result.add_parts();
    sentinelPart->set_type(EKeyPartType::MinSentinel);
    return result;
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

TRefCountedInputChunk::TRefCountedInputChunk(const NProto::TInputChunk& other, int tableIndex)
    : TableIndex(tableIndex)
{
    CopyFrom(other);
}

TRefCountedInputChunk::TRefCountedInputChunk(const TRefCountedInputChunk& other)
{
    CopyFrom(other);
    TableIndex = other.TableIndex;
}

void TRefCountedInputChunk::GetStatistics(i64* uncompressedDataSize, i64* rowCount) const
{
    auto sizeOverrideExt = FindProtoExtension<NTableClient::NProto::TSizeOverrideExt>(extensions());
    if (!sizeOverrideExt) {
        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(extensions());
        *uncompressedDataSize = miscExt.uncompressed_data_size();
        *rowCount = miscExt.row_count();
    } else {
        *uncompressedDataSize = sizeOverrideExt->uncompressed_data_size();
        *rowCount = sizeOverrideExt->row_count();
    }
}

TRefCountedInputChunkPtr SliceChunk(
    const TRefCountedInputChunk& chunk,
    const TNullable<NProto::TKey>& startKey /*= Null*/,
    const TNullable<NProto::TKey>& endKey /*= Null*/)
{
    auto result = New<TRefCountedInputChunk>(chunk);

    const auto& slice = chunk.slice();

    if (startKey && (!slice.start_limit().has_key() || slice.start_limit().key() < startKey.Get())) {
        *result->mutable_slice()->mutable_start_limit()->mutable_key() = startKey.Get();
    }

    if (endKey && (!slice.end_limit().has_key() || slice.end_limit().key() > endKey.Get())) {
        *result->mutable_slice()->mutable_end_limit()->mutable_key() = endKey.Get();
    }

    return result;
}

std::vector<TRefCountedInputChunkPtr> SliceChunkEvenly(const TRefCountedInputChunk& inputChunk, int count)
{
    YASSERT(count > 0);

    std::vector<TRefCountedInputChunkPtr> result;

    i64 dataSize, rowCount;
    inputChunk.GetStatistics(&dataSize, &rowCount);

    const auto& startLimit = inputChunk.slice().start_limit();
    // Inclusive.
    i64 startRowIndex = startLimit.has_row_index() ? startLimit.row_index() : 0;

    const auto& endLimit = inputChunk.slice().end_limit();
    // Exclusive.
    i64 endRowIndex = endLimit.has_row_index() ? endLimit.row_index() : rowCount;

    rowCount = endRowIndex - startRowIndex;

    for (int i = 0; i < count; ++i) {
        i64 sliceStartRowIndex = startRowIndex + rowCount * i / count;
        i64 sliceEndRowIndex = startRowIndex + rowCount * (i + 1) / count;
        if (sliceStartRowIndex < sliceEndRowIndex) {
            auto slicedChunk = New<TRefCountedInputChunk>(inputChunk);
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

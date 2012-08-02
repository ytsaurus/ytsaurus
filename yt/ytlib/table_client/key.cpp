#include "stdafx.h"
#include "key.h"

#include <ytlib/misc/string.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>

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

Stroka ToString(const NProto::TKey& key)
{
    return ToString(TNonOwningKey::FromProto(key));
}

NProto::TKey GetSuccessorKey(const NProto::TKey& key)
{
    NProto::TKey result;
    result.CopyFrom(key);
    auto* sentinelPart = result.add_parts();
    sentinelPart->set_type(EKeyPartType::MinSentinel);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TRefCountedInputChunk::TRefCountedInputChunk(const NProto::TInputChunk& other)
{
    CopyFrom(other);
}

TRefCountedInputChunkPtr SliceChunk(
    const NProto::TInputChunk& chunk,
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

std::vector<TRefCountedInputChunkPtr> SliceChunkEvenly(const NProto::TInputChunk& inputChunk, int count)
{
    YASSERT(count > 0);

    std::vector<TRefCountedInputChunkPtr> result;

    auto miscExt = GetProtoExtension<NChunkHolder::NProto::TMiscExt>(inputChunk.extensions());

    const auto& startLimit = inputChunk.slice().start_limit();
    // Inclusive.
    i64 startRowIndex = startLimit.has_row_index() ? startLimit.row_index() : 0;

    const auto& endLimit = inputChunk.slice().end_limit();
    // Exclusive.
    i64 endRowIndex = endLimit.has_row_index() ? endLimit.row_index() : miscExt.row_count();

    i64 rowCount = endRowIndex - startRowIndex;

    for (int i = 0; i < count; ++i) {
        i64 sliceStartRowIndex = startRowIndex + rowCount * i / count;
        i64 sliceEndRowIndex = startRowIndex + rowCount * (i + 1) / count;
        if (sliceStartRowIndex < sliceEndRowIndex) {
            auto slicedChunk = New<TRefCountedInputChunk>(inputChunk);
            slicedChunk->mutable_slice()->mutable_start_limit()->set_row_index(sliceStartRowIndex);
            slicedChunk->mutable_slice()->mutable_end_limit()->set_row_index(sliceEndRowIndex);

            // This is merely an approximation.
            slicedChunk->set_uncompressed_data_size(inputChunk.uncompressed_data_size() / count + 1);
            slicedChunk->set_row_count(sliceEndRowIndex - sliceStartRowIndex);

            result.push_back(slicedChunk);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

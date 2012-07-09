#include "stdafx.h"
#include "key.h"

#include <ytlib/misc/string.h>

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

NProto::TKey GetSuccessorKey(const NProto::TKey& key)
{
    NProto::TKey result;
    result.CopyFrom(key);
    auto* sentinelPart = result.add_parts();
    sentinelPart->set_type(EKeyPartType::MinSentinel);
    return result;
}

NProto::TInputChunk SliceChunk(
    const NProto::TInputChunk& chunk,
    const TNullable<NProto::TKey>& startKey /*= Null*/,
    const TNullable<NProto::TKey>& endKey /*= Null*/)
{
    NProto::TInputChunk result;
    result.CopyFrom(chunk);

    const auto& slice = chunk.slice();

    if (startKey && (!slice.start_limit().has_key() || slice.start_limit().key() < startKey.Get())) {
        *result.mutable_slice()->mutable_start_limit()->mutable_key() = startKey.Get();
    }

    if (endKey && (!slice.end_limit().has_key() || slice.end_limit().key() > endKey.Get())) {
        *result.mutable_slice()->mutable_end_limit()->mutable_key() = endKey.Get();
    }

    return result;
}

Stroka ToString(const NProto::TKey& key)
{
    return ToString(TNonOwningKey::FromProto(key));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

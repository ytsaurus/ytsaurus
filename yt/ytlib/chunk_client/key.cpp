#include "stdafx.h"
#include "key.h"

#include <core/misc/string.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

const size_t MaxKeySize = 4 * 1024;

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

NProto::TKey GetKeySuccessor(const NProto::TKey& key)
{
    NProto::TKey result;
    result.CopyFrom(key);
    auto* sentinelPart = result.add_parts();
    sentinelPart->set_type(EKeyPartType::MinSentinel);
    return result;
}

NProto::TKey GetKeyPrefixSuccessor(const NProto::TKey& key, int prefixLength)
{
    YCHECK(prefixLength >= 0);
    NProto::TKey result;
    result.CopyFrom(key);

    while (result.parts_size() > prefixLength) {
        result.mutable_parts()->RemoveLast();
    }

    auto* sentinelPart = result.add_parts();
    sentinelPart->set_type(EKeyPartType::MaxSentinel);
    return result;
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

#pragma once

#include "public.h"

#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TBound
{
    TValue Value;
    bool Included;

    TBound(
        TValue value,
        bool included)
        : Value(value)
        , Included(included)
    { }

    bool operator == (const TBound& other) const {
        return Value == other.Value
            && Included == other.Included;
    }

    bool operator != (const TBound& other) const {
        return !(*this == other);
    }

};

void UniteBounds(std::vector<std::vector<TBound>>* bounds);
int CompareBound(const TBound& lhs, const TBound& rhs, bool lhsDir, bool rhsDir);

bool Covers(const std::vector<TBound>& bounds, const TValue& point);

std::vector<TBound> IntersectBounds(
    const std::vector<TBound>& lhs,
    const std::vector<TBound>& rhs);

DECLARE_REFCOUNTED_STRUCT(TKeyTrie)

struct TKeyTrie
    : public TIntrinsicRefCounted
{
    size_t Offset = 0;

    std::vector<std::pair<TValue, TKeyTriePtr>> Next; // TODO: rename to Following
    std::vector<TBound> Bounds;

    TKeyTrie(size_t offset)
        : Offset(offset)
    { }

    TKeyTrie(const TKeyTrie& other)
        : Offset(other.Offset)
        , Next(other.Next)
        , Bounds(other.Bounds)
    { }

    TKeyTrie(TKeyTrie&&) = default;

    TKeyTrie& operator=(const TKeyTrie&) = default;
    TKeyTrie& operator=(TKeyTrie&&) = default;

    static TKeyTriePtr Empty()
    {
        return New<TKeyTrie>(0);
    }

    static TKeyTriePtr Universal()
    {
        return nullptr;
    }

    static TKeyTriePtr FromLowerBound(const TOwningKey & bound);
    static TKeyTriePtr FromUpperBound(const TOwningKey & bound);
    static TKeyTriePtr FromRange(const TKeyRange& range);

    friend TKeyTriePtr UniteKeyTrie(TKeyTriePtr lhs, TKeyTriePtr rhs);
    friend TKeyTriePtr UniteKeyTrie(const std::vector<TKeyTriePtr>& tries);
    friend TKeyTriePtr IntersectKeyTrie(TKeyTriePtr lhs, TKeyTriePtr rhs);
};

DEFINE_REFCOUNTED_TYPE(TKeyTrie)

TMutableRowRanges GetRangesFromTrieWithinRange(
    const TRowRange& keyRange,
    TKeyTriePtr trie,
    TRowBufferPtr rowBuffer,
    bool insertUndefined = false,
    ui64 rangeCountLimit = std::numeric_limits<ui64>::max());

TString ToString(TKeyTriePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

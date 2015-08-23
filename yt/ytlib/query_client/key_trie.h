#pragma once

#include "public.h"

#include <ytlib/table_client/unversioned_row.h>
#include <ytlib/table_client/row_buffer.h>

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

    static TKeyTriePtr FromLowerBound(const TKey& bound);
    static TKeyTriePtr FromUpperBound(const TKey& bound);
    static TKeyTriePtr FromRange(const TKeyRange& range);

    friend TKeyTriePtr UniteKeyTrie(TKeyTriePtr lhs, TKeyTriePtr rhs);
    friend TKeyTriePtr UniteKeyTrie(const std::vector<TKeyTriePtr>& tries);
    friend TKeyTriePtr IntersectKeyTrie(TKeyTriePtr lhs, TKeyTriePtr rhs);
};

DEFINE_REFCOUNTED_TYPE(TKeyTrie)

TRowRanges GetRangesFromTrieWithinRange(
    const TRowRange& keyRange,
    TKeyTriePtr trie,
    TRowBufferPtr rowBuffer,
    bool insertUndefined = false);

Stroka ToString(TKeyTriePtr node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

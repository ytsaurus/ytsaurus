#pragma once

#include "public.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/row_buffer.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using NVersionedTableClient::TUnversionedValue;
using NVersionedTableClient::TRowBuffer;

struct TBound
{    
    TUnversionedValue Value;
    bool Included;

    TBound(
        TUnversionedValue value,
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

struct TKeyTrieNode
{
    int Offset = std::numeric_limits<int>::max();

    std::map<TUnversionedValue, TKeyTrieNode> Next;
    std::vector<TBound> Bounds;

    static TKeyTrieNode Empty()
    {
        TKeyTrieNode result;
        result.Offset = std::numeric_limits<int>::min();
        return result;
    }

    static TKeyTrieNode Universal()
    {
        return TKeyTrieNode();
    }

};

TKeyTrieNode UniteKeyTrie(const TKeyTrieNode& lhs, const TKeyTrieNode& rhs);

TKeyTrieNode IntersectKeyTrie(const TKeyTrieNode& lhs, const TKeyTrieNode& rhs);

std::vector<TKeyRange> GetRangesFromTrieWithinRange(
    const TKeyRange& keyRange,
    const TKeyTrieNode& trie);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

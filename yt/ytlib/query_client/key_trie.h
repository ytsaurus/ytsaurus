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

    TKeyTrieNode(const TKeyTrieNode&) = default;
    TKeyTrieNode(TKeyTrieNode&&) = default;

    TKeyTrieNode& operator=(const TKeyTrieNode&) = default;
    TKeyTrieNode& operator=(TKeyTrieNode&&) = default;


    static TKeyTrieNode Empty()
    {
        return TKeyTrieNode(std::numeric_limits<int>::min());
    }

    static TKeyTrieNode Universal()
    {
        return TKeyTrieNode(std::numeric_limits<int>::max());
    }

    TKeyTrieNode& Unite(const TKeyTrieNode& rhs);
    
    friend TKeyTrieNode UniteKeyTrie(const TKeyTrieNode& lhs, const TKeyTrieNode& rhs);

    friend TKeyTrieNode IntersectKeyTrie(const TKeyTrieNode& lhs, const TKeyTrieNode& rhs);

private:
    TKeyTrieNode(int offset)
        : Offset(offset)
    { }

};



std::vector<TKeyRange> GetRangesFromTrieWithinRange(
    const TKeyRange& keyRange,
    const TKeyTrieNode& trie);

Stroka ToString(const TKeyTrieNode& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

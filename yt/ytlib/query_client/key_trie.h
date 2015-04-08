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
    size_t Offset = std::numeric_limits<size_t>::max();

    std::map<TUnversionedValue, TKeyTrieNode> Next;
    std::vector<TBound> Bounds;

    TKeyTrieNode(const TKeyTrieNode&) = default;
    TKeyTrieNode(TKeyTrieNode&&) = default;

    TKeyTrieNode& operator=(const TKeyTrieNode&) = default;
    TKeyTrieNode& operator=(TKeyTrieNode&&) = default;


    static TKeyTrieNode Empty()
    {
        return TKeyTrieNode(0);
    }

    static TKeyTrieNode Universal()
    {
        return TKeyTrieNode(std::numeric_limits<size_t>::max());
    }

    static TKeyTrieNode FromLowerBound(const TKey& bound);
    static TKeyTrieNode FromUpperBound(const TKey& bound);
    static TKeyTrieNode FromRange(const TKeyRange& range);


    TKeyTrieNode& Unite(const TKeyTrieNode& rhs);

    friend TKeyTrieNode UniteKeyTrie(const TKeyTrieNode& lhs, const TKeyTrieNode& rhs);
    friend TKeyTrieNode IntersectKeyTrie(const TKeyTrieNode& lhs, const TKeyTrieNode& rhs);

private:
    TKeyTrieNode(size_t offset)
        : Offset(offset)
    { }

};

std::vector<std::pair<TRow, TRow>> GetRangesFromTrieWithinRange(
    const TKeyRange& keyRange,
    const TKeyTrieNode& trie,
    TRowBuffer* rowBuffer);

Stroka ToString(const TKeyTrieNode& node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

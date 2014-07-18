#pragma once

#include "public.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/row_buffer.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using NVersionedTableClient::TUnversionedValue;
using NVersionedTableClient::TRowBuffer;

struct TKeyTrieNode
{
    int Offset = std::numeric_limits<int>::max();

    std::map<TUnversionedValue, TKeyTrieNode> Next;
    std::vector<TUnversionedValue> Bounds;
};

TKeyTrieNode UniteKeyTrie(const TKeyTrieNode& lhs, const TKeyTrieNode& rhs, TRowBuffer* rowBuffer);

TKeyTrieNode IntersectKeyTrie(const TKeyTrieNode& lhs, const TKeyTrieNode& rhs, TRowBuffer* rowBuffer);

std::vector<TKeyRange> GetRangesFromTrieWithinRange(
    const TKeyRange& keyRange,
    TRowBuffer* rowBuffer,
    int keySize,
    const TKeyTrieNode& trie);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

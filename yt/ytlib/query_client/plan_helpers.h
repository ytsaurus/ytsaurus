#pragma once

#include "public.h"
#include "key_trie.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/row_buffer.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TKeyTrieNode ExtractMultipleConstraints(
    const TConstExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    TRowBuffer* rowBuffer);

////////////////////////////////////////////////////////////////////////////////

//! Returns a minimal key range that cover both inputs.
TKeyRange Unite(const TKeyRange& first, const TKeyRange& second);

//! Returns a maximal key range covered by both inputs.
TKeyRange Intersect(const TKeyRange& first, const TKeyRange& second);

//! Checks whether key range is empty.
bool IsEmpty(const TKeyRange& keyRange);

TConstExpressionPtr MakeAndExpression(const TConstExpressionPtr& lhs, const TConstExpressionPtr& rhs);
TConstExpressionPtr MakeOrExpression(const TConstExpressionPtr& lhs, const TConstExpressionPtr& rhs);

TConstExpressionPtr RefinePredicate(
    const TKeyRange& keyRange,
    int commonPrefixSize,
    const TConstExpressionPtr& expr,
    const TKeyColumns& keyColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


#pragma once

#include "public.h"
#include "key_trie.h"

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/misc/range.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

//! Descends down to conjuncts and disjuncts and extract all constraints.
TKeyTriePtr ExtractMultipleConstraints(
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer,
    const TConstRangeExtractorMapPtr& rangeExtractors = BuiltinRangeExtractorMap);

////////////////////////////////////////////////////////////////////////////////

//! Returns a minimal key range that cover both inputs.
TKeyRange Unite(const TKeyRange& first, const TKeyRange& second);
TRowRange Unite(const TRowRange& first, const TRowRange& second);

//! Returns a maximal key range covered by both inputs.
TKeyRange Intersect(const TKeyRange& first, const TKeyRange& second);
TRowRange Intersect(const TRowRange& first, const TRowRange& second);

//! Checks whether key range is empty.
bool IsEmpty(const TKeyRange& keyRange);
bool IsEmpty(const TRowRange& keyRange);

bool IsTrue(TConstExpressionPtr expr);
TConstExpressionPtr MakeAndExpression(TConstExpressionPtr lhs, TConstExpressionPtr rhs);
TConstExpressionPtr MakeOrExpression(TConstExpressionPtr lhs, TConstExpressionPtr rhs);

TConstExpressionPtr EliminatePredicate(
    const TRange<TRowRange>& keyRanges,
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns);

TConstExpressionPtr EliminatePredicate(
    const TRange<TRow>& lookupKeys,
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns);

TConstExpressionPtr ExtractPredicateForColumnSubset(
    TConstExpressionPtr expr,
    const TTableSchema& tableSchema);

std::pair<TConstExpressionPtr, TConstExpressionPtr> SplitPredicateByColumnSubset(
    TConstExpressionPtr root,
    const TTableSchema& tableSchema);

std::vector<TMutableRowRange> MergeOverlappingRanges(
    std::vector<TMutableRowRange> ranges);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


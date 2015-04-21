#pragma once

#include "public.h"
#include "key_trie.h"
#include "function_registry.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/row_buffer.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

int ColumnNameToKeyPartIndex(const TKeyColumns& keyColumns, const Stroka& columnName);

TKeyTriePtr ExtractMultipleConstraints(
    const TConstExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    TRowBuffer* rowBuffer,
    const IFunctionRegistryPtr functionRegistry);

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

TConstExpressionPtr MakeAndExpression(const TConstExpressionPtr& lhs, const TConstExpressionPtr& rhs);
TConstExpressionPtr MakeOrExpression(const TConstExpressionPtr& lhs, const TConstExpressionPtr& rhs);

TConstExpressionPtr RefinePredicate(
    const TRowRange& keyRange,
    const TConstExpressionPtr& expr,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    TColumnEvaluatorPtr columnEvaluator);

TConstExpressionPtr RefinePredicate(
    const std::vector<TRow>& lookupKeys,
    const TConstExpressionPtr& expr,
    const TKeyColumns& keyColumns);

TConstExpressionPtr ExtractPredicateForColumnSubset(
    const TConstExpressionPtr& expr,
    const TTableSchema& tableSchema);

std::vector<std::pair<TRow, TRow>> MergeOverlappingRanges(
    std::vector<std::pair<TRow, TRow>> ranges);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


#pragma once

#include "public.h"

#include <yt/yt/library/query/base/key_trie.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TRangeExtractor = std::function<TKeyTriePtr(
    const TConstFunctionExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer)>;

struct TRangeExtractorMap
    : public TRefCounted
    , public std::unordered_map<std::string, TRangeExtractor>
{ };

DEFINE_REFCOUNTED_TYPE(TRangeExtractorMap)

////////////////////////////////////////////////////////////////////////////////

//! Descends down to conjuncts and disjuncts and extract all constraints.
TKeyTriePtr ExtractMultipleConstraints(
    TConstExpressionPtr expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer,
    const TConstRangeExtractorMapPtr& rangeExtractors = GetBuiltinRangeExtractors());

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TRowRange> CreateRangeInferrer(
    TConstExpressionPtr predicate,
    const TTableSchemaPtr& schema,
    const TKeyColumns& keyColumns,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

#pragma once

#include "public.h"

#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/key_trie.h>
#include <yt/yt/library/query/base/query.h>

#include <functional>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TRangeExtractorMap
    : public TRefCounted
    , public std::unordered_map<TString, TRangeExtractor>
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

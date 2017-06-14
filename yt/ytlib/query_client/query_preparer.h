#pragma once

#include "query.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

typedef std::function<void(
    const std::vector<TString>& names,
    const TTypeInferrerMapPtr& typeInferrers)> TFetchFunctions;

void DefaultFetchFunctions(const std::vector<TString>& names, const TTypeInferrerMapPtr& typeInferrers);

////////////////////////////////////////////////////////////////////////////////

void ParseJobQuery(const TString& source);

std::pair<TQueryPtr, TDataRanges> PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const TString& source,
    const TFetchFunctions& fetchFunctions = DefaultFetchFunctions,
    i64 inputRowLimit = std::numeric_limits<i64>::max(),
    i64 outputRowLimit = std::numeric_limits<i64>::max(),
    TTimestamp timestamp = NullTimestamp);

TQueryPtr PrepareJobQuery(
    const TString& source,
    const TTableSchema& tableSchema,
    const TFetchFunctions& fetchFunctions);

TConstExpressionPtr PrepareExpression(
    const TString& source,
    TTableSchema initialTableSchema,
    const TConstTypeInferrerMapPtr& functions = BuiltinTypeInferrersMap,
    yhash_set<TString>* references = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

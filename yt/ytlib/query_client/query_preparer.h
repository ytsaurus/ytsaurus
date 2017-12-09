#pragma once

#include "query.h"
#include "ast.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TFunctionsFetcher = std::function<void(
    const std::vector<TString>& names,
    const TTypeInferrerMapPtr& typeInferrers)>;

void DefaultFetchFunctions(
    const std::vector<TString>& names,
    const TTypeInferrerMapPtr& typeInferrers);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EParseMode,
    (Query)
    (JobQuery)
    (Expression)
);

struct TParsedSource
{
    TParsedSource(
        const TString& source,
        const NAst::TAstHead& astHead);

    TString Source;
    NAst::TAstHead AstHead;
};

std::unique_ptr<TParsedSource> ParseSource(
    const TString& source,
    EParseMode mode);

////////////////////////////////////////////////////////////////////////////////

struct TPlanFragment
{
    TQueryPtr Query;
    TDataRanges Ranges;
};

std::unique_ptr<TPlanFragment> PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const TString& source,
    const TFunctionsFetcher& functionsFetcher = DefaultFetchFunctions,
    TTimestamp timestamp = NullTimestamp);

std::unique_ptr<TPlanFragment> PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    const TParsedSource& parsedSource,
    const TFunctionsFetcher& functionsFetcher = DefaultFetchFunctions,
    TTimestamp timestamp = NullTimestamp);

////////////////////////////////////////////////////////////////////////////////

TQueryPtr PrepareJobQuery(
    const TString& source,
    const TTableSchema& tableSchema,
    const TFunctionsFetcher& functionsFetcher);

TConstExpressionPtr PrepareExpression(
    const TString& source,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions = BuiltinTypeInferrersMap,
    yhash_set<TString>* references = nullptr);

TConstExpressionPtr PrepareExpression(
    const TParsedSource& parsedSource,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions = BuiltinTypeInferrersMap,
    yhash_set<TString>* references = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

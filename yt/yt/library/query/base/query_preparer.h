#pragma once

#include "ast.h"
#include "callbacks.h"

#include <library/cpp/yt/memory/memory_usage_tracker.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TFunctionsFetcher = std::function<void(TRange<TString> names, const TTypeInferrerMapPtr& typeInferrers)>;

void DefaultFetchFunctions(TRange<TString> names, const TTypeInferrerMapPtr& typeInferrers);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EParseMode,
    (Query)
    (JobQuery)
    (Expression)
);

struct TParsedSource
{
    TParsedSource(
        TStringBuf source,
        NAst::TAstHead astHead);

    TString Source;
    NAst::TAstHead AstHead;
};

std::unique_ptr<TParsedSource> ParseSource(
    TStringBuf source,
    EParseMode mode,
    NYson::TYsonStringBuf placeholderValues = {},
    int syntaxVersion = 1);

////////////////////////////////////////////////////////////////////////////////

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    NYson::TYsonStringBuf placeholderValues = {},
    int syntaxVersion = 1,
    IMemoryUsageTrackerPtr memoryTracker = nullptr);

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    const NAst::TQuery& query,
    const NAst::TAliasMap& aliasMap,
    IMemoryUsageTrackerPtr memoryTracker = nullptr,
    int depth = 0);

////////////////////////////////////////////////////////////////////////////////

TQueryPtr PrepareJobQuery(
    TStringBuf source,
    const TTableSchemaPtr& tableSchema,
    const TFunctionsFetcher& functionsFetcher);

TConstExpressionPtr PrepareExpression(
    TStringBuf source,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions = GetBuiltinTypeInferrers(),
    THashSet<std::string>* references = nullptr);

TConstExpressionPtr PrepareExpression(
    const TParsedSource& parsedSource,
    const TTableSchema& tableSchema,
    const TConstTypeInferrerMapPtr& functions = GetBuiltinTypeInferrers(),
    THashSet<std::string>* references = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

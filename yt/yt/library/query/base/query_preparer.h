#pragma once

#include "ast.h"
#include "callbacks.h"

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TFunctionsFetcher = std::function<void(
    TRange<std::string> names,
    const TTypeInferrerMapPtr& typeInferrers,
    NCodegen::EExecutionBackend executionBackend)>;

void DefaultFetchFunctions(
    TRange<std::string> names,
    const TTypeInferrerMapPtr& typeInferrers,
    NCodegen::EExecutionBackend executionBackend);

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

    std::string Source;
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
    NCodegen::EExecutionBackend executionBackend,
    NYson::TYsonStringBuf placeholderValues = {},
    int syntaxVersion = 1,
    IMemoryUsageTrackerPtr memoryTracker = nullptr);

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    NAst::TQuery& query,
    NAst::TAstHead& astHead,
    NCodegen::EExecutionBackend executionBackend,
    int builderVersion = 1,
    IMemoryUsageTrackerPtr memoryTracker = nullptr,
    int syntaxVersion = 1,
    bool shouldRewriteCardinalityIntoHyperLogLog = false, // COMPAT(dtorilov): Remove after 25.4.
    int hyperLogLogPrecision = 14,
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
    const TConstTypeInferrerMapPtr& functions);

TConstExpressionPtr PrepareExpression(
    const TParsedSource& parsedSource,
    const TTableSchema& tableSchema,
    int builderVersion = 1,
    const TConstTypeInferrerMapPtr& functions = GetBuiltinTypeInferrers(),
    THashSet<std::string>* references = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

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

struct TPreparePlanFragmentOptions
{
    int SyntaxVersion = 1;
    int BuilderVersion = 1;
    NCodegen::EExecutionBackend ExecutionBackend = NCodegen::EExecutionBackend::Native;
    bool ShouldRewriteCardinalityIntoHyperLogLog = false; // COMPAT(dtorilov): Remove after 25.4.
    int HyperLogLogPrecision = 14;
};

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    const TPreparePlanFragmentOptions& options = {},
    NYson::TYsonStringBuf placeholderValues = {},
    IMemoryUsageTrackerPtr memoryTracker = nullptr);

TPlanFragmentPtr PreparePlanFragment(
    IPrepareCallbacks* callbacks,
    TStringBuf source,
    NAst::TQuery& query,
    NAst::TAstHead& astHead,
    const TPreparePlanFragmentOptions& options = {},
    IMemoryUsageTrackerPtr memoryTracker = nullptr);

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

#pragma once

#include "ql_helpers.h"

#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/evaluator.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TResultMatcher = std::function<void(TRange<TRow>, const TTableSchema&)>;
const TResultMatcher AnyMatcher = [] (TRange<TRow>, const TTableSchema&) { };

using TSource = std::vector<std::string>;
using TSplitMap = std::map<TYPath, TDataSplit>;

////////////////////////////////////////////////////////////////////////////////

struct TEvaluateOptions
{
    i64 InputRowLimit = std::numeric_limits<i64>::max();
    i64 OutputRowLimit = std::numeric_limits<i64>::max();
    NYson::TYsonStringBuf PlaceholderValues = {};
    int SyntaxVersion = 1;
    int MinKeyWidth = 0;
    NCodegen::EExecutionBackend ExecutionBackend = NCodegen::EExecutionBackend::Native;
    bool UseCanonicalNullRelations = false;
    bool AllowUnorderedGroupByWithLimit = true;
};

////////////////////////////////////////////////////////////////////////////////

TOwningRow YsonToRow(TStringBuf yson, const TDataSplit& dataSplit, bool treatMissingAsNull = true);
std::vector<TOwningRow> YsonToRows(TRange<std::string> rowsData, const TDataSplit& split);

TResultMatcher ResultMatcher(std::vector<TOwningRow> expectedResult, TTableSchemaPtr expectedSchema = nullptr);
TResultMatcher OrderedResultMatcher(std::vector<TOwningRow> expectedResult, std::vector<std::string> columns);
TResultMatcher OrderedResultMatcher(std::vector<TOwningRow> expectedResult, const std::vector<int>& indexes);

////////////////////////////////////////////////////////////////////////////////

class TQueryEvaluateTest
    : public ::testing::Test
{
protected:
    const IEvaluatorPtr Evaluator_ = CreateEvaluator(New<TExecutorConfig>());

    const TTypeInferrerMapPtr TypeInferrers_ = New<TTypeInferrerMap>();
    const TFunctionProfilerMapPtr FunctionProfilers_ = New<TFunctionProfilerMap>();
    const TAggregateProfilerMapPtr AggregateProfilers_ = New<TAggregateProfilerMap>();

    StrictMock<TPrepareCallbacksMock> PrepareMock_{TypeInferrers_};
    NConcurrency::TActionQueuePtr ActionQueue_;

    void SetUp() override;

    void TearDown() override;

    std::pair<TQueryPtr, TQueryStatistics> EvaluateWithQueryStatistics(
        TStringBuf query,
        const TSplitMap& dataSplits,
        const std::vector<TSource>& owningSources,
        const TResultMatcher& resultMatcher,
        TEvaluateOptions options);

    TQueryPtr Evaluate(
        TStringBuf query,
        const TSplitMap& dataSplits,
        const std::vector<TSource>& owningSources,
        const TResultMatcher& resultMatcher,
        TEvaluateOptions evaluateOptions = {});

    TQueryPtr EvaluateOnlyViaNativeExecutionBackend(
        TStringBuf query,
        const TSplitMap& dataSplits,
        const std::vector<TSource>& owningSources,
        const TResultMatcher& resultMatcher,
        TEvaluateOptions evaluateOptions = {});

    std::pair<TQueryPtr, TQueryStatistics> EvaluateWithQueryStatistics(
        TStringBuf query,
        const TDataSplit& dataSplit,
        const TSource& owningSourceRows,
        const TResultMatcher& resultMatcher,
        TEvaluateOptions evaluateOptions = {});

    TQueryPtr Evaluate(
        TStringBuf query,
        const TDataSplit& dataSplit,
        const TSource& owningSourceRows,
        const TResultMatcher& resultMatcher,
        TEvaluateOptions evaluateOptions = {});

    TQueryPtr EvaluateWithSyntaxV2(
        TStringBuf query,
        const TDataSplit& dataSplit,
        const TSource& owningSourceRows,
        const TResultMatcher& resultMatcher,
        TEvaluateOptions evaluateOptions = {});

    TQueryPtr EvaluateOnlyViaNativeExecutionBackend(
        TStringBuf query,
        const TDataSplit& dataSplit,
        const TSource& owningSourceRows,
        const TResultMatcher& resultMatcher,
        TEvaluateOptions evaluateOptions = {});

    TQueryPtr EvaluateExpectingError(
        TStringBuf query,
        const TDataSplit& dataSplit,
        const TSource& owningSourceRows,
        TEvaluateOptions evaluateOptions = {},
        std::string expectedError = {});

    TQueryPtr Prepare(
        TStringBuf query,
        const TSplitMap& dataSplits,
        NYson::TYsonStringBuf placeholderValues,
        int syntaxVersion);

    std::pair<TQueryPtr, TQueryStatistics> DoEvaluate(
        TStringBuf query,
        const TSplitMap& dataSplits,
        const std::vector<TSource>& owningSources,
        const TResultMatcher& resultMatcher,
        TEvaluateOptions evaluateOptions,
        std::optional<std::string> expectedError);

    TQueryStatistics EvaluateCoordinatedGroupByImpl(
        TStringBuf query,
        const TDataSplit& dataSplit,
        const std::vector<TSource>& owningSources,
        const TResultMatcher& resultMatcher,
        NCodegen::EExecutionBackend executionBackend);

    TQueryStatistics EvaluateCoordinatedGroupBy(
        TStringBuf query,
        const TDataSplit& dataSplit,
        const std::vector<TSource>& owningSources,
        const TResultMatcher& resultMatcher);

    ISchemafulPipePtr RunOnNodeThread(
        TConstQueryPtr query,
        const TSource& rows,
        NCodegen::EExecutionBackend executionBackend);

    ISchemafulPipePtr RunOnNode(
        TConstQueryPtr nodeQuery,
        const std::vector<TSource>& tabletData,
        NCodegen::EExecutionBackend executionBackend);

    TSharedRange<TUnversionedRow> RunOnCoordinator(
        TQueryPtr primary,
        const std::vector<std::vector<TSource>>& tabletsData,
        NCodegen::EExecutionBackend executionBackend);

    void EvaluateFullCoordinatedGroupByImpl(
        TStringBuf queryString,
        const TDataSplit& dataSplit,
        const std::vector<std::vector<TSource>>& data,
        const TResultMatcher& resultMatcher,
        NCodegen::EExecutionBackend executionBackend);

    void EvaluateFullCoordinatedGroupBy(
        TStringBuf query,
        const TDataSplit& dataSplit,
        const TSource& data,
        const TResultMatcher& resultMatcher,
        int iterations = 100);

    static std::vector<std::vector<TSource>> RandomSplitData(const TSource& data);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

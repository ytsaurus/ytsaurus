#include "test_evaluate.h"

#include <yt/yt/library/query/base/query_helpers.h>

#include <yt/yt/library/query/engine/query_engine_config.h>

#include <yt/yt/library/query/engine_api/coordinator.h>
#include <yt/yt/library/query/engine_api/evaluator.h>

#include <yt/yt/library/web_assembly/engine/builtins.h>

#include <yt/yt/client/query_client/query_statistics.h>
#include <yt/yt/client/table_client/pipe.h>
#include <yt/yt/client/table_client/unordered_schemaful_reader.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <library/cpp/testing/hook/hook.h>

namespace NYT::NQueryClient {

using namespace NApi;
using namespace NCodegen;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

using NChunkClient::NProto::TDataStatistics;

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "TestEvaluate");

namespace {

////////////////////////////////////////////////////////////////////////////////

static bool IsTimeDumpEnabled()
{
    static bool result = (getenv("DUMP_TIME") != nullptr);
    return result;
}

static void SumCodegenExecute(
    const TQueryStatistics& statistics,
    TDuration* codegen,
    TDuration* execute)
{
    *codegen += statistics.CodegenTime.GetTotal();
    *execute += statistics.ExecuteTime.GetTotal();

    for (auto& inner : statistics.InnerStatistics) {
        SumCodegenExecute(inner, codegen, execute);
    }
}

static void DumpTime(const TQueryStatistics& statistics, EExecutionBackend executionBackend)
{
    auto codegen = TDuration();
    auto execute = TDuration();

    SumCodegenExecute(statistics, &codegen, &execute);

    Cerr << Format("%Qlv, Codegen: %v, Execute: %v", executionBackend, codegen, execute) << Endl;
}

////////////////////////////////////////////////////////////////////////////////

class TReaderMock
    : public ISchemafulUnversionedReader
{
public:
    MOCK_METHOD(IUnversionedRowBatchPtr, Read, (const TRowBatchReadOptions& options), (override));
    MOCK_METHOD(TFuture<void>, GetReadyEvent, (), (const, override));
    MOCK_METHOD(bool, IsFetchingCompleted, (), (const, override));
    MOCK_METHOD(std::vector<NChunkClient::TChunkId>, GetFailedChunkIds, (), (const, override));
    MOCK_METHOD(TDataStatistics, GetDataStatistics, (), (const, override));
    MOCK_METHOD(NChunkClient::TCodecStatistics, GetDecompressionStatistics, (), (const, override));
};

class TWriterMock
    : public IUnversionedRowsetWriter
{
public:
    MOCK_METHOD(TFuture<void>, Close, (), (override));
    MOCK_METHOD(bool, Write, (TRange<TUnversionedRow>), (override));
    MOCK_METHOD(TFuture<void>, GetReadyEvent, (), (override));
};

////////////////////////////////////////////////////////////////////////////////

TQueryStatistics DoExecuteQuery(
    IEvaluatorPtr evaluator,
    const TSource& source,
    TFunctionProfilerMapPtr functionProfilers,
    TAggregateProfilerMapPtr aggregateProfilers,
    TConstQueryPtr query,
    IUnversionedRowsetWriterPtr writer,
    const TQueryOptions& options,
    const std::vector<IJoinProfilerPtr>& joinProfilers)
{
    std::vector<TOwningRow> owningSourceRows;
    for (const auto& row : source) {
        owningSourceRows.push_back(NTableClient::YsonToSchemafulRow(row, *query->GetReadSchema(), true));
    }

    std::vector<TRow> sourceRows;
    for (const auto& row : owningSourceRows) {
        sourceRows.push_back(row.Get());
    }

    auto rowsBegin = sourceRows.begin();
    auto rowsEnd = sourceRows.end();

    // Use small batch size for tests.
    const size_t maxBatchSize = 5;

    ssize_t batchSize = maxBatchSize;

    if (query->IsOrdered(/*allowUnorderedGroupByWithLimit*/ true) && query->Offset + query->Limit < batchSize) {
        batchSize = query->Offset + query->Limit;
    }

    bool isFirstRead = true;
    auto readRows = [&] (const TRowBatchReadOptions& options) {
        // Free memory to test correct capturing of data.
        auto readCount = std::distance(sourceRows.begin(), rowsBegin);
        for (ssize_t index = 0; index < readCount; ++index) {
            sourceRows[index] = TRow();
            owningSourceRows[index] = TOwningRow();
        }

        if (isFirstRead && query->IsOrdered(/*allowUnorderedGroupByWithLimit*/ true)) {
            EXPECT_EQ(options.MaxRowsPerRead, std::min(DefaultRowsetProcessingBatchSize, query->Offset + query->Limit));
            isFirstRead = false;
        }

        auto size = std::min<size_t>(options.MaxRowsPerRead, std::distance(rowsBegin, rowsEnd));
        std::vector<TRow> rows(rowsBegin, rowsBegin + size);
        rowsBegin += size;
        batchSize = std::min<size_t>(batchSize * 2, maxBatchSize);
        return rows.empty()
            ? nullptr
            : CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows)));
    };

    auto readerMock = New<NiceMock<TReaderMock>>();
    EXPECT_CALL(*readerMock, Read(_))
        .WillRepeatedly(Invoke(readRows));
    ON_CALL(*readerMock, GetReadyEvent())
        .WillByDefault(Return(OKFuture));

    return evaluator->Run(
        query,
        readerMock,
        writer,
        joinProfilers,
        functionProfilers,
        aggregateProfilers,
        NWebAssembly::GetBuiltinSdk(),
        GetDefaultMemoryChunkProvider(),
        options,
        MostFreshFeatureFlags(),
        MakeFuture(MostFreshFeatureFlags()));
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TRow> OrderRowsBy(TRange<TRow> rows, const std::vector<int>& indexes)
{
    std::vector<TRow> result(rows.begin(), rows.end());
    std::sort(result.begin(), result.end(), [&] (TRow lhs, TRow rhs) {
        for (auto index : indexes) {
            if (lhs[index] == rhs[index]) {
                continue;
            } else {
                return lhs[index] < rhs[index];
            }
        }
        return false;
    });
    return result;
}

std::vector<TRow> OrderRowsBy(TRange<TRow> rows, TRange<std::string> columns, const TTableSchema& tableSchema)
{
    std::vector<int> indexes;
    for (const auto& column : columns) {
        indexes.push_back(tableSchema.GetColumnIndexOrThrow(column));
    }

    return OrderRowsBy(rows, indexes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TOwningRow YsonToRow(TStringBuf yson, const TDataSplit& dataSplit, bool treatMissingAsNull)
{
    auto tableSchema = dataSplit.TableSchema;
    return NTableClient::YsonToSchemafulRow(yson, *tableSchema, treatMissingAsNull);
}

std::vector<TOwningRow> YsonToRows(TRange<std::string> rowsData, const TDataSplit& split)
{
    std::vector<TOwningRow> result;

    for (auto row : rowsData) {
        result.push_back(YsonToRow(row, split, true));
    }

    return result;
}

TResultMatcher ResultMatcher(std::vector<TOwningRow> expectedResult, TTableSchemaPtr expectedSchema)
{
    static constexpr double Epsilon = 1e-5;

    return [
            expectedResult = std::move(expectedResult),
            expectedSchema = std::move(expectedSchema)
        ] (TRange<TRow> result, const TTableSchema& tableSchema) {
            if (expectedSchema) {
                EXPECT_EQ(*expectedSchema, tableSchema);
                if (*expectedSchema != tableSchema) {
                    return;
                }
            }
            EXPECT_EQ(expectedResult.size(), result.Size());
            if (expectedResult.size() != result.Size()) {
                return;
            }

            for (int i = 0; i < std::ssize(expectedResult); ++i) {
                auto expectedRow = expectedResult[i];
                auto row = result[i];
                EXPECT_EQ(expectedRow.GetCount(), static_cast<int>(row.GetCount()));
                if (expectedRow.GetCount() != static_cast<int>(row.GetCount())) {
                    continue;
                }
                for (int j = 0; j < expectedRow.GetCount(); ++j) {
                    const auto& expectedValue = expectedRow[j];
                    const auto& value = row[j];
                    EXPECT_EQ(expectedValue.Type, value.Type);
                    if (expectedValue.Type != value.Type) {
                        continue;
                    }
                    if (expectedValue.Type == EValueType::Double) {
                        EXPECT_NEAR(expectedValue.Data.Double, value.Data.Double, Epsilon);
                    } else if (expectedValue.Type == EValueType::Any || expectedValue.Type == EValueType::Composite) {
                        // Slow path.
                        auto expectedYson = TYsonString(expectedValue.AsString());
                        auto expectedStableYson = ConvertToYsonString(ConvertToNode(expectedYson), EYsonFormat::Text);
                        auto yson = TYsonString(value.AsString());
                        auto stableYson = ConvertToYsonString(ConvertToNode(yson), EYsonFormat::Text);
                        EXPECT_EQ(expectedStableYson, stableYson);
                    } else {
                        // Fast path.
                        EXPECT_EQ(expectedValue, value);
                    }
                }
            }
        };
}

TResultMatcher OrderedResultMatcher(std::vector<TOwningRow> expectedResult, std::vector<std::string> columns)
{
    return [
            expectedResult = std::move(expectedResult),
            columns = std::move(columns)
        ] (TRange<TRow> result, const TTableSchema& tableSchema) {
            EXPECT_EQ(expectedResult.size(), result.Size());

            auto sortedResult = OrderRowsBy(result, columns, tableSchema);

            for (int i = 0; i < std::ssize(expectedResult); ++i) {
                EXPECT_EQ(sortedResult[i], expectedResult[i]);
            }
        };
}

TResultMatcher OrderedResultMatcher(std::vector<TOwningRow> expectedResult, const std::vector<int>& indexes)
{
    return [
            expectedResult = std::move(expectedResult),
            indexes = std::move(indexes)
        ] (TRange<TRow> result, const TTableSchema& /*schema*/) {
            EXPECT_EQ(expectedResult.size(), result.Size());

            auto sortedResult = OrderRowsBy(result, indexes);

            for (int i = 0; i < std::ssize(expectedResult); ++i) {
                EXPECT_EQ(sortedResult[i], expectedResult[i]);
            }
        };
}

////////////////////////////////////////////////////////////////////////////////

void TQueryEvaluateTest::SetUp()
{
    ActionQueue_ = New<TActionQueue>("Test");

    auto bcImplementations = "test_udfs";

    MergeFrom(TypeInferrers_.Get(), *GetBuiltinTypeInferrers());
    MergeFrom(FunctionProfilers_.Get(), *GetBuiltinFunctionProfilers());
    MergeFrom(AggregateProfilers_.Get(), *GetBuiltinAggregateProfilers());

    auto builder = CreateFunctionRegistryBuilder(
        TypeInferrers_.Get(),
        FunctionProfilers_.Get(),
        AggregateProfilers_.Get());

    builder->RegisterFunction(
        "abs_udf",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        bcImplementations,
        ECallingConvention::Simple);
    builder->RegisterFunction(
        "exp_udf",
        std::vector<TType>{EValueType::Int64, EValueType::Int64},
        EValueType::Int64,
        bcImplementations,
        ECallingConvention::Simple);
    builder->RegisterFunction(
        "strtol_udf",
        std::vector<TType>{EValueType::String},
        EValueType::Uint64,
        bcImplementations,
        ECallingConvention::Simple);
    builder->RegisterFunction(
        "tolower_udf",
        std::vector<TType>{EValueType::String},
        EValueType::String,
        bcImplementations,
        ECallingConvention::Simple);
    builder->RegisterFunction(
        "is_null_udf",
        std::vector<TType>{EValueType::String},
        EValueType::Boolean,
        bcImplementations,
        ECallingConvention::UnversionedValue);
    builder->RegisterFunction(
        "string_equals_42_udf",
        std::vector<TType>{EValueType::String},
        EValueType::Boolean,
        bcImplementations,
        ECallingConvention::UnversionedValue);
    builder->RegisterFunction(
        "sum_udf",
        std::unordered_map<TTypeParameter, TUnionType>(),
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        EValueType::Int64,
        bcImplementations);
    builder->RegisterFunction(
        "seventyfive",
        std::vector<TType>{},
        EValueType::Uint64,
        bcImplementations,
        ECallingConvention::Simple);
    builder->RegisterFunction(
        "throw_if_negative_udf",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        bcImplementations,
        ECallingConvention::Simple);
}

void TQueryEvaluateTest::TearDown()
{
    ActionQueue_->Shutdown();
}

std::pair<TQueryPtr, TQueryStatistics> TQueryEvaluateTest::EvaluateWithQueryStatistics(
    TStringBuf query,
    const TSplitMap& dataSplits,
    const std::vector<TSource>& owningSources,
    const TResultMatcher& resultMatcher,
    TEvaluateOptions options)
{
    return BIND(&TQueryEvaluateTest::DoEvaluate, this)
        .AsyncVia(ActionQueue_->GetInvoker())
        .Run(
            query,
            dataSplits,
            owningSources,
            resultMatcher,
            options,
            std::nullopt)
        .Get()
        .ValueOrThrow();
}

TQueryPtr TQueryEvaluateTest::Evaluate(
    TStringBuf query,
    const TSplitMap& dataSplits,
    const std::vector<TSource>& owningSources,
    const TResultMatcher& resultMatcher,
    TEvaluateOptions evaluateOptions)
{
    evaluateOptions.ExecutionBackend = EExecutionBackend::WebAssembly;
    EvaluateWithQueryStatistics(
        query,
        dataSplits,
        owningSources,
        resultMatcher,
        evaluateOptions);

    evaluateOptions.ExecutionBackend = EExecutionBackend::Native;
    return EvaluateWithQueryStatistics(
        query,
        dataSplits,
        owningSources,
        resultMatcher,
        std::move(evaluateOptions)).first;
}

TQueryPtr TQueryEvaluateTest::EvaluateOnlyViaNativeExecutionBackend(
    TStringBuf query,
    const TSplitMap& dataSplits,
    const std::vector<TSource>& owningSources,
    const TResultMatcher& resultMatcher,
    TEvaluateOptions evaluateOptions)
{
    evaluateOptions.ExecutionBackend = EExecutionBackend::Native;
    return EvaluateWithQueryStatistics(
        query,
        dataSplits,
        owningSources,
        resultMatcher,
        std::move(evaluateOptions)).first;
}

std::pair<TQueryPtr, TQueryStatistics> TQueryEvaluateTest::EvaluateWithQueryStatistics(
    TStringBuf query,
    const TDataSplit& dataSplit,
    const TSource& owningSourceRows,
    const TResultMatcher& resultMatcher,
    TEvaluateOptions evaluateOptions)
{
    std::vector<TSource> owningSources = {
        owningSourceRows
    };
    TSplitMap dataSplits = {
        {"//t", dataSplit}
    };

    evaluateOptions.ExecutionBackend = EExecutionBackend::WebAssembly;
    EvaluateWithQueryStatistics(
        query,
        dataSplits,
        owningSources,
        resultMatcher,
        evaluateOptions);

    evaluateOptions.ExecutionBackend = EExecutionBackend::Native;
    return EvaluateWithQueryStatistics(
        query,
        dataSplits,
        owningSources,
        resultMatcher,
        evaluateOptions);
}

TQueryPtr TQueryEvaluateTest::Evaluate(
    TStringBuf query,
    const TDataSplit& dataSplit,
    const TSource& owningSourceRows,
    const TResultMatcher& resultMatcher,
    TEvaluateOptions evaluateOptions)
{
    return EvaluateWithQueryStatistics(
        query,
        dataSplit,
        owningSourceRows,
        resultMatcher,
        std::move(evaluateOptions)).first;
}

TQueryPtr TQueryEvaluateTest::EvaluateWithSyntaxV2(
    TStringBuf query,
    const TDataSplit& dataSplit,
    const TSource& owningSourceRows,
    const TResultMatcher& resultMatcher,
    TEvaluateOptions evaluateOptions)
{
    evaluateOptions.SyntaxVersion = 2;
    return EvaluateWithQueryStatistics(
        query,
        dataSplit,
        owningSourceRows,
        resultMatcher,
        std::move(evaluateOptions)).first;
}

TQueryPtr TQueryEvaluateTest::EvaluateOnlyViaNativeExecutionBackend(
    TStringBuf query,
    const TDataSplit& dataSplit,
    const TSource& owningSourceRows,
    const TResultMatcher& resultMatcher,
    TEvaluateOptions evaluateOptions)
{
    std::vector<TSource> owningSources = {
        owningSourceRows
    };

    TSplitMap dataSplits = {
        {"//t", dataSplit}
    };

    evaluateOptions.ExecutionBackend = EExecutionBackend::Native;
    return EvaluateWithQueryStatistics(
        query,
        dataSplits,
        owningSources,
        resultMatcher,
        std::move(evaluateOptions)).first;
}

TQueryPtr TQueryEvaluateTest::EvaluateExpectingError(
    TStringBuf query,
    const TDataSplit& dataSplit,
    const TSource& owningSourceRows,
    TEvaluateOptions evaluateOptions,
    std::string expectedError)
{
    std::vector<TSource> owningSources = {
        owningSourceRows
    };
    TSplitMap dataSplits = {
        {"//t", dataSplit}
    };

    evaluateOptions.ExecutionBackend = EExecutionBackend::WebAssembly;
    BIND(&TQueryEvaluateTest::DoEvaluate, this)
        .AsyncVia(ActionQueue_->GetInvoker())
        .Run(
            query,
            dataSplits,
            owningSources,
            AnyMatcher,
            evaluateOptions,
            expectedError)
        .Get()
        .ValueOrThrow();

    evaluateOptions.ExecutionBackend = EExecutionBackend::Native;
    return BIND(&TQueryEvaluateTest::DoEvaluate, this)
        .AsyncVia(ActionQueue_->GetInvoker())
        .Run(
            query,
            dataSplits,
            owningSources,
            AnyMatcher,
            std::move(evaluateOptions),
            expectedError)
        .Get()
        .ValueOrThrow().first;
}

TQueryPtr TQueryEvaluateTest::Prepare(
    TStringBuf query,
    const TSplitMap& dataSplits,
    TYsonStringBuf placeholderValues,
    int syntaxVersion)
{
    for (const auto& dataSplit : dataSplits) {
        EXPECT_CALL(PrepareMock_, GetInitialSplit(dataSplit.first))
            .Times(testing::AtMost(1))
            .WillRepeatedly(Return(MakeFuture(dataSplit.second)));
    }

    auto fragment = ParseAndPreparePlanFragment(
        &PrepareMock_,
        query,
        placeholderValues,
        syntaxVersion);

    return fragment->Query;
}

std::pair<TQueryPtr, TQueryStatistics> TQueryEvaluateTest::DoEvaluate(
    TStringBuf query,
    const TSplitMap& dataSplits,
    const std::vector<TSource>& owningSources,
    const TResultMatcher& resultMatcher,
    TEvaluateOptions evaluateOptions,
    std::optional<std::string> expectedError)
{
    SCOPED_TRACE(query);

    if (evaluateOptions.ExecutionBackend == EExecutionBackend::WebAssembly && !EnableWebAssemblyInUnitTests()) {
        return {};
    }

    auto primaryQuery = Prepare(query, dataSplits, evaluateOptions.PlaceholderValues, evaluateOptions.SyntaxVersion);

    TQueryOptions options;
    options.InputRowLimit = evaluateOptions.InputRowLimit;
    options.OutputRowLimit = evaluateOptions.OutputRowLimit;
    options.UseCanonicalNullRelations = evaluateOptions.UseCanonicalNullRelations;
    options.ExecutionBackend = evaluateOptions.ExecutionBackend;
    options.AllowUnorderedGroupByWithLimit = evaluateOptions.AllowUnorderedGroupByWithLimit;

    auto aggregatedStatistics = TQueryStatistics();

    auto consumeSubqueryStatistics = [&] (TQueryStatistics joinSubqueryStatistics) {
        aggregatedStatistics.AddInnerStatistics(std::move(joinSubqueryStatistics));
    };

    auto executePlan = [&] (TPlanFragment fragment, IUnversionedRowsetWriterPtr writer) {
        // TODO(sabdenovch): Switch to name- or id-based source rows navigation.
        // Ideally, do not separate schemas from sources.
        int sourceIndex = 1;
        for (const auto& joinClause : primaryQuery->JoinClauses) {
            if (!joinClause->ArrayExpressions.empty()) {
                continue;
            }
            if (joinClause->ForeignObjectId == fragment.DataSource.ObjectId) {
                break;
            }
            sourceIndex++;
        }

        return BIND(DoExecuteQuery)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run(
                Evaluator_,
                owningSources.at(sourceIndex),
                FunctionProfilers_,
                AggregateProfilers_,
                fragment.Query,
                writer,
                options,
                /*joinProfilers*/ {});
    };

    std::vector<IJoinProfilerPtr> joinProfilers;
    for (const auto& joinClause : primaryQuery->JoinClauses) {
        auto getPrefetchJoinDataSource = [=] () -> std::optional<TDataSource> {
            // This callback is usually dependent on the structure of tablets.
            // Thus, in tests we resort to returning a universal range.

            if (ShouldPrefetchJoinSource(
                primaryQuery,
                *joinClause,
                evaluateOptions.MinKeyWidth,
                primaryQuery->IsOrdered(/*allowUnorderedGroupByWithLimit*/ true)))
            {
                auto buffer = New<TRowBuffer>();
                TRowRanges universalRange{{
                    buffer->CaptureRow(NTableClient::MinKey().Get()),
                    buffer->CaptureRow(NTableClient::MaxKey().Get()),
                }};

                return TDataSource{
                    .Ranges = MakeSharedRange(std::move(universalRange), std::move(buffer)),
                };
            } else {
                return std::nullopt;
            }
        };

        joinProfilers.push_back(CreateJoinSubqueryProfiler(
            joinClause,
            std::move(executePlan),
            std::move(consumeSubqueryStatistics),
            std::move(getPrefetchJoinDataSource),
            GetDefaultMemoryChunkProvider(),
            /*useOrderByInJoinSubqueries=*/ true,
            Logger()));
    }

    auto prepareAndExecute = [&] {
        IUnversionedRowsetWriterPtr writer;
        TFuture<IUnversionedRowsetPtr> asyncResultRowset;

        std::tie(writer, asyncResultRowset) = CreateSchemafulRowsetWriter(primaryQuery->GetTableSchema());

        auto resultStatistics = DoExecuteQuery(
            Evaluator_,
            owningSources.front(),
            FunctionProfilers_,
            AggregateProfilers_,
            primaryQuery,
            writer,
            options,
            joinProfilers);

        resultStatistics.AddInnerStatistics(std::move(aggregatedStatistics));

        if (IsTimeDumpEnabled()) {
            DumpTime(resultStatistics, evaluateOptions.ExecutionBackend);
        }

        auto resultRowset = WaitFor(asyncResultRowset)
            .ValueOrThrow();
        auto rows = resultRowset->GetRows();
        resultMatcher(rows, *primaryQuery->GetTableSchema());

        return std::pair(primaryQuery, resultStatistics);
    };

    if (expectedError) {
        EXPECT_THROW_MESSAGE_HAS_SUBSTR(prepareAndExecute(), TErrorException, *expectedError);
        return {nullptr, TQueryStatistics{}};
    } else {
        return prepareAndExecute();
    }
}

TQueryStatistics TQueryEvaluateTest::EvaluateCoordinatedGroupByImpl(
    TStringBuf query,
    const TDataSplit& dataSplit,
    const std::vector<TSource>& owningSources,
    const TResultMatcher& resultMatcher,
    EExecutionBackend executionBackend)
{
    if (executionBackend == EExecutionBackend::WebAssembly && !EnableWebAssemblyInUnitTests()) {
        return {};
    }

    auto primaryQuery = Prepare(query, TSplitMap{{"//t", dataSplit}}, {}, /*syntaxVersion*/ 1);
    YT_VERIFY(primaryQuery->GroupClause);

    int tabletCount = owningSources.size();

    auto [frontQuery, bottomQuery] = GetDistributedQueryPattern(primaryQuery);

    std::vector<std::vector<TOwningRow>> owningSourceRows(tabletCount);
    std::vector<std::vector<TRow>> sourceRows(tabletCount);
    for (int index = 0; index < tabletCount; ++index) {
        for (const auto& row : owningSources[index]) {
            owningSourceRows[index].push_back(
                NTableClient::YsonToSchemafulRow(row, *bottomQuery->GetReadSchema(), true));
            sourceRows[index].push_back(TRow(owningSourceRows[index].back()));
        }
    }

    int tabletIndex = 0;
    std::vector<int> tabletReadProgress(tabletCount, 0);
    std::vector<TQueryStatistics> resultStatistics(tabletCount);
    auto getNextReader = [&, bottomQuery = bottomQuery] () -> ISchemafulUnversionedReaderPtr {
        int index = tabletIndex++;
        if (index == tabletCount) {
            return nullptr;
        }

        auto readRows = [&] (const TRowBatchReadOptions& options) {
            // Reset memory to test correct capturing of data.
            auto& readSoFar = tabletReadProgress[index];
            for (int rowIndex = 0; rowIndex < readSoFar; ++rowIndex) {
                owningSourceRows[index][rowIndex] = TOwningRow();
            }

            auto size = std::min<i64>(options.MaxRowsPerRead, sourceRows[index].size() - readSoFar);
            std::vector<TRow> rows(
                sourceRows[index].begin() + readSoFar,
                sourceRows[index].begin() + readSoFar + size);

            readSoFar += size;

            return rows.empty()
                ? nullptr
                : CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows)));
        };

        auto readerMock = New<NiceMock<TReaderMock>>();
        EXPECT_CALL(*readerMock, Read(_))
            .WillRepeatedly(Invoke(readRows));
        ON_CALL(*readerMock, GetReadyEvent())
            .WillByDefault(Return(OKFuture));

        auto pipe = New<NTableClient::TSchemafulPipe>(GetDefaultMemoryChunkProvider());
        resultStatistics[index] = Evaluator_->Run(
            bottomQuery,
            readerMock,
            pipe->GetWriter(),
            /*joinProfilers*/ {},
            FunctionProfilers_,
            AggregateProfilers_,
            NWebAssembly::GetBuiltinSdk(),
            GetDefaultMemoryChunkProvider(),
            TQueryOptions{{.ExecutionBackend = executionBackend, .AllowUnorderedGroupByWithLimit = true}},
            MostFreshFeatureFlags(),
            MakeFuture(MostFreshFeatureFlags()));

        return pipe->GetReader();
    };

    auto frontReader = frontQuery->IsOrdered(/*allowUnorderedGroupByWithLimit*/ true)
        ? CreateFullPrefetchingOrderedSchemafulReader(getNextReader)
        : CreateFullPrefetchingShufflingSchemafulReader(getNextReader);

    IUnversionedRowsetWriterPtr writer;
    TFuture<IUnversionedRowsetPtr> asyncResultRowset;
    std::tie(writer, asyncResultRowset) = CreateSchemafulRowsetWriter(frontQuery->GetTableSchema());

    auto frontStatistics = Evaluator_->Run(
        frontQuery,
        frontReader,
        writer,
        /*joinProfilers*/ {},
        FunctionProfilers_,
        AggregateProfilers_,
        NWebAssembly::GetBuiltinSdk(),
        GetDefaultMemoryChunkProvider(),
        TQueryOptions{{.ExecutionBackend = executionBackend, .AllowUnorderedGroupByWithLimit = true}},
        MostFreshFeatureFlags(),
        MakeFuture(MostFreshFeatureFlags()));

    auto rows = WaitFor(asyncResultRowset).ValueOrThrow()->GetRows();
    resultMatcher(rows, *frontQuery->GetTableSchema());

    if (IsTimeDumpEnabled()) {
        DumpTime(frontStatistics, executionBackend);
    }
    for (auto& stat : resultStatistics) {
        frontStatistics.AddInnerStatistics(std::move(stat));
    }

    return frontStatistics;
}

TQueryStatistics TQueryEvaluateTest::EvaluateCoordinatedGroupBy(
    TStringBuf query,
    const TDataSplit& dataSplit,
    const std::vector<TSource>& owningSources,
    const TResultMatcher& resultMatcher)
{
    EvaluateCoordinatedGroupByImpl(query, dataSplit, owningSources, resultMatcher, EExecutionBackend::WebAssembly);
    return EvaluateCoordinatedGroupByImpl(query, dataSplit, owningSources, resultMatcher, EExecutionBackend::Native);
}

TSchemafulPipePtr TQueryEvaluateTest::RunOnNodeThread(
    TConstQueryPtr query,
    const TSource& rows,
    EExecutionBackend executionBackend)
{
    i64 progress = 0;

    auto owningBatch = std::vector<TOwningRow>();

    auto readRows = [&] (const TRowBatchReadOptions& options) {
        i64 size = std::min(options.MaxRowsPerRead, std::ssize(rows) - progress);

        owningBatch.resize(size);
        auto batch = std::vector<TRow>(size);

        for (i64 index = 0; index < size; ++index) {
            owningBatch[index] = YsonToSchemafulRow(rows[progress + index], *query->GetReadSchema(), true);
            batch[index] = owningBatch[index];
        }

        progress += size;

        return (size == 0)
            ? nullptr
            : CreateBatchFromUnversionedRows(MakeSharedRange(std::move(batch)));
    };

    auto readerMock = New<NiceMock<TReaderMock>>();
    EXPECT_CALL(*readerMock, Read(_))
        .WillRepeatedly(Invoke(readRows));
    ON_CALL(*readerMock, GetReadyEvent())
        .WillByDefault(Return(OKFuture));

    auto pipe = New<TSchemafulPipe>(GetDefaultMemoryChunkProvider());

    Evaluator_->Run(
        query,
        readerMock,
        pipe->GetWriter(),
        /*joinProfilers*/ {},
        FunctionProfilers_,
        AggregateProfilers_,
        NWebAssembly::GetBuiltinSdk(),
        GetDefaultMemoryChunkProvider(),
        TQueryOptions{{.ExecutionBackend = executionBackend, .AllowUnorderedGroupByWithLimit = true}},
        MostFreshFeatureFlags(),
        MakeFuture(MostFreshFeatureFlags()));

    return pipe;
}

TSchemafulPipePtr TQueryEvaluateTest::RunOnNode(
    TConstQueryPtr nodeQuery,
    const std::vector<TSource>& tabletData,
    EExecutionBackend executionBackend)
{
    int partCount = std::ssize(tabletData);

    auto [query, bottomQuery] = GetDistributedQueryPattern(nodeQuery);

    int partIndex = 0;

    auto nextReader = [&, bottomQuery = bottomQuery] () -> ISchemafulUnversionedReaderPtr {
        if (partIndex == partCount) {
            return nullptr;
        }

        auto pipe = RunOnNodeThread(
            bottomQuery,
            tabletData[partIndex],
            executionBackend);

        ++partIndex;

        return pipe->GetReader();
    };

    auto reader = query->IsOrdered(/*allowUnorderedGroupByWithLimit*/ true)
        ? CreateFullPrefetchingOrderedSchemafulReader(nextReader)
        : CreateFullPrefetchingShufflingSchemafulReader(nextReader);

    auto pipe = New<TSchemafulPipe>(GetDefaultMemoryChunkProvider());

    Evaluator_->Run(
        query,
        reader,
        pipe->GetWriter(),
        /*joinProfilers*/ {},
        FunctionProfilers_,
        AggregateProfilers_,
        NWebAssembly::GetBuiltinSdk(),
        GetDefaultMemoryChunkProvider(),
        TQueryOptions{{.ExecutionBackend = executionBackend, .AllowUnorderedGroupByWithLimit = true}},
        MostFreshFeatureFlags(),
        MakeFuture(MostFreshFeatureFlags()));

    return pipe;
}

TSharedRange<TUnversionedRow> TQueryEvaluateTest::RunOnCoordinator(
    TQueryPtr primary,
    const std::vector<std::vector<TSource>>& tabletsData,
    EExecutionBackend executionBackend)
{
    int tabletCount = std::ssize(tabletsData);

    auto [frontQuery, nodeQuery] = GetDistributedQueryPattern(primary);

    int tabletIndex = 0;

    auto nextReader = [&, nodeQuery = nodeQuery] () -> ISchemafulUnversionedReaderPtr {
        if (tabletIndex == tabletCount) {
            return nullptr;
        }

        auto pipe = RunOnNode(nodeQuery, tabletsData[tabletIndex], executionBackend);

        ++tabletIndex;

        return pipe->GetReader();
    };

    auto reader = frontQuery->IsOrdered(/*allowUnorderedGroupByWithLimit*/ true)
        ? CreateFullPrefetchingOrderedSchemafulReader(nextReader)
        : CreateFullPrefetchingShufflingSchemafulReader(nextReader);

    auto [writer, asyncResultRowset] = CreateSchemafulRowsetWriter(frontQuery->GetTableSchema());

    Evaluator_->Run(
        frontQuery,
        reader,
        writer,
        /*joinProfilers*/ {},
        FunctionProfilers_,
        AggregateProfilers_,
        NWebAssembly::GetBuiltinSdk(),
        GetDefaultMemoryChunkProvider(),
        TQueryOptions{{.ExecutionBackend = executionBackend, .AllowUnorderedGroupByWithLimit = true}},
        MostFreshFeatureFlags(),
        MakeFuture(MostFreshFeatureFlags()));

    auto rows = WaitFor(asyncResultRowset).ValueOrThrow()->GetRows();

    return rows;
}

void TQueryEvaluateTest::EvaluateFullCoordinatedGroupByImpl(
    TStringBuf queryString,
    const TDataSplit& dataSplit,
    const std::vector<std::vector<TSource>>& data,
    const TResultMatcher& resultMatcher,
    EExecutionBackend executionBackend)
{
    auto query = Prepare(queryString, TSplitMap{{"//t", dataSplit}}, {}, /*syntaxVersion*/ 1);
    auto rows = RunOnCoordinator(query, data, executionBackend);

    resultMatcher(rows, *query->GetTableSchema());
}

std::vector<std::vector<TSource>> TQueryEvaluateTest::RandomSplitData(const TSource& data)
{
    auto result = std::vector<std::vector<TSource>>();

    result.emplace_back();
    result.back().emplace_back();

    for (auto& row : data) {
        if (rand() % 400 == 0) {
            result.emplace_back();
            result.back().emplace_back();
        }

        if (rand() % 400 == 0) {
            result.back().emplace_back();
        }

        result.back().back().emplace_back(row);
    }

    return result;
}

void TQueryEvaluateTest::EvaluateFullCoordinatedGroupBy(
    TStringBuf query,
    const TDataSplit& dataSplit,
    const TSource& data,
    const TResultMatcher& resultMatcher,
    int iterations)
{
    for (int i = 0; i < iterations; ++i) {
        auto sources = RandomSplitData(data);
        EvaluateFullCoordinatedGroupByImpl(query, dataSplit, sources, resultMatcher, EExecutionBackend::Native);
    }

    if (EnableWebAssemblyInUnitTests()) {
        for (int i = 0; i < iterations; ++i) {
            auto sources = RandomSplitData(data);
            EvaluateFullCoordinatedGroupByImpl(query, dataSplit, sources, resultMatcher, EExecutionBackend::WebAssembly);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_TEST_HOOK_AFTER_RUN(GTEST_YT_QUERY_ENGINE_SHUTDOWN)
{
    // Address sanitizer assumes that the cached objects have leaked
    // even though these objects are stored inside LeakyRefCountedSingleton.
    // Therefore, we manually clear the cache at the end of all tests.
    TCodegenCacheSingleton::TearDownForTests();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

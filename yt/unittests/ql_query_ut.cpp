#include "stdafx.h"
#include "framework.h"
#include "ql_helpers.h"

#include "udf/test_udfs.h"
#include "udf/test_udfs_o.h"
#include "udf/malloc_udf.h"
#include "udf/invalid_ir.h"

#include <core/concurrency/action_queue.h>

#include <ytlib/query_client/config.h>
#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/query_preparer.h>
#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/helpers.h>
#include <ytlib/query_client/coordinator.h>
#include <ytlib/query_client/evaluator.h>
#include <ytlib/query_client/column_evaluator.h>

#include <ytlib/query_client/helpers.h>
#include <ytlib/query_client/plan_fragment.pb.h>
#include <ytlib/query_client/user_defined_functions.h>

#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/name_table.h>
#include <ytlib/table_client/schemaful_reader.h>
#include <ytlib/table_client/schemaful_writer.h>

#include <ytlib/query_client/folding_profiler.h>

#include <tuple>

// Tests:
// TQueryPrepareTest
// TJobQueryPrepareTest
// TQueryCoordinateTest
// TQueryEvaluateTest

namespace NYT {
namespace NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NYPath;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

class TQueryPrepareTest
    : public ::testing::Test
{
protected:
    template <class TMatcher>
    void ExpectPrepareThrowsWithDiagnostics(
        const Stroka& query,
        TMatcher matcher)
    {
        EXPECT_THROW_THAT(
            [&] { PreparePlanFragment(&PrepareMock_, query, CreateBuiltinFunctionRegistry()); },
            matcher);
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;

};

TEST_F(TQueryPrepareTest, Simple)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    PreparePlanFragment(&PrepareMock_, "a, b FROM [//t] WHERE k > 3", CreateBuiltinFunctionRegistry());
}

TEST_F(TQueryPrepareTest, BadSyntax)
{
    ExpectPrepareThrowsWithDiagnostics(
        "bazzinga mu ha ha ha",
        HasSubstr("syntax error"));
}

TEST_F(TQueryPrepareTest, BadTableName)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//bad/table", _))
        .WillOnce(Invoke(&RaiseTableNotFound));

    ExpectPrepareThrowsWithDiagnostics(
        "a, b from [//bad/table]",
        HasSubstr("Could not find table //bad/table"));
}

TEST_F(TQueryPrepareTest, BadColumnNameInProject)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "foo from [//t]",
        HasSubstr("Undefined reference \"foo\""));
}

TEST_F(TQueryPrepareTest, BadColumnNameInFilter)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "k from [//t] where bar = 1",
        HasSubstr("Undefined reference \"bar\""));
}

TEST_F(TQueryPrepareTest, BadTypecheck)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "k from [//t] where a > \"xyz\"",
        ContainsRegex("Type mismatch in expression .*"));
}

TEST_F(TQueryPrepareTest, TooBigQuery)
{
    Stroka query = "k from [//t] where a ";
    for (int i = 0; i < 50 ; ++i) {
        query += "+ " + ToString(i);
    }
    query += " > 0";

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        query,
        ContainsRegex("Plan fragment depth limit exceeded"));
}

TEST_F(TQueryPrepareTest, BigQuery)
{
    Stroka query = "k from [//t] where a in (0";
    for (int i = 1; i < 1000; ++i) {
        query += ", " + ToString(i);
    }
    query += ")";

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    PreparePlanFragment(&PrepareMock_, query, CreateBuiltinFunctionRegistry());
}

TEST_F(TQueryPrepareTest, ResultSchemaCollision)
{
    ExpectPrepareThrowsWithDiagnostics(
        "a as x, b as x FROM [//t] WHERE k > 3",
        ContainsRegex("Alias \"x\" has been already used"));
}

TEST_F(TQueryPrepareTest, MisuseAggregateFunction)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "sum(sum(a)) from [//t] group by k",
        ContainsRegex("Misuse of aggregate function .*"));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    ExpectPrepareThrowsWithDiagnostics(
        "sum(a) from [//t]",
        ContainsRegex("Misuse of aggregate function .*"));
}

TEST_F(TQueryPrepareTest, JoinColumnCollision)
{
    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//s", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//s"))));

    ExpectPrepareThrowsWithDiagnostics(
        "a, b from [//t] join [//s] using b",
        ContainsRegex("Column \"a\" occurs both in main and joined tables"));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

    EXPECT_CALL(PrepareMock_, GetInitialSplit("//s", _))
        .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//s"))));

    ExpectPrepareThrowsWithDiagnostics(
        "* from [//t] join [//s] using b",
        ContainsRegex("Column .* occurs both in main and joined tables"));
}

////////////////////////////////////////////////////////////////////////////////

class TJobQueryPrepareTest
    : public ::testing::Test
{
};

TEST_F(TJobQueryPrepareTest, TruePredicate)
{
    PrepareJobQueryAst("* where true");
}

TEST_F(TJobQueryPrepareTest, FalsePredicate)
{
    PrepareJobQueryAst("* where false");
}

////////////////////////////////////////////////////////////////////////////////

class TQueryCoordinateTest
    : public ::testing::Test
{
protected:
    virtual void SetUp() override
    {
        EXPECT_CALL(PrepareMock_, GetInitialSplit("//t", _))
            .WillOnce(Return(WrapInFuture(MakeSimpleSplit("//t"))));

        auto config = New<TColumnEvaluatorCacheConfig>();
        ColumnEvaluatorCache_ = New<TColumnEvaluatorCache>(config, CreateBuiltinFunctionRegistry());
    }

    void Coordinate(const Stroka& source, const TDataSplits& dataSplits, size_t subqueriesCount)
    {
        auto planFragment = PreparePlanFragment(&PrepareMock_, source, CreateBuiltinFunctionRegistry());

        TDataSources sources;
        for (const auto& split : dataSplits) {
            auto range = GetBothBoundsFromDataSplit(split);
    
            TRowRange rowRange(
                planFragment->KeyRangesRowBuffer->Capture(range.first.Get()),
                planFragment->KeyRangesRowBuffer->Capture(range.second.Get()));

            sources.push_back(TDataSource{
                GetObjectIdFromDataSplit(split),
                rowRange});
        }

        auto rowBuffer = New<TRowBuffer>();
        auto groupedRanges = GetPrunedRanges(
            planFragment->Query,
            sources,
            rowBuffer,
            ColumnEvaluatorCache_,
            CreateBuiltinFunctionRegistry(),
            1000,
            true);
        int count = 0;
        for (const auto& group : groupedRanges) {
            count += group.size();
        }

        EXPECT_EQ(count, subqueriesCount);
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    TColumnEvaluatorCachePtr ColumnEvaluatorCache_;
};

TEST_F(TQueryCoordinateTest, EmptySplit)
{
    TDataSplits emptySplits;

    EXPECT_NO_THROW({
        Coordinate("k from [//t]", emptySplits, 0);
    });
}

TEST_F(TQueryCoordinateTest, SingleSplit)
{
    TDataSplits singleSplit;
    singleSplit.emplace_back(MakeSimpleSplit("//t", 1));

    EXPECT_NO_THROW({
        Coordinate("k from [//t]", singleSplit, 1);
    });
}

TEST_F(TQueryCoordinateTest, UsesKeyToPruneSplits)
{
    TDataSplits splits;

    splits.emplace_back(MakeSimpleSplit("//t", 1));
    SetSorted(&splits.back(), true);
    SetLowerBound(&splits.back(), BuildKey("0;0;0"));
    SetUpperBound(&splits.back(), BuildKey("1;0;0"));

    splits.emplace_back(MakeSimpleSplit("//t", 2));
    SetSorted(&splits.back(), true);
    SetLowerBound(&splits.back(), BuildKey("1;0;0"));
    SetUpperBound(&splits.back(), BuildKey("2;0;0"));

    splits.emplace_back(MakeSimpleSplit("//t", 3));
    SetSorted(&splits.back(), true);
    SetLowerBound(&splits.back(), BuildKey("2;0;0"));
    SetUpperBound(&splits.back(), BuildKey("3;0;0"));

    EXPECT_NO_THROW({
        Coordinate("a from [//t] where k = 1 and l = 2 and m = 3", splits, 1);
    });
}

TEST_F(TQueryCoordinateTest, SimpleIn)
{
    TDataSplits singleSplit;
    singleSplit.emplace_back(MakeSimpleSplit("//t", 1));

    EXPECT_NO_THROW({
        Coordinate("k from [//t] where k in (1, 2, 3)", singleSplit, 3);
    });
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFailureLocation,
    (Nowhere)
    (Codegen)
    (Execution)
);

class TReaderMock
    : public ISchemafulReader
{
public:
    MOCK_METHOD1(Read, bool(std::vector<TUnversionedRow>*));
    MOCK_METHOD0(GetReadyEvent, TFuture<void>());
};

class TWriterMock
    : public ISchemafulWriter
{
public:
    MOCK_METHOD0(Close, TFuture<void>());
    MOCK_METHOD1(Write, bool(const std::vector<TUnversionedRow>&));
    MOCK_METHOD0(GetReadyEvent, TFuture<void>());
};

class TFunctionRegistryMock
    : public IFunctionRegistry
{
public:
    MOCK_METHOD1(FindFunction, IFunctionDescriptorPtr(const Stroka&));
    MOCK_METHOD1(FindAggregateFunction, IAggregateFunctionDescriptorPtr(const Stroka&));

    void WithFunction(IFunctionDescriptorPtr function)
    {
        EXPECT_CALL(*this, FindFunction(function->GetName()))
            .WillRepeatedly(Return(function));
        EXPECT_CALL(*this, FindAggregateFunction(function->GetName()))
            .WillRepeatedly(Return(nullptr));
    }

    void WithFunction(IAggregateFunctionDescriptorPtr function)
    {
        EXPECT_CALL(*this, FindFunction(function->GetName()))
            .WillRepeatedly(Return(nullptr));
        EXPECT_CALL(*this, FindAggregateFunction(function->GetName()))
            .WillRepeatedly(Return(function));
    }
};


TOwningRow BuildRow(
    const Stroka& yson,
    const TDataSplit& dataSplit,
    bool treatMissingAsNull = true)
{
    auto keyColumns = GetKeyColumnsFromDataSplit(dataSplit);
    auto tableSchema = GetTableSchemaFromDataSplit(dataSplit);

    return NTableClient::BuildRow(
            yson, keyColumns, tableSchema, treatMissingAsNull);
}

TFuture<TQueryStatistics> DoExecuteQuery(
    const std::vector<Stroka>& source,
    IFunctionRegistryPtr functionRegistry,
    EFailureLocation failureLocation,
    TPlanFragmentPtr fragment,
    ISchemafulWriterPtr writer,
    TExecuteQuery executeCallback = nullptr)
{
    std::vector<TOwningRow> owningSource;
    std::vector<TRow> sourceRows;

    auto readerMock = New<StrictMock<TReaderMock>>();

    TKeyColumns emptyKeyColumns;
    for (const auto& row : source) {
        owningSource.push_back(NTableClient::BuildRow(row, emptyKeyColumns, fragment->Query->TableSchema));
    }

    sourceRows.resize(owningSource.size());
    typedef const TRow(TOwningRow::*TGetFunction)() const;

    std::transform(
        owningSource.begin(),
        owningSource.end(),
        sourceRows.begin(),
        std::mem_fn(TGetFunction(&TOwningRow::Get)));

    ON_CALL(*readerMock, Read(_))
        .WillByDefault(DoAll(SetArgPointee<0>(sourceRows), Return(false)));
    if (failureLocation != EFailureLocation::Codegen) {
        EXPECT_CALL(*readerMock, Read(_));
    }

    std::vector<TExecuteQuery> executeCallbacks;

    auto evaluator = New<TEvaluator>(New<TExecutorConfig>());
    return MakeFuture(evaluator->RunWithExecutor(
        fragment->Query,
        readerMock,
        writer,
        executeCallback,
        functionRegistry,
        true));
}


class TQueryEvaluateTest
    : public ::testing::Test
{
protected:
    virtual void SetUp() override
    {
        WriterMock_ = New<StrictMock<TWriterMock>>();

        ActionQueue_ = New<TActionQueue>("Test");

        auto testUdfImplementations = TSharedRef(
            test_udfs_bc,
            test_udfs_bc_len,
            nullptr);

        AbsUdf_ = New<TUserDefinedFunction>(
            "abs_udf",
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            testUdfImplementations,
            ECallingConvention::Simple);
        ExpUdf_ = New<TUserDefinedFunction>(
            "exp_udf",
            std::vector<TType>{EValueType::Int64, EValueType::Int64},
            EValueType::Int64,
            testUdfImplementations,
            ECallingConvention::Simple);
        StrtolUdf_ = New<TUserDefinedFunction>(
            "strtol_udf",
            std::vector<TType>{EValueType::String},
            EValueType::Uint64,
            testUdfImplementations,
            ECallingConvention::Simple);
        TolowerUdf_ = New<TUserDefinedFunction>(
            "tolower_udf",
            std::vector<TType>{EValueType::String},
            EValueType::String,
            testUdfImplementations,
            ECallingConvention::Simple);
        IsNullUdf_ = New<TUserDefinedFunction>(
            "is_null_udf",
            std::vector<TType>{EValueType::String},
            EValueType::Boolean,
            testUdfImplementations,
            ECallingConvention::UnversionedValue);
        SumUdf_ = New<TUserDefinedFunction>(
            "sum_udf",
            std::unordered_map<TTypeArgument, TUnionType>(),
            std::vector<TType>{EValueType::Int64},
            EValueType::Int64,
            EValueType::Int64,
            testUdfImplementations);
        SeventyFiveUdf_ = New<TUserDefinedFunction>(
            "seventyfive",
            std::vector<TType>{},
            EValueType::Uint64,
            testUdfImplementations,
            ECallingConvention::Simple);
    }

    virtual void TearDown() override
    {
        ActionQueue_->Shutdown();
    }

    void Evaluate(
        const Stroka& query,
        const TDataSplit& dataSplit,
        const std::vector<Stroka>& owningSource,
        const std::vector<TOwningRow>& owningResult,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max(),
        IFunctionRegistryPtr functionRegistry = CreateBuiltinFunctionRegistry())
    {
        std::vector<std::vector<Stroka>> owningSources(1, owningSource);
        std::map<Stroka, TDataSplit> dataSplits;
        dataSplits["//t"] = dataSplit;

        BIND(&TQueryEvaluateTest::DoEvaluate, this)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run(
                query,
                dataSplits,
                owningSources,
                owningResult,
                inputRowLimit,
                outputRowLimit,
                EFailureLocation::Nowhere,
                functionRegistry)
            .Get()
            .ThrowOnError();
    }

    void Evaluate(
        const Stroka& query,
        const std::map<Stroka, TDataSplit>& dataSplits,
        const std::vector<std::vector<Stroka>>& owningSources,
        const std::vector<TOwningRow>& owningResult,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max(),
        IFunctionRegistryPtr functionRegistry = CreateBuiltinFunctionRegistry())
    {
        BIND(&TQueryEvaluateTest::DoEvaluate, this)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run(
                query,
                dataSplits,
                owningSources,
                owningResult,
                inputRowLimit,
                outputRowLimit,
                EFailureLocation::Nowhere,
                functionRegistry)
            .Get()
            .ThrowOnError();
    }

    void EvaluateExpectingError(
        const Stroka& query,
        const TDataSplit& dataSplit,
        const std::vector<Stroka>& owningSource,
        EFailureLocation failureLocation,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max(),
        IFunctionRegistryPtr functionRegistry = CreateBuiltinFunctionRegistry())
    {
        std::vector<std::vector<Stroka>> owningSources(1, owningSource);
        std::map<Stroka, TDataSplit> dataSplits;
        dataSplits["//t"] = dataSplit;

        BIND(&TQueryEvaluateTest::DoEvaluate, this)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run(
                query,
                dataSplits,
                owningSources,
                std::vector<TOwningRow>(),
                inputRowLimit,
                outputRowLimit,
                failureLocation,
                functionRegistry)
            .Get()
            .ThrowOnError();
    }

    void DoEvaluate(
        const Stroka& query,
        const std::map<Stroka, TDataSplit>& dataSplits,
        const std::vector<std::vector<Stroka>>& owningSources,
        const std::vector<TOwningRow>& owningResult,
        i64 inputRowLimit,
        i64 outputRowLimit,
        EFailureLocation failureLocation,
        IFunctionRegistryPtr functionRegistry)
    {
        std::vector<std::vector<TRow>> results;
        typedef const TRow(TOwningRow::*TGetFunction)() const;

        for (auto iter = owningResult.begin(), end = owningResult.end(); iter != end;) {
            size_t writeSize = std::min(static_cast<int>(end - iter), NQueryClient::MaxRowsPerWrite);
            std::vector<TRow> result(writeSize);

            std::transform(
                iter,
                iter + writeSize,
                result.begin(),
                std::mem_fn(TGetFunction(&TOwningRow::Get)));

            results.push_back(result);

            iter += writeSize;
        }

        for (const auto& dataSplit : dataSplits) {
            EXPECT_CALL(PrepareMock_, GetInitialSplit(dataSplit.first, _))
                .WillOnce(Return(WrapInFuture(dataSplit.second)));
        }

        {
            testing::InSequence s;

            for (auto& result : results) {
                EXPECT_CALL(*WriterMock_, Write(result))
                    .WillOnce(Return(true));
            }

            ON_CALL(*WriterMock_, Close())
                .WillByDefault(Return(WrapVoidInFuture()));
            if (failureLocation == EFailureLocation::Nowhere) {
                EXPECT_CALL(*WriterMock_, Close());
            }
        }

        auto prepareAndExecute = [&] () {
            auto primaryFragment = PreparePlanFragment(&PrepareMock_, query, functionRegistry, inputRowLimit, outputRowLimit);

            size_t foreignSplitIndex = 1;
            auto executeCallback = [&] (
                const TQueryPtr& subquery,
                TGuid foreignDataId,
                ISchemafulWriterPtr writer) mutable -> TQueryStatistics
            {
                auto planFragment = New<TPlanFragment>();

                planFragment->Timestamp = primaryFragment->Timestamp;
                planFragment->DataSources.push_back({
                    foreignDataId,
                    {
                        planFragment->KeyRangesRowBuffer->Capture(MinKey().Get()),
                        planFragment->KeyRangesRowBuffer->Capture(MaxKey().Get())
                    }});
                planFragment->Query = subquery;

                auto subqueryResult = DoExecuteQuery(
                    owningSources[foreignSplitIndex++],
                    functionRegistry,
                    failureLocation,
                    planFragment,
                    writer);

                return WaitFor(subqueryResult)
                    .ValueOrThrow();
            };

            return DoExecuteQuery(
                owningSources.front(),
                functionRegistry,
                failureLocation,
                primaryFragment,
                WriterMock_,
                executeCallback);
        };

        if (failureLocation != EFailureLocation::Nowhere) {
            EXPECT_THROW(prepareAndExecute(), TErrorException);
        } else {
            prepareAndExecute();
        }
    }

    StrictMock<TPrepareCallbacksMock> PrepareMock_;
    TIntrusivePtr<StrictMock<TWriterMock>> WriterMock_;
    TActionQueuePtr ActionQueue_;

    IFunctionDescriptorPtr AbsUdf_;
    IFunctionDescriptorPtr ExpUdf_;
    IFunctionDescriptorPtr StrtolUdf_;
    IFunctionDescriptorPtr TolowerUdf_;
    IFunctionDescriptorPtr IsNullUdf_;
    IFunctionDescriptorPtr SumUdf_;
    IFunctionDescriptorPtr SeventyFiveUdf_;
};

std::vector<TOwningRow> BuildRows(std::initializer_list<const char*> rowsData, const TDataSplit& split)
{
    std::vector<TOwningRow> result;

    for (auto row : rowsData) {
        result.push_back(BuildRow(row, split, true));
    }

    return result;
}

TEST_F(TQueryEvaluateTest, Simple)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=4;b=5",
        "a=10;b=11"
    };

    auto result = BuildRows({
        "a=4;b=5",
        "a=10;b=11"
    }, split);

    Evaluate("a, b FROM [//t]", split, source, result);
}

TEST_F(TQueryEvaluateTest, SelectAll)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=4;b=5",
        "a=10;b=11"
    };

    auto result = BuildRows({
        "a=4;b=5",
        "a=10;b=11"
    }, split);

    Evaluate("* FROM [//t]", split, source, result);
}

TEST_F(TQueryEvaluateTest, SimpleCmpInt)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=4;b=5",
        "a=6;b=6"
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Boolean},
        {"r2", EValueType::Boolean},
        {"r3", EValueType::Boolean},
        {"r4", EValueType::Boolean},
        {"r5", EValueType::Boolean}
    });

    auto result = BuildRows({
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true"
    }, resultSplit);

    Evaluate("a < b as r1, a > b as r2, a <= b as r3, a >= b as r4, a = b as r5 FROM [//t]", split, source, result);
}

TEST_F(TQueryEvaluateTest, SimpleCmpString)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
        {"b", EValueType::String}
    });

    std::vector<Stroka> source = {
        "a=\"a\";b=\"aa\"",
        "a=\"aa\";b=\"aa\""
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Boolean},
        {"r2", EValueType::Boolean},
        {"r3", EValueType::Boolean},
        {"r4", EValueType::Boolean},
        {"r5", EValueType::Boolean}
    });

    auto result = BuildRows({
        "r1=%true;r2=%false;r3=%true;r4=%false;r5=%false",
        "r1=%false;r2=%false;r3=%true;r4=%true;r5=%true"
    }, resultSplit);

    Evaluate("a < b as r1, a > b as r2, a <= b as r3, a >= b as r4, a = b as r5 FROM [//t]", split, source, result);
}

TEST_F(TQueryEvaluateTest, SimpleBetweenAnd)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=4;b=5", 
        "a=10;b=11",
        "a=15;b=11"
    };

    auto result = BuildRows({
        "a=10;b=11"
    }, split);

    Evaluate("a, b FROM [//t] where a between 9 and 11", split, source, result);
}

TEST_F(TQueryEvaluateTest, SimpleIn)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=4;b=5", 
        "a=-10;b=11",
        "a=15;b=11"
    };

    auto result = BuildRows({
        "a=4;b=5",
        "a=-10;b=11"
    }, split);

    Evaluate("a, b FROM [//t] where a in (4, -10)", split, source, result);
}

TEST_F(TQueryEvaluateTest, SimpleWithNull)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=4;b=5",
        "a=10;b=11;c=9",
        "a=16"
    };

    auto result = BuildRows({
        "a=4;b=5",
        "a=10;b=11;c=9",
        "a=16"
    }, split);

    Evaluate("a, b, c FROM [//t] where a > 3", split, source, result);
}

TEST_F(TQueryEvaluateTest, SimpleWithNull2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=2;c=3",
        "a=4",
        "a=5;b=5",
        "a=7;c=8",
        "a=10;b=1",
        "a=10;c=1"
    };

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"x", EValueType::Int64}
    });

    auto result = BuildRows({
        "a=1;x=5",
        "a=4;",
        "a=5;",
        "a=7;"
    }, resultSplit);

    Evaluate("a, b + c as x FROM [//t] where a < 10", split, source, result);
}

TEST_F(TQueryEvaluateTest, SimpleStrings)
{
    auto split = MakeSplit({
        {"s", EValueType::String}
    });

    std::vector<Stroka> source = {
        "s=foo",
        "s=bar",
        "s=baz"
    };

    auto result = BuildRows({
        "s=foo",
        "s=bar",
        "s=baz"
    }, split);

    Evaluate("s FROM [//t]", split, source, result);
}

TEST_F(TQueryEvaluateTest, SimpleStrings2)
{
    auto split = MakeSplit({
        {"s", EValueType::String},
        {"u", EValueType::String}
    });

    std::vector<Stroka> source = {
        "s=foo; u=x",
        "s=bar; u=y",
        "s=baz; u=x",
        "s=olala; u=z"
    };
    
    auto result = BuildRows({
        "s=foo; u=x",
        "s=baz; u=x"
    }, split);

    Evaluate("s, u FROM [//t] where u = \"x\"", split, source, result);
}

TEST_F(TQueryEvaluateTest, IsPrefixStrings)
{
    auto split = MakeSplit({
        {"s", EValueType::String}
    });

    std::vector<Stroka> source = {
        "s=foobar",
        "s=bar",
        "s=baz"
    };

    auto result = BuildRows({
        "s=foobar"
    }, split);

    Evaluate("s FROM [//t] where is_prefix(\"foo\", s)", split, source, result);
}

TEST_F(TQueryEvaluateTest, IsSubstrStrings)
{
    auto split = MakeSplit({
        {"s", EValueType::String}
    });

    std::vector<Stroka> source = {
        "s=foobar",
        "s=barfoo",
        "s=abc",
        "s=\"baz foo bar\"",
        "s=\"baz fo bar\"",
        "s=xyz",
        "s=baz"
    };

    auto result = BuildRows({
        "s=foobar",
        "s=barfoo",
        "s=\"baz foo bar\"",
        "s=baz"
    }, split);

    Evaluate("s FROM [//t] where is_substr(\"foo\", s) or is_substr(s, \"XX baz YY\")", split, source, result);
}

TEST_F(TQueryEvaluateTest, GroupByBool)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean},
        {"t", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=%false;t=200",
        "x=%true;t=240"
    }, resultSplit);

    Evaluate("x, sum(b) as t FROM [//t] where a > 1 group by a % 2 = 1 as x", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexWithAliases)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=0;t=200",
        "x=1;t=241"
    }, resultSplit);

    Evaluate("a % 2 as x, sum(b) + x as t FROM [//t] where a > 1 group by x", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, Complex)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=0;t=200",
        "x=1;t=241"
    }, resultSplit);

    Evaluate("x, sum(b) + x as t FROM [//t] where a > 1 group by a % 2 as x", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, Complex2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"q", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=0;q=0;t=200",
        "x=1;q=0;t=241"
    }, resultSplit);

    Evaluate("x, q, sum(b) + x as t FROM [//t] where a > 1 group by a % 2 as x, 0 as q", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexBigResult)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source;
    for (size_t i = 0; i < 10000; ++i) {
        source.push_back(Stroka() + "a=" + ToString(i) + ";b=" + ToString(i * 10));
    }

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64}
    });

    std::vector<TOwningRow> result;

    for (size_t i = 2; i < 10000; ++i) {
        result.push_back(BuildRow(Stroka() + "x=" + ToString(i) + ";t=" + ToString(i * 10 + i), resultSplit, false));
    }

    Evaluate("x, sum(b) + x as t FROM [//t] where a > 1 group by a as x", split, source, result);
}

TEST_F(TQueryEvaluateTest, ComplexWithNull)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90",
        "a=10",
        "b=1",
        "b=2",
        "b=3"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64},
        {"y", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=1;t=251;y=250",
        "x=0;t=200;y=200",
        "y=6"
    }, resultSplit);

    Evaluate("x, sum(b) + x as t, sum(b) as y FROM [//t] group by a % 2 as x", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, HavingClause1)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=1;b=10",
        "a=2;b=20",
        "a=2;b=20",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64},
    });

    auto result = BuildRows({
        "x=1;t=20",
    }, resultSplit);

    Evaluate("a as x, sum(b) as t FROM [//t] group by a having a = 1", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, HavingClause2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=1;b=10",
        "a=2;b=20",
        "a=2;b=20",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"t", EValueType::Int64},
    });

    auto result = BuildRows({
        "x=1;t=20",
    }, resultSplit);

    Evaluate("a as x, sum(b) as t FROM [//t] group by a having sum(b) = 20", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, HavingClause3)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=1;b=10",
        "a=2;b=20",
        "a=2;b=20",
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=1",
    }, resultSplit);

    Evaluate("a as x FROM [//t] group by a having sum(b) = 20", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, IsNull)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=9;b=90",
        "a=10",
        "b=1",
        "b=2",
        "b=3"
    };

    auto resultSplit = MakeSplit({
        {"b", EValueType::Int64}
    });

    auto result = BuildRows({
        "b=1",
        "b=2",
        "b=3"
    }, resultSplit);

    Evaluate("b FROM [//t] where is_null(a)", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, DoubleSum)
{
    auto split = MakeSplit({
        {"a", EValueType::Double}
    });

    std::vector<Stroka> source = {
        "a=1.",
        "a=1.",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Double},
        {"t", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=2.;t=3"
    }, resultSplit);

    Evaluate("sum(a) as x, sum(1) as t FROM [//t] group by 1", split, source, result);
}

TEST_F(TQueryEvaluateTest, ComplexStrings)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"s", EValueType::String}
    });

    std::vector<Stroka> source = {
        "a=10;s=x",
        "a=20;s=y",
        "a=30;s=x",
        "a=40;s=x",
        "a=42",
        "a=50;s=x",
        "a=60;s=y",
        "a=70;s=z",
        "a=72",
        "a=80;s=y",
        "a=85",
        "a=90;s=z"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"t", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=y;t=160",
        "x=x;t=120",
        "t=199",
        "x=z;t=160"
    }, resultSplit);

    Evaluate("x, sum(a) as t FROM [//t] where a > 10 group by s as x", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexStringsLower)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
        {"s", EValueType::String}
    });

    std::vector<Stroka> source = {
        "a=XyZ;s=one",
        "a=aB1C;s=two",
        "a=cs1dv;s=three",
        "a=HDs;s=four",
        "a=kIu;s=five",
        "a=trg1t;s=six"
    };

    auto resultSplit = MakeSplit({
        {"s", EValueType::String}
    });

    auto result = BuildRows({
        "s=one",
        "s=two",
        "s=four",
        "s=five"
    }, resultSplit);

    Evaluate("s FROM [//t] where lower(a) in (\"xyz\",\"ab1c\",\"hds\",\"kiu\")", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestIf)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"t", EValueType::Double}
    });

    auto result = BuildRows({
        "x=b;t=251.",
        "x=a;t=201."
    }, resultSplit);
    
    Evaluate("if(q = 4, \"a\", \"b\") as x, double(sum(b)) + 1.0 as t FROM [//t] group by if(a % 2 = 0, 4, 5) as"
                 " q", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestInputRowLimit)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto result = BuildRows({
        "a=2;b=20",
        "a=3;b=30"
    }, split);

    Evaluate("a, b FROM [//t] where uint64(a) > 1u and uint64(a) < 9u", split, source, result, 3);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestOutputRowLimit)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto result = BuildRows({
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40"
    }, split);

    Evaluate("a, b FROM [//t] where a > 1 and a < 9", split, source, result, std::numeric_limits<i64>::max(), 3);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestOutputRowLimit2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source;
    for (size_t i = 0; i < 10000; ++i) {
        source.push_back(Stroka() + "a=" + ToString(i) + ";b=" + ToString(i * 10));
    }

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    std::vector<TOwningRow> result;
    result.push_back(BuildRow(Stroka() + "x=" + ToString(10000), resultSplit, false));

    Evaluate("sum(1) as x FROM [//t] group by 0 as q", split, source, result, std::numeric_limits<i64>::max(),
             100);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestTypeInference)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"t", EValueType::Double}
    });

    auto result = BuildRows({
        "x=b;t=251.",
        "x=a;t=201."
    }, resultSplit);
    
    Evaluate("if(int64(q) = 4, \"a\", \"b\") as x, double(sum(uint64(b) * 1u)) + 1.0 as t FROM [//t] group by if"
                 "(a % 2 = 0, double(4u), 5.0) as q", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinEmpty)
{
    std::map<Stroka, TDataSplit> splits;
    std::vector<std::vector<Stroka>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;b=10",
        "a=3;b=30",
        "a=5;b=50",
        "a=7;b=70",
        "a=9;b=90"
    });

    auto rightSplit = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    splits["//right"] = rightSplit;
    sources.push_back({
        "c=2;b=20",
        "c=4;b=40",
        "c=6;b=60",
        "c=8;b=80"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::Int64},
        {"z", EValueType::Int64}
    });

    auto result = BuildRows({ }, resultSplit);

    Evaluate("sum(a) as x, sum(b) as y, z FROM [//left] join [//right] using b group by c % 2 as z", splits, sources, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinSimple2)
{
    std::map<Stroka, TDataSplit> splits;
    std::vector<std::vector<Stroka>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    });

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=2"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    });

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=2",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=1",
        "x=2"
    }, resultSplit);

    Evaluate("a as x FROM [//left] join [//right] using a", splits, sources, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinSimple3)
{
    std::map<Stroka, TDataSplit> splits;
    std::vector<std::vector<Stroka>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    });

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=1"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    });

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=2",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=1",
        "x=1"
    }, resultSplit);

    Evaluate("a as x FROM [//left] join [//right] using a", splits, sources, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinSimple4)
{
    std::map<Stroka, TDataSplit> splits;
    std::vector<std::vector<Stroka>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    });

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=2"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    });

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=1",
        "x=1"
    }, resultSplit);

    Evaluate("a as x FROM [//left] join [//right] using a", splits, sources, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinSimple5)
{
    std::map<Stroka, TDataSplit> splits;
    std::vector<std::vector<Stroka>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64}
    });

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1",
        "a=1"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64}
    });

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1",
        "a=1"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=1",
        "x=1",
        "x=1",
        "x=1"
    }, resultSplit);

    Evaluate("a as x FROM [//left] join [//right] using a", splits, sources, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinNonPrefixColumns)
{
    std::map<Stroka, TDataSplit> splits;
    std::vector<std::vector<Stroka>> sources;

    auto leftSplit = MakeSplit({
        {"x", EValueType::String},
        {"y", EValueType::String}
    }, {"x"});

    splits["//left"] = leftSplit;
    sources.push_back({
        "x=a",
        "x=b",
        "x=c"
    });

    auto rightSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"x", EValueType::String}
    }, {"a"});

    splits["//right"] = rightSplit;
    sources.push_back({
        "a=1;x=a",
        "a=2;x=b",
        "a=3;x=c"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::String},
        {"a", EValueType::Int64}
    });

    auto result = BuildRows({
        "a=1;x=a",
        "a=2;x=b",
        "a=3;x=c"
    }, resultSplit);

    Evaluate("* FROM [//left] join [//right] using x", splits, sources, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinManySimple)
{
    std::map<Stroka, TDataSplit> splits;
    std::vector<std::vector<Stroka>> sources;

    splits["//a"] = MakeSplit({
        {"a", EValueType::Int64},
        {"c", EValueType::String}
    });
    sources.push_back({
        "a=2;c=b",
        "a=3;c=c",
        "a=4;c=a"
    });

    splits["//b"] = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::String},
        {"d", EValueType::String}
    });
    sources.push_back({
        "b=100;c=a;d=X",
        "b=200;c=b;d=Y",
        "b=300;c=c;d=X",
        "b=400;c=a;d=Y",
        "b=500;c=b;d=X",
        "b=600;c=c;d=Y"
    });

    splits["//c"] = MakeSplit({
        {"d", EValueType::String},
        {"e", EValueType::Int64},
    });
    sources.push_back({
        "d=X;e=1234",
        "d=Y;e=5678"
    });


    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"c", EValueType::String},
        {"b", EValueType::Int64},
        {"d", EValueType::String},
        {"e", EValueType::Int64}
    });

    auto result = BuildRows({
         "a=2;c=b;b=200;d=Y;e=5678",
         "a=2;c=b;b=500;d=X;e=1234",
         "a=3;c=c;b=300;d=X;e=1234",
         "a=3;c=c;b=600;d=Y;e=5678",
         "a=4;c=a;b=100;d=X;e=1234",
         "a=4;c=a;b=400;d=Y;e=5678"
    }, resultSplit);

    Evaluate("a, c, b, d, e from [//a] join [//b] using c join [//c] using d order by a, b limit 100", splits, sources, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoin)
{
    std::map<Stroka, TDataSplit> splits;
    std::vector<std::vector<Stroka>> sources;

    auto leftSplit = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    splits["//left"] = leftSplit;
    sources.push_back({
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    });

    auto rightSplit = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    splits["//right"] = rightSplit;
    sources.push_back({
        "c=1;b=10",
        "c=2;b=20",
        "c=3;b=30",
        "c=4;b=40",
        "c=5;b=50",
        "c=6;b=60",
        "c=7;b=70",
        "c=8;b=80",
        "c=9;b=90"
    });

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"z", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=25;z=1",
        "x=20;z=0",
    }, resultSplit);

    Evaluate("sum(a) as x, z FROM [//left] join [//right] using b group by c % 2 as z", splits, sources, result);
    Evaluate("sum(a) as x, z FROM [//left] join [//right] on b = b group by c % 2 as z", splits, sources, result);
    Evaluate("sum(l.a) as x, z FROM [//left] as l join [//right] as r on l.b = r.b group by r.c % 2 as z", splits, sources, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, ComplexAlias)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"s", EValueType::String}
    });

    std::vector<Stroka> source = {
        "a=10;s=x",
        "a=20;s=y",
        "a=30;s=x",
        "a=40;s=x",
        "a=42",
        "a=50;s=x",
        "a=60;s=y",
        "a=70;s=z",
        "a=72",
        "a=80;s=y",
        "a=85",
        "a=90;s=z"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::String},
        {"t", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=y;t=160",
        "x=x;t=120",
        "t=199",
        "x=z;t=160"
    }, resultSplit);

    Evaluate("x, sum(p.a) as t FROM [//t] as p where p.a > 10 group by p.s as x", split, source, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestJoinMany)
{
    std::map<Stroka, TDataSplit> splits;
    std::vector<std::vector<Stroka>> sources;

    splits["//primary"] = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });
    sources.push_back({
        "a=1;b=10",
        "a=2;b=20",
        "a=3;b=30",
        "a=4;b=40",
        "a=5;b=50",
        "a=6;b=60",
        "a=7;b=70",
        "a=8;b=80",
        "a=9;b=90"
    });

    splits["//secondary"] = MakeSplit({
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });
    sources.push_back({
        "c=1;b=10",
        "c=2;b=20",
        "c=3;b=30",
        "c=4;b=40",
        "c=5;b=50",
        "c=6;b=60",
        "c=7;b=70",
        "c=8;b=80",
        "c=9;b=90"
    });

    splits["//tertiary"] = MakeSplit({
        {"c", EValueType::Int64},
        {"d", EValueType::Int64}
    });
    sources.push_back({
        "c=1;d=10",
        "c=2;d=20",
        "c=3;d=30",
        "c=4;d=40",
        "c=5;d=50",
        "c=6;d=60",
        "c=7;d=70",
        "c=8;d=80",
        "c=9;d=90"
    });


    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64},
        {"y", EValueType::Int64},
        {"z", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=25;y=250;z=1",
        "x=20;y=200;z=0",
    }, resultSplit);

    Evaluate("sum(a) as x, sum(d) as y, z FROM [//primary] join [//secondary] using b join [//tertiary] using c group by c % 2 as z", splits, sources, result);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestOrderBy)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source;
    
    for (int i = 0; i < 10000; ++i) {
        auto value = std::rand() % 100000 + 10000;
        source.push_back(Stroka() + "a=" + ToString(value) + ";b=" + ToString(value * 10));
    }

    for (int i = 0; i < 10000; ++i) {
        auto value = 10000 - i;
        source.push_back(Stroka() + "a=" + ToString(value) + ";b=" + ToString(value * 10));
    }

    std::vector<TOwningRow> result;
    
    for (const auto& row : source) {
        result.push_back(BuildRow(row, split, false));
    }

    std::vector<TOwningRow> limitedResult;

    std::sort(result.begin(), result.end());
    limitedResult.assign(result.begin(), result.begin() + 100);
    Evaluate("* FROM [//t] order by a limit 100", split, source, limitedResult);

    std::reverse(result.begin(), result.end());
    limitedResult.assign(result.begin(), result.begin() + 100);
    Evaluate("* FROM [//t] order by a desc limit 100", split, source, limitedResult);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestUdf)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=-2;b=20",
        "a=9;b=90",
        "a=-10"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=1",
        "x=2",
        "x=9",
        "x=10"
    }, resultSplit);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(AbsUdf_);

    Evaluate("abs_udf(a) as x FROM [//t]", split, source, result, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestZeroArgumentUdf)
{
    auto split = MakeSplit({
        {"a", EValueType::Uint64},
    });

    std::vector<Stroka> source = {
        "a=1u",
        "a=2u",
        "a=75u",
        "a=10u",
        "a=75u",
        "a=10u",
    };

    auto resultSplit = MakeSplit({
        {"a", EValueType::Int64}
    });

    auto result = BuildRows({
        "a=75u",
        "a=75u"
    }, resultSplit);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(SeventyFiveUdf_);

    Evaluate("a FROM [//t] where a = seventyfive()", split, source, result, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestInvalidUdfImpl)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
    };

    auto fileRef = TSharedRef(
        invalid_ir_bc,
        invalid_ir_bc_len,
        nullptr);

    auto invalidUdfDescriptor = New<TUserDefinedFunction>(
        "invalid_ir",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        fileRef,
        ECallingConvention::Simple);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(invalidUdfDescriptor);

    EvaluateExpectingError("invalid_ir(a) as x FROM [//t]", split, source, EFailureLocation::Codegen, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);
}

TEST_F(TQueryEvaluateTest, TestInvalidUdfArity)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
    };

    auto fileRef = TSharedRef(
        test_udfs_bc,
        test_udfs_bc_len,
        nullptr);

    auto twoArgumentUdf = New<TUserDefinedFunction>(
        "abs_udf",
        std::vector<TType>{EValueType::Int64, EValueType::Int64},
        EValueType::Int64,
        fileRef,
        ECallingConvention::Simple);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(twoArgumentUdf);

    EvaluateExpectingError("abs_udf(a, b) as x FROM [//t]", split, source, EFailureLocation::Codegen, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);
}

TEST_F(TQueryEvaluateTest, TestInvalidUdfType)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
    };

    auto fileRef = TSharedRef(
        test_udfs_bc,
        test_udfs_bc_len,
        nullptr);

    auto invalidArgumentUdf = New<TUserDefinedFunction>(
        "abs_udf",
        std::vector<TType>{EValueType::Double},
        EValueType::Int64,
        fileRef,
        ECallingConvention::Simple);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(invalidArgumentUdf);

    EvaluateExpectingError("abs_udf(a) as x FROM [//t]", split, source, EFailureLocation::Codegen, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);
}

TEST_F(TQueryEvaluateTest, TestUdfNullPropagation)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;",
        "a=-2;b=-20",
        "a=9;",
        "b=-10"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = BuildRows({
        "",
        "x=20",
        "",
        "x=10"
    }, resultSplit);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(AbsUdf_);

    Evaluate("abs_udf(b) as x FROM [//t]", split, source, result, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestUdfNullPropagation2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;",
        "a=2;b=10",
        "b=9",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = BuildRows({
        "",
        "x=1024",
        "",
        ""
    }, resultSplit);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(ExpUdf_);

    Evaluate("exp_udf(a, b) as x FROM [//t]", split, source, result, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestUdfStringArgument)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    std::vector<Stroka> source = {
        "a=\"123\"",
        "a=\"50\"",
        "a=\"\"",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Uint64}
    });

    auto result = BuildRows({
        "x=123u",
        "x=50u",
        "x=0u",
        ""
    }, resultSplit);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(StrtolUdf_);

    Evaluate("strtol_udf(a) as x FROM [//t]", split, source, result, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestUdfStringResult)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    std::vector<Stroka> source = {
        "a=\"HELLO\"",
        "a=\"HeLlO\"",
        "a=\"\"",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Uint64}
    });

    auto result = BuildRows({
        "x=\"hello\"",
        "x=\"hello\"",
        "x=\"\"",
        ""
    }, resultSplit);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(TolowerUdf_);

    Evaluate("tolower_udf(a) as x FROM [//t]", split, source, result, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestUnversionedValueUdf)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    std::vector<Stroka> source = {
        "a=\"Hello\"",
        "a=\"\"",
        ""
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean}
    });

    auto result = BuildRows({
        "x=%false",
        "x=%false",
        "x=%true"
    }, resultSplit);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(IsNullUdf_);

    Evaluate("is_null_udf(a) as x FROM [//t]", split, source, result, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestVarargUdf)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1",
        "a=2"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Boolean}
    });

    auto result = BuildRows({
        "x=1",
        "x=2"
    }, resultSplit);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(SumUdf_);

    Evaluate("a as x FROM [//t] where sum_udf(7, 3, a) in (11, 12)", split, source, result, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestObjectUdf)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=1;b=10",
        "a=2;b=2",
        "a=3;b=3",
        "a=10"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Int64}
    });

    auto result = BuildRows({
        "x=10",
        "x=4",
        "x=27",
        ""
    }, resultSplit);

    auto testUdfObjectImpl = TSharedRef(
        test_udfs_o_o,
        test_udfs_o_o_len,
        nullptr);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    auto expUdf = New<TUserDefinedFunction>(
        "exp_udf",
        std::vector<TType>{
            EValueType::Int64,
            EValueType::Int64},
        EValueType::Int64,
        testUdfObjectImpl,
        ECallingConvention::Simple);
    registry->WithFunction(expUdf);

    Evaluate("exp_udf(b, a) as x FROM [//t]", split, source, result, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestFunctionWhitelist)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=3",
        "a=4"
    };

    auto mallocUdf = New<TUserDefinedFunction>(
        "malloc_udf",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        TSharedRef(
            malloc_udf_bc,
            malloc_udf_bc_len,
            nullptr),
        ECallingConvention::Simple);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(mallocUdf);

    EvaluateExpectingError("malloc_udf(a) as x FROM [//t]", split, source, EFailureLocation::Codegen, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestFarmHash)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::String},
        {"c", EValueType::Boolean}
    });

    std::vector<Stroka> source = {
        "a=3;b=\"hello\";c=%true",
        "a=54;c=%false"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Uint64}
    });

    auto result = BuildRows({
        "x=13185060272037541714u",
        "x=1607147011416532415u"
    }, resultSplit);

    Evaluate("farm_hash(a, b, c) as x FROM [//t]", split, source, result, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max());

    SUCCEED();
}

TEST_F(TQueryEvaluateTest, TestAverageAgg)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=3",
        "a=53",
        "a=8",
        "a=24",
        "a=33"
    };

    auto resultSplit = MakeSplit({
        {"x", EValueType::Double}
    });

    auto result = BuildRows({
        "x=24.2",
    }, resultSplit);

    Evaluate("avg(a) as x from [//t] group by 1", split, source, result);
}

TEST_F(TQueryEvaluateTest, TestAverageAgg2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64},
        {"c", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=3;b=3;c=1",
        "a=53;b=2;c=3",
        "a=8;b=5;c=32",
        "a=24;b=7;c=4",
        "a=33;b=4;c=9",
        "a=33;b=3;c=43",
        "a=23;b=0;c=0",
        "a=33;b=8;c=2"
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Double},
        {"x", EValueType::Int64},
        {"r2", EValueType::Int64},
        {"r3", EValueType::Double},
        {"r4", EValueType::Int64},
    });

    auto result = BuildRows({
        "r1=17.0;x=1;r2=43;r3=20.0;r4=3",
        "r1=35.5;x=0;r2=9;r3=3.5;r4=23"
    }, resultSplit);

    Evaluate("avg(a) as r1, x, max(c) as r2, avg(c) as r3, min(a) as r4 from [//t] group by b % 2 as x", split, source, result);
}

TEST_F(TQueryEvaluateTest, TestAverageAgg3)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
        {"b", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=3;b=1",
        "b=1",
        "b=0",
        "a=7;b=1",
    };

    auto resultSplit = MakeSplit({
        {"b", EValueType::Int64},
        {"x", EValueType::Double}
    });

    auto result = BuildRows({
        "b=1;x=5.0",
        "b=0"
    }, resultSplit);

    Evaluate("b, avg(a) as x from [//t] group by b", split, source, result);
}

TEST_F(TQueryEvaluateTest, TestStringAgg)
{
    auto split = MakeSplit({
        {"a", EValueType::String},
    });

    std::vector<Stroka> source = {
        "a=\"one\"",
        "a=\"two\"",
        "a=\"three\"",
        "a=\"four\"",
        "a=\"fo\"",
    };

    auto resultSplit = MakeSplit({
        {"b", EValueType::String},
    });

    auto result = BuildRows({
        "b=\"fo\";c=\"two\"",
    }, resultSplit);

    Evaluate("min(a) as b, max(a) as c from [//t] group by 1", split, source, result);
}

TEST_F(TQueryEvaluateTest, WronglyTypedAggregate)
{
    auto split = MakeSplit({
        {"a", EValueType::String}
    });

    std::vector<Stroka> source = {
        "a=\"\""
    };

    EvaluateExpectingError("avg(a) from [//t] group by 1", split, source, EFailureLocation::Codegen);
}

TEST_F(TQueryEvaluateTest, CardinalityAggregate)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<Stroka> source;
    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 2000; j++) {
            source.push_back("a=" + ToString(j));
        }
    }

    auto resultSplit = MakeSplit({
        {"upper", EValueType::Boolean},
        {"lower", EValueType::Boolean},
    });

    auto result = BuildRows({
        "upper=%true;lower=%true"
    }, resultSplit);

    Evaluate("cardinality(a) < 2020u as upper, cardinality(a) > 1980u as lower from [//t] group by 1", split, source, result);
}

TEST_F(TQueryEvaluateTest, TestObjectUdaf)
{
    auto split = MakeSplit({
        {"a", EValueType::Uint64}
    });

    std::vector<Stroka> source = {
        "a=3u",
        "a=53u",
        "a=8u",
        "a=24u",
        "a=33u",
        "a=333u",
        "a=23u",
        "a=33u"
    };

    auto resultSplit = MakeSplit({
        {"r", EValueType::Uint64},
    });

    auto result = BuildRows({
        "r=333u"
    }, resultSplit);

    auto testUdfObjectImpl = TSharedRef(
        test_udfs_o_o,
        test_udfs_o_o_len,
        nullptr);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(New<TUserDefinedAggregateFunction>(
        "max_udaf",
        std::unordered_map<TTypeArgument, TUnionType>(),
        EValueType::Uint64,
        EValueType::Uint64,
        EValueType::Uint64,
        testUdfObjectImpl,
        ECallingConvention::Simple));

    Evaluate("max_udaf(a) as r from [//t] group by 1", split, source, result, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);
}

TEST_F(TQueryEvaluateTest, TestLinkingError1)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=3",
    };

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(AbsUdf_);
    registry->WithFunction(ExpUdf_);

    EvaluateExpectingError("exp_udf(abs_udf(a), 3) from [//t]", split, source, EFailureLocation::Codegen, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);
    EvaluateExpectingError("abs_udf(exp_udf(a, 3)) from [//t]", split, source, EFailureLocation::Codegen, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);
}

TEST_F(TQueryEvaluateTest, TestLinkingError2)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=3"
    };

    auto testUdfObjectImpl = TSharedRef(
        test_udfs_o_o,
        test_udfs_o_o_len,
        nullptr);
    auto absUdfSo = New<TUserDefinedFunction>(
        "abs_udf",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        testUdfObjectImpl,
        ECallingConvention::Simple);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(absUdfSo);
    registry->WithFunction(SumUdf_);

    EvaluateExpectingError("sum_udf(abs_udf(a), 3) as r from [//t]", split, source, EFailureLocation::Codegen, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);
    EvaluateExpectingError("abs_udf(sum_udf(a, 3)) as r from [//t]", split, source, EFailureLocation::Codegen, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);
}

TEST_F(TQueryEvaluateTest, TestLinkingError3)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64}
    });

    std::vector<Stroka> source = {
        "a=3"
    };

    auto testUdfObjectImpl = TSharedRef(
        test_udfs_o_o,
        test_udfs_o_o_len,
        nullptr);
    auto absUdfSo = New<TUserDefinedFunction>(
        "abs_udf",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        testUdfObjectImpl,
        ECallingConvention::Simple);
    auto expUdfSo = New<TUserDefinedFunction>(
        "exp_udf",
        std::vector<TType>{EValueType::Int64, EValueType::Int64},
        EValueType::Int64,
        testUdfObjectImpl,
        ECallingConvention::Simple);

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    registry->WithFunction(absUdfSo);
    registry->WithFunction(expUdfSo);

    EvaluateExpectingError("abs_udf(exp_udf(a, 3)) as r from [//t]", split, source, EFailureLocation::Codegen, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);
    EvaluateExpectingError("exp_udf(abs_udf(a), 3) as r from [//t]", split, source, EFailureLocation::Codegen, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);
}

TEST_F(TQueryEvaluateTest, TestCasts)
{
    auto split = MakeSplit({
        {"a", EValueType::Uint64},
        {"b", EValueType::Int64},
        {"c", EValueType::Double}
    });

    std::vector<Stroka> source = {
        "a=3u;b=34",
        "c=1.23",
        "a=12u",
        "b=0;c=1.0",
        "a=5u",
    };

    auto resultSplit = MakeSplit({
        {"r1", EValueType::Int64},
        {"r2", EValueType::Double},
        {"r3", EValueType::Uint64},
    });

    auto result = BuildRows({
        "r1=3;r2=34.0",
        "r3=1u",
        "r1=12",
        "r2=0.0;r3=1u",
        "r1=5",
    }, resultSplit);

    Evaluate("int64(a) as r1, double(b) as r2, uint64(c) as r3 from [//t]", split, source, result);
}

TEST_F(TQueryEvaluateTest, TestUdfException)
{
    auto split = MakeSplit({
        {"a", EValueType::Int64},
    });

    std::vector<Stroka> source = {
        "a=-3",
    };

    auto registry = New<StrictMock<TFunctionRegistryMock>>();
    auto throwImpl = TSharedRef(
        test_udfs_bc,
        test_udfs_bc_len,
        nullptr);
    auto throwUdf = New<TUserDefinedFunction>(
        "throw_if_negative_udf",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        throwImpl,
        ECallingConvention::Simple);
    
    auto resultSplit = MakeSplit({
        {"r", EValueType::Int64},
    });

    auto result = BuildRows({
    }, resultSplit);

    registry->WithFunction(throwUdf);

    EvaluateExpectingError("throw_if_negative_udf(a) from [//t]", split, source, EFailureLocation::Execution, std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max(), registry);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NQueryClient
} // namespace NYT

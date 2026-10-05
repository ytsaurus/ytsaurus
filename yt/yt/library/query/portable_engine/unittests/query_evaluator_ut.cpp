#include "semantics_cases.h"

#include <yt/yt/library/query/portable_engine/evaluation_helpers.h>
#include <yt/yt/library/query/portable_engine/program.h>
#include <yt/yt/library/query/portable_engine/registry.h>

#include <yt/yt/library/query/engine_api/query_evaluator.h>

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/bind.h>

#include <yt/yt/core/concurrency/propagating_storage.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/string.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <iterator>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

namespace NYT::NQueryClient::NPortable::NTest {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EContextFactory,
    (Typed)
    (Parsed)
);

TQueryEvaluationContextPtr CreateContext(
    const std::string& source,
    const TTableSchemaPtr& schema,
    EContextFactory factory)
{
    auto parsedSource = ParseSource(source, EParseMode::Expression);
    if (factory == EContextFactory::Parsed) {
        return CreateQueryEvaluationContext(*parsedSource, schema);
    }

    return CreateQueryEvaluationContext(PrepareExpression(*parsedSource, *schema), schema);
}

std::vector<TScalarExpressionCase> GetV1ScalarExpressionCases()
{
    std::vector<TScalarExpressionCase> cases;
    std::ranges::copy_if(
        GetScalarExpressionCases(),
        std::back_inserter(cases),
        [] (const TScalarExpressionCase& testCase) {
            return testCase.BuilderVersion == 1;
        });
    return cases;
}

class TPortableQueryEvaluatorSemanticsTest
    : public ::testing::TestWithParam<std::tuple<TScalarExpressionCase, EContextFactory>>
{ };

TEST_P(TPortableQueryEvaluatorSemanticsTest, Evaluate)
{
    const auto& [testCase, factory] = GetParam();
    auto evaluate = [&] {
        auto context = CreateContext(testCase.Source, New<TTableSchema>(testCase.Schema), factory);
        auto rowBuffer = New<TRowBuffer>();
        return TOwningValue(EvaluateQuery(*context, testCase.InputRow.Elements(), rowBuffer));
    };

    if (!testCase.ExpectedError.empty()) {
        EXPECT_THROW_WITH_SUBSTRING(evaluate(), testCase.ExpectedError);
        return;
    }

    auto result = evaluate();
    EXPECT_EQ(static_cast<TValue>(testCase.ExpectedValue), static_cast<TValue>(result));
}

INSTANTIATE_TEST_SUITE_P(
    ScalarSemantics,
    TPortableQueryEvaluatorSemanticsTest,
    ::testing::Combine(
        ::testing::ValuesIn(GetV1ScalarExpressionCases()),
        ::testing::Values(EContextFactory::Typed, EContextFactory::Parsed)),
    [] (const ::testing::TestParamInfo<TPortableQueryEvaluatorSemanticsTest::ParamType>& info) {
        return std::get<0>(info.param).Name + ToString(std::get<1>(info.param));
    });

////////////////////////////////////////////////////////////////////////////////

class TPortableQueryEvaluatorTest
    : public ::testing::TestWithParam<EContextFactory>
{ };

TEST_P(TPortableQueryEvaluatorTest, VariadicFunctionWithDifferentArgumentCounts)
{
    for (int argumentCount : {15, 16, 32}) {
        SCOPED_TRACE(argumentCount);

        std::vector<std::string> columnNames;
        std::vector<TColumnSchema> columns;
        std::vector<TValue> inputValues;
        for (int index = 0; index < argumentCount; ++index) {
            auto name = Format("u%v", index);
            columnNames.push_back(name);
            columns.push_back(TColumnSchema(name, EValueType::Uint64));
            inputValues.push_back(MakeUnversionedUint64Value(index + 1));
        }

        auto context = CreateContext(
            Format("farm_hash(%v)", JoinToString(columnNames)),
            New<TTableSchema>(std::move(columns)),
            GetParam());
        auto rowBuffer = New<TRowBuffer>();

        for (int iteration = 0; iteration < 2; ++iteration) {
            SCOPED_TRACE(iteration);
            auto expected = MakeUnversionedUint64Value(GetFarmFingerprint(TRange<TValue>(inputValues)));
            EXPECT_EQ(expected, EvaluateQuery(*context, inputValues, rowBuffer));

            for (auto& value : inputValues) {
                ++value.Data.Uint64;
            }
        }
    }
}

TEST_P(TPortableQueryEvaluatorTest, ContextOutlivesPreparationInputs)
{
    TQueryEvaluationContextPtr context;
    {
        std::string source = "value % 3";
        auto parsedSource = ParseSource(source, EParseMode::Expression);
        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("value", EValueType::Int64),
        });
        if (GetParam() == EContextFactory::Typed) {
            auto expression = PrepareExpression(*parsedSource, *schema);
            context = CreateQueryEvaluationContext(expression, schema);
            EXPECT_EQ(expression, context->Expression);
        } else {
            context = CreateQueryEvaluationContext(*parsedSource, schema);
        }
        source.assign(source.size(), 'x');
        parsedSource->Source.assign(parsedSource->Source.size(), 'x');
    }

    ASSERT_TRUE(context->Expression);
    auto rowBuffer = New<TRowBuffer>();
    std::array row{MakeUnversionedInt64Value(8)};
    EXPECT_EQ(MakeUnversionedInt64Value(2), EvaluateQuery(*context, row, rowBuffer));
}

TEST_P(TPortableQueryEvaluatorTest, StringResultsOutliveContextAndInput)
{
    const std::string expected = "a string long enough to require separately allocated storage";
    for (bool useReference : {false, true}) {
        SCOPED_TRACE(useReference);
        auto rowBuffer = New<TRowBuffer>();
        auto result = MakeUnversionedNullValue();
        {
            auto schema = New<TTableSchema>(std::vector{
                TColumnSchema("value", EValueType::String),
            });
            auto context = CreateContext(useReference ? "value" : '"' + expected + '"', schema, GetParam());
            std::string input = expected;
            std::array row{MakeUnversionedStringValue(input)};
            result = EvaluateQuery(*context, row, rowBuffer);

            EXPECT_NE(input.data(), result.Data.String);
            input.assign(input.size(), 'x');
        }

        EXPECT_EQ(EValueType::String, result.Type);
        EXPECT_EQ(expected, result.AsString());
    }
}

TEST_P(TPortableQueryEvaluatorTest, RejectsUnsupportedFunctionBeforeEvaluation)
{
    const std::string source = "farm_hash(is_null(value))";
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("value", EValueType::Int64),
    });
    try {
        CreateContext(source, schema, GetParam());
        ADD_FAILURE() << "Expected context creation to reject an unsupported function";
    } catch (const TErrorException& ex) {
        const auto& error = ex.Error();
        auto compilationError = error.FindMatching([] (const TError& innerError) {
            return innerError.Attributes().Contains("operation");
        });
        ASSERT_TRUE(compilationError);
        EXPECT_EQ("is_null", compilationError->Attributes().Get<std::string>("operation"));
        EXPECT_EQ("function", compilationError->Attributes().Get<std::string>("expression_kind"));
        EXPECT_EQ("root.arguments[0]", compilationError->Attributes().Get<std::string>("expression_path"));
        EXPECT_EQ(
            std::vector<EValueType>({EValueType::Int64}),
            compilationError->Attributes().Get<std::vector<EValueType>>("argument_types"));
        EXPECT_EQ(EValueType::Boolean, compilationError->Attributes().Get<EValueType>("result_type"));

        auto sourceError = error.FindMatching([] (const TError& innerError) {
            return innerError.Attributes().Contains("source");
        });
        if (GetParam() == EContextFactory::Parsed) {
            ASSERT_TRUE(sourceError);
            EXPECT_EQ(source, sourceError->Attributes().Get<std::string>("source"));
        } else {
            EXPECT_FALSE(sourceError);
        }
    }
}

TEST_P(TPortableQueryEvaluatorTest, RepeatedEvaluationRecoversAfterError)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("lhs", EValueType::Int64),
        TColumnSchema("rhs", EValueType::Int64),
    });
    auto context = CreateContext("lhs % rhs", schema, GetParam());
    auto rowBuffer = New<TRowBuffer>();
    std::array row{MakeUnversionedInt64Value(17), MakeUnversionedInt64Value(3)};
    EXPECT_EQ(MakeUnversionedInt64Value(2), EvaluateQuery(*context, row, rowBuffer));

    row[1] = MakeUnversionedInt64Value(0);
    EXPECT_THROW_WITH_SUBSTRING(EvaluateQuery(*context, row, rowBuffer), "Division by zero");

    row[0] = MakeUnversionedInt64Value(-17);
    row[1] = MakeUnversionedInt64Value(5);
    EXPECT_EQ(MakeUnversionedInt64Value(-2), EvaluateQuery(*context, row, rowBuffer));

    row[0] = MakeUnversionedNullValue();
    EXPECT_EQ(MakeUnversionedNullValue(), EvaluateQuery(*context, row, rowBuffer));
}

TEST_P(TPortableQueryEvaluatorTest, ConcurrentCallsUseIndependentStorageFromFirstCall)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("value", EValueType::Int64),
    });
    auto context = CreateContext("value % 97", schema, GetParam());
    constexpr int ThreadCount = 4;
    constexpr int IterationCount = 1'000;
    std::barrier synchronization(ThreadCount);
    std::atomic<bool> successful = true;
    std::vector<std::thread> threads;
    for (int threadIndex = 0; threadIndex < ThreadCount; ++threadIndex) {
        threads.emplace_back([&, threadIndex] {
            auto rowBuffer = New<TRowBuffer>();
            for (int iteration = 0; iteration < IterationCount; ++iteration) {
                i64 value = threadIndex * IterationCount + iteration;
                std::array row{MakeUnversionedInt64Value(value)};
                synchronization.arrive_and_wait();
                try {
                    if (EvaluateQuery(*context, row, rowBuffer) != MakeUnversionedInt64Value(value % 97)) {
                        successful = false;
                    }
                } catch (...) {
                    successful = false;
                }
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_TRUE(successful);
}

INSTANTIATE_TEST_SUITE_P(
    ContextFactories,
    TPortableQueryEvaluatorTest,
    ::testing::Values(EContextFactory::Typed, EContextFactory::Parsed),
    [] (const ::testing::TestParamInfo<EContextFactory>& info) {
        return ToString(info.param);
    });

////////////////////////////////////////////////////////////////////////////////

TEST(TPortableQueryEvaluatorSignatureTest, TypedFactoryDoesNotPrepareUnsupportedSignatureAgain)
{
    auto schema = New<TTableSchema>();
    auto expression = New<TFunctionExpression>(
        EValueType::Int64,
        "farm_hash",
        std::vector<TConstExpressionPtr>{
            New<TLiteralExpression>(EValueType::String, TOwningValue(MakeUnversionedStringValue("value"))),
        });
    try {
        CreateQueryEvaluationContext(expression, schema);
        ADD_FAILURE() << "Expected context creation to reject an unsupported signature";
    } catch (const TErrorException& ex) {
        const auto& attributes = ex.Error().Attributes();
        EXPECT_EQ("farm_hash", attributes.Get<std::string>("operation"));
        EXPECT_EQ(EValueType::Int64, attributes.Get<EValueType>("result_type"));
        EXPECT_EQ(
            std::vector<EValueType>({EValueType::String}),
            attributes.Get<std::vector<EValueType>>("argument_types"));
        EXPECT_FALSE(attributes.Contains("source"));
    }
}

TEST(TPortableExpressionImageTest, InstancesOutliveImageAndKeepOutputMetadata)
{
    TCGExpressionInstance first;
    TCGExpressionInstance second;
    {
        auto expression = New<TLiteralExpression>(
            EValueType::String,
            TOwningValue(MakeUnversionedStringValue("owned by the program")));
        auto image = CreateExpressionImage(CompileExpression(expression, /*schema*/ {}, /*registry*/ {}));
        first = image.Instantiate();
        second = image.Instantiate();
    }

    auto rowBuffer = New<TRowBuffer>();
    auto result = MakeUnversionedNullValue(/*id*/ 7, EValueFlags::Aggregate);
    first.Run(
        /*literalValues*/ {},
        /*opaqueData*/ {},
        /*opaqueDataSizes*/ {},
        &result,
        /*inputRow*/ {},
        rowBuffer);
    EXPECT_EQ("owned by the program", result.AsString());
    EXPECT_EQ(7, result.Id);
    EXPECT_EQ(EValueFlags::Aggregate, result.Flags);

    first = {};
    second.Run(
        /*literalValues*/ {},
        /*opaqueData*/ {},
        /*opaqueDataSizes*/ {},
        &result,
        /*inputRow*/ {},
        rowBuffer);
    second = {};
    EXPECT_EQ("owned by the program", result.AsString());
}

TEST(TPortableExpressionImageTest, UsesCallingPropagatingStorageInsteadOfCreationStorage)
{
    struct TExecutionMarker
    {
        int Value = 0;
    };

    TCGExpressionImage image;
    {
        NConcurrency::TPropagatingValueGuard<TExecutionMarker> creationGuard({.Value = 17});
        TExpressionRegistryBuilder builder;
        builder.RegisterFunction(
            "read_context",
            {
                .ResultType = EValueType::Int64,
                .Implementation = {
                    .Callback = BIND_NO_PROPAGATE([] (
                        TValue* result,
                        TRange<TValue> /*arguments*/,
                        const TRowBufferPtr& /*rowBuffer*/) {
                        *result = MakeUnversionedInt64Value(
                            NConcurrency::GetCurrentPropagatingStorage().GetOrCrash<TExecutionMarker>().Value);
                    }),
                },
            });
        auto expression = New<TFunctionExpression>(
            EValueType::Int64,
            "read_context",
            /*arguments*/ std::vector<TConstExpressionPtr>{});
        image = CreateExpressionImage(CompileExpression(expression, /*schema*/ {}, builder.Build()));
    }

    NConcurrency::TPropagatingValueGuard<TExecutionMarker> executionGuard({.Value = 42});
    auto instance = image.Instantiate();
    auto result = MakeUnversionedNullValue();
    auto rowBuffer = New<TRowBuffer>();
    instance.Run(
        /*literalValues*/ {},
        /*opaqueData*/ {},
        /*opaqueDataSizes*/ {},
        &result,
        /*inputRow*/ {},
        rowBuffer);
    EXPECT_EQ(MakeUnversionedInt64Value(42), result);
    EXPECT_EQ(42, NConcurrency::GetCurrentPropagatingStorage().GetOrCrash<TExecutionMarker>().Value);
}

TEST(TPortableExpressionImageTest, RecursiveCallsToSameInstanceKeepOuterArguments)
{
    TCGExpressionInstance instance;
    int callCount = 0;
    {
        TExpressionRegistryBuilder builder;
        builder.RegisterFunction(
            "reenter",
            {
                .ArgumentTypes = {EValueType::Int64},
                .ResultType = EValueType::Int64,
                .Implementation = {
                    .Callback = BIND([&] (
                        TValue* result,
                        TRange<TValue> arguments,
                        const TRowBufferPtr& /*rowBuffer*/) {
                        ++callCount;
                        i64 value = arguments[0].Data.Int64;
                        if (value == 0) {
                            *result = MakeUnversionedInt64Value(0);
                            return;
                        }

                        std::array nestedRow{MakeUnversionedInt64Value(value - 1)};
                        auto nestedResult = MakeUnversionedNullValue();
                        auto nestedBuffer = New<TRowBuffer>();
                        instance.Run(
                            /*literalValues*/ {},
                            /*opaqueData*/ {},
                            /*opaqueDataSizes*/ {},
                            &nestedResult,
                            nestedRow,
                            nestedBuffer);

                        EXPECT_EQ(value, arguments[0].Data.Int64);
                        *result = MakeUnversionedInt64Value(arguments[0].Data.Int64 + nestedResult.Data.Int64);
                    }),
                },
            });
        TTableSchema schema({TColumnSchema("value", EValueType::Int64)});
        auto expression = New<TFunctionExpression>(
            EValueType::Int64,
            "reenter",
            std::vector<TConstExpressionPtr>{
                New<TReferenceExpression>(schema.Columns()[0].LogicalType(), "value"),
            });
        auto image = CreateExpressionImage(CompileExpression(expression, schema, builder.Build()));
        instance = image.Instantiate();
    }

    std::array row{MakeUnversionedInt64Value(4)};
    auto result = MakeUnversionedNullValue();
    auto rowBuffer = New<TRowBuffer>();
    instance.Run(
        /*literalValues*/ {},
        /*opaqueData*/ {},
        /*opaqueDataSizes*/ {},
        &result,
        row,
        rowBuffer);
    EXPECT_EQ(MakeUnversionedInt64Value(10), result);
    EXPECT_EQ(5, callCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient::NPortable::NTest

#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_common.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <array>
#include <atomic>
#include <barrier>
#include <string>
#include <thread>
#include <vector>

namespace NYT::NQueryClient::NPortable::NTest {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TPortableColumnEvaluatorTest
    : public ::testing::TestWithParam<bool>
{
protected:
    bool GetPreserveColumnIds() const
    {
        return GetParam();
    }
};

TEST_P(TPortableColumnEvaluatorTest, EvaluateKeyAndUnversionedKeys)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("remainder", EValueType::Int64).SetExpression("lhs % rhs"),
        TColumnSchema("lhs", EValueType::Int64),
        TColumnSchema("hash", EValueType::Uint64).SetExpression("farm_hash(rhs, lhs, rhs)"),
        TColumnSchema("rhs", EValueType::Int64),
    });
    auto evaluator = TColumnEvaluator::Create(schema, GetBuiltinTypeInferrers(), GetBuiltinFunctionProfilers());
    auto rowBuffer = New<TRowBuffer>();
    std::array values{
        MakeUnversionedNullValue(/*id*/ 10),
        MakeUnversionedInt64Value(17, /*id*/ 11, EValueFlags::Aggregate),
        MakeUnversionedNullValue(/*id*/ 12),
        MakeUnversionedInt64Value(5, /*id*/ 13),
    };
    auto row = rowBuffer->CaptureRow(values);
    std::array hashArguments{values[3], values[1], values[3]};
    auto expectedHash = MakeUnversionedUint64Value(GetFarmFingerprint(hashArguments));

    evaluator->EvaluateKey(row, rowBuffer, /*index*/ 2, GetPreserveColumnIds());
    EXPECT_EQ(values[0], row[0]);
    EXPECT_EQ(10, row[0].Id);
    EXPECT_EQ(expectedHash, row[2]);
    EXPECT_EQ(GetPreserveColumnIds() ? 12 : 2, row[2].Id);

    evaluator->EvaluateKeys(row, rowBuffer, GetPreserveColumnIds());
    EXPECT_EQ(MakeUnversionedInt64Value(2), row[0]);
    EXPECT_EQ(GetPreserveColumnIds() ? 10 : 0, row[0].Id);
    EXPECT_EQ(expectedHash, row[2]);
    EXPECT_EQ(GetPreserveColumnIds() ? 12 : 2, row[2].Id);
    for (int index : {1, 3}) {
        EXPECT_EQ(values[index], row[index]);
        EXPECT_EQ(values[index].Id, row[index].Id);
        EXPECT_EQ(values[index].Flags, row[index].Flags);
    }
}

TEST_P(TPortableColumnEvaluatorTest, EvaluateVersionedKeys)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("remainder", EValueType::Int64, ESortOrder::Ascending).SetExpression("lhs % rhs"),
        TColumnSchema("lhs", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("rhs", EValueType::Int64, ESortOrder::Ascending),
        TColumnSchema("value", EValueType::Int64),
    });
    auto evaluator = TColumnEvaluator::Create(schema, GetBuiltinTypeInferrers(), GetBuiltinFunctionProfilers());
    auto rowBuffer = New<TRowBuffer>();
    auto row = rowBuffer->AllocateVersioned(
        /*keyCount*/ 3,
        /*valueCount*/ 1,
        /*writeTimestampCount*/ 1,
        /*deleteTimestampCount*/ 1);
    row.Keys()[0] = MakeUnversionedNullValue(/*id*/ 10);
    row.Keys()[1] = MakeUnversionedInt64Value(17, /*id*/ 11);
    row.Keys()[2] = MakeUnversionedInt64Value(5, /*id*/ 12);
    auto value = MakeVersionedInt64Value(42, TTimestamp(123), /*id*/ 3);
    row.Values()[0] = value;
    row.WriteTimestamps()[0] = TTimestamp(123);
    row.DeleteTimestamps()[0] = TTimestamp(100);

    evaluator->EvaluateKeys(row, rowBuffer, GetPreserveColumnIds());

    EXPECT_EQ(MakeUnversionedInt64Value(2), row.Keys()[0]);
    EXPECT_EQ(GetPreserveColumnIds() ? 10 : 0, row.Keys()[0].Id);
    EXPECT_EQ(MakeUnversionedInt64Value(17), row.Keys()[1]);
    EXPECT_EQ(11, row.Keys()[1].Id);
    EXPECT_EQ(MakeUnversionedInt64Value(5), row.Keys()[2]);
    EXPECT_EQ(12, row.Keys()[2].Id);
    EXPECT_EQ(value, row.Values()[0]);
    EXPECT_EQ(3, row.Values()[0].Id);
    EXPECT_EQ(TTimestamp(123), row.Values()[0].Timestamp);
    EXPECT_EQ(TTimestamp(123), row.WriteTimestamps()[0]);
    EXPECT_EQ(TTimestamp(100), row.DeleteTimestamps()[0]);
}

TEST_P(TPortableColumnEvaluatorTest, SchemaWithoutExpressions)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("value", EValueType::Int64),
    });
    auto evaluator = TColumnEvaluator::Create(schema, GetBuiltinTypeInferrers(), GetBuiltinFunctionProfilers());
    auto rowBuffer = New<TRowBuffer>();
    std::array values{MakeUnversionedInt64Value(42, /*id*/ 10, EValueFlags::Aggregate)};
    auto row = rowBuffer->CaptureRow(values);

    evaluator->EvaluateKeys(row, rowBuffer, GetPreserveColumnIds());

    EXPECT_EQ(values[0], row[0]);
    EXPECT_EQ(values[0].Id, row[0].Id);
    EXPECT_EQ(values[0].Flags, row[0].Flags);
    EXPECT_FALSE(evaluator->GetExpression(0));
    EXPECT_TRUE(evaluator->GetReferenceIds(0).empty());
    EXPECT_FALSE(evaluator->IsAggregate(0));
}

INSTANTIATE_TEST_SUITE_P(
    ColumnIds,
    TPortableColumnEvaluatorTest,
    ::testing::Bool());

////////////////////////////////////////////////////////////////////////////////

TEST(TPortableColumnEvaluatorPreparationTest, ExpressionsAndReferencesMatchPreparation)
{
    const std::string source = "farm_hash(rhs, uint64(lhs), rhs)";
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("hash", EValueType::Uint64).SetExpression(source),
        TColumnSchema("lhs", EValueType::Uint64),
        TColumnSchema("unused", EValueType::String),
        TColumnSchema("rhs", EValueType::Int64),
    });
    THashSet<std::string> references;
    auto expression = PrepareExpression(source, *schema, GetBuiltinTypeInferrers(), &references);
    auto evaluator = TColumnEvaluator::Create(schema, GetBuiltinTypeInferrers(), GetBuiltinFunctionProfilers());

    ASSERT_TRUE(evaluator->GetExpression(0));
    EXPECT_TRUE(Compare(expression, evaluator->GetExpression(0)));
    EXPECT_EQ(THashSet<std::string>({"lhs", "rhs"}), references);
    EXPECT_EQ(std::vector<int>({1, 3}), evaluator->GetReferenceIds(0));
    auto* function = evaluator->GetExpression(0)->As<TFunctionExpression>();
    ASSERT_TRUE(function);
    ASSERT_EQ(3u, function->Arguments.size());
    EXPECT_TRUE(function->Arguments[1]->As<TReferenceExpression>());
    for (int index = 0; index < schema->GetColumnCount(); ++index) {
        EXPECT_FALSE(evaluator->IsAggregate(index));
        if (index != 0) {
            EXPECT_FALSE(evaluator->GetExpression(index));
            EXPECT_TRUE(evaluator->GetReferenceIds(index).empty());
        }
    }
}

TEST(TPortableColumnEvaluatorPreparationTest, UsesProvidedTypeInferrers)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("hash", EValueType::Uint64).SetExpression("farm_hash(value)"),
        TColumnSchema("value", EValueType::Int64),
    });
    auto typeInferrers = New<TTypeInferrerMap>();
    EXPECT_THROW_WITH_SUBSTRING(
        TColumnEvaluator::Create(schema, typeInferrers, GetBuiltinFunctionProfilers()),
        "Undefined function");

    typeInferrers->emplace("farm_hash", GetBuiltinTypeInferrers()->GetFunction("farm_hash"));
    auto evaluator = TColumnEvaluator::Create(schema, typeInferrers, GetBuiltinFunctionProfilers());
    auto rowBuffer = New<TRowBuffer>();
    std::array values{MakeUnversionedNullValue(), MakeUnversionedInt64Value(17)};
    auto row = rowBuffer->CaptureRow(values);
    evaluator->EvaluateKeys(row, rowBuffer, /*preserveColumnsIds*/ false);

    std::array hashArguments{values[1]};
    EXPECT_EQ(MakeUnversionedUint64Value(GetFarmFingerprint(hashArguments)), row[0]);
}

TEST(TPortableColumnEvaluatorPreparationTest, RejectsAggregatesWithAndWithoutExpression)
{
    for (bool hasExpression : {false, true}) {
        SCOPED_TRACE(hasExpression);
        auto column = TColumnSchema("value", EValueType::Int64).SetAggregate("sum");
        if (hasExpression) {
            column.SetExpression("1");
        }
        auto schema = New<TTableSchema>(std::vector{column});
        EXPECT_THROW_WITH_SUBSTRING(
            TColumnEvaluator::Create(schema, GetBuiltinTypeInferrers(), GetBuiltinFunctionProfilers()),
            "does not support aggregate column");
    }
}

TEST(TPortableColumnEvaluatorPreparationTest, UnsupportedExpressionPreservesErrorContext)
{
    const std::string source = "farm_hash(is_null(value))";
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("hash", EValueType::Uint64).SetExpression(source),
        TColumnSchema("value", EValueType::Int64),
    });
    try {
        TColumnEvaluator::Create(schema, GetBuiltinTypeInferrers(), GetBuiltinFunctionProfilers());
        ADD_FAILURE() << "Expected column creation to reject an unsupported function";
    } catch (const TErrorException& ex) {
        const auto& error = ex.Error();
        EXPECT_EQ("hash", error.Attributes().Get<std::string>("column"));
        EXPECT_EQ(source, error.Attributes().Get<std::string>("expression"));
        auto compilationError = error.FindMatching([] (const TError& innerError) {
            return innerError.Attributes().Contains("operation");
        });
        ASSERT_TRUE(compilationError);
        EXPECT_EQ("is_null", compilationError->Attributes().Get<std::string>("operation"));
    }
}

TEST(TPortableColumnEvaluatorPreparationTest, BuiltinProfilersAreSharedAndEmpty)
{
    auto profilers = GetBuiltinFunctionProfilers();
    ASSERT_TRUE(profilers);
    EXPECT_TRUE(profilers->empty());
    EXPECT_EQ(profilers.Get(), GetBuiltinFunctionProfilers().Get());
}

TEST(TPortableColumnEvaluatorPreparationTest, RejectsNonemptyProfilers)
{
    auto profilers = New<TFunctionProfilerMap>();
    profilers->emplace("unused", nullptr);
    EXPECT_THROW_WITH_SUBSTRING(
        TColumnEvaluator::Create(New<TTableSchema>(), GetBuiltinTypeInferrers(), profilers),
        "does not support custom function profilers");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPortableColumnEvaluatorExecutionTest, OutlivesSchemaAndSource)
{
    TColumnEvaluatorPtr evaluator;
    {
        std::string source = "value % 3";
        auto schema = New<TTableSchema>(std::vector{
            TColumnSchema("remainder", EValueType::Int64).SetExpression(source),
            TColumnSchema("value", EValueType::Int64),
        });
        evaluator = TColumnEvaluator::Create(schema, GetBuiltinTypeInferrers(), GetBuiltinFunctionProfilers());
        source.assign(source.size(), 'x');
    }

    ASSERT_TRUE(evaluator->GetExpression(0));
    EXPECT_EQ(std::vector<int>({1}), evaluator->GetReferenceIds(0));
    auto rowBuffer = New<TRowBuffer>();
    std::array values{MakeUnversionedNullValue(), MakeUnversionedInt64Value(8)};
    auto row = rowBuffer->CaptureRow(values);
    evaluator->EvaluateKeys(row, rowBuffer, /*preserveColumnsIds*/ false);
    EXPECT_EQ(MakeUnversionedInt64Value(2), row[0]);
}

TEST(TPortableColumnEvaluatorExecutionTest, StringResultOutlivesEvaluatorAndInput)
{
    const std::string expected = "a string long enough to require separately allocated storage";
    for (bool useReference : {false, true}) {
        SCOPED_TRACE(useReference);
        auto rowBuffer = New<TRowBuffer>();
        auto result = MakeUnversionedNullValue();
        {
            TColumnEvaluatorPtr evaluator;
            {
                std::string source = useReference ? "value" : '"' + expected + '"';
                auto schema = New<TTableSchema>(std::vector{
                    TColumnSchema("result", EValueType::String).SetExpression(source),
                    TColumnSchema("value", EValueType::String),
                });
                evaluator = TColumnEvaluator::Create(schema, GetBuiltinTypeInferrers(), GetBuiltinFunctionProfilers());
                source.assign(source.size(), 'x');
            }
            std::string input = expected;
            std::array values{MakeUnversionedNullValue(), MakeUnversionedStringValue(input)};
            auto row = rowBuffer->CaptureRow(values, /*captureValues*/ false);
            evaluator->EvaluateKeys(row, rowBuffer, /*preserveColumnsIds*/ false);
            result = row[0];

            ASSERT_EQ(EValueType::String, result.Type);
            EXPECT_NE(input.data(), result.Data.String);
            input.assign(input.size(), 'x');
        }
        EXPECT_EQ(expected, result.AsString());
    }
}

TEST(TPortableColumnEvaluatorExecutionTest, SuccessfulEvaluationAfterRowError)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("remainder", EValueType::Int64).SetExpression("lhs % rhs"),
        TColumnSchema("lhs", EValueType::Int64),
        TColumnSchema("rhs", EValueType::Int64),
    });
    auto evaluator = TColumnEvaluator::Create(schema, GetBuiltinTypeInferrers(), GetBuiltinFunctionProfilers());
    auto rowBuffer = New<TRowBuffer>();
    std::array values{
        MakeUnversionedNullValue(),
        MakeUnversionedInt64Value(17),
        MakeUnversionedInt64Value(0),
    };
    auto row = rowBuffer->CaptureRow(values);
    EXPECT_THROW_WITH_SUBSTRING(
        evaluator->EvaluateKeys(row, rowBuffer, /*preserveColumnsIds*/ false),
        "Division by zero");

    row[2] = MakeUnversionedInt64Value(5);
    evaluator->EvaluateKeys(row, rowBuffer, /*preserveColumnsIds*/ false);
    EXPECT_EQ(MakeUnversionedInt64Value(2), row[0]);
}

TEST(TPortableColumnEvaluatorExecutionTest, ConcurrentCallsUseIndependentStorageFromFirstCall)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("remainder", EValueType::Int64).SetExpression("value % 97"),
        TColumnSchema("value", EValueType::Int64),
    });
    auto evaluator = TColumnEvaluator::Create(schema, GetBuiltinTypeInferrers(), GetBuiltinFunctionProfilers());
    constexpr int ThreadCount = 4;
    constexpr int IterationCount = 1'000;
    std::barrier synchronization(ThreadCount);
    std::atomic<bool> successful = true;
    std::vector<std::thread> threads;
    for (int threadIndex = 0; threadIndex < ThreadCount; ++threadIndex) {
        threads.emplace_back([&, threadIndex] {
            auto rowBuffer = New<TRowBuffer>();
            auto row = rowBuffer->AllocateUnversioned(/*valueCount*/ 2);
            for (int iteration = 0; iteration < IterationCount; ++iteration) {
                i64 value = threadIndex * IterationCount + iteration;
                row[0] = MakeUnversionedNullValue();
                row[1] = MakeUnversionedInt64Value(value);
                synchronization.arrive_and_wait();
                try {
                    evaluator->EvaluateKeys(row, rowBuffer, /*preserveColumnsIds*/ false);
                    if (row[0] != MakeUnversionedInt64Value(value % 97)) {
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient::NPortable::NTest

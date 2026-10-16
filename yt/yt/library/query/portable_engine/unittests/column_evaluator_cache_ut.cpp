#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/library/query/base/functions.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <array>
#include <string>
#include <vector>

namespace NYT::NQueryClient::NPortable::NTest {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr CreateSchema(const std::string& expression)
{
    return New<TTableSchema>(std::vector{
        TColumnSchema("x", EValueType::Int64),
        TColumnSchema("y", EValueType::Int64),
        TColumnSchema("result", EValueType::Int64).SetExpression(expression),
    });
}

TUnversionedValue Evaluate(
    const TColumnEvaluatorPtr& evaluator,
    i64 first,
    i64 second)
{
    auto rowBuffer = New<TRowBuffer>();
    auto row = rowBuffer->AllocateUnversioned(/*valueCount*/ 3);
    row[0] = MakeUnversionedInt64Value(first, /*id*/ 0);
    row[1] = MakeUnversionedInt64Value(second, /*id*/ 1);
    row[2] = MakeUnversionedNullValue(/*id*/ 2);

    evaluator->EvaluateKeys(row, rowBuffer, /*preserveColumnsIds*/ false);
    return row[2];
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPortableColumnEvaluatorCacheTest, CreatesPortableEvaluatorWithDefaultArguments)
{
    auto cache = CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>());
    auto evaluator = cache->Find(CreateSchema("x % y"));

    EXPECT_EQ(
        MakeUnversionedInt64Value(2, /*id*/ 2),
        Evaluate(evaluator, /*first*/ 17, /*second*/ 5));
}

TEST(TPortableColumnEvaluatorCacheTest, ReusesEqualSchemas)
{
    auto cache = CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>());
    auto firstSchema = CreateSchema("x % y");
    auto secondSchema = CreateSchema("x % y");

    EXPECT_NE(firstSchema, secondSchema);
    EXPECT_EQ(cache->Find(firstSchema), cache->Find(secondSchema));
}

TEST(TPortableColumnEvaluatorCacheTest, DistinguishesColumnOrderAndExpressions)
{
    auto cache = CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>());
    auto original = cache->Find(CreateSchema("x % y"));
    auto reordered = cache->Find(New<TTableSchema>(std::vector{
        TColumnSchema("y", EValueType::Int64),
        TColumnSchema("x", EValueType::Int64),
        TColumnSchema("result", EValueType::Int64).SetExpression("x % y"),
    }));
    auto changedExpression = cache->Find(CreateSchema("x % 4"));

    EXPECT_NE(original, reordered);
    EXPECT_NE(original, changedExpression);
    EXPECT_EQ(
        MakeUnversionedInt64Value(2, /*id*/ 2),
        Evaluate(original, /*first*/ 17, /*second*/ 5));
    EXPECT_EQ(
        MakeUnversionedInt64Value(5, /*id*/ 2),
        Evaluate(reordered, /*first*/ 17, /*second*/ 5));
    EXPECT_EQ(
        MakeUnversionedInt64Value(1, /*id*/ 2),
        Evaluate(changedExpression, /*first*/ 17, /*second*/ 5));
}

TEST(TPortableColumnEvaluatorCacheTest, OwnsSchemaKey)
{
    auto cache = CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>());
    TColumnEvaluatorPtr original;
    TColumnEvaluatorPtr changed;
    {
        auto schema = CreateSchema("x % y");
        original = cache->Find(schema);
        *schema = *CreateSchema("y % x");
        changed = cache->Find(schema);
    }

    EXPECT_NE(original, changed);
    EXPECT_EQ(original, cache->Find(CreateSchema("x % y")));
    EXPECT_EQ(changed, cache->Find(CreateSchema("y % x")));
    EXPECT_EQ(
        MakeUnversionedInt64Value(2, /*id*/ 2),
        Evaluate(original, /*first*/ 17, /*second*/ 5));
    EXPECT_EQ(
        MakeUnversionedInt64Value(5, /*id*/ 2),
        Evaluate(changed, /*first*/ 17, /*second*/ 5));
}

TEST(TPortableColumnEvaluatorCacheTest, EvaluatorOutlivesCacheAndSchema)
{
    TColumnEvaluatorPtr evaluator;
    {
        auto cache = CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>());
        auto schema = CreateSchema("x % y");
        evaluator = cache->Find(schema);
    }

    EXPECT_EQ(
        MakeUnversionedInt64Value(2, /*id*/ 2),
        Evaluate(evaluator, /*first*/ 17, /*second*/ 5));
}

TEST(TPortableColumnEvaluatorCacheTest, DoesNotCachePreparationErrors)
{
    auto cache = CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>());
    auto validSchema = CreateSchema("x % y");
    auto evaluator = cache->Find(validSchema);
    auto invalidSchema = CreateSchema("x + y");
    ASSERT_EQ(1, cache->GetSize());

    for (int attempt = 0; attempt < 2; ++attempt) {
        SCOPED_TRACE(attempt);
        EXPECT_THROW_WITH_SUBSTRING(cache->Find(invalidSchema), "Unsupported portable");
        EXPECT_EQ(1, cache->GetSize());
        EXPECT_EQ(evaluator, cache->Find(validSchema));
        EXPECT_EQ(
            MakeUnversionedInt64Value(2, /*id*/ 2),
            Evaluate(evaluator, /*first*/ 17, /*second*/ 5));
    }
}

TEST(TPortableColumnEvaluatorCacheTest, UsesProvidedTypeInferrers)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("x", EValueType::Int64),
        TColumnSchema("hash", EValueType::Uint64).SetExpression("farm_hash(x)"),
    });
    auto cache = CreateColumnEvaluatorCache(
        New<TColumnEvaluatorCacheConfig>(),
        New<TTypeInferrerMap>());

    EXPECT_THROW_WITH_SUBSTRING(cache->Find(schema), "Undefined function");

    auto defaultCache = CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>());
    auto evaluator = defaultCache->Find(schema);
    auto rowBuffer = New<TRowBuffer>();
    auto row = rowBuffer->AllocateUnversioned(/*valueCount*/ 2);
    row[0] = MakeUnversionedInt64Value(17, /*id*/ 0);
    row[1] = MakeUnversionedNullValue(/*id*/ 1);
    auto expected = MakeUnversionedUint64Value(GetFarmFingerprint(std::array{row[0]}), /*id*/ 1);

    evaluator->EvaluateKeys(row, rowBuffer, /*preserveColumnsIds*/ false);
    EXPECT_EQ(expected, row[1]);
}

TEST(TPortableColumnEvaluatorCacheTest, RejectsNonemptyProfilersAtCreation)
{
    auto profilers = New<TFunctionProfilerMap>();
    profilers->emplace("unused", nullptr);

    EXPECT_THROW_WITH_SUBSTRING(
        CreateColumnEvaluatorCache(
            New<TColumnEvaluatorCacheConfig>(),
            GetBuiltinTypeInferrers(),
            profilers),
        "does not support custom function profilers");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient::NPortable::NTest

#include <yt/yt/orm/library/query/heavy/expression_evaluator.h>
#include <yt/yt/orm/library/query/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/fluent.h>

#include <thread>

namespace NYT::NOrm::NQuery::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT;
using namespace NYT::NYTree;
using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

using NTableClient::EValueType;

////////////////////////////////////////////////////////////////////////////////

TEST(TExpressionEvaluatorTest, OrmSimple)
{
    auto rowBuffer = New<NTableClient::TRowBuffer>();
    auto evaluator = CreateOrmExpressionEvaluator(
        NQueryClient::ParseSource("double([/meta/x]) + 5 * double([/meta/y])", NQueryClient::EParseMode::Expression),
        {"/meta"});
    auto value = evaluator->Evaluate(
        BuildYsonStringFluently().BeginMap()
            .Item("x").Value(1.0)
            .Item("y").Value(100.0)
        .EndMap(),
        rowBuffer)
        .ValueOrThrow();
    EXPECT_EQ(value.Type, EValueType::Double);
    EXPECT_EQ(value.Data.Double, 501);
}

TEST(TExpressionEvaluatorTest, OrmManyArguments)
{
    auto rowBuffer = New<NTableClient::TRowBuffer>();
    auto evaluator = CreateOrmExpressionEvaluator(
        NQueryClient::ParseSource("int64([/meta/x]) + int64([/lambda/y/z]) + 16 + int64([/theta])", NQueryClient::EParseMode::Expression),
        {"/meta", "/lambda/y", "/theta"});
    auto value = evaluator->Evaluate({
            BuildYsonStringFluently()
                .BeginMap()
                    .Item("x").Value(1)
                .EndMap(),
            BuildYsonStringFluently()
                .BeginMap()
                    .Item("z").Value(10)
                    .Item("y").Value(100)
                .EndMap(),
            BuildYsonStringFluently()
                .Value(1000)
        },
        rowBuffer)
        .ValueOrThrow();
    EXPECT_EQ(value.Type, EValueType::Int64);
    EXPECT_EQ(value.Data.Int64, 1 + 10 + 16 + 1000);
}

TEST(TExpressionEvaluatorTest, OrmTypedAttributePaths)
{
    for (auto type : {EValueType::Max, EValueType::Min, EValueType::Null, EValueType::Composite}) {
        EXPECT_THROW_WITH_SUBSTRING(
            CreateOrmExpressionEvaluator(
                NQueryClient::ParseSource("[/meta/type]", NQueryClient::EParseMode::Expression),
                /*attributePaths*/ {
                    TTypedAttributePath{
                        .Path = "/meta/type",
                        .TypeResolver = GetTypeResolver(type),
                    },
                }),
            Format("Attribute type %Qlv is not supported", type));
    }

    static const auto typedAttributePaths = {
        TTypedAttributePath{
            .Path = "/meta/id",
            .TypeResolver = GetTypeResolver(EValueType::String),
        },
        TTypedAttributePath{
            .Path = "/spec/weight",
            .TypeResolver = GetTypeResolver(EValueType::Double),
        },
        TTypedAttributePath{
            .Path = "/status/read_count",
            .TypeResolver = GetTypeResolver(EValueType::Int64),
        },
        TTypedAttributePath{
            .Path = "/labels",
            .TypeResolver = GetTypeResolver(EValueType::Any),
        },
        TTypedAttributePath{
            .Path = "/extras/special",
            .TypeResolver = GetTypeResolver(EValueType::Boolean),
        },
    };
    auto createEvaluator = [&] (const std::string& filter) {
        return CreateOrmExpressionEvaluator(
            NQueryClient::ParseSource(filter, NQueryClient::EParseMode::Expression),
            typedAttributePaths);
    };

    EXPECT_THROW_WITH_SUBSTRING(
        createEvaluator("[/meta/id/name] * 5"),
        "does not support nested attributes");

    EXPECT_THROW_WITH_SUBSTRING(
        createEvaluator("[/spec/weight/2] = %true"),
        "does not support nested attributes");

    EXPECT_THROW_WITH_SUBSTRING(
        createEvaluator("[/status/read_count/per_year/2023] - 143"),
        "does not support nested attributes");

    EXPECT_THROW(createEvaluator("[/meta/id] IN (5, 6, 7)"), TErrorException);
    EXPECT_THROW(createEvaluator("[/spec/weight] = \"10\""), TErrorException);
    EXPECT_THROW(createEvaluator("[/status/read_count] = %true"), TErrorException);

    auto rowBuffer = New<NTableClient::TRowBuffer>();
    auto evaluator = createEvaluator(
        "(is_prefix(\"abc\", [/meta/id]) OR (int64([/spec/weight] * 100) > [/status/read_count])) AND "
        "([/extras/special] OR try_get_string([/labels/details], \"/color\") IN (\"blue\", \"purple\", \"orange\"))");

    auto buildAndEvaluateExpression = [&] (
        const std::string& id,
        double weight,
        int readCount,
        const std::string& color,
        bool special)
    {
        std::vector<NYson::TYsonString> valueList = {
            BuildYsonStringFluently().Value(id),
            BuildYsonStringFluently().Value(weight),
            BuildYsonStringFluently().Value(readCount),
            BuildYsonStringFluently().BeginMap()
                .Item("details").BeginMap()
                    .Item("color").Value(color)
                .EndMap()
            .EndMap(),
            BuildYsonStringFluently().Value(special),
        };
        auto value = evaluator->Evaluate({valueList.begin(), valueList.end()}, rowBuffer).ValueOrThrow();

        EXPECT_EQ(value.Type, EValueType::Boolean);
        return value.Data.Boolean;
    };

    EXPECT_TRUE(buildAndEvaluateExpression("abcd", 3.23, 500, "purple", false));
    EXPECT_TRUE(buildAndEvaluateExpression("xyz", 4.12, 321, "orange", false));
    EXPECT_FALSE(buildAndEvaluateExpression("abcd", 3.23, 500, "magenta", false));
    EXPECT_FALSE(buildAndEvaluateExpression("xyz", 4.12, 321, "cyan", false));
    EXPECT_TRUE(buildAndEvaluateExpression("xyz", 4.12, 321, "cyan", true));
}

TEST(TExpressionEvaluatorTest, OrmManyFunctions)
{
    auto rowBuffer = New<NTableClient::TRowBuffer>();
    auto evaluator = CreateOrmExpressionEvaluator(
        NQueryClient::ParseSource(
            "((((string([/meta/str_id]))||(\";\"))||(numeric_to_string(int64([/meta/i64_id]))))||(\";\"))||(regex_replace_first(\"u\", numeric_to_string(uint64([/meta/ui64_id])), \"\"))",
            NQueryClient::EParseMode::Expression),
        {"/meta"});
    auto value = evaluator->Evaluate(
        BuildYsonStringFluently()
        .BeginMap()
            .Item("str_id").Value("abacaba")
            .Item("i64_id").Value(25)
            .Item("ui64_id").Value(315u)
        .EndMap(),
        rowBuffer)
        .ValueOrThrow()
        .AsString();
    EXPECT_EQ(value, std::string("abacaba;25;315"));
}

TEST(TExpressionEvaluatorTest, ThreadLocalFunctionState)
{
    auto createEvaluator = [] {
        return CreateOrmExpressionEvaluator(
            NQueryClient::ParseSource("running_sum(int64([/value]))", NQueryClient::EParseMode::Expression),
            {"/value"},
            EExpressionEvaluatorMode::ThreadSafe);
    };

    auto evaluator = createEvaluator();
    auto otherEvaluator = createEvaluator();

    auto expectSum = [] (const IExpressionEvaluatorPtr& evaluator, int input, i64 expected) {
        auto rowBuffer = New<NTableClient::TRowBuffer>();
        auto result = evaluator->Evaluate(BuildYsonStringFluently().Value(input), rowBuffer);
        ASSERT_TRUE(result.IsOK()) << ToString(result);
        ASSERT_EQ(result.Value().Type, EValueType::Int64);
        EXPECT_EQ(result.Value().Data.Int64, expected);
    };

    expectSum(evaluator, 1, 1);
    for (int iteration = 0; iteration < 2; ++iteration) {
        std::jthread thread([&] {
            expectSum(evaluator, 10, 10);
            expectSum(evaluator, 2, 12);
            expectSum(otherEvaluator, 4, 4);
        });
    }
    expectSum(evaluator, 2, 3);
    expectSum(otherEvaluator, 1, 1);
}

TEST(TExpressionEvaluatorTest, ThreadLocalStringResultOutlivesWorker)
{
    auto evaluator = CreateOrmExpressionEvaluator(
        NQueryClient::ParseSource(
            "if(int64([/value]) IN (1, 2), 'one', 'other')",
            NQueryClient::EParseMode::Expression),
        {"/value"},
        EExpressionEvaluatorMode::ThreadSafe);

    auto firstRowBuffer = New<NTableClient::TRowBuffer>();
    auto secondRowBuffer = New<NTableClient::TRowBuffer>();
    auto first = NTableClient::MakeUnversionedNullValue();
    auto second = NTableClient::MakeUnversionedNullValue();
    bool independentStorage = false;
    {
        std::jthread thread([&] {
            auto input = BuildYsonStringFluently().Value(1);
            auto firstResult = evaluator->Evaluate(input, firstRowBuffer);
            auto secondResult = evaluator->Evaluate(input, secondRowBuffer);
            ASSERT_TRUE(firstResult.IsOK()) << ToString(firstResult);
            ASSERT_TRUE(secondResult.IsOK()) << ToString(secondResult);
            first = firstResult.Value();
            second = secondResult.Value();
            ASSERT_EQ(first.Type, EValueType::String);
            ASSERT_EQ(second.Type, EValueType::String);
            independentStorage = first.Data.String != second.Data.String;
        });
    }

    ASSERT_TRUE(independentStorage);
    EXPECT_EQ(first.AsString(), "one");
    EXPECT_EQ(second.AsString(), "one");
}

TEST(TExpressionEvaluatorTest, ThreadSafeEvaluatorValidatesEagerly)
{
    EXPECT_THROW_WITH_SUBSTRING(
        CreateOrmExpressionEvaluator(
            NQueryClient::ParseSource("unknown_function([/value])", NQueryClient::EParseMode::Expression),
            {"/value"},
            EExpressionEvaluatorMode::ThreadSafe),
        "Failed to create query evaluation context");
}

TEST(TExpressionEvaluatorTest, Simple)
{
    NQueryClient::TSchemaColumns columns = {
        {
            "meta.x", EValueType::Int64
        },
        {
            "lambda.y.z", EValueType::Double,
        },
        {
            "theta", EValueType::Uint64
        }
    };

    auto rowBuffer = New<NTableClient::TRowBuffer>();
    auto evaluator = CreateExpressionEvaluator(
        "[meta.x] + int64([lambda.y.z]) + 16 + int64([theta])",
        std::move(columns));

    auto value = evaluator->Evaluate({
            BuildYsonStringFluently().Value(1),
            BuildYsonStringFluently().Value(10.0),
            BuildYsonStringFluently().Value(1000u)
        },
        rowBuffer)
        .ValueOrThrow();

    EXPECT_EQ(value.Type, EValueType::Int64);
    EXPECT_EQ(value.Data.Int64, 1 + 10 + 16 + 1000);
}

TEST(TExpressionEvaluatorTest, TableName)
{
    NQueryClient::TSchemaColumns columns = {
        {
            "meta.str_id", EValueType::String,
        },
        {
            "meta.i64_id", EValueType::Int64,
        },
        {
            "meta.ui64_id", EValueType::Uint64
        }
    };

    auto rowBuffer = New<NTableClient::TRowBuffer>();
    auto evaluator = CreateExpressionEvaluator(
        "((((p.`meta.str_id`)||(\";\"))||(numeric_to_string(p.`meta.i64_id`)))||(\";\"))||(regex_replace_first(\"u\", numeric_to_string(p.`meta.ui64_id`), \"\"))",
        columns);

    auto value = evaluator->Evaluate({
            BuildYsonStringFluently().Value("abacaba"),
            BuildYsonStringFluently().Value(25),
            BuildYsonStringFluently().Value(315u)
        },
        rowBuffer)
        .ValueOrThrow()
        .AsString();

    EXPECT_EQ(value, std::string("abacaba;25;315"));

    EXPECT_THROW_WITH_SUBSTRING(
        CreateExpressionEvaluator("p.`meta.i64_id` + int64(s.`meta.ui64_id`)", columns),
        "contains conflicting table names:");
}

TEST(TExpressionEvaluatorTest, NullValue)
{
    NQueryClient::TSchemaColumns columns = {
        {
            "value.x", EValueType::Int64,
        },
        {
            "value.y", EValueType::Int64
        }
    };

    auto rowBuffer = New<NTableClient::TRowBuffer>();
    auto evaluator = CreateExpressionEvaluator("is_null([value.x]) and not is_null([value.y])", columns);
    auto value = evaluator->Evaluate({
            BuildYsonStringFluently().Entity(),
            BuildYsonStringFluently().Value(10)
        },
        rowBuffer)
        .ValueOrThrow();
    EXPECT_EQ(value.Type, EValueType::Boolean);
    EXPECT_EQ(value.Data.Boolean, true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NQuery::NTests

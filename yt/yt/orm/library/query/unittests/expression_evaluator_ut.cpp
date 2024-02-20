#include <yt/yt/orm/library/query/expression_evaluator.h>

#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NOrm::NQuery::NTests {
namespace {

using namespace NYT;
using namespace NYT::NYTree;
using namespace NYT::NConcurrency;

using NTableClient::EValueType;

////////////////////////////////////////////////////////////////////////////////

TEST(TExpressionEvaluatorTest, Simple)
{
    auto evaluator = CreateExpressionEvaluator("double([/meta/x]) + 5 * double([/meta/y])", {"/meta"});
    auto value = evaluator->Evaluate(
        BuildYsonStringFluently().BeginMap()
            .Item("x").Value(1.0)
            .Item("y").Value(100.0)
        .EndMap()
    ).ValueOrThrow();
    EXPECT_EQ(value.Type, EValueType::Double);
    EXPECT_EQ(value.Data.Double, 501);
}

TEST(TExpressionEvaluatorTest, ManyArguments)
{
    auto evaluator = CreateExpressionEvaluator(
        "int64([/meta/x]) + int64([/lambda/y/z]) + 16 + int64([/theta])",
        {"/meta", "/lambda/y", "/theta"});
    auto value = evaluator->Evaluate({
        BuildYsonStringFluently().BeginMap()
            .Item("x").Value(1)
        .EndMap(),
        BuildYsonStringFluently().BeginMap()
            .Item("z").Value(10)
            .Item("y").Value(100)
        .EndMap(),
        BuildYsonStringFluently().Value(1000)
    }).ValueOrThrow();
    EXPECT_EQ(value.Type, EValueType::Int64);
    EXPECT_EQ(value.Data.Int64, 1 + 10 + 16 + 1000);
}

TEST(TExpressionEvaluatorTest, TypedAttributePaths)
{
    for (auto type : {EValueType::Max, EValueType::Min, EValueType::Null, EValueType::Composite}) {
        EXPECT_THROW_WITH_SUBSTRING(
            CreateExpressionEvaluator(
                "[/meta/type]",
                /*attributePaths*/ {
                    TTypedAttributePath{
                        .Path = "/meta/type",
                        .Type = type
                    },
                }),
            Format("Attribute type %Qlv is not supported", type));
    }

    static const auto typedAttributePaths = {
        TTypedAttributePath{
            .Path = "/meta/id",
            .Type = EValueType::String,
        },
        TTypedAttributePath{
            .Path = "/spec/weight",
            .Type = EValueType::Double,
        },
        TTypedAttributePath{
            .Path = "/status/read_count",
            .Type = EValueType::Int64,
        },
        TTypedAttributePath{
            .Path = "/labels",
            .Type = EValueType::Any,
        },
        TTypedAttributePath{
            .Path = "/extras/special",
            .Type = EValueType::Boolean,
        },
    };
    auto createEvaluator = [&] (const TString& filter) {
        return CreateExpressionEvaluator(filter, typedAttributePaths);
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

    auto evaluator = createEvaluator(
        "(is_prefix(\"abc\", [/meta/id]) OR (int64([/spec/weight] * 100) > [/status/read_count])) AND "
        "([/extras/special] OR try_get_string([/labels/details], \"/color\") IN (\"blue\", \"purple\", \"orange\"))");

    auto buildAndEvaluateExpression = [&] (
        const TString& id,
        double weight,
        int readCount,
        const TString& color,
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
        auto value = evaluator->Evaluate({valueList.begin(), valueList.end()}).ValueOrThrow();

        EXPECT_EQ(value.Type, EValueType::Boolean);
        return value.Data.Boolean;
    };

    EXPECT_TRUE(buildAndEvaluateExpression("abcd", 3.23, 500, "purple", false));
    EXPECT_TRUE(buildAndEvaluateExpression("xyz", 4.12, 321, "orange", false));
    EXPECT_FALSE(buildAndEvaluateExpression("abcd", 3.23, 500, "magenta", false));
    EXPECT_FALSE(buildAndEvaluateExpression("xyz", 4.12, 321, "cyan", false));
    EXPECT_TRUE(buildAndEvaluateExpression("xyz", 4.12, 321, "cyan", true));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NQuery::NTests

#include <yt/yt/orm/library/query/expression_evaluator.h>

#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NOrm::NQuery::NTests {
namespace {

using namespace NYT;
using namespace NYT::NYTree;
using namespace NYT::NConcurrency;
using NYT::NYson::TYsonStringBuf;

////////////////////////////////////////////////////////////////////////////////

TEST(ExpressionEvaluator, Simple)
{
    auto evaluator = CreateExpressionEvaluator("double([/meta/x]) + 5 * double([/meta/y])", {"/meta"});
    auto value = evaluator->Evaluate(
        BuildYsonStringFluently().BeginMap()
            .Item("x").Value(1.0)
            .Item("y").Value(100.0)
        .EndMap()
    ).ValueOrThrow();
    EXPECT_EQ(value.Type, NTableClient::EValueType::Double);
    EXPECT_EQ(value.Data.Double, 501);
}

TEST(ExpressionEvaluator, ManyArguments)
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
    EXPECT_EQ(value.Type, NTableClient::EValueType::Int64);
    EXPECT_EQ(value.Data.Int64, 1 + 10 + 16 + 1000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NQuery::NTests

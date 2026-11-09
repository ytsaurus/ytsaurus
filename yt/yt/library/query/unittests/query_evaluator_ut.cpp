#include <yt/yt/library/query/engine_api/query_evaluator.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <array>

namespace NYT::NQueryClient {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TQueryEvaluationContextTest, ParsedSourceIsIncludedInPreparationError)
{
    const std::string source = "missing % 5";
    auto parsedSource = ParseSource(source, EParseMode::Expression);
    auto schema = New<TTableSchema>();
    try {
        CreateQueryEvaluationContext(*parsedSource, schema);
        ADD_FAILURE() << "Expected context creation to reject an unknown column";
    } catch (const TErrorException& ex) {
        const auto& error = ex.Error();
        EXPECT_EQ(source, error.Attributes().Get<std::string>("source"));
        EXPECT_FALSE(error.InnerErrors().empty());
        EXPECT_TRUE(error.FindMatching([] (const TError& innerError) {
            return innerError.GetMessage().find("Undefined reference") != std::string::npos;
        }));
    }
}

TEST(TQueryEvaluationContextTest, ParsedAndTypedFactoriesEvaluateExpression)
{
    auto parsedSource = ParseSource("value % 3", EParseMode::Expression);
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("value", EValueType::Int64),
    });
    auto expression = PrepareExpression(*parsedSource, *schema);
    auto rowBuffer = New<TRowBuffer>();
    std::array row{MakeUnversionedInt64Value(8)};
    for (const auto& context : {
        CreateQueryEvaluationContext(*parsedSource, schema),
        CreateQueryEvaluationContext(expression, schema),
    }) {
        EXPECT_EQ(MakeUnversionedInt64Value(2), EvaluateQuery(*context, row, rowBuffer));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient

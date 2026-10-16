#include "semantics_cases.h"

#include <yt/yt/library/query/portable_engine/builtin_registry.h>
#include <yt/yt/library/query/portable_engine/program.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/resource/resource.h>

#include <util/generic/hash_set.h>

namespace NYT::NQueryClient::NPortable::NTest {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TPortableScalarSemanticsTest
    : public ::testing::TestWithParam<TScalarExpressionCase>
{ };

TEST_P(TPortableScalarSemanticsTest, Evaluate)
{
    const auto& testCase = GetParam();
    SCOPED_TRACE(testCase.Name);

    auto evaluate = [&] {
        auto parsedSource = ParseSource(testCase.Source, EParseMode::Expression);
        auto expression = PrepareExpression(*parsedSource, testCase.Schema, testCase.BuilderVersion);
        auto program = CompileExpression(expression, testCase.Schema, GetBuiltinExpressionRegistry());
        auto rowBuffer = New<TRowBuffer>();
        std::vector<TValue> scratch(program.GetScratchValueCount());
        auto result = testCase.ExpectedValue.Type() == EValueType::Null
            ? MakeUnversionedInt64Value(0)
            : MakeUnversionedNullValue();

        program.Evaluate(&result, testCase.InputRow.Elements(), scratch, rowBuffer);

        return TOwningValue(result);
    };

    if (!testCase.ExpectedError.empty()) {
        EXPECT_THROW_WITH_SUBSTRING(evaluate(), testCase.ExpectedError);
        return;
    }

    auto result = evaluate();
    EXPECT_EQ(static_cast<TValue>(result), static_cast<TValue>(testCase.ExpectedValue));
}

INSTANTIATE_TEST_SUITE_P(
    ScalarSemantics,
    TPortableScalarSemanticsTest,
    ::testing::ValuesIn(GetScalarExpressionCases()),
    [] (const ::testing::TestParamInfo<TScalarExpressionCase>& info) {
        return info.param.Name;
    });

TEST(TPortableScalarCoverageTest, EveryCapabilityHasSemanticCases)
{
    auto manifest = NYTree::ConvertTo<THashMap<std::string, std::string>>(
        NYson::TYsonString(::NResource::Find("portable_expression_capabilities")));
    THashSet<std::string> covered;
    for (const auto& testCase : GetScalarExpressionCases()) {
        SCOPED_TRACE(testCase.Name);
        EXPECT_FALSE(testCase.Capabilities.empty());
        for (const auto& capability : testCase.Capabilities) {
            EXPECT_TRUE(manifest.contains(capability)) << capability;
            covered.insert(capability);
        }
    }

    for (const auto& [capability, signature] : manifest) {
        EXPECT_FALSE(signature.empty()) << capability;
        EXPECT_TRUE(covered.contains(capability)) << capability;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient::NPortable::NTest

#include <yt/yt/orm/library/query/type_inference.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NOrm::NQuery::NTests {
namespace {

using NTableClient::EValueType;

////////////////////////////////////////////////////////////////////////////////

TEST(TTypeInferenceTest, BasicFunctionsReturningAny)
{
    ASSERT_EQ(TryInferFunctionReturnType("try_get_any"), EValueType::Any);
    ASSERT_EQ(TryInferFunctionReturnType("make_map"), EValueType::Any);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NQuery::NTests

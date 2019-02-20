#include <yt/core/test_framework/framework.h>

#include <yt/client/table_client/logical_type.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NTableClient {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TLogicalTypeTest, TestSimplifyLogicalType)
{
    using TPair = std::pair<std::optional<ESimpleLogicalValueType>, bool>;

    EXPECT_EQ(
        SimplifyLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64, true)),
        TPair(ESimpleLogicalValueType::Int64, true));

    EXPECT_EQ(
        SimplifyLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint64, false)),
        TPair(ESimpleLogicalValueType::Uint64, false));

    EXPECT_EQ(
        SimplifyLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64, true))),
        TPair(ESimpleLogicalValueType::Int64, false));

    EXPECT_EQ(
        SimplifyLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64, false))),
        TPair(std::nullopt, false));

    EXPECT_EQ(
        SimplifyLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64, true))),
        TPair(std::nullopt, true));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient

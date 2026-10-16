#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow::NSortedDynamicTable {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSortedDynamicTableRegistryTest, SinksAreRegistered)
{
    const auto& sinkTypeNames = TRegistry::Get()->GetSinkTypeNames();
    EXPECT_THAT(sinkTypeNames, testing::Contains("NYT::NFlow::NSortedDynamicTable::TSyncSink"));
    EXPECT_THAT(sinkTypeNames, testing::Contains("NYT::NFlow::NSortedDynamicTable::TAsyncSink"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NSortedDynamicTable

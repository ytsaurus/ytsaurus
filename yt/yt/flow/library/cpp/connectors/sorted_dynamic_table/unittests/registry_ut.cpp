#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow::NSortedDynamicTable {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSortedDynamicTableRegistryTest, SinkIsRegistered)
{
    EXPECT_THAT(
        TRegistry::Get()->GetSinkTypeNames(),
        testing::Contains("NYT::NFlow::NSortedDynamicTable::TSyncSink"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NSortedDynamicTable

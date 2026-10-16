#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TQueueRegistryTest, ProductionTypesAreRegistered)
{
    EXPECT_THAT(TRegistry::Get()->GetSourceTypeNames(), testing::Contains("NYT::NFlow::TQueueSource"));

    const auto sinkTypeNames = TRegistry::Get()->GetSinkTypeNames();
    EXPECT_THAT(sinkTypeNames, testing::Contains("NYT::NFlow::TAsyncQueueSink"));
    EXPECT_THAT(sinkTypeNames, testing::Contains("NYT::NFlow::TAsyncMultiClusterQueueSink"));
    EXPECT_THAT(sinkTypeNames, testing::Contains("NYT::NFlow::TSyncQueueSink"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TServiceLogRegistryTest, SourceIsRegistered)
{
    EXPECT_THAT(TRegistry::Get()->GetSourceTypeNames(), testing::Contains("NYT::NFlow::TServiceLogSource"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

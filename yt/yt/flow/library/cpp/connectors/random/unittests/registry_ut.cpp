#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TRandomRegistryTest, SourceIsRegistered)
{
    EXPECT_THAT(TRegistry::Get()->GetSourceTypeNames(), testing::Contains("NYT::NFlow::TRandomSource"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

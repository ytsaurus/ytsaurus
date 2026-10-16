#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TResourceRegistryTest, ProductionTypesAreRegistered)
{
    const auto typeNames = TRegistry::Get()->GetResourceTypeNames();

    EXPECT_THAT(typeNames, testing::Contains("NYT::NFlow::TYTClientFactory"));
    EXPECT_THAT(typeNames, testing::Contains("NYT::NFlow::TYTHedgingClient"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCompanionManagerRegistryTest, ProductionTypesAreRegistered)
{
    const auto resourceTypeNames = TRegistry::Get()->GetResourceTypeNames();
    EXPECT_THAT(resourceTypeNames, testing::Contains("NYT::NFlow::NCompanion::TCompanionManager"));
    EXPECT_THAT(resourceTypeNames, testing::Contains("NYT::NFlow::NCompanion::TJavaCompanionManager"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion

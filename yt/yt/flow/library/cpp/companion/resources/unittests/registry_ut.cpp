#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCompanionResourceRegistryTest, ProductionTypesAreRegistered)
{
    const auto resourceTypeNames = TRegistry::Get()->GetResourceTypeNames();
    EXPECT_THAT(resourceTypeNames, testing::Contains("NYT::NFlow::NCompanion::TCompanionResource"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion

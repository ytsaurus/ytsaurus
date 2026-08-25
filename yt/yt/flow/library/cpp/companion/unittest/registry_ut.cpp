#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCompanionRegistryTest, ProductionTypesAreRegistered)
{
    const auto computationTypeNames = TRegistry::Get()->GetComputationTypeNames();
    EXPECT_THAT(computationTypeNames, testing::Contains("NYT::NFlow::NCompanion::TSwiftMapCompanionComputation"));
    EXPECT_THAT(computationTypeNames, testing::Contains("NYT::NFlow::NCompanion::TSwiftOrderedSourceCompanionComputation"));
    EXPECT_THAT(computationTypeNames, testing::Contains("NYT::NFlow::NCompanion::TTransformCompanionComputation"));
    EXPECT_THAT(computationTypeNames, testing::Contains("NYT::NFlow::NCompanion::TTransformOrderedSourceCompanionComputation"));

    const auto resourceTypeNames = TRegistry::Get()->GetResourceTypeNames();
    EXPECT_THAT(resourceTypeNames, testing::Contains("NYT::NFlow::NCompanion::TCompanionManager"));
    EXPECT_THAT(resourceTypeNames, testing::Contains("NYT::NFlow::NCompanion::TCompanionResource"));
    EXPECT_THAT(resourceTypeNames, testing::Contains("NYT::NFlow::NCompanion::TJavaCompanionManager"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion

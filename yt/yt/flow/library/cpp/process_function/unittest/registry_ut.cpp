#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TProcessFunctionHostRegistryTest, AdapterComputationsAreRegistered)
{
    const auto typeNames = TRegistry::Get()->GetComputationTypeNames();

    EXPECT_THAT(typeNames, testing::Contains("NYT::NFlow::TProcessFunctionComputation"));
    EXPECT_THAT(typeNames, testing::Contains("NYT::NFlow::TProcessFunctionSwiftMapComputation"));
    EXPECT_THAT(typeNames, testing::Contains("NYT::NFlow::TProcessFunctionTransformOrderedSourceComputation"));
    EXPECT_THAT(typeNames, testing::Contains("NYT::NFlow::TProcessFunctionSourceComputation"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

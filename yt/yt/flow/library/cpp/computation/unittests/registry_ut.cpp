#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TComputationRegistryTest, ProductionTypesAreRegistered)
{
    EXPECT_THAT(
        TRegistry::Get()->GetComputationTypeNames(),
        testing::Contains("NYT::NFlow::TPassthroughComputation"));

    auto spec = New<TExternalStateManagerSpec>();
    spec->ExternalStateManagerClassName = "NYT::NFlow::TSimpleExternalStateManager";
    spec->Parameters = BuildYsonNodeFluently()
        .BeginMap()
        .Item("path")
        .Value("//tmp/state")
        .EndMap()
        ->AsMap();
    EXPECT_TRUE(TRegistry::Get()->ParseExternalStateManagerParameters(spec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

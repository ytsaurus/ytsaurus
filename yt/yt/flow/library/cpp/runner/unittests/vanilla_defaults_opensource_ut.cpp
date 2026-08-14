#include <yt/yt/flow/library/cpp/runner/vanilla_defaults.h>
#include <yt/yt/flow/library/cpp/runner/vanilla_launcher.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TVanillaConfigTest, OpenSourceBuildHasNoDefaultNetworkProject)
{
    EXPECT_FALSE(GetDefaultVanillaNetworkProject().has_value());

    auto config = ConvertTo<TVanillaConfigPtr>(TYsonStringBuf(R"({pool=test;worker={count=1}})"));
    EXPECT_FALSE(config->NetworkProject.has_value());

    // The out-of-the-box opensource launch shares the exec node's network, so it must ask YT
    // for ports instead of the colliding fixed ones.
    EXPECT_EQ(config->Controller->PortCount, std::optional<int>(2));
    EXPECT_EQ(config->Worker->PortCount, std::optional<int>(3));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

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

TEST(TVanillaConfigTest, InternalBuildUsesFlowNetworkProject)
{
    const auto expected = std::optional<std::string>("yt_flow_common");

    EXPECT_EQ(GetDefaultVanillaNetworkProject(), expected);

    auto config = ConvertTo<TVanillaConfigPtr>(TYsonStringBuf(R"({pool=test;worker={count=1}})"));
    EXPECT_EQ(config->NetworkProject, expected);

    // Under the internal network project the fixed ports stay, keeping internal launches unchanged.
    EXPECT_FALSE(config->Controller->PortCount.has_value());
    EXPECT_FALSE(config->Worker->PortCount.has_value());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

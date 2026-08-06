#include <yt/yt/flow/library/cpp/runner/vanilla_launcher.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TVanillaConfigTest, AllowsCustomNetworkProject)
{
    auto config = ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1};network_project=custom})"));

    EXPECT_EQ(config->NetworkProject, std::optional<std::string>("custom"));
}

TEST(TVanillaConfigTest, EntityDisablesDefaultNetworkProject)
{
    auto config = ConvertTo<TVanillaConfigPtr>(
        TYsonStringBuf(R"({pool=test;worker={count=1};network_project=#})"));

    EXPECT_FALSE(config->NetworkProject.has_value());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

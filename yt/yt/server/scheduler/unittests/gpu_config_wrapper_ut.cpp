#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/strategy/policy/gpu/config_wrapper.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {
namespace {

////////////////////////////////////////////////////////////////////////////////

TGpuSchedulingPolicyConfigWrapper ParseGpuSchedulingPolicyConfig(const std::string& yson)
{
    return TGpuSchedulingPolicyConfigWrapper(
        NYTree::ConvertTo<TGpuSchedulingPolicyConfigPtr>(NYson::TYsonString(yson)));
}

TEST(TGpuSchedulingPolicyConfigWrapperTest, ModulesFromLegacySet)
{
    auto config = ParseGpuSchedulingPolicyConfig("{modules = [SAS; VLA]}");
    EXPECT_EQ(config.GetModules(), (THashSet<std::string>{"SAS", "VLA"}));
}

TEST(TGpuSchedulingPolicyConfigWrapperTest, LegacyModulesMergedIntoModuleConfigs)
{
    auto config = ParseGpuSchedulingPolicyConfig(
        "{modules = [SAS; MAN]; module_configs = {VLA = {}}}");
    EXPECT_EQ(config.GetModules(), (THashSet<std::string>{"SAS", "MAN", "VLA"}));
}

TEST(TGpuSchedulingPolicyConfigWrapperTest, PerModuleReconsiderationTimeout)
{
    auto config = ParseGpuSchedulingPolicyConfig(
        "{module_reconsideration_timeout = 60000; "
        "module_configs = {VLA = {module_reconsideration_timeout = 2000}; SAS = {}}}");
    EXPECT_EQ(config.GetModuleReconsiderationTimeout("VLA"), TDuration::MilliSeconds(2000));
    EXPECT_EQ(config.GetModuleReconsiderationTimeout("SAS"), TDuration::MilliSeconds(60000));
    // Unknown module falls back to the tree-level default.
    EXPECT_EQ(config.GetModuleReconsiderationTimeout("MAN"), TDuration::MilliSeconds(60000));
}

TEST(TGpuSchedulingPolicyConfigWrapperTest, PerModuleShareToNetworkPriority)
{
    auto config = ParseGpuSchedulingPolicyConfig(
        "{module_share_to_network_priority = [{module_share = 0.5; network_priority = 3}]; "
        "module_configs = {"
        "    VLA = {module_share_to_network_priority = [{module_share = 0.9; network_priority = 5}]}; "
        "    SAS = {}}}");

    const auto& vlaTable = config.GetModuleShareToNetworkPriority("VLA");
    ASSERT_EQ(std::ssize(vlaTable), 1);
    EXPECT_EQ(vlaTable[0].ModuleShare, 0.9);
    EXPECT_EQ(vlaTable[0].NetworkPriority, 5);

    const auto& sasTable = config.GetModuleShareToNetworkPriority("SAS");
    ASSERT_EQ(std::ssize(sasTable), 1);
    EXPECT_EQ(sasTable[0].ModuleShare, 0.5);
    EXPECT_EQ(sasTable[0].NetworkPriority, 3);
}

TEST(TGpuSchedulingPolicyConfigWrapperTest, PerModuleShareToNetworkPriorityValidation)
{
    // Shares must be strictly ascending inside a per-module table too.
    EXPECT_THROW(
        ParseGpuSchedulingPolicyConfig(
            "{module_configs = {VLA = {module_share_to_network_priority = ["
            "{module_share = 0.9; network_priority = 5}; "
            "{module_share = 0.5; network_priority = 3}]}}}"),
        std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu

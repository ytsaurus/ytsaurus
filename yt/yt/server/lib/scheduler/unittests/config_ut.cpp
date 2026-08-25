#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

namespace NYT::NScheduler {

namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TStrategyTreeConfigPtr ParseTreeConfig(TStringBuf yson)
{
    return ConvertTo<TStrategyTreeConfigPtr>(TYsonString(yson));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStrategyTreeConfigPolicyKindTest, DefaultsArePaired)
{
    auto config = ParseTreeConfig("{}");

    EXPECT_EQ(config->PolicyKind, EPolicyKind::Classic);
    EXPECT_EQ(config->GpuSchedulingPolicy->Mode, EGpuSchedulingPolicyMode::Noop);
}

TEST(TStrategyTreeConfigPolicyKindTest, GpuPolicyKindRequiresAllocatingMode)
{
    auto config = ParseTreeConfig("{policy_kind=gpu;gpu_scheduling_policy={mode=allocating}}");

    EXPECT_EQ(config->PolicyKind, EPolicyKind::Gpu);
    EXPECT_EQ(config->GpuSchedulingPolicy->Mode, EGpuSchedulingPolicyMode::Allocating);
}

TEST(TStrategyTreeConfigPolicyKindTest, GpuPolicyKindRejectsOtherModes)
{
    // NB: The mode defaults to noop, so naming the policy kind alone is already the forbidden pairing.
    EXPECT_THROW_WITH_SUBSTRING(
        ParseTreeConfig("{policy_kind=gpu}"),
        "GPU policy kind requires GPU scheduling policy to be in \"allocating\" mode");
    EXPECT_THROW_WITH_SUBSTRING(
        ParseTreeConfig("{policy_kind=gpu;gpu_scheduling_policy={mode=noop}}"),
        "GPU policy kind requires GPU scheduling policy to be in \"allocating\" mode");
    EXPECT_THROW_WITH_SUBSTRING(
        ParseTreeConfig("{policy_kind=gpu;gpu_scheduling_policy={mode=dry_run}}"),
        "GPU policy kind requires GPU scheduling policy to be in \"allocating\" mode");
}

TEST(TStrategyTreeConfigPolicyKindTest, ClassicPolicyKindAllowsAnyMode)
{
    // The reverse pairing is legal: with the classic policy kind the classic policy is primary and the
    // GPU policy is only the dry-run side-car, which degrades to noop in the allocating mode. Switching
    // a tree out of the GPU policy leaves exactly this state.
    for (auto mode : TEnumTraits<EGpuSchedulingPolicyMode>::GetDomainValues()) {
        auto config = ParseTreeConfig(Format("{policy_kind=classic;gpu_scheduling_policy={mode=%lv}}", mode));

        EXPECT_EQ(config->PolicyKind, EPolicyKind::Classic);
        EXPECT_EQ(config->GpuSchedulingPolicy->Mode, mode);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

} // namespace NYT::NScheduler

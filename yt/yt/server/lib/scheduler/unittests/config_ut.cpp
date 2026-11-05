#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>

#include <cmath>
#include <limits>

namespace NYT::NScheduler {

namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TStrategyTreeConfigPtr ParseTreeConfig(TStringBuf yson)
{
    return ConvertTo<TStrategyTreeConfigPtr>(TYsonString(yson));
}

TJobResourcesConfigPtr ParseJobResourcesConfig(TStringBuf yson)
{
    return ConvertTo<TJobResourcesConfigPtr>(TYsonString(yson));
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
    // Under the classic policy kind the GPU policy is merely a side-car, so any of its modes is legal.
    for (auto mode : TEnumTraits<EGpuSchedulingPolicyMode>::GetDomainValues()) {
        auto config = ParseTreeConfig(Format("{policy_kind=classic;gpu_scheduling_policy={mode=%lv}}", mode));

        EXPECT_EQ(config->PolicyKind, EPolicyKind::Classic);
        EXPECT_EQ(config->GpuSchedulingPolicy->Mode, mode);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TJobResourcesConfigTest, RejectsInvalidCpu)
{
    for (const auto& cpu : {"%inf", "%-inf", "%nan", "-1", "1e100", "92233720368547760"}) {
        SCOPED_TRACE(cpu);
        EXPECT_THROW_WITH_SUBSTRING(
            ParseJobResourcesConfig(Format("{cpu=%v}", cpu)),
            "cpu");
    }

    EXPECT_THROW_WITH_SUBSTRING(
        ParseJobResourcesConfig(Format("{cpu=%v}", static_cast<double>(std::numeric_limits<TCpuResource>::max()))),
        "cpu");
}

TEST(TJobResourcesConfigTest, AcceptsValidCpu)
{
    double maxValidCpu = std::nextafter(
        static_cast<double>(std::numeric_limits<TCpuResource>::max()),
        0.0);
    for (double cpu : {0.0, 0.01, 0.5, 512.0, maxValidCpu}) {
        SCOPED_TRACE(cpu);
        auto config = ParseJobResourcesConfig(Format("{cpu=%v}", cpu));
        ASSERT_EQ(config->Cpu, cpu);
        EXPECT_GE(ToJobResources(*config, TJobResources()).GetCpu().GetUnderlyingValue(), 0);
    }

    auto config = ParseJobResourcesConfig("{}");
    EXPECT_FALSE(config->Cpu);
}

TEST(TJobResourcesConfigTest, RejectsInvalidIntegerResources)
{
    for (auto resource : {"user_slots", "network", "memory", "gpu"}) {
        for (auto value : {"%inf", "%-inf", "%nan", "-1", "1e100", "9223372036854775808u"}) {
            SCOPED_TRACE(Format("%v=%v", resource, value));
            EXPECT_THROW_WITH_SUBSTRING(
                ParseJobResourcesConfig(Format("{%v=%v}", resource, value)),
                resource);
        }
    }

    for (auto resource : {"user_slots", "network", "gpu"}) {
        EXPECT_THROW_WITH_SUBSTRING(
            ParseJobResourcesConfig(Format("{%v=2147483648}", resource)),
            resource);
    }
}

TEST(TJobResourcesConfigTest, AcceptsValidIntegerResources)
{
    auto config = ParseJobResourcesConfig(
        "{user_slots=2147483647;network=2147483647;memory=9223372036854775807;gpu=2147483647}");
    auto resources = ToJobResources(*config, TJobResources());
    EXPECT_EQ(resources.GetUserSlots(), 2147483647);
    EXPECT_EQ(resources.GetNetwork(), 2147483647);
    EXPECT_EQ(resources.GetMemory(), 9223372036854775807);
    EXPECT_EQ(resources.GetGpu(), 2147483647);

    config = ParseJobResourcesConfig("{user_slots=0;network=0;memory=0;gpu=0}");
    EXPECT_EQ(ToJobResources(*config, TJobResources()), TJobResources());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

} // namespace NYT::NScheduler

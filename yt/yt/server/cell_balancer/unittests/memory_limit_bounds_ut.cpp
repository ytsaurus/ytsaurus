#include "helpers.h"

#include <yt/yt/server/cell_balancer/bundle_scheduler.h>
#include <yt/yt/server/cell_balancer/config.h>
#include <yt/yt/server/cell_balancer/input_state.h>
#include <yt/yt/server/cell_balancer/mutations.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NCellBalancer {
namespace {

using namespace NBundleControllerClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TMemoryLimitBoundsTest
    : public ::testing::TestWithParam<std::string>
{
protected:
    TMemoryLimitBoundsPtr MakeBounds(std::optional<i64> min, std::optional<i64> max)
    {
        return ConvertTo<TMemoryLimitBoundsPtr>(BuildYsonStringFluently()
            .BeginMap()
                .Item(GetParam()).BeginMap()
                    .Item("min").Value(min)
                    .Item("max").Value(max)
                .EndMap()
            .EndMap());
    }

    TMemoryLimitsPtr MakeLimits(std::optional<i64> value)
    {
        return ConvertTo<TMemoryLimitsPtr>(BuildYsonStringFluently()
            .BeginMap()
                .Item(GetParam()).Value(value)
            .EndMap());
    }

    TSchedulerInputState MakeInput(std::optional<i64> value)
    {
        auto input = GenerateSimpleInputContext(/*nodeCount*/ 0);
        const auto& bundleInfo = input.Bundles["bigd"];
        bundleInfo->EnableTabletNodeDynamicConfig = true;
        bundleInfo->TargetConfig->TabletNodeResourceGuarantee->Type = "default";
        bundleInfo->TargetConfig->MemoryLimits = MakeLimits(value);
        return input;
    }

    void CheckInvalidMemoryLimits(const TSchedulerMutations& mutations)
    {
        ASSERT_EQ(1, std::ssize(mutations.AlertsToFire));
        const auto& alert = mutations.AlertsToFire.front();
        EXPECT_EQ("invalid_bundle_memory_limits", alert.Id);
        EXPECT_EQ("bigd", alert.BundleName);
        EXPECT_NE(std::string::npos, alert.Description.find(GetParam()));
    }
};

TEST_P(TMemoryLimitBoundsTest, InclusiveAndOneSidedBounds)
{
    auto bounds = MakeBounds(/*min*/ 10, /*max*/ 20);
    for (i64 value : {10, 15, 20}) {
        EXPECT_NO_THROW(bounds->ValidateMemoryLimits(MakeLimits(value)));
    }
    for (i64 value : {9, 21}) {
        EXPECT_THROW_WITH_SUBSTRING(
            bounds->ValidateMemoryLimits(MakeLimits(value)),
            Format("Memory limit for category %Qv must be in range [10, 20], got %v", GetParam(), value));
    }

    auto lowerBound = MakeBounds(/*min*/ 10, /*max*/ std::nullopt);
    EXPECT_NO_THROW(lowerBound->ValidateMemoryLimits(MakeLimits(100)));
    EXPECT_THROW_WITH_SUBSTRING(
        lowerBound->ValidateMemoryLimits(MakeLimits(9)),
        Format("Memory limit for category %Qv must be at least 10, got 9", GetParam()));

    auto upperBound = MakeBounds(/*min*/ std::nullopt, /*max*/ 20);
    EXPECT_NO_THROW(upperBound->ValidateMemoryLimits(MakeLimits(0)));
    EXPECT_THROW_WITH_SUBSTRING(
        upperBound->ValidateMemoryLimits(MakeLimits(21)),
        Format("Memory limit for category %Qv must be below 20, got 21", GetParam()));

    auto zeroBounds = MakeBounds(/*min*/ 0, /*max*/ 0);
    EXPECT_NO_THROW(zeroBounds->ValidateMemoryLimits(MakeLimits(0)));
    EXPECT_THROW_WITH_SUBSTRING(
        zeroBounds->ValidateMemoryLimits(MakeLimits(1)),
        Format("Memory limit for category %Qv must be in range [0, 0], got 1", GetParam()));
}

TEST_P(TMemoryLimitBoundsTest, MissingLimitsAndBounds)
{
    auto bounds = MakeBounds(/*min*/ 10, /*max*/ 20);
    EXPECT_NO_THROW(bounds->ValidateMemoryLimits(MakeLimits(std::nullopt)));
    EXPECT_NO_THROW(bounds->ValidateMemoryLimits(New<TMemoryLimits>()));

    auto defaultBounds = New<TMemoryLimitBounds>();
    EXPECT_NO_THROW(defaultBounds->ValidateMemoryLimits(MakeLimits(0)));
    EXPECT_NO_THROW(defaultBounds->ValidateMemoryLimits(MakeLimits(std::numeric_limits<i64>::max())));

    auto emptyBounds = MakeBounds(/*min*/ std::nullopt, /*max*/ std::nullopt);
    EXPECT_NO_THROW(emptyBounds->ValidateMemoryLimits(MakeLimits(100)));
}

TEST_P(TMemoryLimitBoundsTest, InvalidBoundsAreRejected)
{
    EXPECT_THROW(MakeBounds(/*min*/ 20, /*max*/ 10), TErrorException);
    EXPECT_THROW(MakeBounds(/*min*/ -1, /*max*/ std::nullopt), TErrorException);
    EXPECT_THROW(MakeBounds(/*min*/ std::nullopt, /*max*/ -1), TErrorException);
}

TEST_P(TMemoryLimitBoundsTest, RejectsInvalidDynamicConfig)
{
    for (bool instanceBounds : {false, true}) {
        for (i64 value : {9, 21}) {
            auto input = MakeInput(value);
            const auto& zoneInfo = input.Zones["default-zone"];
            if (instanceBounds) {
                zoneInfo->TabletNodeSizes["default"]->MemoryLimitBounds = MakeBounds(/*min*/ 10, /*max*/ 20);
            } else {
                zoneInfo->TabletNodeMemoryLimitBounds = MakeBounds(/*min*/ 10, /*max*/ 20);
            }

            TSchedulerMutations mutations;
            ScheduleBundles(input, &mutations);

            CheckInvalidMemoryLimits(mutations);
            EXPECT_FALSE(mutations.BundlesDynamicConfig);
        }
    }
}

TEST_P(TMemoryLimitBoundsTest, PreservesPreviousConfigAndRecovers)
{
    auto input = MakeInput(15);
    const auto& bundleInfo = input.Bundles["bigd"];
    input.Zones["default-zone"]->TabletNodeMemoryLimitBounds = MakeBounds(/*min*/ 10, /*max*/ 20);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);
    CheckEmptyAlerts(mutations);
    ASSERT_TRUE(mutations.BundlesDynamicConfig);
    input.BundlesDynamicConfig = *mutations.BundlesDynamicConfig;

    bundleInfo->TargetConfig->MemoryLimits = MakeLimits(21);
    bundleInfo->TargetConfig->CpuLimits->QueryThreadPoolSize = 7;
    mutations = {};
    ScheduleBundles(input, &mutations);

    CheckInvalidMemoryLimits(mutations);
    EXPECT_FALSE(mutations.BundlesDynamicConfig);

    bundleInfo->TargetConfig->MemoryLimits = MakeLimits(20);
    mutations = {};
    ScheduleBundles(input, &mutations);

    CheckEmptyAlerts(mutations);
    ASSERT_TRUE(mutations.BundlesDynamicConfig);
    const auto& config = GetOrCrash(*mutations.BundlesDynamicConfig, bundleInfo->NodeTagFilter);
    EXPECT_EQ(7, config->CpuLimits->QueryThreadPoolSize);
    EXPECT_TRUE(AreNodesEqual(ConvertTo<INodePtr>(MakeLimits(20)), ConvertTo<INodePtr>(config->MemoryLimits)));
}

TEST_P(TMemoryLimitBoundsTest, InvalidBundleDoesNotBlockOtherBundles)
{
    auto input = MakeInput(15);
    SetBundleInfo(input, "other", /*nodeCount*/ 0);
    input.Bundles["other"]->EnableTabletNodeDynamicConfig = true;
    input.Zones["default-zone"]->TabletNodeMemoryLimitBounds = MakeBounds(/*min*/ 10, /*max*/ 20);

    TSchedulerMutations mutations;
    ScheduleBundles(input, &mutations);
    CheckEmptyAlerts(mutations);
    ASSERT_TRUE(mutations.BundlesDynamicConfig);
    input.BundlesDynamicConfig = *mutations.BundlesDynamicConfig;

    const auto& bundleInfo = input.Bundles["bigd"];
    bundleInfo->TargetConfig->MemoryLimits = MakeLimits(21);
    bundleInfo->TargetConfig->CpuLimits->QueryThreadPoolSize = 7;
    const auto& otherBundleInfo = input.Bundles["other"];
    otherBundleInfo->TargetConfig->CpuLimits->QueryThreadPoolSize = 3;
    mutations = {};
    ScheduleBundles(input, &mutations);

    CheckInvalidMemoryLimits(mutations);
    ASSERT_TRUE(mutations.BundlesDynamicConfig);
    const auto& previous = GetOrCrash(input.BundlesDynamicConfig, bundleInfo->NodeTagFilter);
    const auto& preserved = GetOrCrash(*mutations.BundlesDynamicConfig, bundleInfo->NodeTagFilter);
    EXPECT_TRUE(AreNodesEqual(ConvertTo<INodePtr>(previous), ConvertTo<INodePtr>(preserved)));
    EXPECT_EQ(3, GetOrCrash(*mutations.BundlesDynamicConfig, otherBundleInfo->NodeTagFilter)->CpuLimits->QueryThreadPoolSize);
}

TEST_P(TMemoryLimitBoundsTest, InstanceBoundsOverrideZoneBounds)
{
    for (bool useInstanceType : {false, true}) {
        for (bool enableNetworkLimits : {false, true}) {
            for (i64 value : {15, 20, 30, 35, 40, 45}) {
                auto input = MakeInput(value);
                input.Config->EnableNetworkLimits = enableNetworkLimits;
                const auto& zoneInfo = input.Zones["default-zone"];
                zoneInfo->TabletNodeMemoryLimitBounds = MakeBounds(/*min*/ 10, /*max*/ 30);
                zoneInfo->TabletNodeSizes["default"]->MemoryLimitBounds = MakeBounds(/*min*/ 20, /*max*/ 40);
                auto otherSize = New<TInstanceSize>();
                otherSize->MemoryLimitBounds = MakeBounds(/*min*/ 100, /*max*/ 200);
                zoneInfo->TabletNodeSizes["other"] = otherSize;
                if (!useInstanceType) {
                    input.Bundles["bigd"]->TargetConfig->TabletNodeResourceGuarantee->Type.clear();
                }

                TSchedulerMutations mutations;
                ScheduleBundles(input, &mutations);

                if (value >= 20 && value <= 40) {
                    CheckEmptyAlerts(mutations);
                    EXPECT_TRUE(mutations.BundlesDynamicConfig);
                } else {
                    CheckInvalidMemoryLimits(mutations);
                    EXPECT_FALSE(mutations.BundlesDynamicConfig);
                }
            }
        }
    }
}

TEST_P(TMemoryLimitBoundsTest, PartialInstanceBoundsInheritZoneBounds)
{
    for (const auto& [min, max] : std::vector<std::pair<std::optional<i64>, std::optional<i64>>>{
        {5, std::nullopt},
        {std::nullopt, 40},
        {std::nullopt, std::nullopt},
    }) {
        for (i64 value : {0, 5, 10, 20, 30, 35, 40, 45}) {
            auto input = MakeInput(value);
            const auto& zoneInfo = input.Zones["default-zone"];
            zoneInfo->TabletNodeMemoryLimitBounds = MakeBounds(/*min*/ 10, /*max*/ 30);
            zoneInfo->TabletNodeSizes["default"]->MemoryLimitBounds = MakeBounds(min, max);

            TSchedulerMutations mutations;
            ScheduleBundles(input, &mutations);

            bool valid = value >= min.value_or(10) && value <= max.value_or(30);
            if (valid) {
                CheckEmptyAlerts(mutations);
                EXPECT_TRUE(mutations.BundlesDynamicConfig);
            } else {
                CheckInvalidMemoryLimits(mutations);
                EXPECT_FALSE(mutations.BundlesDynamicConfig);
            }
        }
    }
}

TEST_P(TMemoryLimitBoundsTest, InstanceSizeIsSelectedByResources)
{
    for (bool enableNetworkLimits : {false, true}) {
        for (i64 value : {35, 45}) {
            auto input = MakeInput(value);
            input.Config->EnableNetworkLimits = enableNetworkLimits;
            const auto& zoneInfo = input.Zones["default-zone"];
            const auto& oldSize = zoneInfo->TabletNodeSizes["default"];
            oldSize->MemoryLimitBounds = value == 35
                ? MakeBounds(/*min*/ 10, /*max*/ 30)
                : MakeBounds(/*min*/ 10, /*max*/ 100);
            auto newSize = New<TInstanceSize>();
            newSize->MemoryLimitBounds = MakeBounds(/*min*/ 20, /*max*/ 40);
            zoneInfo->TabletNodeSizes["new"] = newSize;
            const auto& resources = input.Bundles["bigd"]->TargetConfig->TabletNodeResourceGuarantee;
            resources->Vcpu = newSize->ResourceGuarantee->Vcpu;
            resources->Memory = newSize->ResourceGuarantee->Memory;
            resources->NetBytes = newSize->ResourceGuarantee->NetBytes;

            TSchedulerMutations mutations;
            ScheduleBundles(input, &mutations);

            if (value == 35) {
                CheckEmptyAlerts(mutations);
                EXPECT_TRUE(mutations.BundlesDynamicConfig);
            } else {
                CheckInvalidMemoryLimits(mutations);
                EXPECT_FALSE(mutations.BundlesDynamicConfig);
            }
        }
    }
}

TEST_P(TMemoryLimitBoundsTest, FirstMatchingInstanceSizeIsUsed)
{
    for (i64 value : {15, 30}) {
        auto input = MakeInput(value);
        input.Bundles["bigd"]->TargetConfig->TabletNodeResourceGuarantee->Type.clear();
        const auto& zoneInfo = input.Zones["default-zone"];
        const auto& sizes = zoneInfo->TabletNodeSizes;
        auto otherSize = CloneYsonStruct(GetOrCrash(sizes, "default"));
        zoneInfo->TabletNodeSizes["other"] = otherSize;
        auto sizeIt = sizes.begin();
        sizeIt->second->MemoryLimitBounds = MakeBounds(/*min*/ 10, /*max*/ 40);
        ++sizeIt;
        sizeIt->second->MemoryLimitBounds = MakeBounds(/*min*/ 20, /*max*/ 20);

        TSchedulerMutations mutations;
        ScheduleBundles(input, &mutations);

        CheckEmptyAlerts(mutations);
        EXPECT_TRUE(mutations.BundlesDynamicConfig);
    }
}

TEST(TMemoryLimitBoundsConfigTest, ZoneAndInstanceYson)
{
    auto zoneInfo = ConvertTo<TZoneInfoPtr>(NYson::TYsonStringBuf(R"({
        tablet_node_memory_limit_bounds = {tablet_static = {min = 10;};};
        tablet_node_sizes = {
            small = {
                memory_limit_bounds = {tablet_static = {max = 20;};};
            };
        };
    })"));

    zoneInfo = CloneYsonStruct(zoneInfo);
    EXPECT_EQ(10, zoneInfo->TabletNodeMemoryLimitBounds->TabletStatic.Min);
    EXPECT_FALSE(zoneInfo->TabletNodeMemoryLimitBounds->TabletStatic.Max);
    const auto& instanceBounds = GetOrCrash(zoneInfo->TabletNodeSizes, "small")->MemoryLimitBounds;
    EXPECT_FALSE(instanceBounds->TabletStatic.Min);
    EXPECT_EQ(20, instanceBounds->TabletStatic.Max);

    auto defaultZoneInfo = New<TZoneInfo>();
    EXPECT_FALSE(defaultZoneInfo->TabletNodeMemoryLimitBounds->TabletStatic.Min);
    EXPECT_FALSE(defaultZoneInfo->TabletNodeMemoryLimitBounds->TabletStatic.Max);
    auto defaultInstanceSize = New<TInstanceSize>();
    EXPECT_FALSE(defaultInstanceSize->MemoryLimitBounds->TabletStatic.Min);
    EXPECT_FALSE(defaultInstanceSize->MemoryLimitBounds->TabletStatic.Max);
}

INSTANTIATE_TEST_SUITE_P(
    MemoryCategories,
    TMemoryLimitBoundsTest,
    ::testing::Values(
        "compressed_block_cache",
        "key_filter_block_cache",
        "lookup_row_cache",
        "tablet_dynamic",
        "tablet_static",
        "uncompressed_block_cache",
        "versioned_chunk_meta",
        "reserved",
        "query"));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCellBalancer

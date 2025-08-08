#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/scheduler/packing_detail.h>

#include <yt/yt/core/profiling/public.h>

namespace NYT::NScheduler {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TFairSharePackingTest
    : public testing::Test
{
protected:
    TFairSharePackingTest() = default;

    TDiskQuota CreateDiskQuota(i64 diskSpace)
    {
        TDiskQuota diskQuota;
        diskQuota.DiskSpacePerMedium[NChunkClient::DefaultSlotsMediumIndex] = diskSpace;
        return diskQuota;
    }

    static constexpr double ABS_ERROR = 1e-8;
};

////////////////////////////////////////////////////////////////////////////////

class TFairSharePackingJobResourcesRatioTest
    : public TFairSharePackingTest
{ };

TEST_F(TFairSharePackingJobResourcesRatioTest, TestConstructor)
{
    TJobResources resources;
    resources.SetCpu(TCpuResource(40L));
    resources.SetUserSlots(200);
    resources.SetMemory(100_GB);
    resources.SetNetwork(100);

    TJobResources totalResources;
    totalResources.SetCpu(TCpuResource(100'000L));
    totalResources.SetUserSlots(350'000);
    totalResources.SetMemory(200'000_GB);
    totalResources.SetNetwork(150'000);

    TJobResourcesRatio ratio = ToRatio(resources, totalResources);
    EXPECT_NEAR(4e-4, ratio.GetCpuRatio(), ABS_ERROR);
    EXPECT_NEAR(5e-4, ratio.GetMemoryRatio(), ABS_ERROR);
}

TEST_F(TFairSharePackingJobResourcesRatioTest, TestOnes)
{
    EXPECT_NEAR(1, TJobResourcesRatio::Ones().GetCpuRatio(), ABS_ERROR);
    EXPECT_NEAR(1, TJobResourcesRatio::Ones().GetMemoryRatio(), ABS_ERROR);
}

TEST_F(TFairSharePackingJobResourcesRatioTest, TestZeros)
{
    EXPECT_NEAR(0, TJobResourcesRatio::Zeros().GetCpuRatio(), ABS_ERROR);
    EXPECT_NEAR(0, TJobResourcesRatio::Zeros().GetMemoryRatio(), ABS_ERROR);
}

////////////////////////////////////////////////////////////////////////////////

class TFairSharePackingAnglePackingMetricTest
    : public TFairSharePackingTest
{
protected:
    static TJobResources CreateResourceVector(TCpuResource cpu, i64 memory, int network, int userSlots)
    {
        TJobResources resources;

        resources.SetCpu(TCpuResource(cpu));
        resources.SetMemory(memory);
        resources.SetNetwork(network);
        resources.SetUserSlots(userSlots);

        return resources;
    }

    static TJobResourcesWithQuota CreateJobResourceLimits(TCpuResource cpu, i64 memory, int network)
    {
        return TJobResourcesWithQuota(CreateResourceVector(cpu, memory, network, /*userSlots*/ 1));
    }
};

TEST_F(TFairSharePackingAnglePackingMetricTest, TestPerfectJobHasZeroMetricValue)
{
    auto totalResources = CreateResourceVector(TCpuResource(1000L), 2000_GB, /*network*/ 10000, /*userSlots*/ 10000);

    auto jobResources = CreateJobResourceLimits(TCpuResource(10L), 10_GB, /*network*/ 0);

    auto nodeLimits = CreateResourceVector(TCpuResource(100L), 150_GB, /*network*/ 100, /*userSlots*/ 200);
    auto nodeUsage = CreateResourceVector(
        nodeLimits.GetCpu() - TCpuResource(50L),
        nodeLimits.GetMemory() - 50_GB,
        nodeLimits.GetNetwork() - 100,
        nodeLimits.GetUserSlots() - 150);
    auto nodeResourcesSnapshot = TPackingNodeResourcesSnapshot(nodeUsage, nodeLimits, CreateDiskQuota(100));

    EXPECT_NEAR(0, AnglePackingMetric(nodeResourcesSnapshot, jobResources, totalResources), ABS_ERROR);
}

TEST_F(TFairSharePackingAnglePackingMetricTest, TestCompareDifferentAngles)
{
    auto totalResources = CreateResourceVector(TCpuResource(1000L), 2000_GB, /*network*/ 10000, /*userSlots*/ 10000);

    auto nodeLimits = CreateResourceVector(TCpuResource(500L), 1000_GB, /*network*/ 1000, /*userSlots*/ 2000);
    auto nodeUsage = CreateResourceVector(
        nodeLimits.GetCpu() - TCpuResource(120L),
        nodeLimits.GetMemory() - 10_GB,
        nodeLimits.GetNetwork() - 20,
        nodeLimits.GetUserSlots() - 50);
    auto nodeResourcesSnapshot = TPackingNodeResourcesSnapshot(nodeUsage, nodeLimits, CreateDiskQuota(100));

    EXPECT_LT(
        AnglePackingMetric(nodeResourcesSnapshot, CreateJobResourceLimits(TCpuResource(13L), 1_GB, /*network*/ 0), totalResources),
        AnglePackingMetric(nodeResourcesSnapshot, CreateJobResourceLimits(TCpuResource(11L), 1_GB, /*network*/ 0), totalResources));

    EXPECT_LT(
        AnglePackingMetric(nodeResourcesSnapshot, CreateJobResourceLimits(TCpuResource(12L), 1_GB, /*network*/ 0), totalResources),
        AnglePackingMetric(nodeResourcesSnapshot, CreateJobResourceLimits(TCpuResource(13L), 1_GB, /*network*/ 0), totalResources));

    EXPECT_LT(
        AnglePackingMetric(nodeResourcesSnapshot, CreateJobResourceLimits(TCpuResource(13L), 1_GB, /*network*/ 0), totalResources),
        AnglePackingMetric(nodeResourcesSnapshot, CreateJobResourceLimits(TCpuResource(14L), 1_GB, /*network*/ 0), totalResources));

    EXPECT_LT(
        AnglePackingMetric(nodeResourcesSnapshot, CreateJobResourceLimits(TCpuResource(13L), 1001_MB, /*network*/ 0), totalResources),
        AnglePackingMetric(nodeResourcesSnapshot, CreateJobResourceLimits(TCpuResource(13L), 1000_MB, /*network*/ 0), totalResources));

    EXPECT_NEAR(
        AnglePackingMetric(nodeResourcesSnapshot, CreateJobResourceLimits(TCpuResource(13L), 1000_MB, /*network*/ 0), totalResources),
        AnglePackingMetric(nodeResourcesSnapshot, CreateJobResourceLimits(TCpuResource(19.5), 1500_MB, /*network*/ 0), totalResources),
        ABS_ERROR);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NScheduler

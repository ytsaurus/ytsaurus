#include <yt/yt/library/vector_hdrf/resource_vector.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////

TEST(TResourceVectorTest, AdaptiveEpsilonUsesResourceQuanta)
{
    TJobResources totalLimits;
    totalLimits.SetUserSlots(20507);
    totalLimits.SetCpu(1532.88);
    totalLimits.SetGpu(1344);
    totalLimits.SetMemory(1_TB);
    totalLimits.SetNetwork(10'000);

    auto epsilon = TResourceVector::ComputeAdaptiveEpsilon(totalLimits);

    EXPECT_DOUBLE_EQ(0.001 / 20507, epsilon[EJobResourceType::UserSlots]);
    EXPECT_DOUBLE_EQ(0.00001 / 1532.88, epsilon[EJobResourceType::Cpu]);
    EXPECT_DOUBLE_EQ(0.001 / 1344, epsilon[EJobResourceType::Gpu]);
    EXPECT_DOUBLE_EQ(1e-9, epsilon[EJobResourceType::Memory]);
    EXPECT_DOUBLE_EQ(1e-7, epsilon[EJobResourceType::Network]);
}

TEST(TResourceVectorTest, AdaptiveEpsilonForGpuTreeSizes)
{
    for (int gpuCount : {1, 8, 100, 1344, 2000, 10'000, 999'999, 1'000'000}) {
        SCOPED_TRACE(gpuCount);
        TJobResources totalLimits;
        totalLimits.SetGpu(gpuCount);

        auto epsilon = TResourceVector::ComputeAdaptiveEpsilon(totalLimits);

        EXPECT_DOUBLE_EQ(0.001, epsilon[EJobResourceType::Gpu] * gpuCount);
    }

    TJobResources totalLimits;
    totalLimits.SetGpu(1'000'001);
    auto epsilon = TResourceVector::ComputeAdaptiveEpsilon(totalLimits);
    EXPECT_DOUBLE_EQ(1e-9, epsilon[EJobResourceType::Gpu]);
}

TEST(TResourceVectorTest, AdaptiveEpsilonWithZeroLimits)
{
    TJobResources totalLimits;
    auto epsilon = TResourceVector::ComputeAdaptiveEpsilon(totalLimits);
    for (auto resourceType : TEnumTraits<EJobResourceType>::GetDomainValues()) {
        EXPECT_DOUBLE_EQ(1e-9, epsilon[resourceType]);
    }

    totalLimits.SetCpu(2.0);
    totalLimits.SetUserSlots(1);
    epsilon = TResourceVector::ComputeAdaptiveEpsilon(totalLimits);

    EXPECT_DOUBLE_EQ(0.000005, epsilon[EJobResourceType::Cpu]);
    EXPECT_DOUBLE_EQ(0.001, epsilon[EJobResourceType::UserSlots]);
    EXPECT_DOUBLE_EQ(1e-9, epsilon[EJobResourceType::Gpu]);
    EXPECT_DOUBLE_EQ(1e-9, epsilon[EJobResourceType::Memory]);
    EXPECT_DOUBLE_EQ(1e-9, epsilon[EJobResourceType::Network]);
}

TEST(TResourceVectorTest, AdaptiveEpsilonLowerBound)
{
    TJobResources totalLimits;
    totalLimits.SetUserSlots(1'000'000'000);
    totalLimits.SetCpu(10'000'000);
    totalLimits.SetGpu(1'000'000'000);
    totalLimits.SetMemory(1_TB);
    totalLimits.SetNetwork(1'000'000'000);

    auto epsilon = TResourceVector::ComputeAdaptiveEpsilon(totalLimits);

    for (auto resourceType : TEnumTraits<EJobResourceType>::GetDomainValues()) {
        EXPECT_DOUBLE_EQ(1e-9, epsilon[resourceType]);
    }
    EXPECT_DOUBLE_EQ(1.0, epsilon[EJobResourceType::Gpu] * totalLimits.GetGpu());
    EXPECT_GT(epsilon[EJobResourceType::Memory] * totalLimits.GetMemory(), 1.0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

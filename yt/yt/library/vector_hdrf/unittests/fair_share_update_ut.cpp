#include <yt/yt/library/vector_hdrf/unittests/mock/fair_share_update_mock.h>

#include <yt/yt/library/vector_hdrf/private.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/resource/resource.h>

#include <util/stream/file.h>

namespace NYT::NVectorHdrf {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYsonString ReadTestData(TStringBuf fileName)
{
    return TYsonString(NResource::Find(TString("/data/") + fileName));
}

////////////////////////////////////////////////////////////////////////////////

//! The share of a whole cluster, so that a share of |ratio| in every meaningful component is |Unit * ratio|.
const TResourceVector Unit = {1.0, 1.0, 0.0, 1.0, 0.0};

////////////////////////////////////////////////////////////////////////////////

class TFairShareUpdateTest
    : public testing::Test
{
protected:
    TRootElementMockPtr CreateRootElement()
    {
        return New<TRootElementMock>();
    }

    TPoolElementMockPtr CreateSimplePool(
        std::string id,
        std::optional<double> strongGuaranteeCpu = std::nullopt,
        double weight = 1.0)
    {
        auto strongGuaranteeResourcesConfig = New<TTestJobResourcesConfig>();
        strongGuaranteeResourcesConfig->Cpu = strongGuaranteeCpu;

        auto pool = New<TPoolElementMock>(std::move(id));
        pool->SetStrongGuaranteeResourcesConfig(strongGuaranteeResourcesConfig);
        pool->SetWeight(weight);
        return pool;
    }

    TPoolElementMockPtr CreateIntegralPool(
        std::string id,
        EIntegralGuaranteeType type,
        double flowCpu,
        std::optional<double> burstCpu = std::nullopt,
        std::optional<double> strongGuaranteeCpu = std::nullopt,
        double weight = 1.0)
    {
        auto strongGuaranteeResourcesConfig = New<TTestJobResourcesConfig>();
        strongGuaranteeResourcesConfig->Cpu = strongGuaranteeCpu;

        auto integralGuaranteesConfig = New<TPoolIntegralGuaranteesConfig>();
        integralGuaranteesConfig->GuaranteeType = type;
        integralGuaranteesConfig->ResourceFlow->Cpu = flowCpu;
        if (burstCpu) {
            integralGuaranteesConfig->BurstGuaranteeResources->Cpu = *burstCpu;
        }

        auto pool = New<TPoolElementMock>(std::move(id));
        pool->SetStrongGuaranteeResourcesConfig(strongGuaranteeResourcesConfig);
        pool->SetWeight(weight);
        pool->IntegralGuaranteesConfig() = integralGuaranteesConfig;
        return pool;
    }

    TPoolElementMockPtr CreateBurstPool(
        std::string id,
        double flowCpu,
        double burstCpu,
        std::optional<double> strongGuaranteeCpu = std::nullopt,
        double weight = 1.0)
    {
        return CreateIntegralPool(
            std::move(id),
            EIntegralGuaranteeType::Burst,
            flowCpu,
            burstCpu,
            strongGuaranteeCpu,
            weight);
    }

    TPoolElementMockPtr CreateRelaxedPool(
        std::string id,
        double flowCpu,
        std::optional<double> strongGuaranteeCpu = std::nullopt,
        double weight = 1.0)
    {
        return CreateIntegralPool(
            std::move(id),
            EIntegralGuaranteeType::Relaxed,
            flowCpu,
            /*burstCpu*/ std::nullopt,
            strongGuaranteeCpu,
            weight);
    }

    TOperationElementMockPtr CreateOperation(std::string id)
    {
        return New<TOperationElementMock>(id);
    }

    TOperationElementMockPtr CreateOperation(
        TCompositeElementMock* parent,
        const TJobResources& resourceDemand = {},
        const TJobResources& resourceUsage = {})
    {
        auto operation = New<TOperationElementMock>(Format("Operation%v", OperationIndex_++));
        operation->SetResourceDemand(resourceDemand);
        operation->SetResourceUsage(resourceUsage);
        operation->AttachParent(parent);
        return operation;
    }

    TOperationElementMockPtr CreateGangOperation(
        TCompositeElementMock* parent,
        const TJobResources& resourceDemand = {},
        const TJobResources& resourceUsage = {})
    {
        auto operation = CreateOperation(parent, resourceDemand, resourceUsage);
        operation->SetGangFlag(true);
        return operation;
    }

    TJobResources CreateTotalResourceLimitsWith100CPU()
    {
        TJobResources totalResourceLimits;
        totalResourceLimits.SetUserSlots(100);
        totalResourceLimits.SetCpu(100);
        totalResourceLimits.SetMemory(1000_MB);
        return totalResourceLimits;
    }

    TResourceVolume GetHugeVolume()
    {
        TResourceVolume hugeVolume;
        hugeVolume.SetCpu(TCpuResource(10000000000L));
        hugeVolume.SetUserSlots(10000000000);
        hugeVolume.SetMemory(10000000000_MB);
        return hugeVolume;
    }

    TJobResources GetOnePercentOfCluster()
    {
        TJobResources onePercentOfCluster;
        onePercentOfCluster.SetCpu(1);
        onePercentOfCluster.SetUserSlots(1);
        onePercentOfCluster.SetMemory(10_MB);
        return onePercentOfCluster;
    }

    virtual TTestFairShareUpdateOptions GetOptions()
    {
        return TTestFairShareUpdateOptions{};
    }

    TFairShareUpdateContext DoFairShareUpdate(
        const TJobResources& totalResourceLimits,
        const TRootElementMockPtr& rootElement,
        const TTestFairShareUpdateOptions& testOptions)
    {
        ResetFairShareFunctionsRecursively(rootElement.Get());

        TFairShareUpdateContext context(
            TFairShareUpdateOptions{
                .MainResource = testOptions.MainResource,
                .IntegralPoolCapacitySaturationPeriod = TDuration::Days(1),
                .IntegralSmoothPeriod = TDuration::Minutes(1),
                .EnableStepFunctionForGangOperations = testOptions.EnableStepFunctionForGangOperations,
                .EnableFifoChildrenReorderingForGuaranteeUtilization =
                    testOptions.EnableFifoChildrenReorderingForGuaranteeUtilization,
                .EnableImprovedFairShareByFitFactorComputation = testOptions.EnableImprovedFairShareByFitFactorComputation,
                .EnableImprovedFairShareByFitFactorComputationDistributionGap =
                    testOptions.EnableImprovedFairShareByFitFactorComputationDistributionGap,
                .EnableFastFifoFairShareByFitFactorComputation =
                    testOptions.EnableFastFifoFairShareByFitFactorComputation,
            },
            totalResourceLimits,
            testOptions.Now,
            testOptions.PreviousUpdateTime);

        rootElement->PreUpdate(totalResourceLimits);

        TFairShareUpdateExecutor updateExecutor(rootElement, &context);
        updateExecutor.Run();

        return context;
    }

    TFairShareUpdateContext DoFairShareUpdate(
        const TJobResources& totalResourceLimits,
        const TRootElementMockPtr& rootElement)
    {
        return DoFairShareUpdate(totalResourceLimits, rootElement, GetOptions());
    }

    // TODO(eshcherbin): Remove this method in favour of the previous one.
    TFairShareUpdateContext DoFairShareUpdate(
        const TJobResources& totalResourceLimits,
        const TRootElementMockPtr& rootElement,
        TInstant now,
        std::optional<TInstant> previousUpdateTime = std::nullopt)
    {
        auto options = GetOptions();
        options.Now = now;
        options.PreviousUpdateTime = previousUpdateTime;
        return DoFairShareUpdate(totalResourceLimits, rootElement, options);
    }

private:
    int OperationIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TFairShareUpdateParametrizedTest
    : public TFairShareUpdateTest
    , public testing::WithParamInterface<bool>
{
    TTestFairShareUpdateOptions GetOptions() override
    {
        auto options = TTestFairShareUpdateOptions{};
        options.EnableImprovedFairShareByFitFactorComputation = GetParam();
        return options;
    }
};

INSTANTIATE_TEST_SUITE_P(
    DisableImprovedFairShareByFitFactorComputation,
    TFairShareUpdateParametrizedTest,
    ::testing::Values(false));

INSTANTIATE_TEST_SUITE_P(
    EnableImprovedFairShareByFitFactorComputation,
    TFairShareUpdateParametrizedTest,
    ::testing::Values(true));

////////////////////////////////////////////////////////////////////////////////

MATCHER_P2(ResourceVectorNear, vec, absError, "") {
    return TResourceVector::Near(arg, vec, absError);
}

#define EXPECT_RV_NEAR(vector1, vector2) \
    EXPECT_THAT(vector2, ResourceVectorNear(vector1, 1e-7))

MATCHER_P2(ResourceVolumeNear, vec, absError, "") {
    bool result = true;
    TResourceVolume::ForEachResource([&] (EJobResourceType /*resourceType*/, auto TResourceVolume::* resourceDataMember) {
        result = result && std::abs(static_cast<double>(arg.*resourceDataMember - vec.*resourceDataMember)) < absError;
    });
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TFairShareUpdateParametrizedTest, TestSimple)
{
    constexpr int OperationCount = 4;

    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto poolA = CreateSimplePool("PoolA");
    auto poolB = CreateSimplePool("PoolB");
    auto poolC = CreateSimplePool("PoolC");
    auto poolD = CreateSimplePool("PoolD");

    poolC->SetMode(ESchedulingMode::Fifo);
    poolD->SetMode(ESchedulingMode::Fifo);

    poolA->AttachParent(rootElement.Get());
    poolB->AttachParent(rootElement.Get());
    poolC->AttachParent(rootElement.Get());
    poolD->AttachParent(rootElement.Get());

    TJobResources operationDemand;
    operationDemand.SetUserSlots(10);
    operationDemand.SetCpu(10);
    operationDemand.SetMemory(100);

    std::array<TOperationElementMockPtr, OperationCount> operations;
    for (int i = 0; i < OperationCount; ++i) {
        TCompositeElementMock* parent = i < 2
            ? poolA.Get()
            : poolC.Get();
        operations[i] = CreateOperation(parent, operationDemand);

        if (i == 2) {
            // We need this to ensure FIFO order of operations 2 and 3.
            operations[i]->SetWeight(10.0);
        }
    }

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        auto expectedOperationDemand = TResourceVector::FromJobResources(operationDemand, totalResourceLimits);
        auto poolExpectedDemand = expectedOperationDemand * (OperationCount / 2.0);
        auto totalExpectedDemand = expectedOperationDemand * OperationCount;

        EXPECT_THAT(totalExpectedDemand, ResourceVectorNear(rootElement->Attributes().DemandShare, 1e-7));
        EXPECT_THAT(poolExpectedDemand, ResourceVectorNear(poolA->Attributes().DemandShare, 1e-7));
        EXPECT_THAT(TResourceVector::Zero(), ResourceVectorNear(poolB->Attributes().DemandShare, 1e-7));
        EXPECT_THAT(poolExpectedDemand, ResourceVectorNear(poolC->Attributes().DemandShare, 1e-7));
        EXPECT_THAT(TResourceVector::Zero(), ResourceVectorNear(poolD->Attributes().DemandShare, 1e-7));
        for (const auto& operation : operations) {
            EXPECT_THAT(expectedOperationDemand, ResourceVectorNear(operation->Attributes().DemandShare, 1e-7));
        }

        EXPECT_THAT(totalExpectedDemand, ResourceVectorNear(rootElement->Attributes().FairShare.Total, 1e-7));
        EXPECT_THAT(poolExpectedDemand, ResourceVectorNear(poolA->Attributes().FairShare.Total, 1e-7));
        EXPECT_THAT(TResourceVector::Zero(), ResourceVectorNear(poolB->Attributes().FairShare.Total, 1e-7));
        EXPECT_THAT(poolExpectedDemand, ResourceVectorNear(poolC->Attributes().FairShare.Total, 1e-7));
        EXPECT_THAT(TResourceVector::Zero(), ResourceVectorNear(poolD->Attributes().FairShare.Total, 1e-7));
        for (const auto& operation : operations) {
            EXPECT_THAT(expectedOperationDemand, ResourceVectorNear(operation->Attributes().FairShare.Total, 1e-7));
        }
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestResourceLimits)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());

    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(poolA.Get());

    auto totalLimitsShare = TResourceVector::FromJobResources(totalResourceLimits, totalResourceLimits);
    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        EXPECT_EQ(totalLimitsShare, rootElement->Attributes().LimitsShare);
        EXPECT_EQ(totalLimitsShare, poolA->Attributes().LimitsShare);
        EXPECT_EQ(totalLimitsShare, poolB->Attributes().LimitsShare);
    }

    TJobResources poolAResourceLimits;
    poolAResourceLimits.SetUserSlots(60);
    poolAResourceLimits.SetCpu(70);
    poolAResourceLimits.SetMemory(800);

    poolA->SetResourceLimits(poolAResourceLimits);

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        EXPECT_EQ(totalLimitsShare, rootElement->Attributes().LimitsShare);

        auto poolALimitsShare = TResourceVector::FromJobResources(poolAResourceLimits, totalResourceLimits);
        EXPECT_EQ(poolALimitsShare, poolA->Attributes().LimitsShare);

        EXPECT_EQ(totalLimitsShare, poolB->Attributes().LimitsShare);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestFractionalResourceLimits)
{
    TJobResources totalResourceLimits;
    totalResourceLimits.SetUserSlots(10);
    totalResourceLimits.SetCpu(11.17);
    totalResourceLimits.SetMemory(100_MB);

    auto rootElement = CreateRootElement();

    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());

    TJobResources poolResourceLimits;
    poolResourceLimits.SetUserSlots(10);
    poolResourceLimits.SetCpu(11.06);
    poolResourceLimits.SetMemory(99_MB);
    poolA->SetResourceLimits(poolResourceLimits);

    auto totalLimitsShare = TResourceVector::FromJobResources(totalResourceLimits, totalResourceLimits);
    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        EXPECT_EQ(totalLimitsShare, rootElement->Attributes().LimitsShare);

        auto poolLimitsShare = TResourceVector::FromJobResources(poolResourceLimits, totalResourceLimits);
        EXPECT_EQ(poolLimitsShare, poolA->Attributes().LimitsShare);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestEmptyTree)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values
    EXPECT_EQ(TResourceVector::Zero(), rootElement->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector::Zero(), poolA->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector::Zero(), poolB->Attributes().FairShare.Total);
}

TEST_P(TFairShareUpdateParametrizedTest, TestOneLargeOperation)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create operation with demand larger than the available resources
    TJobResources resourceDemand;
    resourceDemand.SetUserSlots(200);
    resourceDemand.SetCpu(200);
    resourceDemand.SetMemory(4000_MB);

    auto operationX = CreateOperation(poolA.Get(), resourceDemand);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values
    TResourceVector expectedFairShare = {0.5, 0.5, 0.0, 1.0, 0.0};
    EXPECT_EQ(expectedFairShare, rootElement->Attributes().FairShare.Total);
    EXPECT_EQ(expectedFairShare, poolA->Attributes().FairShare.Total);
    EXPECT_EQ(expectedFairShare, operationX->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector::Zero(), poolB->Attributes().FairShare.Total);
}

TEST_P(TFairShareUpdateParametrizedTest, TestOneSmallOperation)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create operation with demand smaller than the available resources
    TJobResources resourceDemand;
    resourceDemand.SetUserSlots(30);
    resourceDemand.SetCpu(30);
    resourceDemand.SetMemory(600_MB);

    auto operationX = CreateOperation(poolA.Get(), resourceDemand);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.6, 0.0}), rootElement->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.6, 0.0}), poolA->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.6, 0.0}), operationX->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector::Zero(), poolB->Attributes().FairShare.Total);
}

TEST_P(TFairShareUpdateParametrizedTest, TestTwoComplementaryOperations)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    TJobResources resourceDemandX;
    resourceDemandX.SetUserSlots(100);
    resourceDemandX.SetCpu(100);
    resourceDemandX.SetMemory(2000_MB);

    auto operationX = CreateOperation(poolA.Get(), resourceDemandX);

    // Second operation with symmetric resource demand
    TJobResources resourceDemandY;
    resourceDemandY.SetUserSlots(100);
    resourceDemandY.SetCpu(200);
    resourceDemandY.SetMemory(1000_MB);

    auto operationY = CreateOperation(poolA.Get(), resourceDemandY);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values
    EXPECT_RV_NEAR(TResourceVector({2.0 / 3, 1.0, 0.0, 1.0, 0.0}), rootElement->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector({2.0 / 3, 1.0, 0.0, 1.0, 0.0}), poolA->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector({1.0 / 3, 1.0 / 3, 0.0, 2.0 / 3, 0.0}), operationX->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector({1.0 / 3, 2.0 / 3, 0.0, 1.0 / 3, 0.0}), operationY->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector::Zero(), poolB->Attributes().FairShare.Total);
}

TEST_P(TFairShareUpdateParametrizedTest, TestComplexCase)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create an operation with resource demand proportion <1, 2> and small jobCount in PoolA
    TJobResources resourceDemandX;
    resourceDemandX.SetUserSlots(10);
    resourceDemandX.SetCpu(10);
    resourceDemandX.SetMemory(200_MB);

    auto operationX = CreateOperation(poolA.Get(), resourceDemandX);

    // Create an operation with resource demand proportion <3, 1> and large jobCount in PoolA
    TJobResources resourceDemandY;
    resourceDemandY.SetUserSlots(1000);
    resourceDemandY.SetCpu(3000);
    resourceDemandY.SetMemory(10000_MB);

    auto operationY = CreateOperation(poolA.Get(), resourceDemandY);

    // Create operation with resource demand proportion <1, 5> and large jobCount in PoolB
    TJobResources resourceDemandZ;
    resourceDemandZ.SetUserSlots(2000);
    resourceDemandZ.SetCpu(2000);
    resourceDemandZ.SetMemory(100000_MB);

    auto operationZ = CreateOperation(poolB.Get(), resourceDemandZ);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values

    // Memory will be saturated first (see the usages of operations bellow)
    EXPECT_RV_NEAR(rootElement->Attributes().FairShare.Total, TResourceVector({16.0 / 40, 30.0 / 40, 0.0, 40.0 / 40, 0.0}));
    EXPECT_RV_NEAR(poolA->Attributes().FairShare.Total, TResourceVector({11.0 / 40, 25.0 / 40, 0.0, 15.0 / 40, 0.0}));
    EXPECT_RV_NEAR(poolB->Attributes().FairShare.Total, TResourceVector({5.0 / 40, 5.0 / 40, 0.0, 25.0 / 40, 0.0}));

    // operation1 uses 4/40 CPU and 8/40 Memory
    EXPECT_RV_NEAR(operationX->Attributes().FairShare.Total, TResourceVector({4.0 / 40, 4.0 / 40, 0.0, 8.0 / 40, 0.0}));
    // operation2 uses 21/40 CPU and 7/40 Memory
    EXPECT_RV_NEAR(operationY->Attributes().FairShare.Total, TResourceVector({7.0 / 40, 21.0 / 40, 0.0, 7.0 / 40, 0.0}));
    // operation3 uses 5/40 CPU and 25/40 Memory
    EXPECT_RV_NEAR(operationZ->Attributes().FairShare.Total, TResourceVector({5.0 / 40, 5.0 / 40, 0.0, 25.0 / 40, 0.0}));
}

TEST_P(TFairShareUpdateParametrizedTest, TestNonContinuousFairShare)
{
    TJobResources totalResourceLimits;
    totalResourceLimits.SetUserSlots(100'000);
    totalResourceLimits.SetCpu(100);
    totalResourceLimits.SetMemory(100_GB);
    totalResourceLimits.SetNetwork(100);

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    // Create an operation with resource demand proportion <1, 1, 4>, weight=10, and small jobCount in PoolA
    TJobResources resourceDemandX;
    resourceDemandX.SetUserSlots(10);
    resourceDemandX.SetCpu(10);
    resourceDemandX.SetMemory(10_GB);
    resourceDemandX.SetNetwork(40);

    auto operationX = CreateOperation(poolA.Get(), resourceDemandX);
    operationX->SetWeight(10.0);

    // Create an operation with resource demand proportion <1, 1, 0>, weight=1, and large jobCount in PoolA
    TJobResources resourceDemandY;
    resourceDemandY.SetUserSlots(1000);
    resourceDemandY.SetCpu(1000);
    resourceDemandY.SetMemory(1000_GB);
    resourceDemandY.SetNetwork(0);

    auto operationY = CreateOperation(poolA.Get(), resourceDemandY);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values

    // Memory will be saturated first (see the usages of operations bellow)
    EXPECT_RV_NEAR(rootElement->Attributes().FairShare.Total, TResourceVector({0.001, 1.0, 0.0, 1.0, 0.4}));
    EXPECT_RV_NEAR(poolA->Attributes().FairShare.Total, TResourceVector({0.001, 1.0, 0.0, 1.0, 0.4}));
    EXPECT_RV_NEAR(poolB->Attributes().FairShare.Total, TResourceVector::Zero());

    // operation1 uses 0.1 CPU, 0.1 Memory, and 0.4 Network
    EXPECT_RV_NEAR(operationX->Attributes().FairShare.Total, TResourceVector({0.0001, 0.1, 0.0, 0.1, 0.4}));
    // operation2 uses 0.9 CPU, 0.9 Memory, and 0 Network
    EXPECT_RV_NEAR(operationY->Attributes().FairShare.Total, TResourceVector({0.0009, 0.9, 0.0, 0.9, 0.0}));
}

TEST_P(TFairShareUpdateParametrizedTest, TestNonContinuousFairShareFunctionIsLeftContinuous)
{
    // Create a cluster with 1 large node.
    TJobResources totalResourceLimits;
    totalResourceLimits.SetUserSlots(100'000);
    totalResourceLimits.SetCpu(100);
    totalResourceLimits.SetMemory(100_GB);
    totalResourceLimits.SetNetwork(100);

    // Create a tree with 2 pools.
    auto rootElement = CreateRootElement();
    // Use fake root to be able to set a CPU limit.
    auto fakeRootElement = CreateSimplePool("FakeRoot");
    fakeRootElement->AttachParent(rootElement.Get());
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(fakeRootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(fakeRootElement.Get());

    // Set CPU limit for fake root.
    fakeRootElement->SetResourceLimits(CreateCpuResourceLimits(40.0));

    // Create an operation with resource demand proportion <1, 1, 4>, weight=10, and small jobCount in PoolA.
    TJobResources resourceDemandX;
    resourceDemandX.SetUserSlots(10);
    resourceDemandX.SetCpu(10);
    resourceDemandX.SetMemory(10_GB);
    resourceDemandX.SetNetwork(40);

    auto operationX = CreateOperation(poolA.Get(), resourceDemandX);
    operationX->SetWeight(10.0);

    // Create an operation with resource demand proportion <1, 1, 0>, weight=1, and large jobCount in PoolA.
    TJobResources resourceDemandY;
    resourceDemandY.SetUserSlots(1000);
    resourceDemandY.SetCpu(1000);
    resourceDemandY.SetMemory(1000_GB);
    resourceDemandY.SetNetwork(0);

    auto operationY = CreateOperation(poolA.Get(), resourceDemandY);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    // Check the values.
    // 0.4 is a discontinuity point of root's FSBS, so the amount of fair share given to poolA equals to
    // the left limit of FSBS at 0.4, even though we have enough resources to allocate the right limit at 0.4.
    // This is a fundamental property of our strategy.
    EXPECT_RV_NEAR(rootElement->Attributes().FairShare.Total, TResourceVector({0.00014, 0.14, 0.0, 0.14, 0.4}));
    EXPECT_RV_NEAR(fakeRootElement->Attributes().FairShare.Total, TResourceVector({0.00014, 0.14, 0.0, 0.14, 0.4}));
    EXPECT_RV_NEAR(poolA->Attributes().FairShare.Total, TResourceVector({0.00014, 0.14, 0.0, 0.14, 0.4}));
    EXPECT_RV_NEAR(poolB->Attributes().FairShare.Total, TResourceVector::Zero());

    // Operation 1 uses 0.1 CPU, 0.1 Memory, and 0.4 Network.
    EXPECT_RV_NEAR(operationX->Attributes().FairShare.Total, TResourceVector({0.0001, 0.1, 0.0, 0.1, 0.4}));
    // Operation 2 uses 0.04 CPU, 0.04 Memory, and 0.0 Network.
    EXPECT_RV_NEAR(operationY->Attributes().FairShare.Total, TResourceVector({0.00004, 0.04, 0.0, 0.04, 0.0}));
}

TEST_P(TFairShareUpdateParametrizedTest, TestImpreciseComposition)
{
    // NB: This test is reconstructed from a core dump. Don't be surprised by precise resource demands. See YT-13864.

    // Create a cluster with 1 large node.
    TJobResources totalResourceLimits;
    totalResourceLimits.SetUserSlots(3);
    totalResourceLimits.SetCpu(3);
    totalResourceLimits.SetMemory(8316576848);

    auto rootElement = CreateRootElement();

    auto pool = CreateSimplePool("Pool", /*strongGuaranteeCpu*/ 3.0);
    pool->AttachParent(rootElement.Get());

    TJobResources jobResourcesX;
    jobResourcesX.SetUserSlots(2);
    jobResourcesX.SetCpu(2);
    jobResourcesX.SetMemory(805306368);
    jobResourcesX.SetNetwork(0);

    auto operationA = CreateOperation(pool.Get());

    TJobResources resourceDemandB;
    resourceDemandB.SetUserSlots(3);
    resourceDemandB.SetCpu(3);
    resourceDemandB.SetMemory(1207959552);

    auto operationB = CreateOperation(pool.Get(), resourceDemandB);

    operationA->IncreaseResourceUsageAndDemand(jobResourcesX);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    EXPECT_FALSE(Dominates(TResourceVector::Ones(), pool->Attributes().FairShare.Total));
}

TEST_P(TFairShareUpdateParametrizedTest, TestPromisedGuaranteeFairShare)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("poolA", /*strongGuaranteeCpu*/ 30.0);
    auto poolA1 = CreateSimplePool("poolA1", /*strongGuaranteeCpu*/ 10.0);
    auto poolA2 = CreateSimplePool("poolA2");
    auto poolA3 = CreateSimplePool("poolA3");
    auto poolB = CreateSimplePool("poolB", /*strongGuaranteeCpu*/ 30.0);
    poolA->AttachParent(rootElement.Get());
    poolA1->AttachParent(poolA.Get());
    poolA2->AttachParent(poolA.Get());
    poolA3->AttachParent(poolA.Get());
    poolB->AttachParent(rootElement.Get());

    poolA->SetPromisedGuaranteeFairShareComputationEnabled(true);
    // Test that only the uppermost pool's config takes effect.
    poolA2->SetPromisedGuaranteeFairShareComputationEnabled(true);

    auto largeResourceDemand = totalResourceLimits;
    auto operationA1 = CreateOperation(poolA1.Get(), largeResourceDemand);
    auto operationA2 = CreateOperation(poolA2.Get(), largeResourceDemand);
    auto operationB = CreateOperation(poolB.Get(), largeResourceDemand);

    TJobResources smallResourceDemand;
    smallResourceDemand.SetUserSlots(2);
    smallResourceDemand.SetCpu(2);
    smallResourceDemand.SetMemory(20_MB);

    auto operationA3 = CreateOperation(poolA3.Get(), smallResourceDemand);

    DoFairShareUpdate(totalResourceLimits, rootElement);

    EXPECT_RV_NEAR(Unit * 0.29, operationA1->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.19, operationA2->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.02, operationA3->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.5, operationB->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.5, poolA->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.29, poolA1->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.19, poolA2->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.02, poolA3->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.5, poolB->Attributes().FairShare.Total);

    EXPECT_RV_NEAR(Unit * 0.19, operationA1->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.09, operationA2->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.02, operationA3->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, operationB->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.3, poolA->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.19, poolA1->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.09, poolA2->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.02, poolA3->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, poolB->Attributes().PromisedGuaranteeFairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, rootElement->Attributes().PromisedGuaranteeFairShare.Total);
}

TEST_P(TFairShareUpdateParametrizedTest, TestNestedPromisedGuaranteeFairSharePools)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("poolA");
    auto poolA1 = CreateSimplePool("poolA1");
    poolA->AttachParent(rootElement.Get());
    poolA1->AttachParent(poolA.Get());

    poolA->SetPromisedGuaranteeFairShareComputationEnabled(true);
    poolA1->SetPromisedGuaranteeFairShareComputationEnabled(true);

    auto checkErrors = [&] (const auto& errors) {
        for (const auto& error : errors) {
            if (error.FindMatching(NVectorHdrf::EErrorCode::NestedPromisedGuaranteeFairSharePools)) {
                return true;
            }
        }
        return false;
    };

    {
        auto context = DoFairShareUpdate(totalResourceLimits, rootElement);
        EXPECT_TRUE(checkErrors(context.Errors));
    }

    poolA1->SetPromisedGuaranteeFairShareComputationEnabled(false);

    {
        auto context = DoFairShareUpdate(totalResourceLimits, rootElement);
        EXPECT_FALSE(checkErrors(context.Errors));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TFairShareUpdateParametrizedTest, TestRelaxedPoolFairShareSimple)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 10.0, /*strongGuaranteeCpu*/ 10.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto operation = CreateOperation(relaxedPool.Get(), totalResourceLimits * 0.3);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        EXPECT_RV_NEAR(Unit * 0.3, operation->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(Unit * 0.3, operation->Attributes().FairShare.Total);

        EXPECT_EQ(Unit * 0.1, relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.1, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.1, relaxedPool->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(Unit * 0.3, relaxedPool->Attributes().FairShare.Total);

        EXPECT_RV_NEAR(Unit * 0.1, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.1, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.1, rootElement->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(Unit * 0.3, rootElement->Attributes().FairShare.Total);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestRelaxedPoolWithIncreasedMultiplierLimit)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto defaultRelaxedPool = CreateRelaxedPool("defaultRelaxed", /*flowCpu*/ 10.0);
    defaultRelaxedPool->AttachParent(rootElement.Get());

    auto increasedLimitRelaxedPool = CreateRelaxedPool("increasedLimitRelaxed", /*flowCpu*/ 10.0);
    increasedLimitRelaxedPool->IntegralGuaranteesConfig()->RelaxedShareMultiplierLimit = 5.0;
    increasedLimitRelaxedPool->AttachParent(rootElement.Get());

    auto operationX = CreateOperation(defaultRelaxedPool.Get(), /*resourceDemand*/ totalResourceLimits);
    auto operationY = CreateOperation(increasedLimitRelaxedPool.Get(), /*resourceDemand*/ totalResourceLimits);

    defaultRelaxedPool->InitAccumulatedResourceVolume(GetHugeVolume());
    increasedLimitRelaxedPool->InitAccumulatedResourceVolume(GetHugeVolume());

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        // Default multiplier is 3.
        // NB: The guarantees are accumulated one flow unit at a time, so they land an ulp away from the
        // exact share and only a near comparison holds.
        EXPECT_RV_NEAR(Unit * 0.3, defaultRelaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.5, increasedLimitRelaxedPool->Attributes().FairShare.IntegralGuarantee);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestBurstPoolFairShareSimple)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 10.0, /*burstCpu*/ 10.0, /*strongGuaranteeCpu*/ 10.0);
    burstPool->AttachParent(rootElement.Get());

    auto operation = CreateOperation(burstPool.Get(), totalResourceLimits * 0.3);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        EXPECT_RV_NEAR(Unit * 0.3, operation->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(Unit * 0.3, operation->Attributes().FairShare.Total);

        EXPECT_EQ(Unit * 0.1, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.1, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.1, burstPool->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(Unit * 0.3, burstPool->Attributes().FairShare.Total);

        EXPECT_EQ(Unit * 0.1, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.1, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.1, rootElement->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(Unit * 0.3, rootElement->Attributes().FairShare.Total);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestAccumulatedVolumeProvidesMore)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 10.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto firstUpdateTime = TInstant::Now();
    {
        // Make first update to accumulate volume
        DoFairShareUpdate(
            totalResourceLimits,
            rootElement,
            /*now*/ firstUpdateTime,
            /*previousUpdateTime*/ firstUpdateTime - TDuration::Minutes(1));
    }

    auto operation = CreateOperation(relaxedPool.Get(), totalResourceLimits * 0.3);

    auto secondUpdateTime = firstUpdateTime + TDuration::Minutes(1);
    {
        DoFairShareUpdate(
            totalResourceLimits,
            rootElement,
            /*now*/ secondUpdateTime,
            /*previousUpdateTime*/ firstUpdateTime);

        EXPECT_RV_NEAR(Unit * 0.3, operation->Attributes().FairShare.WeightProportional);
        EXPECT_RV_NEAR(Unit * 0.3, operation->Attributes().FairShare.Total);

        EXPECT_EQ(TResourceVector::Zero(), relaxedPool->Attributes().FairShare.StrongGuarantee);
        // Here we get two times more share ratio than guaranteed by flow.
        EXPECT_RV_NEAR(Unit * 0.2, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.1, relaxedPool->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestStrongGuaranteePoolVsBurstPool)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 100.0, /*burstCpu*/ 50.0);
    burstPool->AttachParent(rootElement.Get());

    auto strongPool = CreateSimplePool("strong", /*strongGuaranteeCpu*/ 50.0);
    strongPool->AttachParent(rootElement.Get());

    auto burstOperation = CreateOperation(burstPool.Get(), totalResourceLimits);
    auto strongOperation = CreateOperation(strongPool.Get(), totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        EXPECT_RV_NEAR(Unit * 0.5, strongPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(Unit * 0.0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.5, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(Unit * 0.5, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.5, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestStrongGuaranteePoolVsRelaxedPool)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongPool = CreateSimplePool("strong", /*strongGuaranteeCpu*/ 50.0);
    strongPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 100.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto strongOperation = CreateOperation(strongPool.Get(), /*resourceDemand*/ totalResourceLimits);
    auto relaxedOperation = CreateOperation(relaxedPool.Get(), /*resourceDemand*/ totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        EXPECT_RV_NEAR(Unit * 0.5, strongPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(Unit * 0.0, relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.5, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(Unit * 0.5, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.5, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestBurstGetsAll_RelaxedNone)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 100.0, /*burstCpu*/ 100.0);
    burstPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 100.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto burstOperation = CreateOperation(burstPool.Get(), totalResourceLimits);
    auto relaxedOperation = CreateOperation(relaxedPool.Get(), totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        EXPECT_RV_NEAR(Unit * 0.0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 1.0, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(Unit * 0.0, relaxedPool->Attributes().FairShare.Total);

        EXPECT_RV_NEAR(Unit * 0.0, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 1.0, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestBurstGetsBurstGuaranteeOnly_RelaxedGetsRemaining)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 100.0, /*burstCpu*/ 50.0);
    burstPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 100);
    relaxedPool->AttachParent(rootElement.Get());

    auto burstOperation = CreateOperation(burstPool.Get(), totalResourceLimits);
    auto relaxedOperation = CreateOperation(relaxedPool.Get(), totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        EXPECT_RV_NEAR(Unit * 0.0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.5, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(Unit * 0.0, relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.5, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(Unit * 0.0, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 1.0, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestAllKindsOfPoolsShareWeightProportionalComponent)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongPool = CreateSimplePool("strong", /*strongGuaranteeCpu*/ 10.0);
    strongPool->AttachParent(rootElement.Get());

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 10.0, /*burstCpu*/ 10.0, /*strongGuaranteeCpu*/ 0.0);
    burstPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 10.0, /*strongGuaranteeCpu*/ 0.0, /*weight*/ 2.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto noGuaranteePool = CreateSimplePool("noGuarantee", /*strongGuaranteeCpu*/ 0.0, /*weight*/ 3.0);
    noGuaranteePool->AttachParent(rootElement.Get());

    auto strongOperation = CreateOperation(strongPool.Get(), totalResourceLimits);
    auto burstOperation = CreateOperation(burstPool.Get(), totalResourceLimits);
    auto relaxedOperation = CreateOperation(relaxedPool.Get(), totalResourceLimits);
    auto noGuaranteeOperation = CreateOperation(noGuaranteePool.Get(), totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        EXPECT_EQ(Unit * 0.1, strongPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.0, strongPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.1, strongPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(Unit * 0.0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.1, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.1, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(Unit * 0.0, relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.1, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.2, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(Unit * 0.0, noGuaranteePool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.0, noGuaranteePool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.3, noGuaranteePool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(Unit * 0.1, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.2, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.7, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestTwoRelaxedPoolsGetShareRatioProportionalToVolume)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto relaxedPoolA = CreateRelaxedPool("relaxedA", /*flowCpu*/ 100.0);
    relaxedPoolA->AttachParent(rootElement.Get());

    auto relaxedPoolB = CreateRelaxedPool("relaxedB", /*flowCpu*/ 100.0);
    relaxedPoolB->AttachParent(rootElement.Get());

    auto relaxedOperationA = CreateOperation(relaxedPoolA.Get(), totalResourceLimits);
    auto relaxedOperationB = CreateOperation(relaxedPoolB.Get(), totalResourceLimits);

    // 10% of cluster for 1 minute.
    auto volume1 = TResourceVolume(GetOnePercentOfCluster() * 10.0, TDuration::Minutes(1));
    // 30% of cluster for 1 minute.
    auto volume2 = TResourceVolume(GetOnePercentOfCluster() * 30.0, TDuration::Minutes(1));
    relaxedPoolA->InitAccumulatedResourceVolume(volume1);
    relaxedPoolB->InitAccumulatedResourceVolume(volume2);
    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        EXPECT_EQ(Unit * 0.0, relaxedPoolA->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.1, relaxedPoolA->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.3, relaxedPoolA->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(Unit * 0.0, relaxedPoolB->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.3, relaxedPoolB->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.3, relaxedPoolB->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(Unit * 0.0, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.4, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.6, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestStrongGuaranteeAdjustmentToTotalResources)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongPoolA = CreateSimplePool("strongA", /*strongGuaranteeCpu*/ 30.0);
    strongPoolA->AttachParent(rootElement.Get());

    auto strongPoolB = CreateSimplePool("strongB", /*strongGuaranteeCpu*/ 90.0);
    strongPoolB->AttachParent(rootElement.Get());

    auto strongOperationA = CreateOperation(strongPoolA.Get(), totalResourceLimits);
    auto strongOperationB = CreateOperation(strongPoolB.Get(), totalResourceLimits);

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        EXPECT_EQ(Unit * 0.25, strongPoolA->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongPoolA->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongPoolA->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(Unit * 0.75, strongPoolB->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.0, strongPoolB->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongPoolB->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestStrongGuaranteeAdjustmentToTotalResourcesWithLargePool)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongPoolA = CreateSimplePool("strongA", /*strongGuaranteeCpu*/ 30.0);
    strongPoolA->AttachParent(rootElement.Get());

    auto strongPoolB = CreateSimplePool("strongB", /*strongGuaranteeCpu*/ 120.0);
    strongPoolB->AttachParent(rootElement.Get());

    auto strongOperationA = CreateOperation(strongPoolA.Get(), totalResourceLimits);
    auto strongOperationB = CreateOperation(strongPoolB.Get(), totalResourceLimits);

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        EXPECT_EQ(Unit * 0.2, strongPoolA->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongPoolA->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongPoolA->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(Unit * 0.8, strongPoolB->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.0, strongPoolB->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongPoolB->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestStrongGuaranteePlusBurstGuaranteeAdjustmentToTotalResources)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongPool = CreateSimplePool("strong", /*strongGuaranteeCpu*/ 90.0);
    strongPool->AttachParent(rootElement.Get());

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 60.0, /*burstCpu*/ 60.0);
    burstPool->AttachParent(rootElement.Get());

    auto strongOperation = CreateOperation(strongPool.Get(), totalResourceLimits);
    auto burstOperation = CreateOperation(burstPool.Get(), totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        EXPECT_RV_NEAR(Unit * 0.6, strongPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(Unit * 0.0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.4, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, burstPool->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestLimitsLowerThanStrongGuarantee)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongPoolParent = CreateSimplePool("strongParent", /*strongGuaranteeCpu*/ 100.0);
    strongPoolParent->SetResourceLimits(CreateCpuResourceLimits(50.0));
    strongPoolParent->AttachParent(rootElement.Get());

    auto strongPoolChild = CreateSimplePool("strongChild", /*strongGuaranteeCpu*/ 100.0);
    strongPoolChild->AttachParent(strongPoolParent.Get());

    auto operation = CreateOperation(strongPoolChild.Get(), /*resourceDemand*/ totalResourceLimits);

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        EXPECT_EQ(Unit * 0.5, strongPoolParent->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.5, strongPoolParent->Attributes().FairShare.Total);

        EXPECT_EQ(Unit * 0.5, strongPoolChild->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.5, strongPoolChild->Attributes().FairShare.Total);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestParentWithoutGuaranteeAndHisLimitsLowerThanChildBurstShare)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto limitedParent = CreateSimplePool("limitedParent", /*strongGuaranteeCpu*/ 0.0);
    limitedParent->SetResourceLimits(CreateCpuResourceLimits(50.0));
    limitedParent->AttachParent(rootElement.Get());

    auto burstChild = CreateBurstPool("burst", /*flowCpu*/ 100.0, /*burstCpu*/ 100.0, /*strongGuaranteeCpu*/ 0.0);
    burstChild->AttachParent(limitedParent.Get());

    auto operation = CreateOperation(burstChild.Get(), /*resourceDemand*/ totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        EXPECT_EQ(Unit * 0.5, burstChild->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.5, burstChild->Attributes().FairShare.Total);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestParentWithStrongGuaranteeAndHisLimitsLowerThanChildBurstShare)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto limitedParent = CreateSimplePool("limitedParent", /*strongGuaranteeCpu*/ 50.0);
    limitedParent->SetResourceLimits(CreateCpuResourceLimits(50.0));
    limitedParent->AttachParent(rootElement.Get());

    auto burstChild = CreateBurstPool("burst", /*flowCpu*/ 10.0, /*burstCpu*/ 10.0, /*strongGuaranteeCpu*/ 0.0);
    burstChild->AttachParent(limitedParent.Get());

    auto operation = CreateOperation(burstChild.Get(), /*resourceDemand*/ totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        EXPECT_EQ(Unit * 0.0, burstChild->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.0, burstChild->Attributes().FairShare.IntegralGuarantee);  // Integral share wasn't given due to violation of parent limits.
        EXPECT_RV_NEAR(Unit * 0.5, burstChild->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestStrongGuaranteeAndRelaxedPoolVsRelaxedPool)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto strongAndRelaxedPool = CreateRelaxedPool("strong_and_relaxed", /*flowCpu*/ 100.0, /*strongGuaranteeCpu*/ 40.0);
    strongAndRelaxedPool->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 100.0);
    relaxedPool->AttachParent(rootElement.Get());

    auto strongAndRelaxedOperation = CreateOperation(strongAndRelaxedPool.Get(), /*resourceDemand*/ totalResourceLimits);
    auto relaxedOperation = CreateOperation(relaxedPool.Get(), /*resourceDemand*/ totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        EXPECT_EQ(Unit * 0.4, strongAndRelaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.3, strongAndRelaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, strongAndRelaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(Unit * 0.0, relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.3, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(Unit * 0.4, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_RV_NEAR(Unit * 0.6, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, EstimatedGuaranteeShareIgnoresIntegralPools)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto burstPoolParent = CreateSimplePool("burstParent");
    burstPoolParent->AttachParent(rootElement.Get());

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 30.0, /*burstCpu*/ 100.0);
    burstPool->AttachParent(burstPoolParent.Get());

    auto relaxedPoolParent = CreateSimplePool("relaxedParent");
    relaxedPoolParent->AttachParent(rootElement.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 70.0);
    relaxedPool->AttachParent(relaxedPoolParent.Get());

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        EXPECT_EQ(TResourceVector{}, burstPool->Attributes().EstimatedGuaranteeShare);
        EXPECT_EQ(TResourceVector{}, burstPoolParent->Attributes().EstimatedGuaranteeShare);
        EXPECT_EQ(TResourceVector{}, relaxedPool->Attributes().EstimatedGuaranteeShare);
        EXPECT_EQ(TResourceVector{}, relaxedPoolParent->Attributes().EstimatedGuaranteeShare);
        EXPECT_EQ(TResourceVector{}, rootElement->Attributes().EstimatedGuaranteeShare);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestIntegralPoolsWithParent)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto limitedParent = CreateIntegralPool("parent", EIntegralGuaranteeType::None, /*flowCpu*/ 100.0, /*burstCpu*/ 100.0);
    limitedParent->AttachParent(rootElement.Get());

    auto burstPool = CreateBurstPool("burst", /*flowCpu*/ 50.0, /*burstCpu*/ 100.0);
    burstPool->AttachParent(limitedParent.Get());

    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 50.0);
    relaxedPool->AttachParent(limitedParent.Get());

    auto burstOperation = CreateOperation(burstPool.Get(), totalResourceLimits);
    auto relaxedOperation = CreateOperation(relaxedPool.Get(), totalResourceLimits);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(
            totalResourceLimits,
            rootElement,
            now,
            now - TDuration::Minutes(1));

        EXPECT_EQ(Unit * 0.0, burstPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.5, burstPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, burstPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(Unit * 0.0, relaxedPool->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.5, relaxedPool->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, relaxedPool->Attributes().FairShare.WeightProportional);

        EXPECT_EQ(Unit * 0.0, limitedParent->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 1.0, limitedParent->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, limitedParent->Attributes().FairShare.WeightProportional);

        EXPECT_RV_NEAR(Unit * 0.0, rootElement->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 1.0, rootElement->Attributes().FairShare.IntegralGuarantee);
        EXPECT_EQ(Unit * 0.0, rootElement->Attributes().FairShare.WeightProportional);
    }
}

TEST_P(TFairShareUpdateParametrizedTest, TestProposedIntegralSharePrecisionError)
{
    // This test is based on real circumstances, nothing below is random or weird.
    // It works, and this is the most important thing. Enjoy.
    // See: YT-16653.
    TJobResources totalResourceLimits;
    totalResourceLimits.SetUserSlots(2404350);
    totalResourceLimits.SetCpu(285040.54);
    totalResourceLimits.SetMemory(1139022499379170);
    totalResourceLimits.SetNetwork(534300);
    totalResourceLimits.SetGpu(0);

    auto rootElement = CreateRootElement();
    auto integralRootPool = CreateSimplePool("integralRoot", /*strongGuaranteeCpu*/ 473159.00);
    auto burstPool = CreateBurstPool(
        "burstPool",
        /*flowCpu*/ 525.0,
        /*burstCpu*/ 3150.0,
        /*strongGuaranteeCpu*/ 0.0);
    auto firstRelaxedPool = CreateRelaxedPool(
        "firstRelaxedPool",
        /*flowCpu*/ 3500.0,
        /*burstCpu*/ {},
        /*strongGuaranteeCpu*/ 5158.0);

    auto secondRelaxedPool = CreateRelaxedPool(
        "secondRelaxedPool",
        // This is the only random value in this test.
        /*flowCpu*/ 117.0);

    // We need these two additional pools for correct guarantee adjustments.
    auto normalPool = CreateSimplePool("normalPool", /*strongGuaranteeCpu*/ 23071.0);
    auto fakeBurstPool = CreateBurstPool(
        "fakeBurstPool",
        /*flowCpu*/ 0.0,
        /*burstCpu*/ 11700.0);

    integralRootPool->AttachParent(rootElement.Get());
    burstPool->AttachParent(integralRootPool.Get());
    firstRelaxedPool->AttachParent(integralRootPool.Get());
    secondRelaxedPool->AttachParent(integralRootPool.Get());
    normalPool->AttachParent(rootElement.Get());
    fakeBurstPool->AttachParent(rootElement.Get());

    integralRootPool->SetResourceLimits(CreateCpuResourceLimits(50000.00));

    // Don't think we need exact amounts, because any large enough accumulated volume should work.
    TResourceVolume burstPoolAccumulatedVolume;
    burstPoolAccumulatedVolume.SetUserSlots(413098315.744941);
    burstPoolAccumulatedVolume.SetCpu(TCpuResource(45360000.00));
    burstPoolAccumulatedVolume.SetMemory(2.1341920521797664e+17);
    burstPoolAccumulatedVolume.SetNetwork(91794223.939665511);
    burstPoolAccumulatedVolume.SetGpu(0);
    burstPool->InitAccumulatedResourceVolume(burstPoolAccumulatedVolume);

    TResourceVolume relaxedPoolAccumulatedVolume;
    relaxedPoolAccumulatedVolume.SetUserSlots(2541466175.1648531);
    relaxedPoolAccumulatedVolume.SetCpu(TCpuResource(302400000.00));
    relaxedPoolAccumulatedVolume.SetMemory(1.3032554324247532e+18);
    relaxedPoolAccumulatedVolume.SetNetwork(564751061.14774621);
    relaxedPoolAccumulatedVolume.SetGpu(0);
    firstRelaxedPool->InitAccumulatedResourceVolume(relaxedPoolAccumulatedVolume);

    // Yes, we do need these 12 operations.
    // If we simply create a single operation with the total demand of 506 CPU, fair share wouldn't be the same.
    // Again, user slots and memory are unnecessary, but why not.
    std::vector<double> burstOperationCpuDemands = {60, 30, 14, 18, 60, 60, 140, 18, 18, 28, 18, 42};
    std::vector<int> burstOperationUserSlotDemands = {2, 1, 1, 2, 2, 2, 2, 5, 2, 2, 2, 2};
    std::vector<i64> burstOperationMemoryDemands = {3813532248, 1823451694, 1570583626, 2972646113, 382413122943, 3813532249, 11268640098, 2972646107, 2972646122, 3141167254, 2972646125, 3090478642};
    std::vector<TOperationElementMockPtr> burstOperations;
    for (int index = 0; index < std::ssize(burstOperationCpuDemands); ++index) {
        TJobResources operationDemand;
        operationDemand.SetUserSlots(burstOperationUserSlotDemands[index]);
        operationDemand.SetCpu(burstOperationCpuDemands[index]);
        operationDemand.SetMemory(burstOperationMemoryDemands[index]);
        operationDemand.SetNetwork(0);
        operationDemand.SetGpu(0);
        burstOperations.push_back(CreateOperation(burstPool.Get(), operationDemand));
    }

    TJobResources relaxedOperationDemand;
    relaxedOperationDemand.SetUserSlots(101836);
    relaxedOperationDemand.SetCpu(99835.63);
    relaxedOperationDemand.SetMemory(99376091257090);
    relaxedOperationDemand.SetNetwork(0);
    relaxedOperationDemand.SetGpu(0);
    auto relaxedOperation = CreateOperation(firstRelaxedPool.Get(), relaxedOperationDemand);

    {
        DoFairShareUpdate(totalResourceLimits, rootElement);

        const auto& attributes = integralRootPool->Attributes();
        EXPECT_TRUE(Dominates(attributes.LimitsShare, attributes.GetGuaranteeShare()));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TFairShareUpdateParametrizedTest, TestCrashInAdjustProposedIntegralShareOnUpdateBurstPoolIntegralShares)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto limitedStrongGuaranteeParent = CreateSimplePool("parent", /*strongGuaranteeCpu*/ 50.0);
    limitedStrongGuaranteeParent->AttachParent(rootElement.Get());

    TJobResources parentResourceLimits;
    parentResourceLimits.SetCpu(55);
    parentResourceLimits.SetMemory(totalResourceLimits.GetMemory());
    parentResourceLimits.SetUserSlots(totalResourceLimits.GetUserSlots());
    limitedStrongGuaranteeParent->SetResourceLimits(parentResourceLimits);


    auto burstChild1 = CreateIntegralPool("pool1", EIntegralGuaranteeType::Burst, /*flowCpu*/ 10.0, /*burstCpu*/ 10.0);
    burstChild1->AttachParent(limitedStrongGuaranteeParent.Get());

    auto burstChild2 = CreateIntegralPool("pool2", EIntegralGuaranteeType::Burst, /*flowCpu*/ 10.0, /*burstCpu*/ 10.0);
    burstChild2->AttachParent(limitedStrongGuaranteeParent.Get());

    TJobResources resourceDemand;
    resourceDemand.SetUserSlots(5);
    resourceDemand.SetCpu(5);
    resourceDemand.SetMemory(50_MB);

    auto op1 = CreateOperation(burstChild1.Get(), resourceDemand);
    auto op2 = CreateOperation(burstChild2.Get(), resourceDemand);

    {
        auto now = TInstant::Now();
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        // First pool gets integral guarantees until the gap between parent's limit and strong guarantee is filled.
        EXPECT_EQ(Unit * 0.0, burstChild1->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.05, burstChild1->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.0, burstChild1->Attributes().FairShare.WeightProportional);

        // The gap is filled by first pool. Second pool gets only weight proportional share.
        EXPECT_EQ(Unit * 0.0, burstChild2->Attributes().FairShare.StrongGuarantee);
        EXPECT_EQ(Unit * 0.0, burstChild2->Attributes().FairShare.IntegralGuarantee);
        EXPECT_RV_NEAR(Unit * 0.05, burstChild2->Attributes().FairShare.WeightProportional);
    }
}

TEST_F(TFairShareUpdateTest, TestGangOperations)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    TJobResources resourceDemand;
    resourceDemand.SetUserSlots(60);
    resourceDemand.SetCpu(60);
    resourceDemand.SetMemory(600_MB);

    auto operationX = CreateGangOperation(poolA.Get(), resourceDemand);
    auto operationY = CreateGangOperation(poolB.Get(), resourceDemand);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = false,
            .EnableImprovedFairShareByFitFactorComputation = false,
        });
    EXPECT_EQ(TResourceVector({1.0, 1.0, 0.0, 1.0, 0.0}), rootElement->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.5, 0.5, 0.0, 0.5, 0.0}), poolA->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.5, 0.5, 0.0, 0.5, 0.0}), poolB->Attributes().FairShare.Total);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableImprovedFairShareByFitFactorComputation = false,
        });
    EXPECT_EQ(TResourceVector({0.0, 0.0, 0.0, 0.0, 0.0}), rootElement->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.0, 0.0, 0.0, 0.0, 0.0}), poolA->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.0, 0.0, 0.0, 0.0, 0.0}), poolB->Attributes().FairShare.Total);

    DoFairShareUpdate(
        totalResourceLimits * 2.0,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableImprovedFairShareByFitFactorComputation = false,
        });
    EXPECT_EQ(TResourceVector({0.6, 0.6, 0.0, 0.6, 0.0}), rootElement->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), poolA->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), poolB->Attributes().FairShare.Total);


    {
        TJobResources updatedDemand;
        updatedDemand.SetUserSlots(45);
        updatedDemand.SetCpu(45);
        updatedDemand.SetMemory(450_MB);

        operationY->SetResourceDemand(updatedDemand);

        DoFairShareUpdate(
            totalResourceLimits,
            rootElement,
            TTestFairShareUpdateOptions{
                .EnableStepFunctionForGangOperations = true,
                .EnableImprovedFairShareByFitFactorComputation = false,
            });
        EXPECT_EQ(TResourceVector({0.45, 0.45, 0.0, 0.45, 0.0}), rootElement->Attributes().FairShare.Total);
        EXPECT_EQ(TResourceVector({0.0, 0.0, 0.0, 0.0, 0.0}), poolA->Attributes().FairShare.Total);
        EXPECT_EQ(TResourceVector({0.45, 0.45, 0.0, 0.45, 0.0}), poolB->Attributes().FairShare.Total);
    }
}

TEST_F(TFairShareUpdateTest, TestGangOperationsWithImprovedFairShareByFitFactorComputation)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    TJobResources resourceDemand;
    resourceDemand.SetUserSlots(60);
    resourceDemand.SetCpu(60);
    resourceDemand.SetMemory(600_MB);

    auto operationX = CreateGangOperation(poolA.Get(), resourceDemand);
    auto operationY = CreateGangOperation(poolB.Get(), resourceDemand);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });
    EXPECT_EQ(TResourceVector({0.6, 0.6, 0.0, 0.6, 0.0}), rootElement->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.6, 0.6, 0.0, 0.6, 0.0}), poolA->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.0, 0.0, 0.0, 0.0, 0.0}), poolB->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestGangOperationsWithSkewedResources)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->AttachParent(rootElement.Get());
    auto poolB = CreateSimplePool("PoolB");
    poolB->AttachParent(rootElement.Get());

    TJobResources resourceDemandForOperationX;
    resourceDemandForOperationX.SetUserSlots(20);
    resourceDemandForOperationX.SetCpu(20);
    resourceDemandForOperationX.SetMemory(200_MB);

    TJobResources resourceDemandForOperationY;
    resourceDemandForOperationX.SetUserSlots(50);
    resourceDemandForOperationX.SetCpu(60);
    resourceDemandForOperationX.SetMemory(100_MB);

    auto operationX = CreateGangOperation(poolA.Get(), resourceDemandForOperationX);
    auto operationY = CreateGangOperation(poolB.Get(), resourceDemandForOperationY);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });
    EXPECT_EQ(TResourceVector({0.5, 0.6, 0.0, 0.1, 0.0}), rootElement->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.5, 0.6, 0.0, 0.1, 0.0}), poolA->Attributes().FairShare.Total);
    EXPECT_EQ(TResourceVector({0.0, 0.0, 0.0, 0.0, 0.0}), poolB->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestMultipleGangOperations)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    // Create a tree with 2 pools
    auto rootElement = CreateRootElement();
    auto poolA = CreateSimplePool("PoolA");
    poolA->SetMode(ESchedulingMode::Fifo);
    poolA->AttachParent(rootElement.Get());

    auto poolB = CreateSimplePool("PoolB");
    poolB->SetMode(ESchedulingMode::Fifo);
    poolB->AttachParent(rootElement.Get());

    TJobResources resourceDemand;
    resourceDemand.SetUserSlots(30);
    resourceDemand.SetCpu(30);
    resourceDemand.SetMemory(300_MB);

    auto operationA1 = CreateGangOperation(poolA.Get(), resourceDemand);
    auto operationA2 = CreateGangOperation(poolA.Get(), resourceDemand);

    auto operationB1 = CreateGangOperation(poolB.Get(), resourceDemand);
    auto operationB2 = CreateGangOperation(poolB.Get(), resourceDemand);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });
    EXPECT_RV_NEAR(TResourceVector({0.9, 0.9, 0.0, 0.9, 0.0}), rootElement->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector({0.6, 0.6, 0.0, 0.6, 0.0}), poolA->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), poolB->Attributes().FairShare.Total);

    EXPECT_RV_NEAR(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), operationA1->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), operationA2->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector({0.3, 0.3, 0.0, 0.3, 0.0}), operationB1->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(TResourceVector({0.0, 0.0, 0.0, 0.0, 0.0}), operationB2->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilization)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    // The FIFO pool's head gang demands the whole cluster and can never fit into the pool's guarantee,
    // while the two gangs behind it fit into it exactly.
    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 60.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(rootElement.Get());

    // The competitor takes everything the FIFO pool does not claim, so the guarantee is the only share
    // the FIFO pool can rely on.
    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 40.0);
    competitorPool->AttachParent(rootElement.Get());

    // NB: The blocking gang must demand more than the pool's guarantee but strictly less than the whole
    // cluster. A gang demanding exactly the cluster has its step at suggestion 1.0, and since the fair
    // share functions are left-continuous, |FairShareBySuggestion(1.0)| returns the left limit, i.e. zero.
    // Such a gang is transparent to the FIFO cascade and blocks nobody.
    //
    // FIFO children of a mock pool are ordered by descending weight.
    auto blockingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.7);
    blockingGang->SetWeight(3.0);
    auto firstFittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    firstFittingGang->SetWeight(2.0);
    auto secondFittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    secondFittingGang->SetWeight(1.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });

    // With reordering the blocking gang is deferred, so the two fitting gangs are packed into the pool's
    // guarantee and the pool gets its full share.
    EXPECT_RV_NEAR(Unit * 0.6, fifoPool->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, blockingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.3, firstFittingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.3, secondFittingGang->Attributes().FairShare.Total);

    // The canonical order is intact and the order the update went with is reported next to it.
    EXPECT_EQ(0, blockingGang->Attributes().FifoIndex);
    EXPECT_EQ(1, firstFittingGang->Attributes().FifoIndex);
    EXPECT_EQ(2, secondFittingGang->Attributes().FifoIndex);

    EXPECT_EQ(2, blockingGang->Attributes().EffectiveFifoIndex);
    EXPECT_EQ(0, firstFittingGang->Attributes().EffectiveFifoIndex);
    EXPECT_EQ(1, secondFittingGang->Attributes().EffectiveFifoIndex);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationDisabled)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 60.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->AttachParent(rootElement.Get());

    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 40.0);
    competitorPool->AttachParent(rootElement.Get());

    auto blockingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.7);
    blockingGang->SetWeight(3.0);
    auto firstFittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    firstFittingGang->SetWeight(2.0);
    auto secondFittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    secondFittingGang->SetWeight(1.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });

    // The blocking gang keeps the head of the order, so the guarantee stays unused.
    EXPECT_RV_NEAR(Unit * 0.0, fifoPool->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, firstFittingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, secondFittingGang->Attributes().FairShare.Total);

    // Nothing was reordered, so the update went with the canonical order and reports no effective one.
    EXPECT_EQ(std::nullopt, blockingGang->Attributes().EffectiveFifoIndex);
    EXPECT_EQ(std::nullopt, firstFittingGang->Attributes().EffectiveFifoIndex);
    EXPECT_EQ(std::nullopt, secondFittingGang->Attributes().EffectiveFifoIndex);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationForgetsEffectiveOrder)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 60.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(rootElement.Get());

    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 40.0);
    competitorPool->AttachParent(rootElement.Get());

    auto blockingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.7);
    blockingGang->SetWeight(3.0);
    auto fittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.6);
    fittingGang->SetWeight(2.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    auto options = TTestFairShareUpdateOptions{
        .EnableStepFunctionForGangOperations = true,
        .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
        .EnableImprovedFairShareByFitFactorComputation = true,
    };

    DoFairShareUpdate(totalResourceLimits, rootElement, options);

    EXPECT_EQ(1, blockingGang->Attributes().EffectiveFifoIndex);
    EXPECT_EQ(0, fittingGang->Attributes().EffectiveFifoIndex);

    // Attributes survive between updates, so an update that reorders nothing must not leave the previous
    // effective order behind: it would claim an order the update never used.
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(false);

    DoFairShareUpdate(totalResourceLimits, rootElement, options);

    EXPECT_EQ(std::nullopt, blockingGang->Attributes().EffectiveFifoIndex);
    EXPECT_EQ(std::nullopt, fittingGang->Attributes().EffectiveFifoIndex);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationRequiresTreeToAllowIt)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 60.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(rootElement.Get());

    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 40.0);
    competitorPool->AttachParent(rootElement.Get());

    auto blockingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.7);
    blockingGang->SetWeight(2.0);
    auto fittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.6);
    fittingGang->SetWeight(1.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    // The pool asks for reordering, but the tree does not allow it, so nothing is reordered.
    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableFifoChildrenReorderingForGuaranteeUtilization = false,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });

    EXPECT_RV_NEAR(Unit * 0.0, fifoPool->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, fittingGang->Attributes().FairShare.Total);
    EXPECT_EQ(std::nullopt, fittingGang->Attributes().EffectiveFifoIndex);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationRequiresStepFunction)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 60.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(rootElement.Get());

    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 40.0);
    competitorPool->AttachParent(rootElement.Get());

    auto blockingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.7);
    blockingGang->SetWeight(3.0);
    auto firstFittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    firstFittingGang->SetWeight(2.0);
    auto secondFittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    secondFittingGang->SetWeight(1.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = false,
            .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });

    // Without the step function the head gang consumes the guarantee gradually instead of collapsing the
    // pool, so there is no discontinuity to dodge and the order is left alone.
    EXPECT_RV_NEAR(Unit * 0.6, fifoPool->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.6, blockingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, firstFittingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, secondFittingGang->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationRejectsOvershoot)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 60.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(rootElement.Get());

    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 40.0);
    competitorPool->AttachParent(rootElement.Get());

    auto blockingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.7);
    blockingGang->SetWeight(3.0);
    auto fittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    fittingGang->SetWeight(2.0);
    // One CPU too large to join the gang above within the 60 CPU guarantee.
    auto overshootingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.31);
    overshootingGang->SetWeight(1.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });

    EXPECT_RV_NEAR(Unit * 0.3, fifoPool->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.3, fittingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, blockingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, overshootingGang->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationChecksResourcesComponentwise)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 60.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(rootElement.Get());

    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 40.0);
    competitorPool->AttachParent(rootElement.Get());

    // Fits by CPU but not by memory, so a dominant-resource-only check would wrongly accept it.
    auto memoryHungryDemand = totalResourceLimits * 0.3;
    memoryHungryDemand.SetMemory(700_MB);

    auto blockingGang = CreateGangOperation(fifoPool.Get(), memoryHungryDemand);
    blockingGang->SetWeight(3.0);
    auto firstFittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    firstFittingGang->SetWeight(2.0);
    auto secondFittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    secondFittingGang->SetWeight(1.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });

    EXPECT_RV_NEAR(Unit * 0.6, fifoPool->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.3, firstFittingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.3, secondFittingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, blockingGang->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationDoesNotDeferRunningGang)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    // No competitor: the pool may claim more than its guarantee, which is what makes the protection
    // visible. None of the three gangs fits into the 30 CPU guarantee, so the only one that can be
    // accepted is the running one.
    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 30.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(rootElement.Get());

    auto firstPendingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.4);
    firstPendingGang->SetWeight(3.0);
    auto secondPendingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.4);
    secondPendingGang->SetWeight(2.0);
    auto runningGang = CreateGangOperation(
        fifoPool.Get(),
        totalResourceLimits * 0.4,
        totalResourceLimits * 0.1);
    runningGang->SetWeight(1.0);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });

    // The running gang is accepted despite not fitting, so it moves ahead of the two gangs deferred
    // before it and keeps its share. Were it deferred like them, the order would stay unchanged and the
    // cluster would run out before reaching it.
    EXPECT_RV_NEAR(Unit * 0.4, runningGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.4, firstPendingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, secondPendingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.8, fifoPool->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationDefersGangAfterUsageDrops)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 60.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(rootElement.Get());

    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 40.0);
    competitorPool->AttachParent(rootElement.Get());

    auto blockingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.7);
    blockingGang->SetWeight(3.0);
    // Too large for the guarantee, but running, so it is accepted and charges its whole demand.
    auto runningGang = CreateGangOperation(
        fifoPool.Get(),
        totalResourceLimits * 0.7,
        totalResourceLimits * 0.1);
    runningGang->SetWeight(2.0);
    auto pendingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    pendingGang->SetWeight(1.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    auto options = TTestFairShareUpdateOptions{
        .EnableStepFunctionForGangOperations = true,
        .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
        .EnableImprovedFairShareByFitFactorComputation = true,
    };

    DoFairShareUpdate(totalResourceLimits, rootElement, options);

    // While it is running it is accepted first, charges 0.7 and leaves no room for the pending gang.
    EXPECT_RV_NEAR(Unit * 0.0, fifoPool->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, pendingGang->Attributes().FairShare.Total);

    runningGang->SetResourceUsage(TJobResources());

    DoFairShareUpdate(totalResourceLimits, rootElement, options);

    // With its usage gone it is deferrable like any other pending gang, and the smaller one is packed.
    EXPECT_RV_NEAR(Unit * 0.3, fifoPool->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.3, pendingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, runningGang->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationStopsAtNonGang)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    // Contended, so the pool gets exactly its guarantee and the packing decides who is served.
    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 30.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(rootElement.Get());

    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 70.0);
    competitorPool->AttachParent(rootElement.Get());

    auto deferredGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.35);
    deferredGang->SetWeight(3.0);
    auto blockingOperation = CreateOperation(fifoPool.Get(), totalResourceLimits * 0.4);
    blockingOperation->SetWeight(2.0);
    // Would fit on its own, but the non-gang ahead of it ends the packing, so it is never accepted.
    auto trailingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    trailingGang->SetWeight(1.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });

    // The non-gang is promoted ahead of the gang deferred before it and consumes the guarantee that gang
    // was sitting on. Were it deferred instead, the trailing gang would have been packed and taken it.
    EXPECT_RV_NEAR(Unit * 0.3, blockingOperation->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, deferredGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, trailingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.3, fifoPool->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationKeepsOrderBehindNonGang)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    // No competitor here: the pool may claim the whole cluster, which makes the order of the children that
    // were not packed observable through their fair shares.
    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 30.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(rootElement.Get());

    auto deferredGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.35);
    deferredGang->SetWeight(4.0);
    auto blockingOperation = CreateOperation(fifoPool.Get(), totalResourceLimits * 0.4);
    blockingOperation->SetWeight(3.0);
    auto firstTrailingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.2);
    firstTrailingGang->SetWeight(2.0);
    auto secondTrailingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.2);
    secondTrailingGang->SetWeight(1.0);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });

    // Everything not emitted keeps the original order behind the blocker, i.e.
    // |blocking deferred first second| rather than |blocking first second deferred|: the deferred gang
    // still outranks the two trailing ones and is served before them, and the cluster runs out on the
    // last of them rather than on the deferred gang.
    EXPECT_RV_NEAR(Unit * 0.4, blockingOperation->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.35, deferredGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.2, firstTrailingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, secondTrailingGang->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationChargesLimitsShare)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 60.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(rootElement.Get());

    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 40.0);
    competitorPool->AttachParent(rootElement.Get());

    auto blockingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.7);
    blockingGang->SetWeight(3.0);
    // Demands more than the guarantee but is capped at 30 CPU by its own limits, so it is charged 0.3 and
    // fits. Charged at its raw demand it would not fit, and being a non-gang it would stop the packing.
    auto limitedOperation = CreateOperation(fifoPool.Get(), totalResourceLimits * 0.7);
    limitedOperation->SetWeight(2.0);
    limitedOperation->SetResourceLimits(totalResourceLimits * 0.3);
    auto fittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    fittingGang->SetWeight(1.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });

    EXPECT_RV_NEAR(Unit * 0.6, fifoPool->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.3, limitedOperation->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.3, fittingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, blockingGang->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationChargesNothingForLimitedGang)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 60.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(rootElement.Get());

    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 40.0);
    competitorPool->AttachParent(rootElement.Get());

    auto blockingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.7);
    blockingGang->SetWeight(3.0);
    // A gang runs all of its allocations or none, so this one can never reach its demand and gets nothing.
    // Charged its limits share it would reserve half of the guarantee for nothing, leaving the gang behind
    // it deferred and the reserved half idle.
    auto limitedGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.7);
    limitedGang->SetWeight(2.0);
    limitedGang->SetResourceLimits(totalResourceLimits * 0.3);
    auto fittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.6);
    fittingGang->SetWeight(1.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });

    EXPECT_RV_NEAR(Unit * 0.6, fifoPool->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.6, fittingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, limitedGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, blockingGang->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationAcrossFifoModes)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 60.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->AttachParent(rootElement.Get());

    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 40.0);
    competitorPool->AttachParent(rootElement.Get());

    auto blockingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.7);
    blockingGang->SetWeight(3.0);
    auto firstFittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    firstFittingGang->SetWeight(2.0);
    auto secondFittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.3);
    secondFittingGang->SetWeight(1.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    auto run = [&] (bool enableReordering, bool enableImproved, bool enableFastFifo) {
        fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(enableReordering);
        DoFairShareUpdate(
            totalResourceLimits,
            rootElement,
            TTestFairShareUpdateOptions{
                .EnableStepFunctionForGangOperations = true,
                .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
                .EnableImprovedFairShareByFitFactorComputation = enableImproved,
                .EnableFastFifoFairShareByFitFactorComputation = enableFastFifo,
            });
    };

    // Repeated updates on the same tree also check that the packing does not compound: every update
    // restarts from the canonical FIFO order rebuilt by |PrepareFifoPool|.
    for (bool enableImproved : {false, true}) {
        for (bool enableFastFifo : {false, true}) {
            SCOPED_TRACE(Format("Improved: %v, FastFifo: %v", enableImproved, enableFastFifo));

            run(/*enableReordering*/ false, enableImproved, enableFastFifo);

            EXPECT_RV_NEAR(Unit * 0.0, fifoPool->Attributes().FairShare.Total);

            run(/*enableReordering*/ true, enableImproved, enableFastFifo);

            EXPECT_RV_NEAR(Unit * 0.6, fifoPool->Attributes().FairShare.Total);
            EXPECT_RV_NEAR(Unit * 0.3, firstFittingGang->Attributes().FairShare.Total);
            EXPECT_RV_NEAR(Unit * 0.3, secondFittingGang->Attributes().FairShare.Total);
            EXPECT_RV_NEAR(Unit * 0.0, blockingGang->Attributes().FairShare.Total);
        }
    }
}

TEST_F(TFairShareUpdateTest, TestFifoChildrenReorderingForGuaranteeUtilizationUnderIntegralPool)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    // An integral pool prepares and caches fair share functions for its whole subtree and resets only
    // itself afterwards. This pins the reordering stage before the integral passes: were it moved after
    // them, the nested FIFO pool would keep a function built from the original order while the top-down
    // pass indexes the reordered one.
    // NB: The burst guarantee counts towards the root's guarantee budget alongside the strong ones, so
    // these must sum to at most the cluster, or every strong guarantee is scaled down proportionally.
    auto burstPool = CreateBurstPool(
        "BurstPool",
        /*flowCpu*/ 5.0,
        /*burstCpu*/ 5.0,
        /*strongGuaranteeCpu*/ 50.0);
    burstPool->AttachParent(rootElement.Get());

    auto fifoPool = CreateSimplePool("FifoPool", /*strongGuaranteeCpu*/ 50.0);
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->SetFifoChildrenReorderingForGuaranteeUtilizationEnabled(true);
    fifoPool->AttachParent(burstPool.Get());

    auto competitorPool = CreateSimplePool("CompetitorPool", /*strongGuaranteeCpu*/ 40.0);
    competitorPool->AttachParent(rootElement.Get());

    auto blockingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.6);
    blockingGang->SetWeight(3.0);
    auto firstFittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.25);
    firstFittingGang->SetWeight(2.0);
    auto secondFittingGang = CreateGangOperation(fifoPool.Get(), totalResourceLimits * 0.25);
    secondFittingGang->SetWeight(1.0);

    CreateOperation(competitorPool.Get(), totalResourceLimits);

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableFifoChildrenReorderingForGuaranteeUtilization = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
        });

    EXPECT_RV_NEAR(Unit * 0.5, fifoPool->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.25, firstFittingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.25, secondFittingGang->Attributes().FairShare.Total);
    EXPECT_RV_NEAR(Unit * 0.0, blockingGang->Attributes().FairShare.Total);
}

TEST_F(TFairShareUpdateTest, TestFifoFastPathEquivalence)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto fifoPool = CreateSimplePool("FifoPool");
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->AttachParent(rootElement.Get());

    // A spread of demands/usages so the operations' FairShareBySuggestion functions
    // have several segments of differing slopes, exercising the concatenation logic.
    struct TOperationSpec
    {
        const TJobResources Demand;
        const TJobResources Usage;
        const double Weight;
    };

    auto makeResources = [] (int userSlots, double cpu, i64 memory) {
        TJobResources result;
        result.SetUserSlots(userSlots);
        result.SetCpu(cpu);
        result.SetMemory(memory);
        return result;
    };

    std::vector<TOperationSpec> specs = {
        {makeResources(10, 10, 100_MB), makeResources(5, 3, 80_MB), /*weight*/ 5.0},
        {makeResources(20, 5, 50_MB),  makeResources(1, 4, 10_MB), /*weight*/ 4.0},
        {makeResources(5, 30, 200_MB), makeResources(5, 0, 0),     /*weight*/ 3.0},
        {makeResources(15, 15, 150_MB), makeResources(7, 7, 70_MB), /*weight*/ 2.0},
        {makeResources(8, 8, 8_MB),    makeResources(0, 0, 0),     /*weight*/ 1.0},
    };

    std::vector<TOperationElementMockPtr> operations;
    for (const auto& spec : specs) {
        auto operation = CreateOperation(fifoPool.Get(), spec.Demand, spec.Usage);
        operation->SetWeight(spec.Weight);
        operations.push_back(operation);
    }

    std::vector<TElementMock*> allElements;
    allElements.push_back(rootElement.Get());
    allElements.push_back(fifoPool.Get());
    for (const auto& operation : operations) {
        allElements.push_back(operation.Get());
    }

    auto runWithFlag = [&] (bool enableFastFifo) {
        TTestFairShareUpdateOptions options;
        options.EnableFastFifoFairShareByFitFactorComputation = enableFastFifo;
        DoFairShareUpdate(totalResourceLimits, rootElement, options);

        std::vector<TResourceVector> fairShares;
        for (auto* element : allElements) {
            fairShares.push_back(element->Attributes().FairShare.Total);
        }
        return fairShares;
    };

    auto slowFairShares = runWithFlag(/*enableFastFifo*/ false);
    int slowFifoSize = fifoPool->GetFairShareFunctionsStatistics()->FairShareByFitFactorSize;

    auto fastFairShares = runWithFlag(/*enableFastFifo*/ true);
    int fastFifoSize = fifoPool->GetFairShareFunctionsStatistics()->FairShareByFitFactorSize;

    // The fast path is mathematically (and bit-for-bit) identical to the generic Sum, so the outputs
    // and the function representation size must match exactly.
    EXPECT_EQ(slowFifoSize, fastFifoSize);
    ASSERT_EQ(slowFairShares.size(), fastFairShares.size());
    for (int i = 0; i < std::ssize(slowFairShares); ++i) {
        EXPECT_THAT(fastFairShares[i], ResourceVectorNear(slowFairShares[i], 0.0))
            << "Mismatch at element index " << i;
    }
}

TEST_F(TFairShareUpdateTest, TestFifoFastPathEdgeCases)
{
    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();

    auto makeResources = [] (int userSlots, double cpu, i64 memory) {
        TJobResources result;
        result.SetUserSlots(userSlots);
        result.SetCpu(cpu);
        result.SetMemory(memory);
        return result;
    };

    auto compareFlags = [&] (const TRootElementMockPtr& rootElement, const std::vector<TElementMock*>& allElements) {
        auto run = [&] (bool enableFastFifo) {
            TTestFairShareUpdateOptions options;
            options.EnableFastFifoFairShareByFitFactorComputation = enableFastFifo;
            DoFairShareUpdate(totalResourceLimits, rootElement, options);
            std::vector<TResourceVector> result;
            for (auto* element : allElements) {
                result.push_back(element->Attributes().FairShare.Total);
            }
            return result;
        };
        auto slow = run(false);
        auto fast = run(true);
        ASSERT_EQ(slow.size(), fast.size());
        for (int i = 0; i < std::ssize(slow); ++i) {
            EXPECT_THAT(fast[i], ResourceVectorNear(slow[i], 0.0)) << "Mismatch at element index " << i;
        }
    };

    // Case 1: single child.
    {
        auto rootElement = CreateRootElement();
        auto fifoPool = CreateSimplePool("FifoPoolSingle");
        fifoPool->SetMode(ESchedulingMode::Fifo);
        fifoPool->AttachParent(rootElement.Get());
        auto operation = CreateOperation(fifoPool.Get(), makeResources(10, 10, 100_MB), makeResources(3, 3, 30_MB));
        compareFlags(rootElement, {rootElement.Get(), fifoPool.Get(), operation.Get()});
    }

    // Case 2: a child with zero demand (all-zero fair-share function) alongside a normal child.
    {
        auto rootElement = CreateRootElement();
        auto fifoPool = CreateSimplePool("FifoPoolZero");
        fifoPool->SetMode(ESchedulingMode::Fifo);
        fifoPool->AttachParent(rootElement.Get());
        auto zeroOperation = CreateOperation(fifoPool.Get(), makeResources(0, 0, 0), makeResources(0, 0, 0));
        zeroOperation->SetWeight(2.0);
        auto normalOperation = CreateOperation(fifoPool.Get(), makeResources(10, 10, 100_MB), makeResources(5, 5, 50_MB));
        normalOperation->SetWeight(1.0);
        compareFlags(rootElement, {rootElement.Get(), fifoPool.Get(), zeroOperation.Get(), normalOperation.Get()});
    }

    // Case 3: every child has zero demand (all FairShareBySuggestion functions flat at Zero), exercising
    // the horizontal+horizontal merge that collapses the concatenation into a single segment.
    {
        auto rootElement = CreateRootElement();
        auto fifoPool = CreateSimplePool("FifoPoolAllZero");
        fifoPool->SetMode(ESchedulingMode::Fifo);
        fifoPool->AttachParent(rootElement.Get());
        auto firstOperation = CreateOperation(fifoPool.Get(), makeResources(0, 0, 0), makeResources(0, 0, 0));
        firstOperation->SetWeight(2.0);
        auto secondOperation = CreateOperation(fifoPool.Get(), makeResources(0, 0, 0), makeResources(0, 0, 0));
        secondOperation->SetWeight(1.0);
        compareFlags(rootElement, {rootElement.Get(), fifoPool.Get(), firstOperation.Get(), secondOperation.Get()});
    }
}

TEST_F(TFairShareUpdateTest, TestFifoFastPathScaling)
{
    constexpr int OperationCount = 2000;

    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    auto fifoPool = CreateSimplePool("FifoPoolLarge");
    fifoPool->SetMode(ESchedulingMode::Fifo);
    fifoPool->AttachParent(rootElement.Get());

    TJobResources operationDemand;
    operationDemand.SetUserSlots(1);
    operationDemand.SetCpu(1);
    operationDemand.SetMemory(1_MB);

    for (int i = 0; i < OperationCount; ++i) {
        auto operation = CreateOperation(fifoPool.Get(), operationDemand);
        operation->SetWeight(static_cast<double>(OperationCount - i));
    }

    TTestFairShareUpdateOptions options;
    options.EnableFastFifoFairShareByFitFactorComputation = true;

    // Just needs to complete quickly and satisfy the internal invariants
    // (IsTrimmed / VerifyNondecreasing are YT_VERIFY-ed inside the update).
    EXPECT_NO_THROW(DoFairShareUpdate(totalResourceLimits, rootElement, options));
}

TEST_F(TFairShareUpdateTest, TestExampleFromProductionCluster)
{
    auto Logger = FairShareLogger;

    auto elementsNode = ConvertToNode(ReadTestData("gpu_tree_elements.yson"));

    TJobResources totalResourceLimits;
    TResourceVector originalRootFairShare;

    TRootElementMockPtr rootElement;
    THashMap<std::string, TCompositeElementMockPtr> pools;
    THashMap<std::string, TOperationElementMockPtr> operations;
    for (auto child : elementsNode->AsList()->GetChildren()) {
        auto childMap = child->AsMap();

        auto name = childMap->GetChildValueOrThrow<std::string>("name");
        auto type = childMap->GetChildValueOrThrow<EElementType>("type");

        TElementMockPtr element;
        switch (type) {
            case EElementType::Pool: {
                TCompositeElementMockPtr compositeElement;
                if (name == "<Root>") {
                    rootElement = CreateRootElement();

                    totalResourceLimits = ConvertTo<TJobResources>(childMap->GetChildOrThrow("resource_limits"));
                    originalRootFairShare = ConvertTo<TResourceVector>(childMap->GetChildOrThrow("total_fair_share"));

                    compositeElement = rootElement;
                } else {
                    auto pool = CreateSimplePool(name);

                    auto strongGuaranteeResources = ConvertTo<TJobResources>(childMap->GetChildOrThrow("strong_guarantee_resources"));

                    auto strongGuaranteeResourcesConfig = New<TTestJobResourcesConfig>();
                    strongGuaranteeResourcesConfig->Gpu = strongGuaranteeResources.GetGpu();

                    pool->SetStrongGuaranteeResourcesConfig(strongGuaranteeResourcesConfig);
                    pool->SetMode(childMap->GetChildValueOrThrow<ESchedulingMode>("mode"));

                    element = pool;
                    compositeElement = pool;
                }

                pools.insert(std::pair(name, compositeElement));

                break;
            }
            case EElementType::Operation: {
                auto operation = CreateOperation(name);
                auto operationType = childMap->GetChildValueOrThrow<std::string>("operation_type");
                // TODO(ignat): support is_gang in orchid.
                if (operationType == "vanilla") {
                    operation->SetGangFlag(true);
                }
                operation->SetResourceDemand(ConvertTo<TJobResources>(childMap->GetChildOrThrow("resource_demand")));
                operation->SetResourceUsage(ConvertTo<TJobResources>(childMap->GetChildOrThrow("resource_usage")));

                operations.insert(std::pair(name, operation));

                element = operation;
                break;
            }
        }

        if (element) {
            double weight = childMap->GetChildValueOrThrow<double>("weight");
            element->SetWeight(weight);
        }

        if (name != "<Root>") {
            auto parentName = childMap->GetChildValueOrThrow<std::string>("parent");
            element->AttachParent(GetOrCrash(pools, parentName).Get());
        }
    }

    DoFairShareUpdate(
        totalResourceLimits,
        rootElement,
        TTestFairShareUpdateOptions{
            .EnableStepFunctionForGangOperations = true,
            .EnableImprovedFairShareByFitFactorComputation = true,
            .EnableImprovedFairShareByFitFactorComputationDistributionGap = true,
        });

    YT_TLOG_INFO("Root element")
        .With("FairShare", rootElement->Attributes().FairShare.Total)
        .With("OriginalFairShare", originalRootFairShare);
}

TEST_P(TFairShareUpdateParametrizedTest, TestRelaxedPoolWithGuaranteeOvercommitment)
{
    // This test reproduces a crash scenario where guarantee overcommitment
    // causes negative availableShare in UpdateRelaxedPoolIntegralShares.
    // Before the fix, this would violate the precondition of FloatingPointInverseLowerBound.

    auto totalResourceLimits = CreateTotalResourceLimitsWith100CPU();
    auto rootElement = CreateRootElement();

    // Create pools with strong guarantees that sum to more than 100% when combined with demand
    auto poolA = CreateSimplePool("poolA", /*strongGuaranteeCpu*/ 60.0);
    poolA->AttachParent(rootElement.Get());
    auto opA = CreateOperation(poolA.Get(), totalResourceLimits * 0.6, totalResourceLimits * 0.6);

    auto poolB = CreateSimplePool("poolB", /*strongGuaranteeCpu*/ 50.0);
    poolB->AttachParent(rootElement.Get());
    auto opB = CreateOperation(poolB.Get(), totalResourceLimits * 0.5, totalResourceLimits * 0.5);

    // Create a relaxed pool that should get nothing due to overcommitment
    auto relaxedPool = CreateRelaxedPool("relaxed", /*flowCpu*/ 10.0);
    relaxedPool->AttachParent(rootElement.Get());
    auto opRelaxed = CreateOperation(relaxedPool.Get(), totalResourceLimits * 0.1);

    {
        auto now = TInstant::Now();
        // Initialize accumulated volume for the relaxed pool
        relaxedPool->InitAccumulatedResourceVolume(GetHugeVolume());

        // This should not crash even though availableShare would be negative
        DoFairShareUpdate(totalResourceLimits, rootElement, now, now - TDuration::Minutes(1));

        // The relaxed pool should get minimal or zero integral share due to overcommitment
        // The exact values depend on guarantee adjustment, but the test should not crash
        EXPECT_TRUE(true); // If we reach here, the crash is fixed
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

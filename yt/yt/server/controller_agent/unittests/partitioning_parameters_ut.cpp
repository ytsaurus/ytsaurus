#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/partitioning_parameters_evaluator.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NControllerAgent {
namespace {

using namespace NControllerAgent;
using namespace NScheduler;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TPartitioningParametersLegacy
    : public ::testing::Test
{
protected:
    TSortOperationOptionsBasePtr Options_;
    TSortOperationSpecPtr Spec_;

    void SetUp()
    {
        Options_ = New<TSortOperationOptions>();
        auto optionsYson = BuildYsonNodeFluently().BeginMap().EndMap();
        Options_->Load(optionsYson->AsMap());

        Spec_ = New<TSortOperationSpec>();
        auto specYson = BuildYsonNodeFluently()
            .BeginMap()
                .Item("input_table_paths")
                    .BeginList()
                        .Item().Value("//in")
                    .EndList()
                .Item("output_table_path").Value("//out")
                .Item("sort_by")
                    .BeginList()
                        .Item().Value("")
                    .EndList()
            .EndMap();
        Spec_->Load(specYson->AsMap(), true, true);
    }
};

class TPartitioningParametersNew
    : public TPartitioningParametersLegacy
{
protected:
    void SetUp()
    {
        TPartitioningParametersLegacy::SetUp();
        Spec_->UseNewPartitionsHeuristic = true;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPartitioningParametersLegacy, PartitionCountIsSet)
{
    Spec_->SamplesPerPartition = 10;
    Spec_->PartitionCount = 16;

    auto evaluator = CreatePartitioningParametersEvaluator(
        Spec_,
        Options_,
        /*totalEstimatedInputDataWeight*/ 100_GB,
        /*totalEstimatedInputUncompressedDataSize*/ 100_GB,
        /*totalEstimatedInputValueCount*/ 1'000'000,
        /*inputCompressionRatio*/ 1.0,
        /*partitionJobCount*/ 100);

    EXPECT_EQ(evaluator->SuggestSampleCount(), 16 * 10);
    EXPECT_EQ(evaluator->SuggestPartitionCount(), 16);
    EXPECT_EQ(evaluator->SuggestPartitionCount(/*fetchedSamplesCount*/ 9), 10);
    EXPECT_EQ(evaluator->SuggestPartitionCount(/*fetchedSamplesCount*/ 20), 16);
    EXPECT_EQ(evaluator->SuggestMaxPartitionFactor(16), 16);
}

TEST_F(TPartitioningParametersLegacy, PartitionDataWeightIsSet)
{
    Spec_->PartitionDataWeight = 1_GB;

    auto evaluator = CreatePartitioningParametersEvaluator(
        Spec_,
        Options_,
        /*totalEstimatedInputDataWeight*/ 6_TB,
        /*totalEstimatedInputUncompressedDataSize*/ 6_TB,
        /*totalEstimatedInputValueCount*/ 1'000'000,
        /*inputCompressionRatio*/ 1.0,
        /*partitionJobCount*/ 3000);

    int expected = 6_TB / 1_GB;
    EXPECT_NEAR(evaluator->SuggestPartitionCount(), expected, expected / 100);
    EXPECT_EQ(evaluator->SuggestMaxPartitionFactor(expected), static_cast<int>(expected));
}

// Example from https://wiki.yandex-team.ru/yt/design/partitioncount/
TEST_F(TPartitioningParametersLegacy, PartitionCountIsComputedByBlockSize)
{
    Options_->CompressedBlockSize = 10_MB;

    auto evaluator = CreatePartitioningParametersEvaluator(
        Spec_,
        Options_,
        /*totalEstimatedInputDataWeight*/ 200_GB,
        /*totalEstimatedInputUncompressedDataSize*/ 200_GB,
        /*totalEstimatedInputValueCount*/ 1'000'000,
        /*inputCompressionRatio*/ 1.0,
        /*partitionJobCount*/ 3000);

    EXPECT_NEAR(evaluator->SuggestPartitionCount(), 142, 10);
}

TEST_F(TPartitioningParametersLegacy, PartitionCountIsAdjustedWrtBufferSize)
{
    Spec_->PartitionCount = 1'000'000;

    auto evaluator = CreatePartitioningParametersEvaluator(
        Spec_,
        Options_,
        /*totalEstimatedInputDataWeight*/ 10_GB,
        /*totalEstimatedInputUncompressedDataSize*/ 10_GB,
        /*totalEstimatedInputValueCount*/ 1'000'000,
        /*inputCompressionRatio*/ 1.0,
        /*partitionJobCount*/ 3000);

    // Partition count is too large, it should be adjusted
    // w.r.t. MinUncompressedBlockSize, which is 100KB by default.
    EXPECT_NEAR(evaluator->SuggestPartitionCount(), 34, 10);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPartitioningParametersNew, MaxPartitionFactorIsSmall)
{
    Spec_->SamplesPerPartition = 10;
    Spec_->PartitionCount = 19;

    Options_->MaxPartitionFactor = 10;

    auto evaluator = CreatePartitioningParametersEvaluator(
        Spec_,
        Options_,
        /*totalEstimatedInputDataWeight*/ 100_GB,
        /*totalEstimatedInputUncompressedDataSize*/ 100_GB,
        /*totalEstimatedInputValueCount*/ 1'000'000,
        /*inputCompressionRatio*/ 1.0,
        /*partitionJobCount*/ 100);

    EXPECT_EQ(evaluator->SuggestSampleCount(), 19 * 10);
    EXPECT_EQ(evaluator->SuggestPartitionCount(), 19);
    EXPECT_EQ(evaluator->SuggestPartitionCount(/*fetchedSamplesCount*/ 9), 10);
    EXPECT_EQ(evaluator->SuggestPartitionCount(/*fetchedSamplesCount*/ 25), 19);
    EXPECT_EQ(evaluator->SuggestMaxPartitionFactor(19), 5);
}

TEST_F(TPartitioningParametersNew, PartitionDataWeightIsSet)
{
    Spec_->PartitionDataWeight = 16_MB;
    Spec_->PartitionJobIO->TableWriter->MaxBufferSize = 16_MB;

    Options_->MinUncompressedBlockSize = 1_MB;

    auto evaluator = CreatePartitioningParametersEvaluator(
        Spec_,
        Options_,
        /*totalEstimatedInputDataWeight*/ 6_TB,
        /*totalEstimatedInputUncompressedDataSize*/ 6_TB,
        /*totalEstimatedInputValueCount*/ 1'000'000,
        /*inputCompressionRatio*/ 1.0,
        /*partitionJobCount*/ 3000);

    EXPECT_NEAR(evaluator->SuggestPartitionCount(), 6_TB / 16_MB, 6_TB / 16_MB / 100);
    EXPECT_NEAR(evaluator->SuggestMaxPartitionFactor(evaluator->SuggestPartitionCount()), 14, 5);
}

TEST_F(TPartitioningParametersNew, PartitionCountIsComputedBySortJobSize)
{
    Spec_->DataWeightPerShuffleJob = 1_GB;

    // Limit partitioning factor to use hierarchical partitioning.
    Options_->MaxPartitionFactor = 10;

    auto evaluator = CreatePartitioningParametersEvaluator(
        Spec_,
        Options_,
        /*totalEstimatedInputDataWeight*/ 200_GB,
        /*totalEstimatedInputUncompressedDataSize*/ 200_GB,
        /*totalEstimatedInputValueCount*/ 1'000'000,
        /*inputCompressionRatio*/ 1.0,
        /*partitionJobCount*/ 3000);

    int expected = 200_GB / (1_GB * 0.7);
    EXPECT_NEAR(evaluator->SuggestPartitionCount(), expected, expected / 100);
}

TEST_F(TPartitioningParametersNew, InputDataWeightIsVeryLarge)
{
    Spec_->DataWeightPerShuffleJob = 16_MB;

    auto evaluator = CreatePartitioningParametersEvaluator(
        Spec_,
        Options_,
        /*totalEstimatedInputDataWeight*/ 500_PB,
        /*totalEstimatedInputUncompressedDataSize*/ 500_PB,
        /*totalEstimatedInputValueCount*/ 1'000'000'000,
        /*inputCompressionRatio*/ 1.0,
        /*partitionJobCount*/ 300000);

    EXPECT_EQ(evaluator->SuggestPartitionCount(), Options_->MaxNewPartitionCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent

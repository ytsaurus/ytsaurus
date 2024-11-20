#include <yt/yt/server/controller_agent/helpers.h>

#include <yt/yt/library/query/engine_api/config.h>
#include <yt/yt/library/query/engine_api/expression_evaluator.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NControllerAgent {
namespace {

using namespace NLogging;
using namespace NQueryClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TPartitionKeysBuilderTest
    : public ::testing::Test
{
protected:
    TLogger Logger_;
    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();
    std::mt19937 Generator_;
    IExpressionEvaluatorCachePtr EvaluatorCache_ =
        CreateExpressionEvaluatorCache(New<TExpressionEvaluatorCacheConfig>());
    std::vector<TUnversionedOwningRow> Rows_;

    TTableSchemaPtr SampleSchema_;
    TTableSchemaPtr UploadSchema_;

    void SetUp()
    {
        SampleSchema_ = New<TTableSchema>(
            std::vector{TColumnSchema("x", ESimpleLogicalValueType::Int64)});
        UploadSchema_ = New<TTableSchema>(
            std::vector{TColumnSchema("x", ESimpleLogicalValueType::Int64, ESortOrder::Ascending)});
    }

    std::vector<TSample> GenerateIntSamples(const std::vector<int>& values)
    {
        std::vector<TSample> samples;
        for (auto value : values) {
            Rows_.emplace_back(MakeUnversionedOwningRow(value));
            samples.emplace_back(TSample{
                .Key = Rows_.back(),
                .Incomplete = false,
                .Weight = 8,
            });
        }
        return samples;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPartitionKeysBuilderTest, TwoPartitions)
{
    auto samples = GenerateIntSamples({2, 8, 10, 15, 15, 25});
    std::shuffle(samples.begin(), samples.end(), Generator_);

    auto keys = BuildPartitionKeysFromSamples(
        samples,
        SampleSchema_,
        UploadSchema_,
        EvaluatorCache_,
        /*partitionCount*/ 2,
        RowBuffer_,
        Logger_);

    ASSERT_EQ(keys.size(), 1U);
    const auto& key = keys[0];
    EXPECT_TRUE(key.LowerBound.Prefix > Rows_.front());
    EXPECT_TRUE(key.LowerBound.Prefix < Rows_.back());
    EXPECT_FALSE(key.Maniac);
}

TEST_F(TPartitionKeysBuilderTest, SinglePartition)
{
    auto samples = GenerateIntSamples({2, 8, 10, 15, 15, 25});
    std::shuffle(samples.begin(), samples.end(), Generator_);

    auto keys = BuildPartitionKeysFromSamples(
        samples,
        SampleSchema_,
        UploadSchema_,
        EvaluatorCache_,
        /*partitionCount*/ 1,
        RowBuffer_,
        Logger_);

    EXPECT_TRUE(keys.empty());
}

TEST_F(TPartitionKeysBuilderTest, ManiacPartition)
{
    auto samples = GenerateIntSamples({1, 8, 8, 8, 8, 9});

    auto keys = BuildPartitionKeysFromSamples(
        samples,
        SampleSchema_,
        UploadSchema_,
        EvaluatorCache_,
        /*partitionCount*/ 3,
        RowBuffer_,
        Logger_);

    ASSERT_EQ(keys.size(), 2U);
    EXPECT_TRUE(keys[0].Maniac);
    EXPECT_EQ(
        keys[0].LowerBound,
        TKeyBound::FromRow(MakeUnversionedOwningRow(8), /*isInclusive*/ true, /*isUpper*/ false));

    EXPECT_FALSE(keys[1].Maniac);
    EXPECT_EQ(
        keys[1].LowerBound,
        TKeyBound::FromRow(MakeUnversionedOwningRow(8), /*isInclusive*/ false, /*isUpper*/ false));
}

TEST_F(TPartitionKeysBuilderTest, IncompleteSample)
{
    auto samples = GenerateIntSamples({1, 8, 8, 8, 8, 9});
    for (int i = 1; i <= 4; ++i) {
        samples[i].Incomplete = true;
    }

    auto keys = BuildPartitionKeysFromSamples(
        samples,
        SampleSchema_,
        UploadSchema_,
        EvaluatorCache_,
        /*partitionCount*/ 3,
        RowBuffer_,
        Logger_);

    ASSERT_EQ(keys.size(), 1U);
    EXPECT_FALSE(keys[0].Maniac);
}

TEST_F(TPartitionKeysBuilderTest, ShiftedRowWeights)
{
    auto samples = GenerateIntSamples({1, 2, 3, 4, 5});
    samples.back().Weight = 100500;

    auto keys = BuildPartitionKeysFromSamples(
        samples,
        SampleSchema_,
        UploadSchema_,
        EvaluatorCache_,
        /*partitionCount*/ 2,
        RowBuffer_,
        Logger_);

    ASSERT_EQ(keys.size(), 1U);

    EXPECT_FALSE(keys[0].Maniac);
    EXPECT_EQ(
        keys[0].LowerBound,
        TKeyBound::FromRow(MakeUnversionedOwningRow(5), /*isInclusive*/ true, /*isUpper*/ false));
}

TEST_F(TPartitionKeysBuilderTest, ColumnWithExpression)
{
    std::vector<TSample> samples;
    for (auto value : {1, 2, 3, 4, 5}) {
        Rows_.emplace_back(MakeUnversionedOwningRow(value, -2 * value));
        samples.emplace_back(TSample{
            .Key = Rows_.back(),
            .Incomplete = false,
            .Weight = 16,
        });
    }

    TColumnSchema uploadColumn("x", ESimpleLogicalValueType::Int64, ESortOrder::Ascending);
    uploadColumn.SetExpression("x + y");
    UploadSchema_ = New<TTableSchema>(std::vector{uploadColumn});

    SampleSchema_ = New<TTableSchema>(
        std::vector{
            TColumnSchema("x", ESimpleLogicalValueType::Int64),
            TColumnSchema("y", ESimpleLogicalValueType::Int64)});

    auto keys = BuildPartitionKeysFromSamples(
        samples,
        SampleSchema_,
        UploadSchema_,
        EvaluatorCache_,
        /*partitionCount*/ 2,
        RowBuffer_,
        Logger_);

    ASSERT_EQ(keys.size(), 1U);

    EXPECT_FALSE(keys[0].Maniac);
    EXPECT_EQ(
        keys[0].LowerBound,
        TKeyBound::FromRow(MakeUnversionedOwningRow(-3), /*isInclusive*/ true, /*isUpper*/ false));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent

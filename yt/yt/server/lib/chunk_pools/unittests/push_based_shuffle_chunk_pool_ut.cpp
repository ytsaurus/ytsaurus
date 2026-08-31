#include "chunk_pools_helpers.h"

#include <yt/yt/server/lib/chunk_pools/push_based_shuffle_chunk_pool.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/blob_output.h>

#include <limits>

namespace NYT::NChunkPools {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;
using namespace NDistributedChunkSessionClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TSessionSealSummary MakeSealSummary(
    i64 recordCount = 0,
    i64 physicalCompressedDataSize = 0)
{
    return {
        .RecordCount = recordCount,
        .PhysicalCompressedDataSize = physicalCompressedDataSize,
    };
}

////////////////////////////////////////////////////////////////////////////////

class TPushBasedShuffleChunkPoolTest
    : public TChunkPoolTestBase
{
protected:
    IPushBasedShuffleChunkPoolPtr CreatePool(
        int partitionCount,
        i64 targetUncompressedDataSizePerJob,
        i64 maxDataSliceCountPerJob,
        NLogging::TLogger logger)
    {
        return CreatePushBasedShuffleChunkPool({
            .PartitionCount = partitionCount,
            .TargetUncompressedDataSizePerJob = targetUncompressedDataSizePerJob,
            .MaxDataSliceCountPerJob = maxDataSliceCountPerJob,
            .SealFallbackCompressionRatio = 0.25,
            .SealFallbackRowCountPerRecord = 10,
            .Logger = std::move(logger),
        });
    }
};

class TPushBasedShuffleChunkPoolDeathTest
    : public TPushBasedShuffleChunkPoolTest
{
protected:
    void SetUp() override
    {
        TChunkPoolTestBase::SetUp();

        ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPushBasedShuffleChunkPoolDeathTest, RegisterAfterFinishAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        pool->GetInput()->Finish();

        try {
            pool->RegisterChunkWriteSession(
                /*partitionIndex*/ 0,
                MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42)),
                {});
        } catch (...) {
        }
    }, "!Finished");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, InvalidPartitionIndexAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());

        try {
            pool->RegisterChunkWriteSession(
                /*partitionIndex*/ 1,
                MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42)),
                {});
        } catch (...) {
        }
    }, "partitionIndex >= 0");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, DuplicateChunkWriteSessionAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});

        try {
            pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
        } catch (...) {
        }
    }, "EmplaceOrCrash");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, UpdateFinishedChunkWriteSessionAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
        pool->FinishChunkWriteSession(chunkId, {});

        try {
            pool->UpdateChunkWriteSession(chunkId, {});
        } catch (...) {
        }
    }, "!session\\.Finished");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, UpdateWithoutRecordProgressAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});

        try {
            pool->UpdateChunkWriteSession(chunkId, {
                .DataWeight = 1,
            });
        } catch (...) {
        }
    }, "delta\\.RecordCount > 0");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, NegativeExactStatisticsAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});

        try {
            pool->UpdateChunkWriteSession(chunkId, {
                .RecordCount = -1,
            });
        } catch (...) {
        }
    }, "IsNonnegative\\(progress\\)");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, RegressingExactStatisticsAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
        pool->UpdateChunkWriteSession(chunkId, {
            .DataWeight = 2,
            .CompressedDataSize = 2,
            .UncompressedDataSize = 2,
            .RecordCount = 1,
            .RowCount = 2,
        });

        try {
            pool->UpdateChunkWriteSession(chunkId, {
                .DataWeight = 1,
                .CompressedDataSize = 2,
                .UncompressedDataSize = 2,
                .RecordCount = 2,
                .RowCount = 2,
            });
        } catch (...) {
        }
    }, "IsComponentwiseLessOrEqual");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, EmptyRecordsAbort)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});

        try {
            pool->UpdateChunkWriteSession(chunkId, {
                .DataWeight = 20,
                .UncompressedDataSize = 1,
                .RecordCount = 2,
            });
        } catch (...) {
        }
    }, "progress\\.UncompressedDataSize >= progress\\.RecordCount");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, NegativeSealRecordCountAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});

        try {
            pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary(-1));
        } catch (...) {
        }
    }, "summary\\.RecordCount >= 0");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, NegativeSealCompressedDataSizeAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});

        try {
            pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary(0, -1));
        } catch (...) {
        }
    }, "summary\\.PhysicalCompressedDataSize >= 0");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, SealRecordCountBehindReportedProgressAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
        pool->UpdateChunkWriteSession(chunkId, {
            .DataWeight = 2,
            .CompressedDataSize = 2,
            .UncompressedDataSize = 2,
            .RecordCount = 2,
            .RowCount = 2,
        });

        pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary(1));
    }, "summary\\.RecordCount >= session\\.Progress\\.RecordCount");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, SealCompressedDataSizeBehindReportedProgressAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
        pool->UpdateChunkWriteSession(chunkId, {
            .DataWeight = 2,
            .CompressedDataSize = 2,
            .UncompressedDataSize = 2,
            .RecordCount = 2,
            .RowCount = 2,
        });

        pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary(2, 1));
    }, "summary\\.PhysicalCompressedDataSize >= session\\.Progress\\.CompressedDataSize");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, OverflowingSealFallbackEstimateAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePushBasedShuffleChunkPool(TPushBasedShuffleChunkPoolOptions{
            .PartitionCount = 1,
            .TargetUncompressedDataSizePerJob = 1,
            .MaxDataSliceCountPerJob = 1,
            .SealFallbackCompressionRatio = std::numeric_limits<double>::min(),
            .SealFallbackRowCountPerRecord = 1,
            .Logger = GetTestLogger(),
        });
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
        pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary(1, 2));
    }, "std::isfinite\\(result\\)");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, OverflowingSealFallbackRowCountAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePushBasedShuffleChunkPool(TPushBasedShuffleChunkPoolOptions{
            .PartitionCount = 1,
            .TargetUncompressedDataSizePerJob = 1,
            .MaxDataSliceCountPerJob = 1,
            .SealFallbackCompressionRatio = 1.0,
            .SealFallbackRowCountPerRecord = std::numeric_limits<i64>::max(),
            .Logger = GetTestLogger(),
        });
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
        pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary(2, 2));
    }, "rhs <= std::numeric_limits<i64>::max\\(\\) / lhs");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, UpdateUnknownChunkWriteSessionAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));

        try {
            pool->UpdateChunkWriteSession(chunkId, {});
        } catch (...) {
        }
    }, "key is not found in map");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, FinishUnknownChunkWriteSessionAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));

        try {
            pool->FinishChunkWriteSession(chunkId, {});
        } catch (...) {
        }
    }, "key is not found in map");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, SealUnknownChunkWriteSessionAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));

        try {
            pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary());
        } catch (...) {
        }
    }, "key is not found in map");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, RepeatedExactFinishAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
        pool->FinishChunkWriteSession(chunkId, {});

        try {
            pool->FinishChunkWriteSession(chunkId, {});
        } catch (...) {
        }
    }, "!session\\.Finished");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, SealFinishAfterExactFinishAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
        pool->FinishChunkWriteSession(chunkId, {});

        try {
            pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary());
        } catch (...) {
        }
    }, "!session\\.Finished");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, ExactFinishAfterSealFinishAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
        pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary());

        try {
            pool->FinishChunkWriteSession(chunkId, {});
        } catch (...) {
        }
    }, "!session\\.Finished");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, RepeatedSealFinishAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
        pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary());

        try {
            pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary());
        } catch (...) {
        }
    }, "!session\\.Finished");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, BuilderStatisticsOverflowAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto firstChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        auto secondChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, firstChunkId, {});
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, secondChunkId, {});
        pool->FinishChunkWriteSession(firstChunkId, {
            .DataWeight = std::numeric_limits<i64>::max(),
            .CompressedDataSize = 1,
            .UncompressedDataSize = 1,
            .RecordCount = 1,
            .RowCount = 1,
        });

        try {
            pool->UpdateChunkWriteSession(secondChunkId, {
                .DataWeight = 1,
                .CompressedDataSize = 1,
                .UncompressedDataSize = 1,
                .RecordCount = 1,
                .RowCount = 1,
            });
        } catch (...) {
        }
    }, "rhs > 0");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, ObservedStatisticsOverflowAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 2,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto firstChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        auto secondChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));

        // Distinct partitions, so the per-partition builders cannot overflow first and the
        // pool-wide sample is the only accumulator left.
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, firstChunkId, {});
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 1, secondChunkId, {});
        pool->FinishChunkWriteSession(firstChunkId, {
            .DataWeight = std::numeric_limits<i64>::max(),
            .CompressedDataSize = 1,
            .UncompressedDataSize = 1,
            .RecordCount = 1,
            .RowCount = 1,
        });

        try {
            pool->UpdateChunkWriteSession(secondChunkId, {
                .DataWeight = 1,
                .CompressedDataSize = 1,
                .UncompressedDataSize = 1,
                .RecordCount = 1,
                .RowCount = 1,
            });
        } catch (...) {
        }
    }, "rhs > 0");
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, SealEstimateOverflowAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
        pool->UpdateChunkWriteSession(chunkId, {
            .DataWeight = std::numeric_limits<i64>::max() - 1,
            .CompressedDataSize = 1,
            .UncompressedDataSize = 1,
            .RecordCount = 1,
            .RowCount = 1,
        });

        try {
            pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary(2, 2));
        } catch (...) {
        }
    }, "rhs > 0");
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPushBasedShuffleChunkPoolTest, AcceptsSealPaddingWithoutNewRecords)
{
    auto pool = CreatePool(
        /*partitionCount*/ 1,
        /*targetUncompressedDataSizePerJob*/ 1000,
        /*maxDataSliceCountPerJob*/ 10,
        GetTestLogger());
    auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
    pool->UpdateChunkWriteSession(chunkId, {
        .DataWeight = 40,
        .CompressedDataSize = 20,
        .UncompressedDataSize = 40,
        .RecordCount = 2,
        .RowCount = 20,
    });

    // Sealed compressed data size exceeds the reported one because of on-disk padding.
    pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary(2, 30));
    pool->GetInput()->Finish();

    auto output = pool->GetOutput(0);
    ASSERT_EQ(1, output->GetJobCounter()->GetPending());
    auto cookie = output->Extract();
    ASSERT_NE(IChunkPoolOutput::NullCookie, cookie);
    auto stripeList = output->GetStripeList(cookie);
    ASSERT_EQ(1u, stripeList->Stripes().front()->DataSlices().size());
    const auto& chunkSlice =
        stripeList->Stripes().front()->DataSlices().front()->ChunkSlices.front();

    // The padding is dropped: the emitted slice keeps the reported sizes.
    EXPECT_EQ(0, chunkSlice->LowerLimit().RowIndex);
    EXPECT_EQ(2, chunkSlice->UpperLimit().RowIndex);
    EXPECT_EQ(20, chunkSlice->GetCompressedDataSize());
    EXPECT_EQ(40, chunkSlice->GetDataWeight());
}

TEST_F(TPushBasedShuffleChunkPoolTest, EmptyPoolFinalizesEveryPartition)
{
    auto pool = CreatePool(
        /*partitionCount*/ 2,
        /*targetUncompressedDataSizePerJob*/ 100,
        /*maxDataSliceCountPerJob*/ 2,
        GetTestLogger());

    auto firstOutput = pool->GetOutput(0);
    auto secondOutput = pool->GetOutput(1);

    EXPECT_FALSE(firstOutput->IsCompleted());
    EXPECT_FALSE(secondOutput->IsCompleted());
    EXPECT_EQ(0, pool->GetTotalDataSliceCount());
    EXPECT_EQ(0, pool->GetTotalJobCount());

    pool->GetInput()->Finish();

    EXPECT_TRUE(firstOutput->IsCompleted());
    EXPECT_TRUE(secondOutput->IsCompleted());
    EXPECT_EQ(IChunkPoolOutput::NullCookie, firstOutput->Extract());
    EXPECT_EQ(IChunkPoolOutput::NullCookie, secondOutput->Extract());
}

TEST_F(TPushBasedShuffleChunkPoolTest, SplitsConfirmedRecordsAtTargetAndFlushesRemainder)
{
    auto pool = CreatePool(
        /*partitionCount*/ 1,
        /*targetUncompressedDataSizePerJob*/ 100,
        /*maxDataSliceCountPerJob*/ 2,
        GetTestLogger());
    auto output = pool->GetOutput(0);
    auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));

    pool->RegisterChunkWriteSession(0, chunkId, {});
    pool->UpdateChunkWriteSession(chunkId, {
        .DataWeight = 250,
        .CompressedDataSize = 125,
        .UncompressedDataSize = 250,
        .RecordCount = 10,
        .RowCount = 100,
    });

    EXPECT_EQ(3, pool->GetTotalDataSliceCount());
    EXPECT_EQ(3, pool->GetTotalJobCount());
    EXPECT_EQ(2, output->GetJobCounter()->GetPending());
    EXPECT_EQ(1, output->GetJobCounter()->GetBlocked());
    EXPECT_EQ(3, output->GetDataSliceCounter()->GetTotal());

    pool->FinishChunkWriteSession(chunkId, {
        .DataWeight = 250,
        .CompressedDataSize = 125,
        .UncompressedDataSize = 250,
        .RecordCount = 10,
        .RowCount = 100,
    });
    pool->GetInput()->Finish();

    EXPECT_EQ(3, output->GetJobCounter()->GetPending());
    EXPECT_EQ(0, output->GetJobCounter()->GetBlocked());
    EXPECT_EQ(3, output->GetDataSliceCounter()->GetTotal());
    EXPECT_EQ(3, pool->GetTotalDataSliceCount());
    EXPECT_EQ(3, pool->GetTotalJobCount());

    struct TExpectedSlice
    {
        i64 LowerRecordIndex;
        i64 UpperRecordIndex;
        i64 DataWeight;
        i64 CompressedDataSize;
        i64 UncompressedDataSize;
        i64 RowCount;
    };

    const std::vector<TExpectedSlice> expectedSlices{
        {0, 4, 100, 50, 100, 40},
        {4, 8, 100, 50, 100, 40},
        {8, 10, 50, 25, 50, 20},
    };

    for (const auto& expected : expectedSlices) {
        auto cookie = output->Extract();
        ASSERT_NE(IChunkPoolOutput::NullCookie, cookie);
        EXPECT_EQ(1, output->GetStripeListSliceCount(cookie));

        auto stripeList = output->GetStripeList(cookie);
        ASSERT_TRUE(stripeList->IsApproximate());
        ASSERT_EQ(1u, stripeList->Stripes().size());
        ASSERT_EQ(1u, stripeList->Stripes().front()->DataSlices().size());

        const auto& dataSlice = stripeList->Stripes().front()->DataSlices().front();
        ASSERT_EQ(1u, dataSlice->ChunkSlices.size());
        const auto& chunkSlice = dataSlice->ChunkSlices.front();

        EXPECT_EQ(chunkId, chunkSlice->GetInputChunk()->GetChunkId());
        EXPECT_EQ(expected.LowerRecordIndex, chunkSlice->LowerLimit().RowIndex);
        EXPECT_EQ(expected.UpperRecordIndex, chunkSlice->UpperLimit().RowIndex);
        EXPECT_EQ(expected.DataWeight, chunkSlice->GetDataWeight());
        EXPECT_EQ(expected.CompressedDataSize, chunkSlice->GetCompressedDataSize());
        EXPECT_EQ(expected.UncompressedDataSize, chunkSlice->GetUncompressedDataSize());
        EXPECT_EQ(expected.RowCount, chunkSlice->GetRowCount());
    }

    EXPECT_EQ(IChunkPoolOutput::NullCookie, output->Extract());
}

TEST_F(TPushBasedShuffleChunkPoolTest, CorrectsLargeApproximateSplitBoundary)
{
    constexpr i64 builderSize = 3036101392884274;
    constexpr i64 targetSize = 10431723682226412;
    constexpr i64 incomingRecordCount = 5546261973570985;
    constexpr i64 incomingSize = 9007199254749137;

    auto pool = CreatePool(
        /*partitionCount*/ 1,
        targetSize,
        /*maxDataSliceCountPerJob*/ 10,
        GetTestLogger());
    auto firstChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    auto secondChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));

    pool->RegisterChunkWriteSession(0, firstChunkId, {});
    pool->RegisterChunkWriteSession(0, secondChunkId, {});
    pool->FinishChunkWriteSession(firstChunkId, {
        .DataWeight = builderSize,
        .CompressedDataSize = builderSize,
        .UncompressedDataSize = builderSize,
        .RecordCount = 1,
        .RowCount = 1,
    });
    pool->FinishChunkWriteSession(secondChunkId, {
        .DataWeight = incomingSize,
        .CompressedDataSize = incomingSize,
        .UncompressedDataSize = incomingSize,
        .RecordCount = incomingRecordCount,
        .RowCount = incomingRecordCount,
    });
    pool->GetInput()->Finish();

    EXPECT_EQ(2, pool->GetTotalJobCount());
    EXPECT_EQ(3, pool->GetTotalDataSliceCount());
}

TEST_F(TPushBasedShuffleChunkPoolTest, CombinesInterleavedRangesAndHonorsSliceLimit)
{
    auto pool = CreatePool(
        /*partitionCount*/ 1,
        /*targetUncompressedDataSizePerJob*/ 1000,
        /*maxDataSliceCountPerJob*/ 2,
        GetTestLogger());
    auto output = pool->GetOutput(0);

    auto firstChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    auto secondChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    auto thirdChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    pool->RegisterChunkWriteSession(0, firstChunkId, {});
    pool->RegisterChunkWriteSession(0, secondChunkId, {});
    pool->RegisterChunkWriteSession(0, thirdChunkId, {});

    pool->UpdateChunkWriteSession(firstChunkId, {
        .DataWeight = 20,
        .CompressedDataSize = 10,
        .UncompressedDataSize = 20,
        .RecordCount = 2,
        .RowCount = 20,
    });
    pool->UpdateChunkWriteSession(secondChunkId, {
        .DataWeight = 20,
        .CompressedDataSize = 10,
        .UncompressedDataSize = 20,
        .RecordCount = 2,
        .RowCount = 20,
    });
    pool->UpdateChunkWriteSession(firstChunkId, {
        .DataWeight = 40,
        .CompressedDataSize = 20,
        .UncompressedDataSize = 40,
        .RecordCount = 4,
        .RowCount = 40,
    });

    EXPECT_EQ(0, output->GetJobCounter()->GetPending());

    pool->UpdateChunkWriteSession(thirdChunkId, {
        .DataWeight = 20,
        .CompressedDataSize = 10,
        .UncompressedDataSize = 20,
        .RecordCount = 2,
        .RowCount = 20,
    });

    EXPECT_EQ(1, output->GetJobCounter()->GetPending());

    pool->FinishChunkWriteSession(firstChunkId, {
        .DataWeight = 40,
        .CompressedDataSize = 20,
        .UncompressedDataSize = 40,
        .RecordCount = 4,
        .RowCount = 40,
    });
    pool->FinishChunkWriteSession(secondChunkId, {
        .DataWeight = 20,
        .CompressedDataSize = 10,
        .UncompressedDataSize = 20,
        .RecordCount = 2,
        .RowCount = 20,
    });
    pool->FinishChunkWriteSession(thirdChunkId, {
        .DataWeight = 20,
        .CompressedDataSize = 10,
        .UncompressedDataSize = 20,
        .RecordCount = 2,
        .RowCount = 20,
    });
    pool->GetInput()->Finish();

    ASSERT_EQ(2, output->GetJobCounter()->GetPending());
    EXPECT_EQ(2, pool->GetTotalJobCount());

    auto firstCookie = output->Extract();
    ASSERT_NE(IChunkPoolOutput::NullCookie, firstCookie);
    EXPECT_EQ(2, output->GetStripeListSliceCount(firstCookie));
    auto firstStripeList = output->GetStripeList(firstCookie);
    ASSERT_EQ(1u, firstStripeList->Stripes().size());
    const auto& firstJobSlices = firstStripeList->Stripes().front()->DataSlices();
    ASSERT_EQ(2u, firstJobSlices.size());

    const auto& combinedSlice = firstJobSlices.front()->ChunkSlices.front();
    EXPECT_EQ(firstChunkId, combinedSlice->GetInputChunk()->GetChunkId());
    EXPECT_EQ(0, combinedSlice->LowerLimit().RowIndex);
    EXPECT_EQ(4, combinedSlice->UpperLimit().RowIndex);
    EXPECT_EQ(40, combinedSlice->GetUncompressedDataSize());

    const auto& interleavedSlice = firstJobSlices.back()->ChunkSlices.front();
    EXPECT_EQ(secondChunkId, interleavedSlice->GetInputChunk()->GetChunkId());
    EXPECT_EQ(0, interleavedSlice->LowerLimit().RowIndex);
    EXPECT_EQ(2, interleavedSlice->UpperLimit().RowIndex);

    auto secondCookie = output->Extract();
    ASSERT_NE(IChunkPoolOutput::NullCookie, secondCookie);
    EXPECT_EQ(1, output->GetStripeListSliceCount(secondCookie));
    auto secondStripeList = output->GetStripeList(secondCookie);
    const auto& cappedSlice = secondStripeList->Stripes().front()->DataSlices().front()->ChunkSlices.front();
    EXPECT_EQ(thirdChunkId, cappedSlice->GetInputChunk()->GetChunkId());
    EXPECT_EQ(0, cappedSlice->LowerLimit().RowIndex);
    EXPECT_EQ(2, cappedSlice->UpperLimit().RowIndex);
}

TEST_F(TPushBasedShuffleChunkPoolTest, EstimatesSealedSuffixFromSameSession)
{
    auto pool = CreatePool(
        /*partitionCount*/ 1,
        /*targetUncompressedDataSizePerJob*/ 1000,
        /*maxDataSliceCountPerJob*/ 10,
        GetTestLogger());
    auto output = pool->GetOutput(0);
    auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    pool->RegisterChunkWriteSession(0, chunkId, {});
    pool->UpdateChunkWriteSession(chunkId, {
        .DataWeight = 40,
        .CompressedDataSize = 20,
        .UncompressedDataSize = 60,
        .RecordCount = 2,
        .RowCount = 200,
    });

    pool->GetInput()->Finish();
    EXPECT_EQ(0, output->GetJobCounter()->GetPending());

    pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary(5, 999));

    ASSERT_EQ(1, output->GetJobCounter()->GetPending());
    auto cookie = output->Extract();
    ASSERT_NE(IChunkPoolOutput::NullCookie, cookie);
    EXPECT_EQ(1, output->GetStripeListSliceCount(cookie));

    auto stripeList = output->GetStripeList(cookie);
    EXPECT_TRUE(stripeList->IsApproximate());
    ASSERT_EQ(1u, stripeList->Stripes().size());
    ASSERT_EQ(1u, stripeList->Stripes().front()->DataSlices().size());
    const auto& chunkSlice =
        stripeList->Stripes().front()->DataSlices().front()->ChunkSlices.front();

    EXPECT_EQ(0, chunkSlice->LowerLimit().RowIndex);
    EXPECT_EQ(5, chunkSlice->UpperLimit().RowIndex);
    EXPECT_EQ(100, chunkSlice->GetDataWeight());
    EXPECT_EQ(999, chunkSlice->GetCompressedDataSize());
    EXPECT_EQ(150, chunkSlice->GetUncompressedDataSize());
    EXPECT_EQ(500, chunkSlice->GetRowCount());
}

TEST_F(TPushBasedShuffleChunkPoolTest, EstimatesSealedSuffixFromPoolSample)
{
    auto pool = CreatePool(
        /*partitionCount*/ 2,
        /*targetUncompressedDataSizePerJob*/ 1000,
        /*maxDataSliceCountPerJob*/ 10,
        GetTestLogger());
    auto sampledChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    auto sealedChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    pool->RegisterChunkWriteSession(0, sampledChunkId, {});
    pool->RegisterChunkWriteSession(1, sealedChunkId, {});

    pool->FinishChunkWriteSession(sampledChunkId, {
        .DataWeight = 40,
        .CompressedDataSize = 20,
        .UncompressedDataSize = 60,
        .RecordCount = 2,
        .RowCount = 200,
    });
    pool->FinishChunkWriteSessionFromSeal(sealedChunkId, MakeSealSummary(3, 999));
    pool->GetInput()->Finish();

    EXPECT_EQ(2, pool->GetTotalDataSliceCount());
    EXPECT_EQ(2, pool->GetTotalJobCount());

    auto output = pool->GetOutput(1);
    ASSERT_EQ(1, output->GetJobCounter()->GetPending());
    auto cookie = output->Extract();
    auto stripeList = output->GetStripeList(cookie);
    EXPECT_TRUE(stripeList->IsApproximate());
    const auto& chunkSlice =
        stripeList->Stripes().front()->DataSlices().front()->ChunkSlices.front();

    EXPECT_EQ(sealedChunkId, chunkSlice->GetInputChunk()->GetChunkId());
    EXPECT_EQ(0, chunkSlice->LowerLimit().RowIndex);
    EXPECT_EQ(3, chunkSlice->UpperLimit().RowIndex);
    EXPECT_EQ(60, chunkSlice->GetDataWeight());
    EXPECT_EQ(999, chunkSlice->GetCompressedDataSize());
    EXPECT_EQ(90, chunkSlice->GetUncompressedDataSize());
    EXPECT_EQ(300, chunkSlice->GetRowCount());
}

TEST_F(TPushBasedShuffleChunkPoolTest, ApproximatesLargeStatisticsWithoutOverflow)
{
    auto pool = CreatePool(
        /*partitionCount*/ 2,
        /*targetUncompressedDataSizePerJob*/ std::numeric_limits<i64>::max(),
        /*maxDataSliceCountPerJob*/ 10,
        GetTestLogger());
    auto sampledChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    auto sealedChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    pool->RegisterChunkWriteSession(0, sampledChunkId, {});
    pool->RegisterChunkWriteSession(1, sealedChunkId, {});

    const i64 maxValue = std::numeric_limits<i64>::max();
    const i64 halfMaxValue = maxValue / 2;
    pool->FinishChunkWriteSession(sampledChunkId, {
        .DataWeight = maxValue - 2,
        .CompressedDataSize = maxValue - 2,
        .UncompressedDataSize = maxValue - 2,
        .RecordCount = halfMaxValue + 1,
        .RowCount = maxValue - 2,
    });
    pool->FinishChunkWriteSessionFromSeal(
        sealedChunkId,
        MakeSealSummary(halfMaxValue, halfMaxValue));
    pool->GetInput()->Finish();

    auto output = pool->GetOutput(1);
    ASSERT_EQ(1, output->GetJobCounter()->GetPending());
    IChunkPoolOutput::TCookie cookie = output->Extract();
    ASSERT_NE(IChunkPoolOutput::NullCookie, cookie);
    auto stripeList = output->GetStripeList(cookie);
    const auto& chunkSlice =
        stripeList->Stripes().front()->DataSlices().front()->ChunkSlices.front();

    EXPECT_EQ(maxValue, chunkSlice->GetDataWeight());
    EXPECT_EQ(maxValue, chunkSlice->GetRowCount());
    EXPECT_EQ(halfMaxValue, chunkSlice->UpperLimit().RowIndex);
}

TEST_F(TPushBasedShuffleChunkPoolTest, EstimatesSealedSuffixWithoutSample)
{
    auto pool = CreatePool(
        /*partitionCount*/ 1,
        /*targetUncompressedDataSizePerJob*/ 1000,
        /*maxDataSliceCountPerJob*/ 10,
        GetTestLogger());
    auto firstChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    auto secondChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    pool->RegisterChunkWriteSession(0, firstChunkId, {});
    pool->RegisterChunkWriteSession(0, secondChunkId, {});
    pool->FinishChunkWriteSessionFromSeal(firstChunkId, MakeSealSummary(5, 100));
    pool->FinishChunkWriteSessionFromSeal(secondChunkId, MakeSealSummary(2, 10));
    pool->GetInput()->Finish();

    auto output = pool->GetOutput(0);
    ASSERT_EQ(1, output->GetJobCounter()->GetPending());
    auto cookie = output->Extract();
    auto stripeList = output->GetStripeList(cookie);
    EXPECT_TRUE(stripeList->IsApproximate());
    const auto& dataSlices = stripeList->Stripes().front()->DataSlices();
    ASSERT_EQ(2u, dataSlices.size());
    const auto& firstChunkSlice = dataSlices[0]->ChunkSlices.front();
    const auto& secondChunkSlice = dataSlices[1]->ChunkSlices.front();

    EXPECT_EQ(firstChunkId, firstChunkSlice->GetInputChunk()->GetChunkId());
    EXPECT_EQ(0, firstChunkSlice->LowerLimit().RowIndex);
    EXPECT_EQ(5, firstChunkSlice->UpperLimit().RowIndex);
    EXPECT_EQ(400, firstChunkSlice->GetDataWeight());
    EXPECT_EQ(100, firstChunkSlice->GetCompressedDataSize());
    EXPECT_EQ(400, firstChunkSlice->GetUncompressedDataSize());
    EXPECT_EQ(50, firstChunkSlice->GetRowCount());

    EXPECT_EQ(secondChunkId, secondChunkSlice->GetInputChunk()->GetChunkId());
    EXPECT_EQ(0, secondChunkSlice->LowerLimit().RowIndex);
    EXPECT_EQ(2, secondChunkSlice->UpperLimit().RowIndex);
    EXPECT_EQ(40, secondChunkSlice->GetDataWeight());
    EXPECT_EQ(10, secondChunkSlice->GetCompressedDataSize());
    EXPECT_EQ(40, secondChunkSlice->GetUncompressedDataSize());
    EXPECT_EQ(20, secondChunkSlice->GetRowCount());
}

TEST_F(TPushBasedShuffleChunkPoolTest, FlushesSameChunkRangesByTarget)
{
    auto pool = CreatePool(
        /*partitionCount*/ 1,
        /*targetUncompressedDataSizePerJob*/ 100,
        /*maxDataSliceCountPerJob*/ 1,
        GetTestLogger());
    auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    pool->RegisterChunkWriteSession(0, chunkId, {});
    pool->UpdateChunkWriteSession(chunkId, {
        .DataWeight = 60,
        .CompressedDataSize = 60,
        .UncompressedDataSize = 60,
        .RecordCount = 1,
        .RowCount = 1,
    });
    pool->UpdateChunkWriteSession(chunkId, {
        .DataWeight = 120,
        .CompressedDataSize = 120,
        .UncompressedDataSize = 120,
        .RecordCount = 2,
        .RowCount = 2,
    });
    pool->FinishChunkWriteSession(chunkId, {
        .DataWeight = 180,
        .CompressedDataSize = 180,
        .UncompressedDataSize = 180,
        .RecordCount = 3,
        .RowCount = 3,
    });
    pool->GetInput()->Finish();

    auto output = pool->GetOutput(0);
    EXPECT_EQ(2, output->GetJobCounter()->GetPending());
    EXPECT_EQ(2, pool->GetTotalDataSliceCount());
}

TEST_F(TPushBasedShuffleChunkPoolTest, ExtrapolationKeepsOneBytePerRecord)
{
    auto pool = CreatePool(
        /*partitionCount*/ 2,
        /*targetUncompressedDataSizePerJob*/ 1000,
        /*maxDataSliceCountPerJob*/ 10,
        GetTestLogger());
    auto sampledChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    auto sealedChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    pool->RegisterChunkWriteSession(0, sampledChunkId, {});
    pool->RegisterChunkWriteSession(1, sealedChunkId, {});

    // A one-byte sample extrapolated over many records would round down to nothing
    // without the floor.
    pool->FinishChunkWriteSession(sampledChunkId, {
        .DataWeight = 1,
        .CompressedDataSize = 1,
        .UncompressedDataSize = 1,
        .RecordCount = 1,
        .RowCount = 1,
    });
    pool->FinishChunkWriteSessionFromSeal(sealedChunkId, MakeSealSummary(100, 100));
    pool->GetInput()->Finish();

    auto output = pool->GetOutput(1);
    auto cookie = output->Extract();
    ASSERT_NE(IChunkPoolOutput::NullCookie, cookie);
    auto stripeList = output->GetStripeList(cookie);
    const auto& chunkSlice =
        stripeList->Stripes().front()->DataSlices().front()->ChunkSlices.front();

    EXPECT_EQ(100, chunkSlice->UpperLimit().RowIndex);
    EXPECT_EQ(100, chunkSlice->GetUncompressedDataSize());
}

TEST_F(TPushBasedShuffleChunkPoolDeathTest, SealWithEmptyRecordsAborts)
{
    EXPECT_DEATH({
        auto pool = CreatePool(
            /*partitionCount*/ 1,
            /*targetUncompressedDataSizePerJob*/ 1000,
            /*maxDataSliceCountPerJob*/ 10,
            GetTestLogger());
        auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
        pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});

        // A sealed chunk cannot hold records without occupying compressed bytes.
        try {
            pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary(5));
        } catch (...) {
        }
    }, "missingCompressedDataSize >= missingRecordCount");
}

TEST_F(TPushBasedShuffleChunkPoolTest, RestoresOptions)
{
    TPushBasedShuffleChunkPoolOptions options{
        .PartitionCount = 7,
        .TargetUncompressedDataSizePerJob = 11,
        .MaxDataSliceCountPerJob = 13,
        .SealFallbackCompressionRatio = 0.5,
        .SealFallbackRowCountPerRecord = 19,
        .Logger = GetTestLogger(),
    };

    TBlobOutput output;
    TSaveContext saveContext(&output);
    Save(saveContext, options);
    saveContext.Finish();
    auto blob = output.Flush();

    TPushBasedShuffleChunkPoolOptions restoredOptions;
    auto rowBuffer = New<NTableClient::TRowBuffer>();
    TMemoryInput input(blob.Begin(), blob.Size());
    TLoadContext loadContext(
        &input,
        rowBuffer,
        NControllerAgent::GetCurrentSnapshotVersion());
    Load(loadContext, restoredOptions);

    EXPECT_EQ(7, restoredOptions.PartitionCount);
    EXPECT_EQ(11, restoredOptions.TargetUncompressedDataSizePerJob);
    EXPECT_EQ(13, restoredOptions.MaxDataSliceCountPerJob);
    EXPECT_DOUBLE_EQ(0.5, restoredOptions.SealFallbackCompressionRatio);
    EXPECT_EQ(19, restoredOptions.SealFallbackRowCountPerRecord);
    EXPECT_EQ(options.Logger.GetCategory()->Name, restoredOptions.Logger.GetCategory()->Name);
}

TEST_F(TPushBasedShuffleChunkPoolTest, RestoresConfiguredSealFallbacks)
{
    auto pool = CreatePushBasedShuffleChunkPool(TPushBasedShuffleChunkPoolOptions{
        .PartitionCount = 1,
        .TargetUncompressedDataSizePerJob = 1000,
        .MaxDataSliceCountPerJob = 10,
        .SealFallbackCompressionRatio = 0.5,
        .SealFallbackRowCountPerRecord = 12,
        .Logger = GetTestLogger(),
    });
    auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    pool->RegisterChunkWriteSession(0, chunkId, {});
    pool->GetInput()->Finish();

    TBlobOutput output;
    TSaveContext saveContext(&output);
    Save(saveContext, pool);
    saveContext.Finish();
    auto blob = output.Flush();
    pool.Reset();

    auto rowBuffer = New<NTableClient::TRowBuffer>();
    TMemoryInput input(blob.Begin(), blob.Size());
    TLoadContext loadContext(
        &input,
        rowBuffer,
        NControllerAgent::GetCurrentSnapshotVersion());
    Load(loadContext, pool);

    pool->FinishChunkWriteSessionFromSeal(chunkId, MakeSealSummary(5, 101));

    auto restoredOutput = pool->GetOutput(0);
    ASSERT_EQ(1, restoredOutput->GetJobCounter()->GetPending());
    auto cookie = restoredOutput->Extract();
    ASSERT_NE(IChunkPoolOutput::NullCookie, cookie);
    auto stripeList = restoredOutput->GetStripeList(cookie);
    const auto& chunkSlice =
        stripeList->Stripes().front()->DataSlices().front()->ChunkSlices.front();

    EXPECT_EQ(202, chunkSlice->GetDataWeight());
    EXPECT_EQ(101, chunkSlice->GetCompressedDataSize());
    EXPECT_EQ(202, chunkSlice->GetUncompressedDataSize());
    EXPECT_EQ(60, chunkSlice->GetRowCount());
}

TEST_F(TPushBasedShuffleChunkPoolTest, AcceptsMaximumRepresentableSealFallbackEstimate)
{
    auto pool = CreatePushBasedShuffleChunkPool(TPushBasedShuffleChunkPoolOptions{
        .PartitionCount = 1,
        .TargetUncompressedDataSizePerJob = std::numeric_limits<i64>::max(),
        .MaxDataSliceCountPerJob = 1,
        .SealFallbackCompressionRatio = 1.0,
        .SealFallbackRowCountPerRecord = 1,
        .Logger = GetTestLogger(),
    });
    auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    pool->RegisterChunkWriteSession(0, chunkId, {});

    EXPECT_NO_THROW(pool->FinishChunkWriteSessionFromSeal(
        chunkId,
        MakeSealSummary(1, std::numeric_limits<i64>::max())));
    pool->GetInput()->Finish();

    auto output = pool->GetOutput(0);
    ASSERT_EQ(1, output->GetJobCounter()->GetPending());
    auto cookie = output->Extract();
    ASSERT_NE(IChunkPoolOutput::NullCookie, cookie);
    auto stripeList = output->GetStripeList(cookie);
    const auto& chunkSlice =
        stripeList->Stripes().front()->DataSlices().front()->ChunkSlices.front();

    EXPECT_EQ(std::numeric_limits<i64>::max(), chunkSlice->GetDataWeight());
    EXPECT_EQ(std::numeric_limits<i64>::max(), chunkSlice->GetCompressedDataSize());
    EXPECT_EQ(std::numeric_limits<i64>::max(), chunkSlice->GetUncompressedDataSize());
}

TEST_F(TPushBasedShuffleChunkPoolTest, ValidatesSessionLifecycle)
{
    auto pool = CreatePool(
        /*partitionCount*/ 1,
        /*targetUncompressedDataSizePerJob*/ 1000,
        /*maxDataSliceCountPerJob*/ 10,
        GetTestLogger());
    auto exactChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    auto sealedChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    const TDistributedChunkSessionProgress exactStatistics{
        .DataWeight = 40,
        .CompressedDataSize = 20,
        .UncompressedDataSize = 60,
        .RecordCount = 2,
        .RowCount = 200,
    };
    const auto sealSummary = MakeSealSummary(3, 90);

    pool->RegisterChunkWriteSession(0, exactChunkId, {});
    pool->RegisterChunkWriteSession(0, sealedChunkId, {});

    pool->UpdateChunkWriteSession(exactChunkId, exactStatistics);
    EXPECT_NO_THROW(pool->UpdateChunkWriteSession(exactChunkId, exactStatistics));

    pool->FinishChunkWriteSession(exactChunkId, exactStatistics);

    pool->FinishChunkWriteSessionFromSeal(sealedChunkId, sealSummary);

    pool->GetInput()->Finish();

    auto output = pool->GetOutput(0);
    ASSERT_EQ(1, output->GetJobCounter()->GetPending());
    auto cookie = output->Extract();
    ASSERT_NE(IChunkPoolOutput::NullCookie, cookie);
    EXPECT_EQ(2, output->GetStripeListSliceCount(cookie));
    auto stripeList = output->GetStripeList(cookie);
    ASSERT_EQ(2u, stripeList->Stripes().front()->DataSlices().size());
    EXPECT_EQ(
        2,
        stripeList->Stripes().front()->DataSlices().front()->ChunkSlices.front()->UpperLimit().RowIndex);
    EXPECT_EQ(
        3,
        stripeList->Stripes().front()->DataSlices().back()->ChunkSlices.front()->UpperLimit().RowIndex);
}

TEST_F(TPushBasedShuffleChunkPoolTest, SerializesDistributedJournalInputChunkMetadata)
{
    auto pool = CreatePool(
        /*partitionCount*/ 1,
        /*targetUncompressedDataSizePerJob*/ 64_MB,
        /*maxDataSliceCountPerJob*/ 10,
        GetTestLogger());
    auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    const TDistributedChunkSessionProgress statistics{
        .DataWeight = 11,
        .CompressedDataSize = 32_MB,
        .UncompressedDataSize = 22,
        .RecordCount = 2,
        .RowCount = 200,
    };

    pool->RegisterChunkWriteSession(/*partitionIndex*/ 0, chunkId, {});
    pool->FinishChunkWriteSession(chunkId, statistics);
    pool->GetInput()->Finish();

    auto output = pool->GetOutput(0);
    auto cookie = output->Extract();
    ASSERT_NE(IChunkPoolOutput::NullCookie, cookie);
    const auto& chunkSlice =
        output->GetStripeList(cookie)->Stripes().front()->DataSlices().front()->ChunkSlices.front();
    auto inputChunk = chunkSlice->GetInputChunk();

    EXPECT_EQ(EChunkFormat::JournalDistributed, inputChunk->GetChunkFormat());

    NChunkClient::NProto::TChunkSpec chunkSpec;
    ToProto(
        &chunkSpec,
        chunkSlice,
        NTableClient::TComparator(),
        EDataSourceType::UnversionedTable);

    // The spec must describe the chunk truthfully; per-slice sizes travel in the slice
    // overrides, exactly as they do for table chunks.
    EXPECT_EQ(EChunkType::Journal, FromProto<EChunkType>(chunkSpec.chunk_meta().type()));
    EXPECT_EQ(EChunkFormat::JournalDistributed, FromProto<EChunkFormat>(chunkSpec.chunk_meta().format()));
    EXPECT_EQ(statistics.RecordCount, chunkSpec.upper_limit().row_index());
}

TEST_F(TPushBasedShuffleChunkPoolTest, RestoresLiveSessionsAndOpenBuilder)
{
    auto pool = CreatePool(
        /*partitionCount*/ 2,
        /*targetUncompressedDataSizePerJob*/ 1000,
        /*maxDataSliceCountPerJob*/ 10,
        GetTestLogger());
    auto exactChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    auto sealedChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    // This session stays without progress until after the load, so that its first range
    // exercises the counters of a partition that is untouched at save time.
    auto idleChunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    pool->RegisterChunkWriteSession(0, exactChunkId, {});
    pool->RegisterChunkWriteSession(0, sealedChunkId, {});
    pool->RegisterChunkWriteSession(1, idleChunkId, {});

    const TDistributedChunkSessionProgress exactPrefixStatistics{
        .DataWeight = 40,
        .CompressedDataSize = 20,
        .UncompressedDataSize = 60,
        .RecordCount = 2,
        .RowCount = 200,
    };
    const TDistributedChunkSessionProgress exactFinalStatistics{
        .DataWeight = 80,
        .CompressedDataSize = 40,
        .UncompressedDataSize = 120,
        .RecordCount = 4,
        .RowCount = 400,
    };
    const TDistributedChunkSessionProgress sealedPrefixStatistics{
        .DataWeight = 20,
        .CompressedDataSize = 10,
        .UncompressedDataSize = 30,
        .RecordCount = 1,
        .RowCount = 100,
    };
    const TDistributedChunkSessionProgress idleFinalStatistics{
        .DataWeight = 10,
        .CompressedDataSize = 5,
        .UncompressedDataSize = 15,
        .RecordCount = 1,
        .RowCount = 50,
    };
    pool->UpdateChunkWriteSession(exactChunkId, exactPrefixStatistics);
    pool->UpdateChunkWriteSession(sealedChunkId, sealedPrefixStatistics);
    pool->GetInput()->Finish();

    TBlobOutput output;
    TSaveContext saveContext(&output);
    Save(saveContext, pool);
    saveContext.Finish();
    auto blob = output.Flush();
    pool.Reset();

    auto rowBuffer = New<NTableClient::TRowBuffer>();
    TMemoryInput input(blob.Begin(), blob.Size());
    TLoadContext loadContext(
        &input,
        rowBuffer,
        NControllerAgent::GetCurrentSnapshotVersion());
    Load(loadContext, pool);

    ASSERT_EQ(2, pool->GetTotalDataSliceCount());
    ASSERT_EQ(1, pool->GetTotalJobCount());

    // The first range of the idle session opens a job and a data slice in a partition that
    // held neither at save time, so the pool-level totals move only if the per-partition
    // counters were reattached to them exactly once during the load.
    pool->UpdateChunkWriteSession(idleChunkId, idleFinalStatistics);
    EXPECT_EQ(3, pool->GetTotalDataSliceCount());
    EXPECT_EQ(2, pool->GetTotalJobCount());

    ASSERT_NO_THROW(pool->FinishChunkWriteSession(exactChunkId, exactFinalStatistics));
    EXPECT_EQ(3, pool->GetTotalDataSliceCount());
    ASSERT_NO_THROW(pool->FinishChunkWriteSessionFromSeal(
        sealedChunkId,
        MakeSealSummary(3, 999)));
    ASSERT_NO_THROW(pool->FinishChunkWriteSession(idleChunkId, idleFinalStatistics));
    EXPECT_EQ(3, pool->GetTotalDataSliceCount());
    EXPECT_EQ(2, pool->GetTotalJobCount());

    auto restoredOutput = pool->GetOutput(0);
    ASSERT_EQ(1, restoredOutput->GetJobCounter()->GetPending());
    auto cookie = restoredOutput->Extract();
    ASSERT_NE(IChunkPoolOutput::NullCookie, cookie);
    auto stripeList = restoredOutput->GetStripeList(cookie);
    EXPECT_TRUE(stripeList->IsApproximate());

    const auto& dataSlices = stripeList->Stripes().front()->DataSlices();
    ASSERT_EQ(2u, dataSlices.size());
    const auto& exactChunkSlice = dataSlices[0]->ChunkSlices.front();
    const auto& sealedChunkSlice = dataSlices[1]->ChunkSlices.front();

    EXPECT_EQ(exactChunkId, exactChunkSlice->GetInputChunk()->GetChunkId());
    EXPECT_EQ(0, exactChunkSlice->LowerLimit().RowIndex);
    EXPECT_EQ(4, exactChunkSlice->UpperLimit().RowIndex);
    EXPECT_EQ(80, exactChunkSlice->GetDataWeight());
    EXPECT_EQ(40, exactChunkSlice->GetCompressedDataSize());
    EXPECT_EQ(120, exactChunkSlice->GetUncompressedDataSize());
    EXPECT_EQ(400, exactChunkSlice->GetRowCount());

    EXPECT_EQ(sealedChunkId, sealedChunkSlice->GetInputChunk()->GetChunkId());
    EXPECT_EQ(0, sealedChunkSlice->LowerLimit().RowIndex);
    EXPECT_EQ(3, sealedChunkSlice->UpperLimit().RowIndex);
    EXPECT_EQ(60, sealedChunkSlice->GetDataWeight());
    EXPECT_EQ(999, sealedChunkSlice->GetCompressedDataSize());
    EXPECT_EQ(90, sealedChunkSlice->GetUncompressedDataSize());
    EXPECT_EQ(300, sealedChunkSlice->GetRowCount());
}

TEST_F(TPushBasedShuffleChunkPoolTest, RequeuesImmutableJobAfterFailureAbortAndLoss)
{
    auto pool = CreatePool(
        /*partitionCount*/ 1,
        /*targetUncompressedDataSizePerJob*/ 1000,
        /*maxDataSliceCountPerJob*/ 10,
        GetTestLogger());
    auto chunkId = MakeRandomId(EObjectType::JournalChunk, TCellTag(0x42));
    pool->RegisterChunkWriteSession(0, chunkId, {});
    pool->FinishChunkWriteSession(chunkId, {
        .DataWeight = 10,
        .CompressedDataSize = 5,
        .UncompressedDataSize = 10,
        .RecordCount = 1,
        .RowCount = 10,
    });
    pool->GetInput()->Finish();

    auto output = pool->GetOutput(0);
    int completedSignalCount = 0;
    int uncompletedSignalCount = 0;
    output->SubscribeCompleted(BIND([&] {
        EXPECT_TRUE(output->IsCompleted());
        ++completedSignalCount;
    }));
    output->SubscribeUncompleted(BIND([&] {
        EXPECT_FALSE(output->IsCompleted());
        ++uncompletedSignalCount;
    }));

    auto cookie = output->Extract();
    ASSERT_NE(IChunkPoolOutput::NullCookie, cookie);
    auto stripeList = output->GetStripeList(cookie);

    output->Failed(cookie);
    EXPECT_EQ(1, output->GetJobCounter()->GetFailed());
    EXPECT_EQ(cookie, output->Extract());

    output->Aborted(cookie, NScheduler::EAbortReason::Scheduler);
    EXPECT_EQ(1, output->GetJobCounter()->GetAbortedTotal());
    EXPECT_EQ(cookie, output->Extract());

    NControllerAgent::TCompletedJobSummary summary;
    output->Completed(cookie, summary);
    EXPECT_TRUE(output->IsCompleted());
    EXPECT_EQ(1, completedSignalCount);
    EXPECT_EQ(0, uncompletedSignalCount);

    output->Lost(cookie);
    EXPECT_FALSE(output->IsCompleted());
    EXPECT_EQ(1, output->GetJobCounter()->GetLost());
    EXPECT_EQ(1, completedSignalCount);
    EXPECT_EQ(1, uncompletedSignalCount);
    EXPECT_EQ(stripeList, output->GetStripeList(cookie));
    EXPECT_EQ(cookie, output->Extract());

    output->Completed(cookie, summary);
    EXPECT_TRUE(output->IsCompleted());
    EXPECT_EQ(2, completedSignalCount);
    EXPECT_EQ(1, uncompletedSignalCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkPools

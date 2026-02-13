#include "chunk_pools_helpers.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/chunk_pool_outputs_merger.h>
#include <yt/yt/server/lib/chunk_pools/unordered_chunk_pool.h>

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/blob_output.h>

#include <random>

namespace NYT::NChunkPools {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;

using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputMergerTest
    : public TChunkPoolTestBase
{
protected:
    struct TExpectedCounters
    {
        TExpectedCounter JobCounter;
        TExpectedCounter DataWeightCounter;
        TExpectedCounter RowCounter;
        TExpectedCounter DataSliceCounter;
    };

    void CheckCounters(const TExpectedCounters& counters) const
    {
        CheckCounter(Merger_->GetJobCounter(), counters.JobCounter);
        CheckCounter(Merger_->GetDataWeightCounter(), counters.DataWeightCounter);
        CheckCounter(Merger_->GetRowCounter(), counters.RowCounter);
        CheckCounter(Merger_->GetDataSliceCounter(), counters.DataSliceCounter);
    }

    TInputChunkPtr CreateChunk(
        i64 weight = 1_KB,
        i64 rowCount = 1000)
    {
        auto inputChunk = New<TInputChunk>();
        inputChunk->SetChunkId(MakeId(
            EObjectType::Chunk,
            TCellTag(0x42),
            std::uniform_int_distribution<ui64>()(Gen_),
            std::uniform_int_distribution<ui32>()(Gen_)));
        inputChunk->SetCompressedDataSize(weight);
        inputChunk->SetTotalUncompressedDataSize(weight);
        inputChunk->SetTotalDataWeight(weight);
        inputChunk->SetTableRowIndex(0);
        inputChunk->SetTotalRowCount(rowCount);
        return inputChunk;
    }

    IPersistentChunkPoolPtr CreateUnorderedChunkPool(int jobCount = 1, bool explicitJobCount = true)
    {
        TUnorderedChunkPoolOptions options{
            .JobSizeConstraints = CreateExplicitJobSizeConstraints(
                /*canAdjustDataSizePerJob*/ false,
                /*isExplicitJobCount*/ explicitJobCount,
                /*jobCount*/ jobCount,
                /*dataWeightPerJob*/ Inf64,
                /*primaryDataWeightPerJob*/ Inf64,
                /*compressedDataSizePerJob*/ Inf64,
                /*primaryCompressedDataSizePerJob*/ Inf64,
                /*maxDataSlicesPerJob*/ Inf32,
                /*maxDataSizePerJob*/ Inf64,
                /*maxPrimaryDataWeightPerJob*/ 0,
                /*maxCompressedDataSizePerJob*/ Inf64,
                /*maxPrimaryCompressedDataSizePerJob*/ Inf64,
                /*inputSliceDataWeight*/ Inf64,
                /*inputSliceRowCount*/ Inf64,
                /*batchRowCount*/ {},
                /*foreignSliceDataWeight*/ 0,
                /*samplingRate*/ {}),
            .MinTeleportChunkSize = Inf64,
            .RowBuffer = New<TRowBuffer>(),
            .Logger = GetTestLogger(),
        };

        return NChunkPools::CreateUnorderedChunkPool(options, {});
    }

    static TLegacyDataSlicePtr BuildDataSliceByChunk(const TInputChunkPtr& chunk)
    {
        auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
        dataSlice->SetInputStreamIndex(chunk->GetTableIndex());
        dataSlice->TransformToNewKeyless();
        return dataSlice;
    }

    std::mt19937 Gen_;
    IPersistentChunkPoolOutputPtr Merger_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TChunkPoolOutputMergerTest, SimpleMerge)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(2u, stripeList->Stripes().size());

    TCompletedJobSummary summary{};
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2},
    });
}

TEST_F(TChunkPoolOutputMergerTest, ExtractWithoutAllPoolsReady)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    // Don't finish pool2.

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    // Should be suspended because not all pools are ready.
    CheckCounters({
        .JobCounter = {.Total = 1, .Suspended = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Suspended = 2_KBs},
        .RowCounter = {.Total = 2000, .Suspended = 2000},
        .DataSliceCounter = {.Total = 2, .Suspended = 2},
    });

    // Now finish pool2.
    pool2->Finish();

    // Should become pending.
    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    // Now extraction should succeed.
    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(2u, stripeList->Stripes().size());

    TCompletedJobSummary summary{};
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2},
    });
}

TEST_F(TChunkPoolOutputMergerTest, FailedJob)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    Merger_->Failed(cookie);

    EXPECT_FALSE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1, .Failed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs, .Failed = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000, .Failed = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2, .Failed = 2},
    });

    // Can extract again after failure.
    auto cookie2 = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1, .Failed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs, .Failed = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000, .Failed = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2, .Failed = 2},
    });

    TCompletedJobSummary summary{};
    Merger_->Completed(cookie2, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1, .Failed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs, .Failed = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000, .Failed = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2, .Failed = 2},
    });
}

TEST_F(TChunkPoolOutputMergerTest, AbortedJob)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    Merger_->Aborted(cookie, EAbortReason::Scheduler);

    EXPECT_FALSE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1, .Aborted = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs, .Aborted = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000, .Aborted = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2, .Aborted = 2},
    });

    // Can extract again after abort.
    auto cookie2 = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie2);
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1, .Aborted = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs, .Aborted = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000, .Aborted = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2, .Aborted = 2},
    });

    TCompletedJobSummary summary{};
    Merger_->Completed(cookie2, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1, .Aborted = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs, .Aborted = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000, .Aborted = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2, .Aborted = 2},
    });
}

TEST_F(TChunkPoolOutputMergerTest, LostJob)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    TCompletedJobSummary summary{};
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2},
    });

    // Job output is lost.
    Merger_->Lost(cookie);

    EXPECT_FALSE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1, .Lost = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs, .Lost = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000, .Lost = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2, .Lost = 2},
    });

    // Can extract again after lost.
    auto cookie2 = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie2);
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1, .Lost = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs, .Lost = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000, .Lost = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2, .Lost = 2},
    });

    Merger_->Completed(cookie2, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1, .Lost = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs, .Lost = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000, .Lost = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2, .Lost = 2},
    });
}

TEST_F(TChunkPoolOutputMergerTest, MergeMultiplePools)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();
    auto chunk3 = CreateChunk();
    auto chunk4 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool2->Finish();

    auto pool3 = CreateUnorderedChunkPool();
    pool3->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk3)));
    pool3->Finish();

    auto pool4 = CreateUnorderedChunkPool();
    pool4->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk4)));
    pool4->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2, pool3, pool4}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 4_KBs, .Pending = 4_KBs},
        .RowCounter = {.Total = 4000, .Pending = 4000},
        .DataSliceCounter = {.Total = 4, .Pending = 4},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 4_KBs, .Running = 4_KBs},
        .RowCounter = {.Total = 4000, .Running = 4000},
        .DataSliceCounter = {.Total = 4, .Running = 4},
    });

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(4u, stripeList->Stripes().size());

    TCompletedJobSummary summary{};
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 4_KBs, .Completed = 4_KBs},
        .RowCounter = {.Total = 4000, .Completed = 4000},
        .DataSliceCounter = {.Total = 4, .Completed = 4},
    });
}

TEST_F(TChunkPoolOutputMergerTest, Persistence)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    // Persist and restore.
    TBlobOutput output;
    TSaveContext saveContext(&output);
    Save(saveContext, Merger_);
    saveContext.Finish();
    auto blob = output.Flush();
    Merger_.Reset();

    TMemoryInput input(blob.Begin(), blob.Size());
    TLoadContext loadContext(&input, New<TRowBuffer>(), GetCurrentSnapshotVersion());
    Load(loadContext, Merger_);

    // Check state after restore.
    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(2u, stripeList->Stripes().size());

    TCompletedJobSummary summary{};
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2},
    });
}

TEST_PI(
    TChunkPoolOutputMergerTest,
    EmptyUnfinishedPool,
    Bool())
{
    bool isExplicitJobCount = GetParam();

    auto pool1 = CreateUnorderedChunkPool(/*jobCount*/ 1, /*explicitJobCount*/ isExplicitJobCount);
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool(/*jobCount*/ 1, /*explicitJobCount*/ isExplicitJobCount);

    ASSERT_FALSE(pool2->IsCompleted());

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    ASSERT_FALSE(Merger_->IsCompleted());
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    if (isExplicitJobCount) {
        CheckCounters({
            .JobCounter = {.Total = 1, .Suspended = 1},
            .DataWeightCounter = {.Total = 0},
            .RowCounter = {.Total = 0},
            .DataSliceCounter = {.Total = 0},
        });
    } else {
        CheckCounters({
            .JobCounter = {.Total = 0},
            .DataWeightCounter = {.Total = 0},
            .RowCounter = {.Total = 0},
            .DataSliceCounter = {.Total = 0},
        });
    }

    pool2->Finish();
    ASSERT_TRUE(pool2->IsCompleted());

    EXPECT_TRUE(Merger_->IsCompleted());
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 0},
        .DataWeightCounter = {.Total = 0},
        .RowCounter = {.Total = 0},
        .DataSliceCounter = {.Total = 0},
    });
}

TEST_F(TChunkPoolOutputMergerTest, PoolsWithMultipleStripes)
{
    auto chunk1a = CreateChunk();
    auto chunk1b = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1a)));
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1b)));
    pool1->Finish();

    EXPECT_EQ(1, pool1->GetJobCounter()->GetPending());

    auto chunk2a = CreateChunk();
    auto chunk2b = CreateChunk();
    auto chunk2c = CreateChunk();

    auto pool2 = CreateUnorderedChunkPool();
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2a)));
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2b)));
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2c)));
    pool2->Finish();

    EXPECT_EQ(1, pool2->GetJobCounter()->GetPending());

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 5_KBs, .Pending = 5_KBs},
        .RowCounter = {.Total = 5000, .Pending = 5000},
        .DataSliceCounter = {.Total = 5, .Pending = 5},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 5_KBs, .Running = 5_KBs},
        .RowCounter = {.Total = 5000, .Running = 5000},
        .DataSliceCounter = {.Total = 5, .Running = 5},
    });

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(2u, stripeList->Stripes().size());

    EXPECT_EQ(5, Merger_->GetStripeListSliceCount(cookie));

    TCompletedJobSummary summary{};
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 5_KBs, .Completed = 5_KBs},
        .RowCounter = {.Total = 5000, .Completed = 5000},
        .DataSliceCounter = {.Total = 5, .Completed = 5},
    });
}

TEST_F(TChunkPoolOutputMergerTest, PoolsWithMultipleJobs)
{
    auto chunk1a = CreateChunk();
    auto chunk1b = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool(/*jobCount*/ 2);
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1a)));
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1b)));
    pool1->Finish();

    EXPECT_EQ(2, pool1->GetJobCounter()->GetPending());

    auto chunk2a = CreateChunk();
    auto chunk2b = CreateChunk();
    auto chunk2c = CreateChunk();

    auto pool2 = CreateUnorderedChunkPool(/*jobCount*/ 3);
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2a)));
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2b)));
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2c)));
    pool2->Finish();

    EXPECT_EQ(3, pool2->GetJobCounter()->GetPending());

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 5_KBs, .Pending = 5_KBs},
        .RowCounter = {.Total = 5000, .Pending = 5000},
        .DataSliceCounter = {.Total = 5, .Pending = 5},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 5_KBs, .Running = 5_KBs},
        .RowCounter = {.Total = 5000, .Running = 5000},
        .DataSliceCounter = {.Total = 5, .Running = 5},
    });

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(5u, stripeList->Stripes().size());

    TCompletedJobSummary summary{};
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 5_KBs, .Completed = 5_KBs},
        .RowCounter = {.Total = 5000, .Completed = 5000},
        .DataSliceCounter = {.Total = 5, .Completed = 5},
    });
}

enum class EJobOutcome
{
    Completed,
    Failed,
    Aborted,
    Lost,
};

TEST_PI(
    TChunkPoolOutputMergerTest,
    MergeTwoPoolsWithDifferentEmptyPatternsAndJobOutcomes,
    Combine(
        ValuesIn({
            std::pair(true, true),
            std::pair(true, false),
            std::pair(false, true),
        }),
        ValuesIn({
            EJobOutcome::Completed,
            EJobOutcome::Failed,
            EJobOutcome::Aborted,
            EJobOutcome::Lost,
        })
    ),
    std::tuple<std::pair<bool, bool>, EJobOutcome>)
{
    auto [emptyPattern, outcome] = GetParam();
    auto [firstPoolEmpty, secondPoolEmpty] = emptyPattern;

    auto chunk = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    if (!firstPoolEmpty) {
        pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk)));
    }
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    if (!secondPoolEmpty) {
        pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk)));
    }
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    bool bothEmpty = firstPoolEmpty && secondPoolEmpty;

    if (bothEmpty) {
        EXPECT_TRUE(Merger_->IsCompleted());
        CheckCounters({
            .JobCounter = {.Total = 0},
            .DataWeightCounter = {.Total = 0},
            .RowCounter = {.Total = 0},
            .DataSliceCounter = {.Total = 0},
        });
        return;
    }

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 1_KB, .Pending = 1_KB},
        .RowCounter = {.Total = 1000, .Pending = 1000},
        .DataSliceCounter = {.Total = 1, .Pending = 1},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 1_KB, .Running = 1_KB},
        .RowCounter = {.Total = 1000, .Running = 1000},
        .DataSliceCounter = {.Total = 1, .Running = 1},
    });
    EXPECT_EQ(1u, Merger_->GetStripeList(cookie)->Stripes().size());

    TCompletedJobSummary summary{};
    bool needsRetry = outcome != EJobOutcome::Completed;

    switch (outcome) {
        case EJobOutcome::Completed:
            Merger_->Completed(cookie, summary);
            break;
        case EJobOutcome::Failed:
            Merger_->Failed(cookie);
            CheckCounters({
                .JobCounter = {.Total = 1, .Pending = 1, .Failed = 1},
                .DataWeightCounter = {.Total = 1_KB, .Pending = 1_KB, .Failed = 1_KB},
                .RowCounter = {.Total = 1000, .Pending = 1000, .Failed = 1000},
                .DataSliceCounter = {.Total = 1, .Pending = 1, .Failed = 1},
            });
            break;
        case EJobOutcome::Aborted:
            Merger_->Aborted(cookie, EAbortReason::Scheduler);
            CheckCounters({
                .JobCounter = {.Total = 1, .Pending = 1, .Aborted = 1},
                .DataWeightCounter = {.Total = 1_KB, .Pending = 1_KB, .Aborted = 1_KB},
                .RowCounter = {.Total = 1000, .Pending = 1000, .Aborted = 1000},
                .DataSliceCounter = {.Total = 1, .Pending = 1, .Aborted = 1},
            });
            break;
        case EJobOutcome::Lost:
            Merger_->Completed(cookie, summary);
            EXPECT_TRUE(Merger_->IsCompleted());
            Merger_->Lost(cookie);
            CheckCounters({
                .JobCounter = {.Total = 1, .Pending = 1, .Lost = 1},
                .DataWeightCounter = {.Total = 1_KB, .Pending = 1_KB, .Lost = 1_KB},
                .RowCounter = {.Total = 1000, .Pending = 1000, .Lost = 1000},
                .DataSliceCounter = {.Total = 1, .Pending = 1, .Lost = 1},
            });
            break;
    }

    if (needsRetry) {
        auto retryCookie = Merger_->Extract();
        EXPECT_NE(IChunkPoolOutput::NullCookie, retryCookie);
        EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

        Merger_->Completed(retryCookie, summary);
    }

    EXPECT_TRUE(Merger_->IsCompleted());
    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    switch (outcome) {
        case EJobOutcome::Completed:
            CheckCounters({
                .JobCounter = {.Total = 1, .Completed = 1},
                .DataWeightCounter = {.Total = 1_KB, .Completed = 1_KB},
                .RowCounter = {.Total = 1000, .Completed = 1000},
                .DataSliceCounter = {.Total = 1, .Completed = 1},
            });
            break;
        case EJobOutcome::Failed:
            CheckCounters({
                .JobCounter = {.Total = 1, .Completed = 1, .Failed = 1},
                .DataWeightCounter = {.Total = 1_KB, .Completed = 1_KB, .Failed = 1_KB},
                .RowCounter = {.Total = 1000, .Completed = 1000, .Failed = 1000},
                .DataSliceCounter = {.Total = 1, .Completed = 1, .Failed = 1},
            });
            break;
        case EJobOutcome::Aborted:
            CheckCounters({
                .JobCounter = {.Total = 1, .Completed = 1, .Aborted = 1},
                .DataWeightCounter = {.Total = 1_KB, .Completed = 1_KB, .Aborted = 1_KB},
                .RowCounter = {.Total = 1000, .Completed = 1000, .Aborted = 1000},
                .DataSliceCounter = {.Total = 1, .Completed = 1, .Aborted = 1},
            });
            break;
        case EJobOutcome::Lost:
            CheckCounters({
                .JobCounter = {.Total = 1, .Completed = 1, .Lost = 1},
                .DataWeightCounter = {.Total = 1_KB, .Completed = 1_KB, .Lost = 1_KB},
                .RowCounter = {.Total = 1000, .Completed = 1000, .Lost = 1000},
                .DataSliceCounter = {.Total = 1, .Completed = 1, .Lost = 1},
            });
            break;
    }
}

TEST_F(TChunkPoolOutputMergerTest, SuspendedChunksInUnderlyingPool)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();
    auto chunk3 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    auto inputCookie3 = pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk3)));
    pool2->Suspend(inputCookie3);
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Suspended = 1},
        .DataWeightCounter = {.Total = 3_KBs, .Suspended = 3_KBs},
        .RowCounter = {.Total = 3000, .Suspended = 3000},
        .DataSliceCounter = {.Total = 3, .Suspended = 3},
    });

    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    pool2->Resume(inputCookie3);

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 3_KBs, .Pending = 3_KBs},
        .RowCounter = {.Total = 3000, .Pending = 3000},
        .DataSliceCounter = {.Total = 3, .Pending = 3},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 3_KBs, .Running = 3_KBs},
        .RowCounter = {.Total = 3000, .Running = 3000},
        .DataSliceCounter = {.Total = 3, .Running = 3},
    });

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(2u, stripeList->Stripes().size());

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 3_KBs, .Completed = 3_KBs},
        .RowCounter = {.Total = 3000, .Completed = 3000},
        .DataSliceCounter = {.Total = 3, .Completed = 3},
    });
}

TEST_F(TChunkPoolOutputMergerTest, MultipleSuspendedChunksInDifferentPools)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();
    auto chunk3 = CreateChunk();
    auto chunk4 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    auto inputCookie2 = pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool1->Suspend(inputCookie2);
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk3)));
    auto inputCookie4 = pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk4)));
    pool2->Suspend(inputCookie4);
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Suspended = 1},
        .DataWeightCounter = {.Total = 4_KBs, .Suspended = 4_KBs},
        .RowCounter = {.Total = 4000, .Suspended = 4000},
        .DataSliceCounter = {.Total = 4, .Suspended = 4},
    });

    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    pool1->Resume(inputCookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Suspended = 1},
        .DataWeightCounter = {.Total = 4_KBs, .Suspended = 4_KBs},
        .RowCounter = {.Total = 4000, .Suspended = 4000},
        .DataSliceCounter = {.Total = 4, .Suspended = 4},
    });

    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    pool2->Resume(inputCookie4);

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 4_KBs, .Pending = 4_KBs},
        .RowCounter = {.Total = 4000, .Pending = 4000},
        .DataSliceCounter = {.Total = 4, .Pending = 4},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 4_KBs, .Running = 4_KBs},
        .RowCounter = {.Total = 4000, .Running = 4000},
        .DataSliceCounter = {.Total = 4, .Running = 4},
    });

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(2u, stripeList->Stripes().size());

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 4_KBs, .Completed = 4_KBs},
        .RowCounter = {.Total = 4000, .Completed = 4000},
        .DataSliceCounter = {.Total = 4, .Completed = 4},
    });
}

TEST_F(TChunkPoolOutputMergerTest, SuspendChunkAfterMergerCreated)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();
    auto chunk3 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    auto inputCookie3 = pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk3)));
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 3_KBs, .Pending = 3_KBs},
        .RowCounter = {.Total = 3000, .Pending = 3000},
        .DataSliceCounter = {.Total = 3, .Pending = 3},
    });

    pool2->Suspend(inputCookie3);

    CheckCounters({
        .JobCounter = {.Total = 1, .Suspended = 1},
        .DataWeightCounter = {.Total = 3_KBs, .Suspended = 3_KBs},
        .RowCounter = {.Total = 3000, .Suspended = 3000},
        .DataSliceCounter = {.Total = 3, .Suspended = 3},
    });

    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    pool2->Resume(inputCookie3);

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 3_KBs, .Pending = 3_KBs},
        .RowCounter = {.Total = 3000, .Pending = 3000},
        .DataSliceCounter = {.Total = 3, .Pending = 3},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 3_KBs, .Running = 3_KBs},
        .RowCounter = {.Total = 3000, .Running = 3000},
        .DataSliceCounter = {.Total = 3, .Running = 3},
    });

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 3_KBs, .Completed = 3_KBs},
        .RowCounter = {.Total = 3000, .Completed = 3000},
        .DataSliceCounter = {.Total = 3, .Completed = 3},
    });
}

TEST_F(TChunkPoolOutputMergerTest, SuspendAndResumeWhileJobRunning)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    auto inputCookie2 = pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    pool2->Suspend(inputCookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2},
    });
}

TEST_F(TChunkPoolOutputMergerTest, FailedJobWithSuspendedChunks)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    auto inputCookie2 = pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    Merger_->Failed(cookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1, .Failed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs, .Failed = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000, .Failed = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2, .Failed = 2},
    });

    pool2->Suspend(inputCookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Suspended = 1, .Failed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Suspended = 2_KBs, .Failed = 2_KBs},
        .RowCounter = {.Total = 2000, .Suspended = 2000, .Failed = 2000},
        .DataSliceCounter = {.Total = 2, .Suspended = 2, .Failed = 2},
    });

    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    pool2->Resume(inputCookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1, .Failed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs, .Failed = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000, .Failed = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2, .Failed = 2},
    });

    auto cookie2 = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1, .Failed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs, .Failed = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000, .Failed = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2, .Failed = 2},
    });

    TCompletedJobSummary summary;
    Merger_->Completed(cookie2, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1, .Failed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs, .Failed = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000, .Failed = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2, .Failed = 2},
    });
}

TEST_F(TChunkPoolOutputMergerTest, AbortedJobWithSuspendedChunks)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    auto inputCookie2 = pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    Merger_->Aborted(cookie, EAbortReason::Scheduler);

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1, .Aborted = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs, .Aborted = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000, .Aborted = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2, .Aborted = 2},
    });

    pool2->Suspend(inputCookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Suspended = 1, .Aborted = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Suspended = 2_KBs, .Aborted = 2_KBs},
        .RowCounter = {.Total = 2000, .Suspended = 2000, .Aborted = 2000},
        .DataSliceCounter = {.Total = 2, .Suspended = 2, .Aborted = 2},
    });

    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    pool2->Resume(inputCookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1, .Aborted = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs, .Aborted = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000, .Aborted = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2, .Aborted = 2},
    });

    auto cookie2 = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1, .Aborted = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs, .Aborted = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000, .Aborted = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2, .Aborted = 2},
    });

    TCompletedJobSummary summary;
    Merger_->Completed(cookie2, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1, .Aborted = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs, .Aborted = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000, .Aborted = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2, .Aborted = 2},
    });
}

TEST_F(TChunkPoolOutputMergerTest, LostJobWithSuspendedChunks)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    auto inputCookie2 = pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2},
    });

    Merger_->Lost(cookie);

    EXPECT_FALSE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1, .Lost = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs, .Lost = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000, .Lost = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2, .Lost = 2},
    });

    pool2->Suspend(inputCookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Suspended = 1, .Lost = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Suspended = 2_KBs, .Lost = 2_KBs},
        .RowCounter = {.Total = 2000, .Suspended = 2000, .Lost = 2000},
        .DataSliceCounter = {.Total = 2, .Suspended = 2, .Lost = 2},
    });

    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    pool2->Resume(inputCookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1, .Lost = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs, .Lost = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000, .Lost = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2, .Lost = 2},
    });

    auto cookie2 = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1, .Lost = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs, .Lost = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000, .Lost = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2, .Lost = 2},
    });

    Merger_->Completed(cookie2, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1, .Lost = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs, .Lost = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000, .Lost = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2, .Lost = 2},
    });
}

TEST_F(TChunkPoolOutputMergerTest, AllChunksSuspended)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    auto inputCookie1 = pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    auto inputCookie2 = pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    pool1->Suspend(inputCookie1);
    pool2->Suspend(inputCookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Suspended = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Suspended = 2_KBs},
        .RowCounter = {.Total = 2000, .Suspended = 2000},
        .DataSliceCounter = {.Total = 2, .Suspended = 2},
    });

    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    pool1->Resume(inputCookie1);

    CheckCounters({
        .JobCounter = {.Total = 1, .Suspended = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Suspended = 2_KBs},
        .RowCounter = {.Total = 2000, .Suspended = 2000},
        .DataSliceCounter = {.Total = 2, .Suspended = 2},
    });

    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    pool2->Resume(inputCookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2},
    });
}

TEST_F(TChunkPoolOutputMergerTest, CompletedThenSuspendThenLostThenCompleted)
{
    auto chunk1 = CreateChunk();
    auto chunk2 = CreateChunk();

    auto pool1 = CreateUnorderedChunkPool();
    pool1->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk1)));
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    auto inputCookie2 = pool2->Add(New<TChunkStripe>(BuildDataSliceByChunk(chunk2)));
    pool2->Finish();

    Merger_ = MergeChunkPoolOutputs({pool1, pool2}, GetTestLogger());

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2},
    });

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2},
    });

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2},
    });

    pool2->Suspend(inputCookie2);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2},
    });

    Merger_->Lost(cookie);

    EXPECT_FALSE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Suspended = 1, .Lost = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Suspended = 2_KBs, .Lost = 2_KBs},
        .RowCounter = {.Total = 2000, .Suspended = 2000, .Lost = 2000},
        .DataSliceCounter = {.Total = 2, .Suspended = 2, .Lost = 2},
    });

    EXPECT_EQ(Merger_->Extract(), IChunkPoolOutput::NullCookie);

    pool2->Resume(inputCookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Pending = 1, .Lost = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Pending = 2_KBs, .Lost = 2_KBs},
        .RowCounter = {.Total = 2000, .Pending = 2000, .Lost = 2000},
        .DataSliceCounter = {.Total = 2, .Pending = 2, .Lost = 2},
    });

    auto cookie2 = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie2);

    CheckCounters({
        .JobCounter = {.Total = 1, .Running = 1, .Lost = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Running = 2_KBs, .Lost = 2_KBs},
        .RowCounter = {.Total = 2000, .Running = 2000, .Lost = 2000},
        .DataSliceCounter = {.Total = 2, .Running = 2, .Lost = 2},
    });

    Merger_->Completed(cookie2, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckCounters({
        .JobCounter = {.Total = 1, .Completed = 1, .Lost = 1},
        .DataWeightCounter = {.Total = 2_KBs, .Completed = 2_KBs, .Lost = 2_KBs},
        .RowCounter = {.Total = 2000, .Completed = 2000, .Lost = 2000},
        .DataSliceCounter = {.Total = 2, .Completed = 2, .Lost = 2},
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkPools

#include "chunk_pools_helpers.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/chunk_pools_output_merger.h>
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
    void CheckJobCounter(const TExpectedCounter& expected)
    {
        CheckCounter(Merger_->GetJobCounter(), expected);
    }

    void CheckDataWeightCounter(const TExpectedCounter& expected)
    {
        CheckCounter(Merger_->GetDataWeightCounter(), expected);
    }

    void CheckRowCounter(const TExpectedCounter& expected)
    {
        CheckCounter(Merger_->GetRowCounter(), expected);
    }

    void CheckDataSliceCounter(const TExpectedCounter& expected)
    {
        CheckCounter(Merger_->GetDataSliceCounter(), expected);
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

    IPersistentChunkPoolPtr CreateUnorderedChunkPool(int jobCount = 1)
    {
        TUnorderedChunkPoolOptions options{
            .JobSizeConstraints = CreateExplicitJobSizeConstraints(
                /*canAdjustDataSizePerJob*/ false,
                /*isExplicitJobCount*/ true,
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

    Merger_ = MergeChunkPoolsOutputs({pool1, pool2}, GetTestLogger());

    CheckJobCounter({.Total = 1, .Pending = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Pending = 2_KBs});
    CheckRowCounter({.Total = 2000, .Pending = 2000});
    CheckDataSliceCounter({.Total = 2, .Pending = 2});

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckJobCounter({.Total = 1, .Running = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Running = 2_KBs});
    CheckRowCounter({.Total = 2000, .Running = 2000});
    CheckDataSliceCounter({.Total = 2, .Running = 2});

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(2u, stripeList->Stripes().size());

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 1, .Completed = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Completed = 2_KBs});
    CheckRowCounter({.Total = 2000, .Completed = 2000});
    CheckDataSliceCounter({.Total = 2, .Completed = 2});
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

    Merger_ = MergeChunkPoolsOutputs({pool1, pool2}, GetTestLogger());

    // Should be suspended because not all pools are ready.
    CheckJobCounter({.Total = 1, .Suspended = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Suspended = 2_KBs});
    CheckRowCounter({.Total = 2000, .Suspended = 2000});
    CheckDataSliceCounter({.Total = 2, .Suspended = 2});

    // Now finish pool2.
    pool2->Finish();

    // Should become pending.
    CheckJobCounter({.Total = 1, .Pending = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Pending = 2_KBs});
    CheckRowCounter({.Total = 2000, .Pending = 2000});
    CheckDataSliceCounter({.Total = 2, .Pending = 2});

    // Now extraction should succeed.
    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckJobCounter({.Total = 1, .Running = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Running = 2_KBs});
    CheckRowCounter({.Total = 2000, .Running = 2000});
    CheckDataSliceCounter({.Total = 2, .Running = 2});

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(2u, stripeList->Stripes().size());

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 1, .Completed = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Completed = 2_KBs});
    CheckRowCounter({.Total = 2000, .Completed = 2000});
    CheckDataSliceCounter({.Total = 2, .Completed = 2});
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

    Merger_ = MergeChunkPoolsOutputs({pool1, pool2}, GetTestLogger());

    CheckJobCounter({.Total = 1, .Pending = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Pending = 2_KBs});
    CheckRowCounter({.Total = 2000, .Pending = 2000});
    CheckDataSliceCounter({.Total = 2, .Pending = 2});

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckJobCounter({.Total = 1, .Running = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Running = 2_KBs});
    CheckRowCounter({.Total = 2000, .Running = 2000});
    CheckDataSliceCounter({.Total = 2, .Running = 2});

    Merger_->Failed(cookie);

    EXPECT_FALSE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 1, .Pending = 1, .Failed = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Pending = 2_KBs, .Failed = 2_KBs});
    CheckRowCounter({.Total = 2000, .Pending = 2000, .Failed = 2000});
    CheckDataSliceCounter({.Total = 2, .Pending = 2, .Failed = 2});

    // Can extract again after failure.
    auto cookie2 = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie2);

    CheckJobCounter({.Total = 1, .Running = 1, .Failed = 1});

    TCompletedJobSummary summary;
    Merger_->Completed(cookie2, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 1, .Completed = 1, .Failed = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Completed = 2_KBs, .Failed = 2_KBs});
    CheckRowCounter({.Total = 2000, .Completed = 2000, .Failed = 2000});
    CheckDataSliceCounter({.Total = 2, .Completed = 2, .Failed = 2});
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

    Merger_ = MergeChunkPoolsOutputs({pool1, pool2}, GetTestLogger());

    CheckJobCounter({.Total = 1, .Pending = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Pending = 2_KBs});
    CheckRowCounter({.Total = 2000, .Pending = 2000});
    CheckDataSliceCounter({.Total = 2, .Pending = 2});

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckJobCounter({.Total = 1, .Running = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Running = 2_KBs});
    CheckRowCounter({.Total = 2000, .Running = 2000});
    CheckDataSliceCounter({.Total = 2, .Running = 2});

    Merger_->Aborted(cookie, EAbortReason::Scheduler);

    EXPECT_FALSE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 1, .Pending = 1, .Aborted = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Pending = 2_KBs, .Aborted = 2_KBs});
    CheckRowCounter({.Total = 2000, .Pending = 2000, .Aborted = 2000});
    CheckDataSliceCounter({.Total = 2, .Pending = 2, .Aborted = 2});

    // Can extract again after abort.
    auto cookie2 = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie2);

    CheckJobCounter({.Total = 1, .Running = 1, .Aborted = 1});

    TCompletedJobSummary summary;
    Merger_->Completed(cookie2, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 1, .Completed = 1, .Aborted = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Completed = 2_KBs, .Aborted = 2_KBs});
    CheckRowCounter({.Total = 2000, .Completed = 2000, .Aborted = 2000});
    CheckDataSliceCounter({.Total = 2, .Completed = 2, .Aborted = 2});
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

    Merger_ = MergeChunkPoolsOutputs({pool1, pool2}, GetTestLogger());

    CheckJobCounter({.Total = 1, .Pending = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Pending = 2_KBs});
    CheckRowCounter({.Total = 2000, .Pending = 2000});
    CheckDataSliceCounter({.Total = 2, .Pending = 2});

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckJobCounter({.Total = 1, .Running = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Running = 2_KBs});
    CheckRowCounter({.Total = 2000, .Running = 2000});
    CheckDataSliceCounter({.Total = 2, .Running = 2});

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 1, .Completed = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Completed = 2_KBs});
    CheckRowCounter({.Total = 2000, .Completed = 2000});
    CheckDataSliceCounter({.Total = 2, .Completed = 2});

    // Job output is lost.
    Merger_->Lost(cookie);

    EXPECT_FALSE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 2, .Pending = 1, .Completed = 1, .Lost = 1});
    CheckDataWeightCounter({.Total = 4_KBs, .Pending = 2_KBs, .Completed = 2_KBs, .Lost = 2_KBs});
    CheckRowCounter({.Total = 4000, .Pending = 2000, .Completed = 2000, .Lost = 2000});
    CheckDataSliceCounter({.Total = 4, .Pending = 2, .Completed = 2, .Lost = 2});

    // Can extract again after lost.
    auto cookie2 = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie2);

    CheckJobCounter({.Total = 2, .Running = 1, .Completed = 1, .Lost = 1});

    Merger_->Completed(cookie2, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 2, .Completed = 2, .Lost = 1});
    CheckDataWeightCounter({.Total = 4_KBs, .Completed = 4_KBs, .Lost = 2_KBs});
    CheckRowCounter({.Total = 4000, .Completed = 4000, .Lost = 2000});
    CheckDataSliceCounter({.Total = 4, .Completed = 4, .Lost = 2});
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

    Merger_ = MergeChunkPoolsOutputs({pool1, pool2, pool3, pool4}, GetTestLogger());

    CheckJobCounter({.Total = 1, .Pending = 1});
    CheckDataWeightCounter({.Total = 4_KBs, .Pending = 4_KBs});
    CheckRowCounter({.Total = 4000, .Pending = 4000});
    CheckDataSliceCounter({.Total = 4, .Pending = 4});

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckJobCounter({.Total = 1, .Running = 1});
    CheckDataWeightCounter({.Total = 4_KBs, .Running = 4_KBs});
    CheckRowCounter({.Total = 4000, .Running = 4000});
    CheckDataSliceCounter({.Total = 4, .Running = 4});

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(4u, stripeList->Stripes().size());

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 1, .Completed = 1});
    CheckDataWeightCounter({.Total = 4_KBs, .Completed = 4_KBs});
    CheckRowCounter({.Total = 4000, .Completed = 4000});
    CheckDataSliceCounter({.Total = 4, .Completed = 4});
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

    Merger_ = MergeChunkPoolsOutputs({pool1, pool2}, GetTestLogger());

    CheckJobCounter({.Total = 1, .Pending = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Pending = 2_KBs});
    CheckRowCounter({.Total = 2000, .Pending = 2000});
    CheckDataSliceCounter({.Total = 2, .Pending = 2});

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckJobCounter({.Total = 1, .Running = 1});

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
    CheckJobCounter({.Total = 1, .Running = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Running = 2_KBs});
    CheckRowCounter({.Total = 2000, .Running = 2000});
    CheckDataSliceCounter({.Total = 2, .Running = 2});

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(2u, stripeList->Stripes().size());

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 1, .Completed = 1});
    CheckDataWeightCounter({.Total = 2_KBs, .Completed = 2_KBs});
    CheckRowCounter({.Total = 2000, .Completed = 2000});
    CheckDataSliceCounter({.Total = 2, .Completed = 2});
}

TEST_F(TChunkPoolOutputMergerTest, EmptyFinishedPools)
{
    auto pool1 = CreateUnorderedChunkPool();
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();
    pool2->Finish();

    Merger_ = MergeChunkPoolsOutputs({pool1, pool2}, GetTestLogger());

    EXPECT_TRUE(Merger_->IsCompleted());

    CheckJobCounter({.Total = 0});
    CheckDataWeightCounter({.Total = 0});
    CheckRowCounter({.Total = 0});
    CheckDataSliceCounter({.Total = 0});
}

TEST_F(TChunkPoolOutputMergerTest, EmptyUnfinishedPool)
{
    auto pool1 = CreateUnorderedChunkPool();
    pool1->Finish();

    auto pool2 = CreateUnorderedChunkPool();

    Merger_ = MergeChunkPoolsOutputs({pool1, pool2}, GetTestLogger());

    EXPECT_FALSE(Merger_->IsCompleted());

    CheckJobCounter({.Total = 1, .Suspended = 1});
    CheckDataWeightCounter({.Total = 0});
    CheckRowCounter({.Total = 0});
    CheckDataSliceCounter({.Total = 0});

    pool2->Finish();

    EXPECT_TRUE(Merger_->IsCompleted());

    CheckJobCounter({.Total = 0});
    CheckDataWeightCounter({.Total = 0});
    CheckRowCounter({.Total = 0});
    CheckDataSliceCounter({.Total = 0});
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

    Merger_ = MergeChunkPoolsOutputs({pool1, pool2}, GetTestLogger());

    CheckJobCounter({.Total = 1, .Pending = 1});
    CheckDataWeightCounter({.Total = 5_KBs, .Pending = 5_KBs});
    CheckRowCounter({.Total = 5000, .Pending = 5000});
    CheckDataSliceCounter({.Total = 5, .Pending = 5});

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckJobCounter({.Total = 1, .Running = 1});
    CheckDataWeightCounter({.Total = 5_KBs, .Running = 5_KBs});
    CheckRowCounter({.Total = 5000, .Running = 5000});
    CheckDataSliceCounter({.Total = 5, .Running = 5});

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(2u, stripeList->Stripes().size());

    EXPECT_EQ(5, Merger_->GetStripeListSliceCount(cookie));

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 1, .Completed = 1});
    CheckDataWeightCounter({.Total = 5_KBs, .Completed = 5_KBs});
    CheckRowCounter({.Total = 5000, .Completed = 5000});
    CheckDataSliceCounter({.Total = 5, .Completed = 5});
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

    Merger_ = MergeChunkPoolsOutputs({pool1, pool2}, GetTestLogger());

    CheckJobCounter({.Total = 1, .Pending = 1});
    CheckDataWeightCounter({.Total = 5_KBs, .Pending = 5_KBs});
    CheckRowCounter({.Total = 5000, .Pending = 5000});
    CheckDataSliceCounter({.Total = 5, .Pending = 5});

    auto cookie = Merger_->Extract();
    EXPECT_NE(IChunkPoolOutput::NullCookie, cookie);

    CheckJobCounter({.Total = 1, .Running = 1});
    CheckDataWeightCounter({.Total = 5_KBs, .Running = 5_KBs});
    CheckRowCounter({.Total = 5000, .Running = 5000});
    CheckDataSliceCounter({.Total = 5, .Running = 5});

    auto stripeList = Merger_->GetStripeList(cookie);
    EXPECT_EQ(5u, stripeList->Stripes().size());

    TCompletedJobSummary summary;
    Merger_->Completed(cookie, summary);

    EXPECT_TRUE(Merger_->IsCompleted());
    CheckJobCounter({.Total = 1, .Completed = 1});
    CheckDataWeightCounter({.Total = 5_KBs, .Completed = 5_KBs});
    CheckRowCounter({.Total = 5000, .Completed = 5000});
    CheckDataSliceCounter({.Total = 5, .Completed = 5});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkPools

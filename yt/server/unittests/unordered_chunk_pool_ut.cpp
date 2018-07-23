#include "chunk_pools_helpers.h"

#include <yt/core/test_framework/framework.h>

#include <yt/server/controller_agent/helpers.h>
#include <yt/server/controller_agent/job_size_constraints.h>
#include <yt/server/controller_agent/operation_controller.h>

#include <yt/server/chunk_pools/unordered_chunk_pool.h>

#include <yt/client/table_client/row_buffer.h>

#include <yt/core/misc/blob_output.h>
#include <yt/core/misc/phoenix.h>

#include <util/stream/null.h>

#include <random>

namespace NYT {
namespace NChunkPools {
namespace {

using namespace NControllerAgent;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NTableClient;

using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

//! A unit to measure all sizes in this file.
static constexpr i64 KB = 1024;
static constexpr i32 Inf32 = std::numeric_limits<i32>::max();
static constexpr i64 Inf64 = std::numeric_limits<i64>::max();

////////////////////////////////////////////////////////////////////////////////

class TUnorderedChunkPoolTest
    : public Test
{
protected:
    virtual void SetUp() override
    {
        Options_.MinTeleportChunkSize = Inf64;
        DataSizePerJob_ = Inf64;
        MaxDataSlicesPerJob_ = Inf32;
        InputSliceDataSize_ = Inf64;
        InputSliceRowCount_ = Inf64;
    }

    void InitJobConstraints()
    {
        Options_.JobSizeConstraints = CreateExplicitJobSizeConstraints(
            false /* canAdjustDataSizePerJob */,
            IsExplicitJobCount_ /* isExplicitJobCount */,
            JobCount_ /* jobCount */,
            DataSizePerJob_,
            Inf64,
            MaxDataSlicesPerJob_,
            Inf64 /* maxDataSizePerJob_ */,
            InputSliceDataSize_,
            InputSliceRowCount_,
            SamplingRate_);
    }

    TInputChunkPtr CreateChunk(
        int tableIndex,
        i64 size = 1_KB,
        i64 rowCount = 1000)
    {
        auto inputChunk = New<TInputChunk>();
        inputChunk->ChunkId() = TChunkId::Create();
        inputChunk->SetCompressedDataSize(size);
        inputChunk->SetTotalUncompressedDataSize(size);
        inputChunk->SetTotalDataWeight(size);
        inputChunk->SetTableIndex(tableIndex);
        inputChunk->SetTableRowIndex(UnversionedTableRowCounts_[tableIndex]);
        UnversionedTableRowCounts_[tableIndex] += rowCount;
        if (!InputTables_[tableIndex].IsVersioned()) {
            CreatedUnversionedChunks_.insert(inputChunk);
        }
        inputChunk->SetTotalRowCount(rowCount);
        return inputChunk;
    }

    TInputChunkPtr CopyChunk(const TInputChunkPtr& chunk)
    {
        TInputChunkPtr chunkCopy = New<TInputChunk>();
        chunkCopy->ChunkId() = chunk->ChunkId();
        chunkCopy->SetCompressedDataSize(chunk->GetCompressedDataSize());
        int tableIndex = chunk->GetTableIndex();
        chunkCopy->SetTableIndex(tableIndex);
        chunkCopy->SetTableRowIndex(chunk->GetTableRowIndex());
        chunkCopy->SetTotalRowCount(chunk->GetTotalRowCount());
        if (!InputTables_[tableIndex].IsVersioned()) {
            CreatedUnversionedChunks_.insert(chunkCopy);
        }
        return chunkCopy;
    }

    void InitTables(std::vector<bool> isTeleportable, std::vector<bool> isVersioned)
    {
        YCHECK(isTeleportable.size() == isVersioned.size() && isVersioned.size() > 0);
        for (int index = 0; index < isVersioned.size(); ++index) {
            InputTables_.emplace_back(isTeleportable[index], true /* isPrimary */, isVersioned[index]);
        }
        UnversionedTableRowCounts_.resize(InputTables_.size(), 0);
    }

    void CreateChunkPool()
    {
        ChunkPool_ = CreateUnorderedChunkPool(Options_, TInputStreamDirectory(InputTables_));
    }

    TInputDataSlicePtr BuildDataSliceByChunk(const TInputChunkPtr& chunk)
    {
        auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
        dataSlice->Tag = chunk->ChunkId().Parts64[0] ^ chunk->ChunkId().Parts64[1];
        return dataSlice;
    }

    IChunkPoolInput::TCookie AddChunk(const TInputChunkPtr& chunk)
    {
        auto dataSlice = BuildDataSliceByChunk(chunk);
        ActiveChunks_.insert(chunk->ChunkId());
        OriginalChunks_.push_back(chunk->ChunkId());
        return ChunkPool_->Add(New<TChunkStripe>(dataSlice));
    }

    IChunkPoolInput::TCookie AddMultiChunkStripe(std::vector<TInputChunkPtr> chunks)
    {
        std::vector<TInputDataSlicePtr> dataSlices;
        for (const auto& chunk : chunks) {
            auto dataSlice = BuildDataSliceByChunk(chunk);
            dataSlices.emplace_back(std::move(dataSlice));
        }
        auto stripe = New<TChunkStripe>();
        std::move(dataSlices.begin(), dataSlices.end(), std::back_inserter(stripe->DataSlices));
        return ChunkPool_->Add(stripe);
    }

    void SuspendChunk(IChunkPoolInput::TCookie cookie, const TInputChunkPtr& chunk)
    {
        YCHECK(ActiveChunks_.erase(chunk->ChunkId()));
        ChunkPool_->Suspend(cookie);
    }

    void ResumeChunk(IChunkPoolInput::TCookie cookie, const TInputChunkPtr& chunk)
    {
        auto dataSlice = BuildDataSliceByChunk(chunk);
        ActiveChunks_.insert(chunk->ChunkId());
        return ChunkPool_->Resume(cookie);
    }

    void ExtractOutputCookiesWhilePossible()
    {
        while (ChunkPool_->GetPendingJobCount()) {
            ExtractedCookies_.emplace_back(ExtractCookie(TNodeId(0)));
        }
    }

    IChunkPoolOutput::TCookie ExtractCookie(TNodeId nodeId)
    {
        auto cookie = ChunkPool_->Extract(nodeId);
        if (cookie != IChunkPoolOutput::NullCookie) {
            OutputCookies_.insert(cookie);
        }
        return cookie;
    }

    void PersistAndRestore()
    {
        TBlobOutput output;
        TSaveContext saveContext;
        saveContext.SetVersion(GetCurrentSnapshotVersion());
        saveContext.SetOutput(&output);
        Save(saveContext, ChunkPool_);
        auto blob = output.Flush();
        ChunkPool_.reset();

        TMemoryInput input(blob.Begin(), blob.Size());
        TLoadContext loadContext;
        loadContext.SetVersion(GetCurrentSnapshotVersion());
        loadContext.SetRowBuffer(RowBuffer_);
        loadContext.SetInput(&input);
        Load(loadContext, ChunkPool_);
    }

    std::vector<TChunkStripeListPtr> GetAllStripeLists()
    {
        std::vector<TChunkStripeListPtr> stripeLists;
        for (auto cookie : OutputCookies_) {
            stripeLists.emplace_back(ChunkPool_->GetStripeList(cookie));
        }
        return stripeLists;
    }

    //! Perform all the correctness checks over the given result of ordered chunk pool invocation
    //! (without any suspends nor job interruptions).
    void CheckEverything(
        const std::vector<TChunkStripeListPtr>& stripeLists,
        const std::vector<TInputChunkPtr>& teleportChunks)
    {
        CheckStripeListsContainOnlyActiveChunks();
        CheckDataIntegrity(stripeLists, teleportChunks);
    }

    // TODO(max42): extract all these weird repeating code parts into helpers already! Jeez!
    //! Check that:
    //! * The given stripe lists cover each input chunk with specified read limits without overlapping;
    //! * For each input table the input data slices follow in an ascending order with tie broken by:
    //! *** For the unversioned tables by chunk row index;
    //! *** For the versioned tables by the full key;
    void CheckDataIntegrity(const std::vector<TChunkStripeListPtr>& stripeLists, const std::vector<TInputChunkPtr>& teleportChunks)
    {
        THashMap<TInputChunkPtr, std::vector<TInputChunkSlicePtr>> chunkSlicesByInputChunk;
        THashSet<TInputChunkPtr> teleportChunksSet(teleportChunks.begin(), teleportChunks.end());

        // Check that data slices from each stripe are all from the same table.
        for (const auto& stripeList : stripeLists) {
            for (const auto& stripe : stripeList->Stripes) {
                ASSERT_TRUE(!stripe->DataSlices.empty());
                int tableIndex = stripe->DataSlices.front()->GetTableIndex();

                for (const auto& dataSlice : stripe->DataSlices) {
                    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                        const auto& inputChunk = chunkSlice->GetInputChunk();
                        EXPECT_EQ(tableIndex, inputChunk->GetTableIndex());
                        chunkSlicesByInputChunk[inputChunk].emplace_back(chunkSlice);
                    }
                }
            }
        }

        // First check.
        for (const auto& inputChunk : CreatedUnversionedChunks_) {
            if (teleportChunksSet.has(inputChunk)) {
                continue;
            }
            TKey chunkLowerKey = inputChunk->LowerLimit() && inputChunk->LowerLimit()->HasKey()
                ? inputChunk->LowerLimit()->GetKey()
                : inputChunk->BoundaryKeys()
                ? inputChunk->BoundaryKeys()->MinKey
                : TKey();
            TKey chunkUpperKey = inputChunk->UpperLimit() && inputChunk->UpperLimit()->HasKey()
                ? inputChunk->UpperLimit()->GetKey()
                : inputChunk->BoundaryKeys()
                ? GetKeySuccessor(inputChunk->BoundaryKeys()->MaxKey, RowBuffer_)
                : TKey();
            i64 chunkLowerRowIndex = inputChunk->LowerLimit() && inputChunk->LowerLimit()->HasRowIndex()
                ? inputChunk->LowerLimit()->GetRowIndex()
                : 0;
            i64 chunkUpperRowIndex = inputChunk->UpperLimit() && inputChunk->UpperLimit()->HasRowIndex()
                ? inputChunk->UpperLimit()->GetRowIndex()
                : inputChunk->GetRowCount();

            TKey lastLowerKey;
            TKey lastUpperKey = chunkLowerKey;
            i64 lastLeftRowIndex = -1;
            i64 lastRightRowIndex = chunkLowerRowIndex;
            auto it = chunkSlicesByInputChunk.find(inputChunk);
            ASSERT_TRUE(chunkSlicesByInputChunk.end() != it);
            auto& chunkSlices = it->second;

            std::sort(chunkSlices.begin(), chunkSlices.end(), CompareChunkSlicesByLowerLimit);

            for (const auto& chunkSlice : chunkSlices) {
                TKey chunkSliceLowerKey = chunkSlice->LowerLimit().Key;
                TKey chunkSliceUpperKey = chunkSlice->UpperLimit().Key;
                i64 chunkSliceLowerRowIndex = chunkSlice->LowerLimit().RowIndex
                    ? *chunkSlice->LowerLimit().RowIndex
                    : chunkLowerRowIndex;
                i64 chunkSliceUpperRowIndex = chunkSlice->UpperLimit().RowIndex
                    ? *chunkSlice->UpperLimit().RowIndex
                    : chunkUpperRowIndex;

                bool keysCoincide = lastUpperKey == chunkSliceLowerKey;
                bool rowIndicesCoincide = lastRightRowIndex == chunkSliceLowerRowIndex;
                EXPECT_TRUE(keysCoincide || rowIndicesCoincide);
                if (!keysCoincide) {
                    EXPECT_EQ(lastLowerKey, chunkSliceLowerKey);
                    EXPECT_EQ(lastUpperKey, chunkSliceUpperKey);
                }
                if (!rowIndicesCoincide) {
                    EXPECT_EQ(lastLeftRowIndex, chunkSliceLowerRowIndex);
                    EXPECT_EQ(lastRightRowIndex, chunkSliceUpperRowIndex);
                }
                lastLowerKey = chunkSliceLowerKey;
                lastUpperKey = chunkSliceUpperKey;
                lastLeftRowIndex = chunkSliceLowerRowIndex;
                lastRightRowIndex = chunkSliceUpperRowIndex;
            }

            // For InferLimitsFromBoundaryKeys, lastUpperKey can be larger than chunkUpperKey.
            EXPECT_GE(lastUpperKey, chunkUpperKey);
            EXPECT_EQ(lastRightRowIndex, chunkUpperRowIndex);
        }

        // Second check.
        auto unversionedDataSliceComparator = [] (const TInputDataSlicePtr& lhs, const TInputDataSlicePtr& rhs) {
            auto lhsChunk = lhs->GetSingleUnversionedChunkOrThrow();
            auto rhsChunk = rhs->GetSingleUnversionedChunkOrThrow();
            if (lhsChunk != rhsChunk) {
                return lhsChunk->GetTableRowIndex() < rhsChunk->GetTableRowIndex();
            } else {
                return lhs->LowerLimit().Key <= rhs->LowerLimit().Key;
            }
        };
        auto versionedDataSliceComparator = [] (const TInputDataSlicePtr& lhs, const TInputDataSlicePtr& rhs) {
            return lhs->LowerLimit().Key <= rhs->LowerLimit().Key;
        };

        for (const auto& stripeList : stripeLists) {
            for (const auto& stripe : stripeList->Stripes) {
                ASSERT_TRUE(!stripe->DataSlices.empty());
                int tableIndex = stripe->DataSlices.front()->GetTableIndex();
                if (!InputTables_[tableIndex].IsForeign()) {
                    const auto& comparator = (InputTables_[tableIndex].IsVersioned()) ? versionedDataSliceComparator : unversionedDataSliceComparator;
                    for (int index = 0; index + 1 < stripe->DataSlices.size(); ++index) {
                        const auto& lhs = stripe->DataSlices[index];
                        const auto& rhs = stripe->DataSlices[index + 1];
                        EXPECT_TRUE(comparator(lhs, rhs));
                    }
                }
            }
        }
    }

    void CheckStripeListsContainOnlyActiveChunks()
    {
        for (auto cookie : OutputCookies_) {
            auto stripeList = ChunkPool_->GetStripeList(cookie);
            for (const auto& stripe : stripeList->Stripes) {
                if (stripe) {
                    for (const auto& dataSlice : stripe->DataSlices) {
                        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                            auto chunk = chunkSlice->GetInputChunk();
                            EXPECT_TRUE(chunk);
                            EXPECT_TRUE(ActiveChunks_.has(chunk->ChunkId()));
                        }
                    }
                }
            }
        }
    }

    TCompletedJobSummary SummaryWithSplitJobCount(TChunkStripeListPtr stripeList, int splitJobCount)
    {
        TCompletedJobSummary jobSummary;
        for (const auto& stripe : stripeList->Stripes) {
            std::copy(
                stripe->DataSlices.begin(),
                stripe->DataSlices.end(),
                std::back_inserter(jobSummary.UnreadInputDataSlices));
        }
        jobSummary.SplitJobCount = splitJobCount;
        jobSummary.InterruptReason = EInterruptReason::JobSplit;
        return jobSummary;
    }

    void SplitJob(IChunkPoolOutput::TCookie cookie, int splitJobCount)
    {
        ChunkPool_->Completed(cookie, SummaryWithSplitJobCount(ChunkPool_->GetStripeList(cookie), splitJobCount));
    }

    std::vector<TChunkId> OriginalChunks_;

    std::unique_ptr<IChunkPool> ChunkPool_;

    //! Set containing all unversioned input chunks that have ever been created.
    THashSet<TInputChunkPtr> CreatedUnversionedChunks_;
    //! Set containing all chunks that are added to the pool without being suspended.
    THashSet<TChunkId> ActiveChunks_;

    std::vector<TInputStreamDescriptor> InputTables_;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    THashSet<IChunkPoolOutput::TCookie> OutputCookies_;

    std::vector<int> UnversionedTableRowCounts_;

    TUnorderedChunkPoolOptions Options_;

    i64 DataSizePerJob_;

    i32 MaxDataSlicesPerJob_;

    i64 InputSliceDataSize_;

    i64 InputSliceRowCount_;

    TNullable<double> SamplingRate_;

    i64 JobCount_;

    bool IsExplicitJobCount_ = false;

    std::vector<IChunkPoolOutput::TCookie> ExtractedCookies_;

    std::mt19937 Gen_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TUnorderedChunkPoolTest, UnorderedMergeSimple)
{
    InitTables(
        {true, true, true} /* isTeleportable */,
        {false, false, false} /* isVersioned */
    );

    DataSizePerJob_ = 2_KB;
    Options_.MinTeleportChunkDataWeight = 3_KB;
    JobCount_ = 2;

    InitJobConstraints();

    auto chunkA1 = CreateChunk(0);
    auto chunkB1 = CreateChunk(1);
    auto chunkC = CreateChunk(2);
    auto chunkA2 = CreateChunk(0);
    auto chunkB2 = CreateChunk(1, 5_KB);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkB1);
    AddChunk(chunkC);
    AddChunk(chunkA2);
    AddChunk(chunkB2);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_EQ(teleportChunks.size(), 1);
    EXPECT_EQ(2, stripeLists.size());

    CheckEverything(stripeLists, teleportChunks);
}

TEST_F(TUnorderedChunkPoolTest, InputChunksAreSliced)
{
    InitTables(
        {false} /* isTeleportable */,
        {false} /* isVersioned */
    );

    DataSizePerJob_ = 2_KB / 5;
    IsExplicitJobCount_ = true; // TODO(max42): consider what happens with false here.
    JobCount_ = 5;
    InputSliceDataSize_ = DataSizePerJob_ / 10;
    InitJobConstraints();

    auto chunkA = CreateChunk(0); // 2Kb.
    auto chunkB = CreateChunk(0); // 2Kb.

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_EQ(teleportChunks.size(), 0);
    EXPECT_EQ(5, stripeLists.size());

    CheckEverything(stripeLists, teleportChunks);

    for (const auto& stripeList : stripeLists) {
        EXPECT_GE(stripeList->TotalDataWeight, DataSizePerJob_ * 0.9);
        EXPECT_LE(stripeList->TotalDataWeight, DataSizePerJob_ * 1.1);
    }
}

TEST_F(TUnorderedChunkPoolTest, InterruptionWithSuspendedChunks1)
{
    InitTables(
        {false} /* isTeleportable */,
        {false} /* isVersioned */
    );

    DataSizePerJob_ = 5_KB;
    IsExplicitJobCount_ = true;
    JobCount_ = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(0); // 2Kb.

    CreateChunkPool();

    AddChunk(chunkA);

    ChunkPool_->Finish();

    EXPECT_EQ(1, ChunkPool_->GetPendingJobCount());
    EXPECT_EQ(0, ChunkPool_->Extract(TNodeId()));
    auto stripeList = ChunkPool_->GetStripeList(0);
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();
    EXPECT_EQ(1, stripeList->Stripes.size());
    EXPECT_TRUE(teleportChunks.empty());

    ChunkPool_->Suspend(0);
    ChunkPool_->Aborted(0, EAbortReason::FailedChunks);

    EXPECT_EQ(0, ChunkPool_->GetPendingJobCount());
    ChunkPool_->Resume(0);
    EXPECT_EQ(1, ChunkPool_->GetPendingJobCount());
    EXPECT_EQ(1, ChunkPool_->Extract(TNodeId()));
    ChunkPool_->Suspend(0);
    SplitJob(1 /* cookie */, 1 /* splitJobCount */);
    EXPECT_EQ(0, ChunkPool_->GetPendingJobCount());
    ChunkPool_->Resume(0);
    EXPECT_EQ(1, ChunkPool_->GetPendingJobCount());
    EXPECT_EQ(2, ChunkPool_->Extract(TNodeId()));
}

TEST_F(TUnorderedChunkPoolTest, InterruptionWithSuspendedChunks2)
{
    InitTables(
        {false} /* isTeleportable */,
        {false} /* isVersioned */
    );

    DataSizePerJob_ = 5_KB;
    IsExplicitJobCount_ = true;
    JobCount_ = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(0);
    auto chunkB = CreateChunk(0);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    EXPECT_EQ(1, ChunkPool_->GetPendingJobCount());
    EXPECT_EQ(0, ChunkPool_->Extract(TNodeId()));
    auto stripeList = ChunkPool_->GetStripeList(0);
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();
    EXPECT_EQ(2, stripeList->Stripes.size());
    EXPECT_TRUE(teleportChunks.empty());

    ChunkPool_->Suspend(0);
    ChunkPool_->Suspend(1);
    SplitJob(0, 1);
    ChunkPool_->Resume(0);
    ChunkPool_->Resume(1);

    EXPECT_EQ(1, ChunkPool_->GetPendingJobCount());
    EXPECT_EQ(1, ChunkPool_->Extract(TNodeId()));
    stripeList = ChunkPool_->GetStripeList(1);
    EXPECT_EQ(1, stripeList->Stripes.size());
}

TEST_F(TUnorderedChunkPoolTest, InterruptionWithSuspendedChunks3)
{
    InitTables(
        {false} /* isTeleportable */,
        {false} /* isVersioned */
    );

    InputSliceRowCount_ = 500;
    IsExplicitJobCount_ = true;
    JobCount_ = 1;
    InitJobConstraints();

    CreateChunkPool();

    auto chunk = CreateChunk(0);

    // Should divide this chunk into two parts thus creating two internal cookies.
    AddChunk(chunk);

    ChunkPool_->Finish();

    EXPECT_EQ(1, ChunkPool_->GetPendingJobCount());
    EXPECT_EQ(0, ChunkPool_->Extract(TNodeId()));
    ChunkPool_->Suspend(0);
    SplitJob(0, 1);
    EXPECT_EQ(0, ChunkPool_->GetPendingJobCount());
    ChunkPool_->Resume(0);
    EXPECT_EQ(1, ChunkPool_->GetPendingJobCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NChunkPools
} // namespace NYT

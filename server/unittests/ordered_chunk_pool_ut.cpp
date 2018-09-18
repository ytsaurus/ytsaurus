#include "chunk_pools_helpers.h"

#include <yt/core/test_framework/framework.h>

#include <yt/server/controller_agent/helpers.h>
#include <yt/server/controller_agent/job_size_constraints.h>
#include <yt/server/controller_agent/operation_controller.h>

#include <yt/server/chunk_pools/ordered_chunk_pool.h>
#include <yt/server/chunk_pools/output_order.h>

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

using NControllerAgent::TCompletedJobSummary;

using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

//! A unit to measure all sizes in this file.
static constexpr i64 KB = 1024;
static constexpr i32 Inf32 = std::numeric_limits<i32>::max();
static constexpr i64 Inf64 = std::numeric_limits<i64>::max();

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkPoolTest
    : public Test
{
protected:
    virtual void SetUp() override
    {
        Options_.MinTeleportChunkSize = Inf64;
        Options_.MaxTotalSliceCount = Inf64;
        Options_.ShouldSliceByRowIndices = true;
        DataSizePerJob_ = Inf64;
        MaxDataSlicesPerJob_ = Inf32;
        InputSliceDataSize_ = Inf64;
        InputSliceRowCount_ = Inf64;
    }

    void InitJobConstraints()
    {
        Options_.JobSizeConstraints = CreateExplicitJobSizeConstraints(
            false /* canAdjustDataSizePerJob */,
            false /* isExplicitJobCount */,
            0 /* jobCount */,
            DataSizePerJob_,
            Inf64,
            MaxDataSlicesPerJob_,
            0 /* maxDataSizePerJob_ */,
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
            CreatedUnversionedPrimaryChunks_.insert(inputChunk);
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
            CreatedUnversionedPrimaryChunks_.insert(chunkCopy);
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

    void CreateChunkPool(bool useGenericInputStreamDirectory = false)
    {
        ChunkPool_ = CreateOrderedChunkPool(
            Options_,
            useGenericInputStreamDirectory ? IntermediateInputStreamDirectory : TInputStreamDirectory(InputTables_));
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
        CheckSlicesFollowInOriginalOrder(stripeLists);
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


    void CheckSlicesFollowInOriginalOrder(const std::vector<TChunkStripeListPtr>& stripeLists)
    {
        int chunkIndex = 0;
        for (const auto& stripeList : stripeLists) {
            for (const auto& stripe : stripeList->Stripes) {
                for (const auto& dataSlice : stripe->DataSlices) {
                    if (dataSlice->Type != EDataSourceType::UnversionedTable) {
                        continue;
                    }
                    while (
                        chunkIndex < OriginalChunks_.size() &&
                        dataSlice->GetSingleUnversionedChunkOrThrow()->ChunkId() != OriginalChunks_[chunkIndex])
                    {
                        ++chunkIndex;
                    }
                    EXPECT_NE(chunkIndex, OriginalChunks_.size());
                }
            }
        }
    }

    TCompletedJobSummary SummaryWithSplitJobCount(TChunkStripeListPtr stripeList, int splitJobCount)
    {
        TCompletedJobSummary jobSummary;
        std::move(
            stripeList->Stripes[0]->DataSlices.begin(),
            stripeList->Stripes[0]->DataSlices.end(),
            std::back_inserter(jobSummary.UnreadInputDataSlices));
        jobSummary.SplitJobCount = splitJobCount;
        jobSummary.InterruptReason = EInterruptReason::JobSplit;
        return jobSummary;
    }

    void PrintEntry(const TOutputOrder::TEntry entry)
    {
        if (entry.IsTeleportChunk()) {
            Cerr << "T " << ToString(entry.GetTeleportChunk()->ChunkId()) << Endl;
        } else {
            Cerr << "C ";
            auto stripeList = ChunkPool_->GetStripeList(entry.IsCookie());
            for (const auto& dataSlice : stripeList->Stripes[0]->DataSlices) {
                Cerr << ToString(dataSlice->GetSingleUnversionedChunkOrThrow()->ChunkId()) << " ";
            }
            Cerr << Endl;
        }
    }

    void SplitJob(IChunkPoolOutput::TCookie cookie, int splitJobCount)
    {
        ChunkPool_->Completed(cookie, SummaryWithSplitJobCount(ChunkPool_->GetStripeList(cookie), splitJobCount));
    }

    void ExpectEntryIsTeleportChunk(const TOutputOrder::TEntry& entry, TInputChunkPtr chunk)
    {
        EXPECT_TRUE(entry.IsTeleportChunk());
        EXPECT_EQ(entry.GetTeleportChunk()->ChunkId(), chunk->ChunkId());
    }

    void ExpectEntryIsCookie(const TOutputOrder::TEntry& entry, std::vector<TInputChunkPtr> chunks)
    {
        EXPECT_TRUE(entry.IsCookie());
        auto stripeList = ChunkPool_->GetStripeList(entry.GetCookie());
        EXPECT_EQ(stripeList->Stripes.size(), 1);
        EXPECT_EQ(stripeList->Stripes[0]->DataSlices.size(), chunks.size());
        for (int index = 0; index < stripeList->Stripes[0]->DataSlices.size(); ++index) {
            EXPECT_EQ(stripeList->Stripes[0]->DataSlices[index]->GetSingleUnversionedChunkOrThrow()->ChunkId(), chunks[index]->ChunkId());
        }
    }

    std::vector<TChunkId> OriginalChunks_;

    std::unique_ptr<IChunkPool> ChunkPool_;

    //! Set containing all unversioned primary input chunks that have ever been created.
    THashSet<TInputChunkPtr> CreatedUnversionedPrimaryChunks_;
    //! Set containing all chunks that are added to the pool without being suspended.
    THashSet<TChunkId> ActiveChunks_;

    std::vector<TInputStreamDescriptor> InputTables_;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    THashSet<IChunkPoolOutput::TCookie> OutputCookies_;

    std::vector<int> UnversionedTableRowCounts_;

    TOrderedChunkPoolOptions Options_;

    i64 DataSizePerJob_;

    i32 MaxDataSlicesPerJob_;

    i64 InputSliceDataSize_;

    i64 InputSliceRowCount_;

    TNullable<double> SamplingRate_;

    std::vector<IChunkPoolOutput::TCookie> ExtractedCookies_;

    std::mt19937 Gen_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TOrderedChunkPoolTest, OrderedMergeSimple)
{
    InitTables(
        {true, true, true} /* isTeleportable */,
        {false, false, false} /* isVersioned */
    );

    DataSizePerJob_ = 2_KB;

    InitJobConstraints();

    auto chunkA1 = CreateChunk(0);
    auto chunkB = CreateChunk(1);
    auto chunkC = CreateChunk(2);
    auto chunkA2 = CreateChunk(0);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkB);
    AddChunk(chunkC);
    AddChunk(chunkA2);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, IsEmpty());
    EXPECT_EQ(2, stripeLists.size());

    CheckEverything(stripeLists, teleportChunks);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TOrderedChunkPoolTest, OrderedMergeOrderedOutput)
{
    InitTables(
        {true, true, true} /* isTeleportable */,
        {false, false, false} /* isVersioned */
    );

    Options_.KeepOutputOrder = true;
    Options_.MinTeleportChunkSize = 2_KB;
    DataSizePerJob_ = 2_KB;

    InitJobConstraints();

    std::vector<TInputChunkPtr> chunks = {
        CreateChunk(0, 1_KB),
        CreateChunk(0, 10_KB),
        CreateChunk(0, 10_KB),
        CreateChunk(0, 1_KB),
        CreateChunk(0, 1_KB),
        CreateChunk(0, 1_KB),
        CreateChunk(0, 1_KB),
        CreateChunk(0, 1_KB),
        CreateChunk(0, 10_KB),
        CreateChunk(0, 1_KB),
    };

    CreateChunkPool();

    for (const auto& chunk : chunks) {
        AddChunk(chunk);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    PersistAndRestore();

    auto order = ChunkPool_->GetOutputOrder();
    ASSERT_TRUE(order);

    auto entries = order->ToEntryVector();
    ASSERT_EQ(entries.size(), 8);
    ExpectEntryIsCookie(entries[0], {chunks[0]});
    ExpectEntryIsTeleportChunk(entries[1], chunks[1]);
    ExpectEntryIsTeleportChunk(entries[2], chunks[2]);
    ExpectEntryIsCookie(entries[3], {chunks[3], chunks[4]});
    ExpectEntryIsCookie(entries[4], {chunks[5], chunks[6]});
    ExpectEntryIsCookie(entries[5], {chunks[7]});
    ExpectEntryIsTeleportChunk(entries[6], chunks[8]);
    ExpectEntryIsCookie(entries[7], {chunks[9]});

    auto originalEntries = entries;

    SplitJob(originalEntries[4].GetCookie(), 10);
    entries = order->ToEntryVector();
    ASSERT_EQ(entries.size(), 10);

    SplitJob(originalEntries[0].GetCookie(), 10);
    entries = order->ToEntryVector();
    ASSERT_EQ(entries.size(), 11);

    SplitJob(originalEntries[7].GetCookie(), 10);
    entries = order->ToEntryVector();
    ASSERT_EQ(entries.size(), 12);

    ExtractOutputCookiesWhilePossible();

    // entries[0] is now invalidated.
    ExpectEntryIsCookie(entries[1], {chunks[0]});
    ExpectEntryIsTeleportChunk(entries[2], chunks[1]);
    ExpectEntryIsTeleportChunk(entries[3], chunks[2]);
    ExpectEntryIsCookie(entries[4], {chunks[3], chunks[4]});
    // entries[5] is now invalidated.
    ExpectEntryIsCookie(entries[6], {chunks[5]});
    ExpectEntryIsCookie(entries[7], {chunks[6]});
    ExpectEntryIsCookie(entries[8], {chunks[7]});
    ExpectEntryIsTeleportChunk(entries[9], chunks[8]);
    // entries[10] is now invalidated.
    ExpectEntryIsCookie(entries[11], {chunks[9]});
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TOrderedChunkPoolTest, OrderedMergeSliceLargeChunks)
{
    InitTables(
        {false} /* isTeleportable */,
        {false} /* isVersioned */
    );

    DataSizePerJob_ = 2_KB;
    InputSliceDataSize_ = 2_KB;
    InputSliceRowCount_ = 100;

    InitJobConstraints();

    auto chunkA = CreateChunk(0, 20_KB, 1000 /* rowCount */);

    CreateChunkPool();

    AddChunk(chunkA);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, IsEmpty());
    EXPECT_LE(9, stripeLists.size());
    EXPECT_LE(stripeLists.size(), 11);

    CheckEverything(stripeLists, teleportChunks);
}

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkPoolTestRandomized
    : public WithParamInterface<int>
    , public TOrderedChunkPoolTest
{
public:
    TOrderedChunkPoolTestRandomized() = default;

    virtual void SetUp() override final
    {
        TOrderedChunkPoolTest::SetUp();
        Gen_.seed(GetParam());
    }
};

static constexpr int NumberOfRepeats = 15;

TEST_P(TOrderedChunkPoolTestRandomized, VariousOperationsWithPoolTest)
{
    InitTables(
        {false} /* isTeleportable */,
        {false} /* isVersioned */
    );
    DataSizePerJob_ = 1_KB;
    InitJobConstraints();

    const int chunkCount = 50;

    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(0);
    }

    CreateChunkPool();

    std::uniform_int_distribution<> dice(0, 99);

    auto chooseRandomElement = [&] (auto container) -> TNullable<typename decltype(container)::value_type> {
        if (container.empty()) {
            return Null;
        } else {
            auto it = container.begin();
            std::advance(it, std::uniform_int_distribution<>(0, container.size() - 1)(Gen_));
            return MakeNullable(*it);
        }
    };

    // All chunks from the IChunkPoolInput point of view.
    THashMap<TChunkId, IChunkPoolInput::TCookie> chunkIdToInputCookie;
    THashSet<TChunkId> suspendedChunks;
    THashSet<TChunkId> resumedChunks;
    // All chunks from the IChunkPoolOutput point of view.
    THashMap<TChunkId, IChunkPoolOutput::TCookie> chunkIdToOutputCookie;
    THashSet<TChunkId> pendingChunks;
    THashSet<TChunkId> startedChunks;
    THashSet<TChunkId> completedChunks;
    THashMap<TChunkId, TInputChunkPtr> chunkIdToChunk;

    for (const auto& chunk : CreatedUnversionedPrimaryChunks_) {
        const auto& chunkId = chunk->ChunkId();
        chunkIdToInputCookie[chunkId] = AddChunk(chunk);
        chunkIdToChunk[chunkId] = chunk;
        resumedChunks.insert(chunkId);
        pendingChunks.insert(chunkId);
    }

    ChunkPool_->Finish();

    ASSERT_EQ(ChunkPool_->GetPendingJobCount(), chunkCount);

    // Set this to true when debugging locally. It helps a lot to understand what happens.
    constexpr bool EnableDebugOutput = false;
    IOutputStream& Cdebug = EnableDebugOutput ? Cerr : Cnull;

    while (completedChunks.size() < chunkCount) {
        EXPECT_FALSE(ChunkPool_->IsCompleted());

        // 0..0 - pool is persisted and restored;
        // 1..29 - chunk is suspended;
        // 30..59 - chunk is resumed;
        // 60..69 - chunk is extracted;
        // 70..79 - chunk is completed;
        // 80..89 - chunk is failed;
        // 90..99 - chunk is aborted.
        int eventType = dice(Gen_);
        if (eventType <= 0) {
            Cdebug << "Persisting and restoring the pool" << Endl;
            PersistAndRestore();
        } else if (eventType <= 29) {
            if (auto randomElement = chooseRandomElement(resumedChunks)) {
                const auto& chunkId = *randomElement;
                Cdebug << Format("Suspending chunk %v", chunkId) << Endl;
                ASSERT_TRUE(resumedChunks.erase(chunkId));
                ASSERT_TRUE(suspendedChunks.insert(chunkId).second);
                auto inputCookie = chunkIdToInputCookie.at(chunkId);
                auto chunk = chunkIdToChunk.at(chunkId);
                SuspendChunk(inputCookie, chunk);
            }
        } else if (eventType <= 59) {
            if (auto randomElement = chooseRandomElement(suspendedChunks)) {
                const auto& chunkId = *randomElement;
                Cdebug << Format("Resuming chunk %v", chunkId) << Endl;
                ASSERT_TRUE(suspendedChunks.erase(chunkId));
                ASSERT_TRUE(resumedChunks.insert(chunkId).second);
                auto inputCookie = chunkIdToInputCookie.at(chunkId);
                auto chunk = chunkIdToChunk.at(chunkId);
                ResumeChunk(inputCookie, chunk);
            }
        } else if (eventType <= 69) {
            if (ChunkPool_->GetPendingJobCount()) {
                auto outputCookie = ExtractCookie(TNodeId(0));
                Cdebug << Format("Extracted cookie %v...", outputCookie);
                // TODO(max42): why the following line leads to the linkage error?
                // ASSERT_NE(outputCookie, IChunkPoolOutput::NullCookie);
                // error: undefined reference to 'NYT::NScheduler::IChunkPoolOutput::NullCookie'
                auto stripeList = ChunkPool_->GetStripeList(outputCookie);
                ASSERT_TRUE(stripeList->Stripes[0]);
                const auto& stripe = stripeList->Stripes[0];
                const auto& dataSlice = stripe->DataSlices.front();
                const auto& chunk = dataSlice->GetSingleUnversionedChunkOrThrow();
                const auto& chunkId = chunk->ChunkId();
                Cdebug << Format(" that corresponds to a chunk %v", chunkId) << Endl;
                ASSERT_TRUE(resumedChunks.has(chunkId));
                ASSERT_TRUE(!suspendedChunks.has(chunkId));
                ASSERT_TRUE(pendingChunks.erase(chunkId));
                ASSERT_TRUE(startedChunks.insert(chunkId).second);
                ASSERT_TRUE(chunkIdToOutputCookie.insert(std::make_pair(chunkId, outputCookie)).second);
            }
        } else if (eventType <= 79) {
            if (auto randomElement = chooseRandomElement(startedChunks)) {
                const auto& chunkId = *randomElement;
                Cdebug << Format("Completed chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(startedChunks.erase(chunkId));
                ASSERT_TRUE(chunkIdToOutputCookie.erase(chunkId));
                ASSERT_TRUE(completedChunks.insert(chunkId).second);
                ChunkPool_->Completed(outputCookie, TCompletedJobSummary());
            }
        } else if (eventType <= 89) {
            if (auto randomElement = chooseRandomElement(startedChunks)) {
                const auto& chunkId = *randomElement;
                Cdebug << Format("Aborted chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(startedChunks.erase(chunkId));
                ASSERT_TRUE(chunkIdToOutputCookie.erase(chunkId));
                ASSERT_TRUE(pendingChunks.insert(chunkId).second);
                ChunkPool_->Aborted(outputCookie, EAbortReason::Unknown);
            }
        } else { // if (eventType <= 99)
            if (auto randomElement = chooseRandomElement(startedChunks)) {
                const auto& chunkId = *randomElement;
                Cdebug << Format("Failed chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(startedChunks.erase(chunkId));
                ASSERT_TRUE(chunkIdToOutputCookie.erase(chunkId));
                ASSERT_TRUE(pendingChunks.insert(chunkId).second);
                ChunkPool_->Failed(outputCookie);
            }
        }
    }
    ASSERT_TRUE(ChunkPool_->IsCompleted());
    ASSERT_EQ(ChunkPool_->GetPendingJobCount(), 0);
    ASSERT_EQ(completedChunks.size(), chunkCount);
    ASSERT_EQ(pendingChunks.size(), 0);
    ASSERT_EQ(startedChunks.size(), 0);
    ASSERT_EQ(resumedChunks.size() + suspendedChunks.size(), chunkCount);
}

INSTANTIATE_TEST_CASE_P(VariousOperationsWithPoolInstantiation,
    TOrderedChunkPoolTestRandomized,
    ::testing::Range(0, NumberOfRepeats));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NChunkPools
} // namespace NYT

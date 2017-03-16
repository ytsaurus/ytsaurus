#include "framework.h"

#include "chunk_slice_fetcher_mock.h"

#include <util/stream/null.h>

#include <yt/core/misc/blob_output.h>
#include <yt/core/misc/phoenix.h>

#include <yt/server/scheduler/sorted_chunk_pool.h>

#include <yt/ytlib/table_client/row_buffer.h>

#include <random>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! We intentionally suppress the default output for TInputChunk as they are
//! identified by their addresses (already printed from the template function
//! for TIntrusivePtr) pretty well.
void PrintTo(const TInputChunk& /* chunk */, std::ostream* /* os */)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namesapce NChunkClient
} // namespace NYT

namespace NYT {
namespace NScheduler {
namespace {

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

class TSortedChunkPoolTest
    : public Test
{
protected:
    virtual void SetUp() override
    {
        ChunkSliceFetcher_ = New<StrictMock<TMockChunkSliceFetcher>>();
        Options_.MinTeleportChunkSize = Inf64;
        Options_.MaxTotalSliceCount = Inf64;
        DataSizePerJob_ = Inf64;
        MaxDataSlicesPerJob_ = Inf32;
        InputSliceDataSize_ = Inf64;
    }

    void InitJobConstraints()
    {
        Options_.JobSizeConstraints = CreateExplicitJobSizeConstraints(
            false /* canAdjustDataSizePerJob */,
            false /* isExplicitJobCount */,
            0 /* jobCount */,
            DataSizePerJob_,
            MaxDataSlicesPerJob_,
            0 /* maxDataSizePerJob_ */,
            InputSliceDataSize_,
            Inf64 /* inputSliceRowCount */);
    }

    std::function<IChunkSliceFetcherPtr()> GetChunkSliceFetcherFactory()
    {
        Expectation expectation = EXPECT_CALL(*ChunkSliceFetcher_, Fetch())
            .After(AllChunksAreAdded_)
            .WillOnce(Return(VoidFuture));

        EXPECT_CALL(*ChunkSliceFetcher_, GetChunkSlices())
            .After(expectation)
            .WillOnce(ReturnPointee(&DataSlices_));

        return [&] () {
            return ChunkSliceFetcher_;
        };
    }


    // In this test we will only deal with integral rows as
    // all the logic inside sorted chunk pool does not depend on
    // actual type of values in keys.
    TKey BuildRow(std::vector<i64> values)
    {
        auto row = RowBuffer_->Allocate(values.size());
        for (int index = 0; index < values.size(); ++index) {
            row[index] = MakeUnversionedInt64Value(values[index], index);
        }
        return row;
    }

    TInputChunkPtr CreateChunk(
        const TKey& minBoundaryKey,
        const TKey& maxBoundaryKey,
        int tableIndex,
        i64 size = 1 * KB,
        const TKey& lowerLimit = TKey(),
        const TKey& upperLimit = TKey())
    {
        auto inputChunk = New<TInputChunk>();
        inputChunk->ChunkId() = TChunkId::Create();
        inputChunk->SetCompressedDataSize(size);
        inputChunk->SetUncompressedDataSize(size);
        inputChunk->BoundaryKeys() = std::make_unique<TBoundaryKeys>(TBoundaryKeys {
            TOwningKey(minBoundaryKey),
            TOwningKey(maxBoundaryKey)
        });
        inputChunk->SetTableIndex(tableIndex);
        inputChunk->SetTableRowIndex(UnversionedTableRowCounts_[tableIndex]++);
        if (lowerLimit) {
            inputChunk->LowerLimit() = std::make_unique<TReadLimit>(TOwningKey(lowerLimit));
        }
        if (upperLimit) {
            inputChunk->UpperLimit() = std::make_unique<TReadLimit>(TOwningKey(upperLimit));
        }
        if (!InputTables_[tableIndex].IsVersioned() && !InputTables_[tableIndex].IsForeign()) {
            CreatedUnversionedPrimaryChunks_.insert(inputChunk);
        }
        return inputChunk;
    }

    TInputChunkPtr CopyChunk(const TInputChunkPtr& chunk)
    {
        TInputChunkPtr chunkCopy = New<TInputChunk>();
        chunkCopy->SetCompressedDataSize(chunk->GetCompressedDataSize());
        chunkCopy->BoundaryKeys() = std::make_unique<TBoundaryKeys>(*chunk->BoundaryKeys());
        int tableIndex = chunk->GetTableIndex();
        chunkCopy->SetTableIndex(tableIndex);
        chunkCopy->SetTableRowIndex(chunk->GetTableRowIndex());
        if (chunk->LowerLimit()) {
            chunkCopy->LowerLimit() = std::make_unique<TReadLimit>(*chunk->LowerLimit());
        }
        if (chunk->UpperLimit()) {
            chunkCopy->UpperLimit() = std::make_unique<TReadLimit>(*chunk->UpperLimit());
        }
        if (!InputTables_[tableIndex].IsVersioned() && !InputTables_[tableIndex].IsForeign()) {
            CreatedUnversionedPrimaryChunks_.insert(chunkCopy);
        }
        return chunkCopy;
    }

    void InitTables(std::vector<bool> isForeign, std::vector<bool> isTeleportable, std::vector<bool> isVersioned)
    {
        YCHECK(isForeign.size() == isTeleportable.size() && isTeleportable.size() == isVersioned.size() && isForeign.size() > 0);
        for (int index = 0; index < isForeign.size(); ++index) {
            InputTables_.emplace_back(isTeleportable[index], !isForeign[index] /* isPrimary */, isVersioned[index]);
        }
        UnversionedTableRowCounts_.resize(InputTables_.size(), 0);
    }

    void RegisterSliceableUnversionedChunk(const TInputChunkPtr& chunk, std::vector<TInputChunkSlicePtr> slices)
    {
        DataSlices_.insert(DataSlices_.end(), slices.begin(), slices.end());
        AllChunksAreAdded_ += EXPECT_CALL(*ChunkSliceFetcher_, AddChunk(chunk));
    }

    void RegisterTriviallySliceableUnversionedChunk(const TInputChunkPtr& chunk)
    {
        auto chunkSlices = SliceUnversionedChunk(chunk, {}, {chunk->GetCompressedDataSize()});
        RegisterSliceableUnversionedChunk(chunk, std::move(chunkSlices));
    }

    std::vector<TInputChunkSlicePtr> SliceUnversionedChunk(
        TInputChunkPtr chunk,
        std::vector<TKey> internalPoints,
        std::vector<i64> sliceSizes)
    {
        YCHECK(internalPoints.size() + 1 == sliceSizes.size());
        YCHECK(!InputTables_[chunk->GetTableIndex()].IsVersioned());
        TKey lastKey = chunk->LowerLimit() ? chunk->LowerLimit()->GetKey() : chunk->BoundaryKeys()->MinKey;
        std::vector<TInputChunkSlicePtr> slices;
        for (int index = 0; index <= internalPoints.size(); ++index) {
            TKey upperLimit = index < internalPoints.size()
                ? GetStrictKeySuccessor(internalPoints[index], Options_.PrimaryPrefixLength, RowBuffer_)
                : (chunk->UpperLimit()
                ? chunk->UpperLimit()->GetKey()
                : GetKeySuccessor(chunk->BoundaryKeys()->MaxKey, RowBuffer_));
            slices.emplace_back(New<TInputChunkSlice>(chunk, lastKey, upperLimit));
            slices.back()->OverrideSize(1 /* rowCount */, sliceSizes[index]);
            lastKey = upperLimit;
        }
        return slices;
    }

    void CreateChunkPool()
    {
        ChunkPool_ = CreateSortedChunkPool(Options_, GetChunkSliceFetcherFactory(), InputTables_);
    }

    IChunkPoolInput::TCookie AddChunk(const TInputChunkPtr& chunk)
    {
        auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
        ActiveChunks_.insert(chunk->ChunkId());
        InferLimitsFromBoundaryKeys(dataSlice, RowBuffer_);
        return ChunkPool_->Add(New<TChunkStripe>(dataSlice));
    }

    void SuspendChunk(IChunkPoolInput::TCookie cookie, const TInputChunkPtr& chunk)
    {
        YCHECK(ActiveChunks_.erase(chunk->ChunkId()));
        ChunkPool_->Suspend(cookie);
    }

    void ResumeChunk(IChunkPoolInput::TCookie cookie, const TInputChunkPtr& chunk)
    {
        auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
        InferLimitsFromBoundaryKeys(dataSlice, RowBuffer_);
        ActiveChunks_.insert(chunk->ChunkId());
        return ChunkPool_->Resume(cookie, New<TChunkStripe>(dataSlice));
    }

    void ExtractOutputCookiesWhilePossible()
    {
        while (ChunkPool_->GetPendingJobCount()) {
            ExtractCookie(TNodeId(0));
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


    std::vector<TChunkStripeListPtr> GetAllStripeLists()
    {
        std::vector<TChunkStripeListPtr> stripeLists;
        for (auto cookie : OutputCookies_) {
            stripeLists.emplace_back(ChunkPool_->GetStripeList(cookie));
        }
        return stripeLists;
    }

    //! Check that:
    //! * The given stripe lists cover each input chunk with specified read limits without overlapping;
    //! * For each input table the input data slices follow in an ascending order with tie broken by:
    //! *** For the unversioned tables by chunk row index;
    //! *** For the versioned tables by the full key;
    void CheckDataIntegrity(const std::vector<TChunkStripeListPtr>& stripeLists, const std::vector<TInputChunkPtr>& teleportChunks)
    {
        yhash_map<TInputChunkPtr, std::vector<TInputChunkSlicePtr>> chunkSlicesByInputChunk;
        yhash_set<TInputChunkPtr> teleportChunksSet(teleportChunks.begin(), teleportChunks.end());

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
        for (const auto& inputChunk : CreatedUnversionedPrimaryChunks_) {
            if (teleportChunksSet.has(inputChunk)) {
                continue;
            }
            TKey chunkLowerKey = inputChunk->LowerLimit()
                ? inputChunk->LowerLimit()->GetKey()
                : inputChunk->BoundaryKeys()->MinKey;
            TKey chunkUpperKey = inputChunk->UpperLimit()
                ? inputChunk->UpperLimit()->GetKey()
                : GetKeySuccessor(inputChunk->BoundaryKeys()->MaxKey, RowBuffer_);

            TKey lastKey = chunkLowerKey;
            auto it = chunkSlicesByInputChunk.find(inputChunk);
            ASSERT_TRUE(chunkSlicesByInputChunk.end() != it);
            auto& chunkSlices = it->second;
            for (const auto& chunkSlice : chunkSlices) {
                EXPECT_EQ(lastKey, chunkSlice->LowerLimit().Key);
                lastKey = chunkSlice->UpperLimit().Key;
            }
            EXPECT_EQ(lastKey, chunkUpperKey);
        }

        // Second check.
        auto unversionedDataSliceComparator = [] (const TInputDataSlicePtr& lhs, const TInputDataSlicePtr& rhs) {
            auto lhsChunk = lhs->GetSingleUnversionedChunkOrThrow();
            auto rhsChunk = rhs->GetSingleUnversionedChunkOrThrow();
            if (lhsChunk != rhsChunk) {
                return lhsChunk->GetTableRowIndex() < rhsChunk->GetTableRowIndex();
            } else {
                return lhs->LowerLimit().Key < rhs->LowerLimit().Key;
            }
        };
        auto versionedDataSliceComparator = [] (const TInputDataSlicePtr& lhs, const TInputDataSlicePtr& rhs) {
            return lhs->LowerLimit().Key < rhs->LowerLimit().Key;
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

    //! Check that:
    //! * All teleport chunks satisfy the Options_.MinTeleportChunkSize constraint;
    //! * All stripe lists have no more than Options_.MaxDataSlicesPerJob + InputTables_.size() - 1 data slices in total.
    //! Unfortunately we cannot check the Options_.MaxPrimaryDataSizePerJob constraint satisfaction as this is not an absolute
    //! restriction, but only a best-effort bound.
    void TryCheckJobConstraintsSatisfaction(
        const std::vector<TChunkStripeListPtr>& stripeLists,
        const std::vector<TInputChunkPtr>& teleportChunks)
    {
        for (const auto& teleportChunk : teleportChunks) {
            EXPECT_TRUE(teleportChunk->IsLargeCompleteChunk(Options_.MinTeleportChunkSize));
        }

        for (const auto& stripeList : stripeLists) {
            int dataSlicesTotalNumber = 0;
            for (const auto& stripe : stripeList->Stripes) {
                if (stripe) {
                    dataSlicesTotalNumber += stripe->DataSlices.size();
                }
            }
            EXPECT_LE(dataSlicesTotalNumber, MaxDataSlicesPerJob_ + InputTables_.size() - 1);
        }
    }

    //! Find all teleport chunks naively (in quadratic time) and check that chunk pool detected exactly
    //! the same chunks.
    void CheckTeleportChunks(const std::vector<TInputChunkPtr>& teleportChunks)
    {
        // TODO(max42): implement a naive procedure for finding the teleport chunks and compare
        // its result with `teleportChunks`.
    }

    //! Check the correctness of joined data (in quadratic time).
    void CheckCorrectnessOfJoin(const std::vector<TChunkStripeListPtr> chunkStripes)
    {
        // TODO(max42): implement a naive procedure here.
    }

    //! Perform all the correctness checks over the given result of sorted chunk pool invocation
    //! (without any suspends nor job interruptions).
    void CheckEverything(
        const std::vector<TChunkStripeListPtr>& stripeLists,
        const std::vector<TInputChunkPtr>& teleportChunks)
    {
        CheckDataIntegrity(stripeLists, teleportChunks);
        TryCheckJobConstraintsSatisfaction(stripeLists, teleportChunks);
        CheckTeleportChunks(teleportChunks);
        CheckStripeListsContainOnlyActiveChunks();
        CheckForeignStripesAreMarkedAsForeign();
    }

    void CheckForeignStripesAreMarkedAsForeign()
    {
        for (auto cookie : OutputCookies_) {
            auto stripeList = ChunkPool_->GetStripeList(cookie);
            for (const auto& stripe : stripeList->Stripes) {
                int tableIndex = stripe->GetTableIndex();
                EXPECT_EQ(InputTables_[tableIndex].IsForeign(), stripe->Foreign);
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

    std::unique_ptr<IChunkPool> ChunkPool_;

    //! Set containing all unversioned primary input chunks that have ever been created.
    yhash_set<TInputChunkPtr> CreatedUnversionedPrimaryChunks_;
    //! Set containing all chunks that are added to the pool without being suspended.
    yhash_set<TChunkId> ActiveChunks_;

    TIntrusivePtr<StrictMock<TMockChunkSliceFetcher>> ChunkSliceFetcher_;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    ExpectationSet AllChunksAreAdded_;

    std::vector<TInputChunkSlicePtr> DataSlices_;

    std::vector<TDataSource> InputTables_;

    yhash_set<IChunkPoolOutput::TCookie> OutputCookies_;

    std::vector<int> UnversionedTableRowCounts_;

    TSortedChunkPoolOptions Options_;

    i64 DataSizePerJob_;

    i32 MaxDataSlicesPerJob_;

    i64 InputSliceDataSize_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedChunkPoolTest, SortedMergeTeleports1)
{
    Options_.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false, false} /* isForeign */,
        {true, true, true, true} /* isTeleportable */,
        {false, false, false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({0, 10}), BuildRow({1, 11}), 0);
    auto chunkB = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 10}), BuildRow({1, 13}), 2);
    auto chunkD = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 3, 1 * KB, BuildRow({1, 13}), BuildRow({1, 17}));
    RegisterTriviallySliceableUnversionedChunk(chunkD);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);
    AddChunk(chunkD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, UnorderedElementsAreArray({chunkA, chunkB, chunkC}));
    EXPECT_EQ(1, stripeLists.size());

    CheckEverything(stripeLists, teleportChunks);
}

TEST_F(TSortedChunkPoolTest, SortedMergeTeleports2)
{
    Options_.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false, false} /* isForeign */,
        {false, true, true, true} /* isTeleportable */,
        {false, false, false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({0, 10}), BuildRow({1, 11}), 0);
    auto chunkB = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 10}), BuildRow({1, 13}), 2);
    auto chunkD = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 3, 1 * KB, BuildRow({1, 13}), BuildRow({1, 17}));
    RegisterTriviallySliceableUnversionedChunk(chunkA);
    RegisterTriviallySliceableUnversionedChunk(chunkD);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);
    AddChunk(chunkD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, UnorderedElementsAreArray({chunkB, chunkC}));
    // Non-teleportable chunks are separated with teleportable ones, so there should be two separate jobs.
    EXPECT_EQ(2, stripeLists.size());

    CheckEverything(stripeLists, teleportChunks);
}

TEST_F(TSortedChunkPoolTest, SortedMergeTeleports3)
{
    Options_.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /* isForeign */,
        {true, true, true} /* isTeleportable */,
        {false, false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({0, 10}), BuildRow({1, 11}), 0);
    auto chunkB = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 10}), BuildRow({1, 13}), 2);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, UnorderedElementsAreArray({chunkA, chunkB, chunkC}));
    EXPECT_EQ(0, stripeLists.size());

    CheckEverything(stripeLists, teleportChunks);
}

TEST_F(TSortedChunkPoolTest, SortedMergeTeleports4)
{
    Options_.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /* isForeign */,
        {true, true, true} /* isTeleportable */,
        {false, false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 2;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({0, 10}), BuildRow({1, 11}), 0);
    auto chunkB = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 10}), BuildRow({1, 13}), 2);
    RegisterTriviallySliceableUnversionedChunk(chunkA);
    RegisterTriviallySliceableUnversionedChunk(chunkB);
    RegisterTriviallySliceableUnversionedChunk(chunkC);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, IsEmpty());
    EXPECT_EQ(1, stripeLists.size());

    CheckEverything(stripeLists, teleportChunks);
}

// NB(max42): completely getting into this test may take several hours of your life.
// Double-think before reading it :)
TEST_F(TSortedChunkPoolTest, SortedMergeAllKindOfTeleports)
{
    Options_.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /* isForeign */,
        {true, true} /* isTeleportable */,
        {false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 3;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    // +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Simple cases no read limits, keys of length exactly PrimaryPrefixLength.
    // +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // Yes.
    // [==]_____
    // _____[==]
    auto chunkA1 = CreateChunk(BuildRow({1, 1, 0}), BuildRow({1, 1, 2}), 0);
    auto chunkB1 = CreateChunk(BuildRow({1, 1, 3}), BuildRow({1, 1, 5}), 1);

    // Yes (they share only one boundary key).
    // [==]___
    // ___[==]
    auto chunkA2 = CreateChunk(BuildRow({2, 1, 0}), BuildRow({2, 1, 2}), 0);
    auto chunkB2 = CreateChunk(BuildRow({2, 1, 2}), BuildRow({2, 1, 4}), 1);

    // No (they partially intersect).
    // [===]__
    // __[===]
    auto chunkA3 = CreateChunk(BuildRow({3, 1, 0}), BuildRow({3, 1, 2}), 0);
    auto chunkB3 = CreateChunk(BuildRow({3, 1, 1}), BuildRow({3, 1, 4}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA3);
    RegisterTriviallySliceableUnversionedChunk(chunkB3);

    // No (one contained in another).
    // [====]__
    // _[==]___
    auto chunkA4 = CreateChunk(BuildRow({4, 1, 0}), BuildRow({4, 1, 3}), 0);
    auto chunkB4 = CreateChunk(BuildRow({4, 1, 1}), BuildRow({4, 1, 2}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA4);
    RegisterTriviallySliceableUnversionedChunk(chunkB4);

    // No (single_key one contained in another).
    // [====]__
    // __[]____
    auto chunkA5 = CreateChunk(BuildRow({5, 1, 0}), BuildRow({5, 1, 3}), 0);
    auto chunkB5 = CreateChunk(BuildRow({5, 1, 1}), BuildRow({5, 1, 1}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA5);
    RegisterTriviallySliceableUnversionedChunk(chunkB5);

    // No (they coincide).
    // [===]__
    // [===]__
    auto chunkA6 = CreateChunk(BuildRow({6, 1, 0}), BuildRow({6, 1, 3}), 0);
    auto chunkB6 = CreateChunk(BuildRow({6, 1, 0}), BuildRow({6, 1, 3}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA6);
    RegisterTriviallySliceableUnversionedChunk(chunkB6);

    // No (one covers another).
    // [===]__
    // [====]_
    auto chunkA7 = CreateChunk(BuildRow({7, 1, 0}), BuildRow({7, 1, 3}), 0);
    auto chunkB7 = CreateChunk(BuildRow({7, 1, 0}), BuildRow({7, 1, 4}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA7);
    RegisterTriviallySliceableUnversionedChunk(chunkB7);

    // No (one covers another).
    // _[===]__
    // [====]__
    auto chunkA8 = CreateChunk(BuildRow({8, 1, 0}), BuildRow({8, 1, 4}), 0);
    auto chunkB8 = CreateChunk(BuildRow({8, 1, 1}), BuildRow({8, 1, 4}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA8);
    RegisterTriviallySliceableUnversionedChunk(chunkB8);

    // Yes (single-key is located exactly at the max boundary key of another).
    // [===]__
    // ___[]__
    auto chunkA9 = CreateChunk(BuildRow({9, 1, 0}), BuildRow({9, 1, 4}), 0);
    auto chunkB9 = CreateChunk(BuildRow({9, 1, 4}), BuildRow({9, 1, 4}), 1);

    // Yes (single-key is located exactly at the min boundary key of another).
    // [===]__
    // []_____
    auto chunkA10 = CreateChunk(BuildRow({10, 1, 0}), BuildRow({10, 1, 4}), 0);
    auto chunkB10 = CreateChunk(BuildRow({10, 1, 0}), BuildRow({10, 1, 0}), 1);

    // Yes (single-key chunks coincide).
    // _[]___
    // _[]___
    auto chunkA11 = CreateChunk(BuildRow({11, 1, 4}), BuildRow({11, 1, 4}), 0);
    auto chunkB11 = CreateChunk(BuildRow({11, 1, 4}), BuildRow({11, 1, 4}), 1);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with read limits, keys of length exactly PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // Yes/No (non-trivial lower limit).
    // NB: chunkB12 may not be teleported because it has non-trivial read limits.
    // _[==]_____
    // ___===[==]
    auto chunkA12 = CreateChunk(BuildRow({12, 1, 0}), BuildRow({12, 1, 4}), 0);
    auto chunkB12 = CreateChunk(BuildRow({12, 1, 2}), BuildRow({12, 1, 8}), 1, 1 * KB, BuildRow({12, 1, 5}));
    RegisterTriviallySliceableUnversionedChunk(chunkB12);

    // Yes/No (non-trivial lower limit coinciding with max key).
    // _[==]_____
    // ___=[====]
    auto chunkA13 = CreateChunk(BuildRow({13, 1, 0}), BuildRow({13, 1, 4}), 0);
    auto chunkB13 = CreateChunk(BuildRow({13, 1, 2}), BuildRow({13, 1, 8}), 1, 1 * KB, BuildRow({13, 1, 4}));
    RegisterTriviallySliceableUnversionedChunk(chunkB13);

    // No/No (they partially intersect with each other).
    // _[===]____
    // ___=[===]_
    auto chunkA14 = CreateChunk(BuildRow({14, 1, 0}), BuildRow({14, 1, 4}), 0);
    auto chunkB14 = CreateChunk(BuildRow({14, 1, 2}), BuildRow({14, 1, 8}), 1, 1 * KB, BuildRow({14, 1, 3}));
    RegisterTriviallySliceableUnversionedChunk(chunkA14);
    RegisterTriviallySliceableUnversionedChunk(chunkB14);

    // Yes/No (second one is de-facto single-key coinciding with the max-key of the first one).
    // _[===]____
    // ___=[]____
    auto chunkA15 = CreateChunk(BuildRow({15, 1, 0}), BuildRow({15, 1, 4}), 0);
    auto chunkB15 = CreateChunk(BuildRow({15, 1, 2}), BuildRow({15, 1, 4}), 1, 1 * KB, BuildRow({15, 1, 4}));
    RegisterTriviallySliceableUnversionedChunk(chunkB15);

    // Yes/No (non-trivial upper limit).
    // ______[===]_
    // _[==)===____
    auto chunkA16 = CreateChunk(BuildRow({16, 1, 4}), BuildRow({16, 1, 8}), 0);
    auto chunkB16 = CreateChunk(BuildRow({16, 1, 0}), BuildRow({16, 1, 6}), 1, 1 * KB, TKey(), BuildRow({16, 1, 3}));
    RegisterTriviallySliceableUnversionedChunk(chunkB16);

    // Yes/No (non-trivial upper limit).
    // ____[===]_
    // _[==)===__
    auto chunkA17 = CreateChunk(BuildRow({17, 1, 4}), BuildRow({17, 1, 8}), 0);
    auto chunkB17 = CreateChunk(BuildRow({17, 1, 0}), BuildRow({17, 1, 6}), 1, 1 * KB, TKey(), BuildRow({17, 1, 4}));
    RegisterTriviallySliceableUnversionedChunk(chunkB17);

    // No/No (non-trivial upper limit).
    // ____[===]_
    // _[====)=__
    auto chunkA18 = CreateChunk(BuildRow({18, 1, 4}), BuildRow({18, 1, 8}), 0);
    auto chunkB18 = CreateChunk(BuildRow({18, 1, 0}), BuildRow({18, 1, 6}), 1, 1 * KB, TKey(), BuildRow({18, 1, 5}));
    RegisterTriviallySliceableUnversionedChunk(chunkA18);
    RegisterTriviallySliceableUnversionedChunk(chunkB18);

    // Yes/No (first one is single-key touching the second one with non-trivial lower limit).
    // __[]_______
    // ===[==)____
    auto chunkA19 = CreateChunk(BuildRow({19, 1, 4}), BuildRow({19, 1, 4}), 0);
    auto chunkB19 = CreateChunk(BuildRow({19, 1, 0}), BuildRow({19, 1, 6}), 1, 1 * KB, BuildRow({19, 1, 4}));
    RegisterTriviallySliceableUnversionedChunk(chunkB19);

    // Yes/No (first one is single-key touching the second one with non-trivial upper limit).
    // _____[]___
    // ___[==)===_
    auto chunkA20 = CreateChunk(BuildRow({20, 1, 4}), BuildRow({20, 1, 4}), 0);
    auto chunkB20 = CreateChunk(BuildRow({20, 1, 0}), BuildRow({20, 1, 6}), 1, 1 * KB, TKey(), BuildRow({20, 1, 4}));
    RegisterTriviallySliceableUnversionedChunk(chunkB20);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with and without read limits, keys longer than PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // Yes (chunks have longer keys than the PrimaryPrefixLength).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6   <- 2-nd (0-based) component is shown here
    // ___________[======]____________________________________
    // ____________________________[======]___________________
    auto chunkA21 = CreateChunk(BuildRow({21, 1, 1, 42}), BuildRow({21, 1, 2, 42}), 0);
    auto chunkB21 = CreateChunk(BuildRow({21, 1, 3, 42}), BuildRow({21, 1, 4, 42}), 1);

    // Yes (after shortening chunks will be touching).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[========]__________________________________
    // __________________[========]___________________________

    auto chunkA22 = CreateChunk(BuildRow({22, 1, 1, 40}), BuildRow({22, 1, 2, 44}), 0);
    auto chunkB22 = CreateChunk(BuildRow({22, 1, 2, 42}), BuildRow({22, 1, 3, 46}), 1);

    // No (after shortening chunks will be intersecting).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // __________________[================]___________________

    auto chunkA23 = CreateChunk(BuildRow({23, 1, 1, 42}), BuildRow({23, 1, 3, 42}), 0);
    auto chunkB23 = CreateChunk(BuildRow({23, 1, 2, 42}), BuildRow({23, 1, 4, 46}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA23);
    RegisterTriviallySliceableUnversionedChunk(chunkB23);

    // Yes (after shortening one of the chunks will be single-key touching the max-key).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // __________________________[==]_________________________

    auto chunkA24 = CreateChunk(BuildRow({24, 1, 1, 42}), BuildRow({24, 1, 3, 42}), 0);
    auto chunkB24 = CreateChunk(BuildRow({24, 1, 3, 42}), BuildRow({24, 1, 4, 42}), 1);

    // Yes (after shortening one of the chunks will be single-key touching the min-key).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // _________[==]__________________________________________

    auto chunkA25 = CreateChunk(BuildRow({25, 1, 1, 42}), BuildRow({25, 1, 3, 42}), 0);
    auto chunkB25 = CreateChunk(BuildRow({25, 1, 1, 42}), BuildRow({25, 1, 1, 42}), 1);

    // Yes (after shortening both chunks will be coinciding and single-key).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ________________[==]___________________________________
    // _________________[===]_________________________________

    auto chunkA26 = CreateChunk(BuildRow({26, 1, 2, 42}), BuildRow({26, 1, 2, 42}), 0);
    auto chunkB26 = CreateChunk(BuildRow({26, 1, 2, 42}), BuildRow({26, 1, 2, 42}), 1);

    // Yes/No (after shortening one of the chunks will be single-key touching the min-key with non-trivial read limits).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // _________[==)======____________________________________

    auto chunkA27 = CreateChunk(BuildRow({27, 1, 1, 42}), BuildRow({27, 1, 3, 42}), 0);
    auto chunkB27 = CreateChunk(BuildRow({27, 1, 1, 42}), BuildRow({27, 1, 2, 42}), 1, 1 * KB, TKey(), BuildRow({27, 1, 1, 46}));
    RegisterTriviallySliceableUnversionedChunk(chunkB27);

    // No/No (after shortening chunks will be intersecting).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // _________[========)======______________________________

    auto chunkA28 = CreateChunk(BuildRow({28, 1, 1, 42}), BuildRow({28, 1, 3, 42}), 0);
    auto chunkB28 = CreateChunk(BuildRow({28, 1, 1, 42}), BuildRow({28, 1, 3, 42}), 1, 1 * KB, TKey(), BuildRow({28, 1, 2, 46}));
    RegisterTriviallySliceableUnversionedChunk(chunkA28);
    RegisterTriviallySliceableUnversionedChunk(chunkB28);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with and without read limits, read limits shorter than PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // No/No (after shortening one chunks will be intersecting).
    //                0              ||              1              ||              2               <- 1-st component is shown here
    //   ...  |  0,0  |  0,1  |  ... || ...  |  1,0  |  1,1  |  ... || ...  |  2,0  |  2,1  |  ...  <- 1-st and 2-nd component are shown here
    // _____________________==========[======================]______________________________________
    // _______________[==============================]______________________________________________

    auto chunkA29 = CreateChunk(BuildRow({29, 0, 1}), BuildRow({29, 1, 1}), 0, 1 * KB, BuildRow({29, 1}));
    auto chunkB29 = CreateChunk(BuildRow({29, 0, 1}), BuildRow({29, 1, 0}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA29);
    RegisterTriviallySliceableUnversionedChunk(chunkB29);

    // No/Yes (after shortening one chunks will be intersecting).
    //                0              ||              1              ||              2               <- 1-st component is shown here
    //   ...  |  0,0  |  0,1  |  ... || ...  |  1,0  |  1,1  |  ... || ...  |  2,0  |  2,1  |  ...  <- 1-st and 2-nd component are shown here
    // ______________________________________[========================)==============________________
    // _____________________________________________________________________[=======]________________

    auto chunkA30 = CreateChunk(BuildRow({30, 1, 0}), BuildRow({30, 2, 1}), 0, 1 * KB, TKey(), BuildRow({30, 2}));
    auto chunkB30 = CreateChunk(BuildRow({30, 2, 0}), BuildRow({30, 2, 0}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA30);

    CreateChunkPool();

    for (const auto& unversionedInputChunk : CreatedUnversionedPrimaryChunks_) {
        AddChunk(unversionedInputChunk);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, UnorderedElementsAreArray({
        chunkA1, chunkB1,
        chunkA2, chunkB2,
        chunkA9, chunkB9,
        chunkA10, chunkB10,
        chunkA11, chunkB11,
        chunkA12,
        chunkA13,
        chunkA15,
        chunkA16,
        chunkA17,
        chunkA19,
        chunkA20,
        chunkA21, chunkB21,
        chunkA22, chunkB22,
        chunkA24, chunkB24,
        chunkA25, chunkB25,
        chunkA26, chunkB26,
        chunkA27,
        chunkB30,
    }));

    CheckEverything(stripeLists, teleportChunks);
}

TEST_F(TSortedChunkPoolTest, SortedMergeSimple)
{
    Options_.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /* isForeign */,
        {true, true, true} /* isTeleportable */,
        {false, false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({3}), 2);
    auto chunkBSlices = SliceUnversionedChunk(chunkB, {BuildRow({3}), BuildRow({6})}, {KB / 4, KB / 2, KB / 4});
    RegisterTriviallySliceableUnversionedChunk(chunkA);
    RegisterSliceableUnversionedChunk(chunkB, chunkBSlices);
    RegisterTriviallySliceableUnversionedChunk(chunkC);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, IsEmpty());
    EXPECT_EQ(1, stripeLists.size());

    CheckEverything(stripeLists, teleportChunks);
}

TEST_F(TSortedChunkPoolTest, SlicingManiacs)
{
    Options_.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /* isForeign */,
        {true, true} /* isTeleportable */,
        {false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 1;
    MaxDataSlicesPerJob_ = 3;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({5}), 0);
    RegisterTriviallySliceableUnversionedChunk(chunkA);
    std::vector<TInputChunkPtr> maniacChunksB;
    for (int i = 0; i < 100; i++) {
        maniacChunksB.emplace_back(CreateChunk(BuildRow({3}), BuildRow({3}), 1));
        RegisterTriviallySliceableUnversionedChunk(maniacChunksB.back());
    }

    CreateChunkPool();

    AddChunk(chunkA);
    for (const auto& chunkB : maniacChunksB) {
        AddChunk(chunkB);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, IsEmpty());

    // In an ideal world we would've split all this stuff into (100 + 2) / 3 == 34 jobs.
    // Since our implementation is not perfect, we ensure that there is at least 34 jobs
    // and at most 100 / 2 + 2
    EXPECT_LE((100 + 2) / 3, stripeLists.size());
    EXPECT_LE(stripeLists.size(), 100 / 2 + 2);

    CheckEverything(stripeLists, teleportChunks);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedChunkPoolTest, SortedReduceSimple)
{
    Options_.EnableKeyGuarantee = true;
    InitTables(
        {false, false} /* isForeign */,
        {true, true} /* isTeleportable */,
        {false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    MaxDataSlicesPerJob_ = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({0, 1}), BuildRow({2, 2}), 0);
    auto chunkB = CreateChunk(BuildRow({2, 6}), BuildRow({5, 8}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA);
    RegisterTriviallySliceableUnversionedChunk(chunkB);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, IsEmpty());
    ASSERT_EQ(2, stripeLists.size());
    // At least one stripe list should be responsible for the shared key {2}.
    EXPECT_TRUE(
        (stripeLists[0]->Stripes[0] && stripeLists[0]->Stripes[1]) ||
        (stripeLists[0]->Stripes[1] && stripeLists[1]->Stripes[1]));

    CheckEverything(stripeLists, teleportChunks);
}

TEST_F(TSortedChunkPoolTest, SortedReduceManiacs)
{
    Options_.EnableKeyGuarantee = true;
    InitTables(
        {false, false} /* isForeign */,
        {true, true} /* isTeleportable */,
        {false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({0, 1}), BuildRow({2, 9}), 0);
    auto chunkB = CreateChunk(BuildRow({2, 6}), BuildRow({2, 8}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA);
    RegisterTriviallySliceableUnversionedChunk(chunkB);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, IsEmpty());

    CheckEverything(stripeLists, teleportChunks);
}

TEST_F(TSortedChunkPoolTest, SortedReduceAllKindOfTeleports)
{
    Options_.EnableKeyGuarantee = true;
    InitTables(
        {false, false} /* isForeign */,
        {true, true} /* isTeleportable */,
        {false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 3;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    // +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Simple cases no read limits, keys of length exactly PrimaryPrefixLength.
    // +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // Yes.
    // [==]_____
    // _____[==]
    auto chunkA1 = CreateChunk(BuildRow({1, 1, 0}), BuildRow({1, 1, 2}), 0);
    auto chunkB1 = CreateChunk(BuildRow({1, 1, 3}), BuildRow({1, 1, 5}), 1);

    // No (they share only one boundary key).
    // [==]___
    // ___[==]
    auto chunkA2 = CreateChunk(BuildRow({2, 1, 0}), BuildRow({2, 1, 2}), 0);
    auto chunkB2 = CreateChunk(BuildRow({2, 1, 2}), BuildRow({2, 1, 4}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA2);
    RegisterTriviallySliceableUnversionedChunk(chunkB2);

    // No (they partially intersect).
    // [===]__
    // __[===]
    auto chunkA3 = CreateChunk(BuildRow({3, 1, 0}), BuildRow({3, 1, 2}), 0);
    auto chunkB3 = CreateChunk(BuildRow({3, 1, 1}), BuildRow({3, 1, 4}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA3);
    RegisterTriviallySliceableUnversionedChunk(chunkB3);

    // No (one contained in another).
    // [====]__
    // _[==]___
    auto chunkA4 = CreateChunk(BuildRow({4, 1, 0}), BuildRow({4, 1, 3}), 0);
    auto chunkB4 = CreateChunk(BuildRow({4, 1, 1}), BuildRow({4, 1, 2}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA4);
    RegisterTriviallySliceableUnversionedChunk(chunkB4);

    // No (single_key one contained in another).
    // [====]__
    // __[]____
    auto chunkA5 = CreateChunk(BuildRow({5, 1, 0}), BuildRow({5, 1, 3}), 0);
    auto chunkB5 = CreateChunk(BuildRow({5, 1, 1}), BuildRow({5, 1, 1}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA5);
    RegisterTriviallySliceableUnversionedChunk(chunkB5);

    // No (they coincide).
    // [===]__
    // [===]__
    auto chunkA6 = CreateChunk(BuildRow({6, 1, 0}), BuildRow({6, 1, 3}), 0);
    auto chunkB6 = CreateChunk(BuildRow({6, 1, 0}), BuildRow({6, 1, 3}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA6);
    RegisterTriviallySliceableUnversionedChunk(chunkB6);

    // No (one covers another).
    // [===]__
    // [====]_
    auto chunkA7 = CreateChunk(BuildRow({7, 1, 0}), BuildRow({7, 1, 3}), 0);
    auto chunkB7 = CreateChunk(BuildRow({7, 1, 0}), BuildRow({7, 1, 4}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA7);
    RegisterTriviallySliceableUnversionedChunk(chunkB7);

    // No (one covers another).
    // _[===]__
    // [====]__
    auto chunkA8 = CreateChunk(BuildRow({8, 1, 0}), BuildRow({8, 1, 4}), 0);
    auto chunkB8 = CreateChunk(BuildRow({8, 1, 1}), BuildRow({8, 1, 4}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA8);
    RegisterTriviallySliceableUnversionedChunk(chunkB8);

    // No (single-key is located exactly at the max boundary key of another).
    // [===]__
    // ___[]__
    auto chunkA9 = CreateChunk(BuildRow({9, 1, 0}), BuildRow({9, 1, 4}), 0);
    auto chunkB9 = CreateChunk(BuildRow({9, 1, 4}), BuildRow({9, 1, 4}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA9);
    RegisterTriviallySliceableUnversionedChunk(chunkB9);

    // No (single-key is located exactly at the min boundary key of another).
    // [===]__
    // []_____
    auto chunkA10 = CreateChunk(BuildRow({10, 1, 0}), BuildRow({10, 1, 4}), 0);
    auto chunkB10 = CreateChunk(BuildRow({10, 1, 0}), BuildRow({10, 1, 0}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA10);
    RegisterTriviallySliceableUnversionedChunk(chunkB10);

    // No (single-key chunks coincide).
    // _[]___
    // _[]___
    auto chunkA11 = CreateChunk(BuildRow({11, 1, 4}), BuildRow({11, 1, 4}), 0);
    auto chunkB11 = CreateChunk(BuildRow({11, 1, 4}), BuildRow({11, 1, 4}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA11);
    RegisterTriviallySliceableUnversionedChunk(chunkB11);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with read limits, keys of length exactly PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // Yes/No (non-trivial lower limit).
    // NB: chunkB12 may not be teleported because it has non-trivial read limits.
    // _[==]_____
    // ___===[==]
    auto chunkA12 = CreateChunk(BuildRow({12, 1, 0}), BuildRow({12, 1, 4}), 0);
    auto chunkB12 = CreateChunk(BuildRow({12, 1, 2}), BuildRow({12, 1, 8}), 1, 1 * KB, BuildRow({12, 1, 5}));
    RegisterTriviallySliceableUnversionedChunk(chunkB12);

    // No/No (non-trivial lower limit coinciding with max key).
    // _[==]_____
    // ___=[====]
    auto chunkA13 = CreateChunk(BuildRow({13, 1, 0}), BuildRow({13, 1, 4}), 0);
    auto chunkB13 = CreateChunk(BuildRow({13, 1, 2}), BuildRow({13, 1, 8}), 1, 1 * KB, BuildRow({13, 1, 4}));
    RegisterTriviallySliceableUnversionedChunk(chunkA13);
    RegisterTriviallySliceableUnversionedChunk(chunkB13);

    // No/No (they partially intersect with each other).
    // _[===]____
    // ___=[===]_
    auto chunkA14 = CreateChunk(BuildRow({14, 1, 0}), BuildRow({14, 1, 4}), 0);
    auto chunkB14 = CreateChunk(BuildRow({14, 1, 2}), BuildRow({14, 1, 8}), 1, 1 * KB, BuildRow({14, 1, 3}));
    RegisterTriviallySliceableUnversionedChunk(chunkA14);
    RegisterTriviallySliceableUnversionedChunk(chunkB14);

    // No/No (second one is de-facto single-key coinciding with the max-key of the first one).
    // _[===]____
    // ___=[]____
    auto chunkA15 = CreateChunk(BuildRow({15, 1, 0}), BuildRow({15, 1, 4}), 0);
    auto chunkB15 = CreateChunk(BuildRow({15, 1, 2}), BuildRow({15, 1, 4}), 1, 1 * KB, BuildRow({15, 1, 4}));
    RegisterTriviallySliceableUnversionedChunk(chunkA15);
    RegisterTriviallySliceableUnversionedChunk(chunkB15);

    // Yes/No (non-trivial upper limit).
    // ______[===]_
    // _[==)===____
    auto chunkA16 = CreateChunk(BuildRow({16, 1, 4}), BuildRow({16, 1, 8}), 0);
    auto chunkB16 = CreateChunk(BuildRow({16, 1, 0}), BuildRow({16, 1, 6}), 1, 1 * KB, TKey(), BuildRow({16, 1, 3}));
    RegisterTriviallySliceableUnversionedChunk(chunkB16);

    // Yes/No (non-trivial upper limit).
    // ____[===]_
    // _[==)===__
    auto chunkA17 = CreateChunk(BuildRow({17, 1, 4}), BuildRow({17, 1, 8}), 0);
    auto chunkB17 = CreateChunk(BuildRow({17, 1, 0}), BuildRow({17, 1, 6}), 1, 1 * KB, TKey(), BuildRow({17, 1, 4}));
    RegisterTriviallySliceableUnversionedChunk(chunkB17);

    // No/No (non-trivial upper limit).
    // ____[===]_
    // _[====)=__
    auto chunkA18 = CreateChunk(BuildRow({18, 1, 4}), BuildRow({18, 1, 8}), 0);
    auto chunkB18 = CreateChunk(BuildRow({18, 1, 0}), BuildRow({18, 1, 6}), 1, 1 * KB, TKey(), BuildRow({18, 1, 5}));
    RegisterTriviallySliceableUnversionedChunk(chunkA18);
    RegisterTriviallySliceableUnversionedChunk(chunkB18);

    // No/No (first one is single-key touching the second one with non-trivial lower limit).
    // __[]_______
    // ===[==)____
    auto chunkA19 = CreateChunk(BuildRow({19, 1, 4}), BuildRow({19, 1, 4}), 0);
    auto chunkB19 = CreateChunk(BuildRow({19, 1, 0}), BuildRow({19, 1, 6}), 1, 1 * KB, BuildRow({19, 1, 4}));
    RegisterTriviallySliceableUnversionedChunk(chunkA19);
    RegisterTriviallySliceableUnversionedChunk(chunkB19);

    // Yes/No (first one is single-key touching the second one with non-trivial upper limit).
    // _____[]___
    // ___[==)===_
    auto chunkA20 = CreateChunk(BuildRow({20, 1, 4}), BuildRow({20, 1, 4}), 0);
    auto chunkB20 = CreateChunk(BuildRow({20, 1, 0}), BuildRow({20, 1, 6}), 1, 1 * KB, TKey(), BuildRow({20, 1, 4}));
    RegisterTriviallySliceableUnversionedChunk(chunkB20);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with and without read limits, keys longer than PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // Yes (chunks have longer keys than the PrimaryPrefixLength).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6   <- 2-nd (0-based) component is shown here
    // ___________[======]____________________________________
    // ____________________________[======]___________________
    auto chunkA21 = CreateChunk(BuildRow({21, 1, 1, 42}), BuildRow({21, 1, 2, 42}), 0);
    auto chunkB21 = CreateChunk(BuildRow({21, 1, 3, 42}), BuildRow({21, 1, 4, 42}), 1);

    // No (after shortening chunks will be touching).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[========]__________________________________
    // __________________[========]___________________________

    auto chunkA22 = CreateChunk(BuildRow({22, 1, 1, 40}), BuildRow({22, 1, 2, 44}), 0);
    auto chunkB22 = CreateChunk(BuildRow({22, 1, 2, 42}), BuildRow({22, 1, 3, 46}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA22);
    RegisterTriviallySliceableUnversionedChunk(chunkB22);

    // No (after shortening chunks will be intersecting).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // __________________[================]___________________

    auto chunkA23 = CreateChunk(BuildRow({23, 1, 1, 42}), BuildRow({23, 1, 3, 42}), 0);
    auto chunkB23 = CreateChunk(BuildRow({23, 1, 2, 42}), BuildRow({23, 1, 4, 46}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA23);
    RegisterTriviallySliceableUnversionedChunk(chunkB23);

    // No (after shortening one of the chunks will be single-key touching the max-key).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // __________________________[==]_________________________

    auto chunkA24 = CreateChunk(BuildRow({24, 1, 1, 42}), BuildRow({24, 1, 3, 42}), 0);
    auto chunkB24 = CreateChunk(BuildRow({24, 1, 3, 42}), BuildRow({24, 1, 4, 42}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA24);
    RegisterTriviallySliceableUnversionedChunk(chunkB24);

    // No (after shortening one of the chunks will be single-key touching the min-key).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // _________[==]__________________________________________

    auto chunkA25 = CreateChunk(BuildRow({25, 1, 1, 42}), BuildRow({25, 1, 3, 42}), 0);
    auto chunkB25 = CreateChunk(BuildRow({25, 1, 1, 42}), BuildRow({25, 1, 1, 42}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA25);
    RegisterTriviallySliceableUnversionedChunk(chunkB25);

    // No (after shortening both chunks will be coinciding and single-key).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ________________[==]___________________________________
    // _________________[===]_________________________________

    auto chunkA26 = CreateChunk(BuildRow({26, 1, 2, 42}), BuildRow({26, 1, 2, 42}), 0);
    auto chunkB26 = CreateChunk(BuildRow({26, 1, 2, 42}), BuildRow({26, 1, 2, 42}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA26);
    RegisterTriviallySliceableUnversionedChunk(chunkB26);

    // No/No (after shortening one of the chunks will be single-key touching the min-key with non-trivial read limits).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // _________[==)======____________________________________

    auto chunkA27 = CreateChunk(BuildRow({27, 1, 1, 42}), BuildRow({27, 1, 3, 42}), 0);
    auto chunkB27 = CreateChunk(BuildRow({27, 1, 1, 42}), BuildRow({27, 1, 2, 42}), 1, 1 * KB, TKey(), BuildRow({27, 1, 1, 46}));
    RegisterTriviallySliceableUnversionedChunk(chunkA27);
    RegisterTriviallySliceableUnversionedChunk(chunkB27);

    // No/No (after shortening chunks will be intersecting).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // _________[========)======______________________________

    auto chunkA28 = CreateChunk(BuildRow({28, 1, 1, 42}), BuildRow({28, 1, 3, 42}), 0);
    auto chunkB28 = CreateChunk(BuildRow({28, 1, 1, 42}), BuildRow({28, 1, 3, 42}), 1, 1 * KB, TKey(), BuildRow({28, 1, 2, 46}));
    RegisterTriviallySliceableUnversionedChunk(chunkA28);
    RegisterTriviallySliceableUnversionedChunk(chunkB28);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with and without read limits, read limits shorter than PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // No/No (after shortening one chunks will be intersecting).
    //                0              ||              1              ||              2               <- 1-st component is shown here
    //   ...  |  0,0  |  0,1  |  ... || ...  |  1,0  |  1,1  |  ... || ...  |  2,0  |  2,1  |  ...  <- 1-st and 2-nd component are shown here
    // _____________________==========[======================]______________________________________
    // _______________[==============================]______________________________________________

    auto chunkA29 = CreateChunk(BuildRow({29, 0, 1}), BuildRow({29, 1, 1}), 0, 1 * KB, BuildRow({29, 1}));
    auto chunkB29 = CreateChunk(BuildRow({29, 0, 1}), BuildRow({29, 1, 0}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA29);
    RegisterTriviallySliceableUnversionedChunk(chunkB29);

    // No/No (after shortening one chunks will be intersecting).
    //                0              ||              1              ||              2               <- 1-st component is shown here
    //   ...  |  0,0  |  0,1  |  ... || ...  |  1,0  |  1,1  |  ... || ...  |  2,0  |  2,1  |  ...  <- 1-st and 2-nd component are shown here
    // ______________________________________[========================)==============________________
    // _____________________________________________________________________[=======]________________

    auto chunkA30 = CreateChunk(BuildRow({30, 1, 0}), BuildRow({30, 2, 1}), 0, 1 * KB, TKey(), BuildRow({30, 2}));
    auto chunkB30 = CreateChunk(BuildRow({30, 2, 0}), BuildRow({30, 2, 0}), 1);
    RegisterTriviallySliceableUnversionedChunk(chunkA30);

    CreateChunkPool();

    for (const auto& chunk : CreatedUnversionedPrimaryChunks_) {
        AddChunk(chunk);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, UnorderedElementsAreArray({
        chunkA1, chunkB1,
        chunkA12,
        chunkA16,
        chunkA17,
        chunkA20,
        chunkA21, chunkB21,
        chunkB30,
    }));

    CheckEverything(stripeLists, teleportChunks);
}

TEST_F(TSortedChunkPoolTest, SortedReduceWithJoin)
{
    Options_.EnableKeyGuarantee = true;
    InitTables(
        {true, true, false, false} /* isForeign */,
        {false, false, false, false} /* isTeleportable */,
        {false, false, false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 2;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({1, 21}), BuildRow({4, 24}), 0);
    auto chunkB = CreateChunk(BuildRow({2, 62}), BuildRow({4, 64}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 101, 11}), BuildRow({4, 402, 18}), 2);
    auto chunkD = CreateChunk(BuildRow({1, 102, 42}), BuildRow({4, 402, 48}), 3);
    RegisterTriviallySliceableUnversionedChunk(chunkC);
    RegisterTriviallySliceableUnversionedChunk(chunkD);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);
    AddChunk(chunkD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, IsEmpty());

    CheckEverything(stripeLists, teleportChunks);
}

TEST_F(TSortedChunkPoolTest, JoinReduce)
{
    Options_.EnableKeyGuarantee = false;
    InitTables(
        {true, true, false, false} /* isForeign */,
        {false, false, false, false} /* isTeleportable */,
        {false, false, false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 2;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({1, 21}), BuildRow({4, 24}), 0);
    auto chunkB = CreateChunk(BuildRow({2, 62}), BuildRow({4, 64}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 101, 11}), BuildRow({4, 402, 18}), 2);
    auto chunkD = CreateChunk(BuildRow({1, 102, 42}), BuildRow({4, 402, 48}), 3);
    RegisterTriviallySliceableUnversionedChunk(chunkC);
    RegisterTriviallySliceableUnversionedChunk(chunkD);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);
    AddChunk(chunkD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    const auto& teleportChunks = ChunkPool_->GetTeleportChunks();

    EXPECT_THAT(teleportChunks, IsEmpty());

    CheckEverything(stripeLists, teleportChunks);
}

TEST_F(TSortedChunkPoolTest, ResumeSuspendMappingTest)
{
    Options_.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /* isForeign */,
        {false, false} /* isTeleportable */,
        {false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 1;
    MaxDataSlicesPerJob_ = 1;
    InitJobConstraints();

    auto chunkAv1 = CreateChunk(BuildRow({5}), BuildRow({15}), 0);
    auto chunkBv1 = CreateChunk(BuildRow({0}), BuildRow({20}), 1, 1 * KB, BuildRow({10}));
    auto chunkAv1Slices = SliceUnversionedChunk(chunkAv1, {BuildRow({8}), BuildRow({12})}, {KB / 4, KB / 2, KB / 4});
    RegisterSliceableUnversionedChunk(chunkAv1, chunkAv1Slices);
    RegisterTriviallySliceableUnversionedChunk(chunkBv1);

    CreateChunkPool();

    int cookieA = AddChunk(chunkAv1);
    int cookieB = AddChunk(chunkBv1);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    CheckStripeListsContainOnlyActiveChunks();

    SuspendChunk(cookieA, chunkAv1);
    auto chunkAv2 = CopyChunk(chunkAv1);
    ResumeChunk(cookieA, chunkAv2);

    CheckStripeListsContainOnlyActiveChunks();

    SuspendChunk(cookieB, chunkBv1);
    auto chunkBv2 = CopyChunk(chunkBv1);
    ResumeChunk(cookieB, chunkBv2);
}

TEST_F(TSortedChunkPoolTest, ManiacIsSliced)
{
    Options_.EnableKeyGuarantee = false;
    InitTables(
        {false} /* isForeign */,
        {false} /* isTeleportable */,
        {false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 1;
    MaxDataSlicesPerJob_ = 1;
    InputSliceDataSize_ = 10;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({1, 2}), BuildRow({1, 42}), 0);
    chunkA->SetRowCount(10000);
    RegisterTriviallySliceableUnversionedChunk(chunkA);

    CreateChunkPool();

    AddChunk(chunkA);

    ChunkPool_->Finish();
    EXPECT_GE(ChunkPool_->GetPendingJobCount(), 100 / 2);
}

TEST_F(TSortedChunkPoolTest, MaxTotalSliceCount)
{
    Options_.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /* isForeign */,
        {false, false, false} /* isTeleportable */,
        {false, false, false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 1;
    Options_.MaxTotalSliceCount = 6;
    DataSizePerJob_ = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({1}), BuildRow({3}), 1);
    auto chunkC1 = CreateChunk(BuildRow({1}), BuildRow({1}), 2);
    auto chunkC2 = CreateChunk(BuildRow({2}), BuildRow({2}), 2);
    auto chunkC3 = CreateChunk(BuildRow({3}), BuildRow({3}), 2);
    RegisterTriviallySliceableUnversionedChunk(chunkA);
    RegisterTriviallySliceableUnversionedChunk(chunkB);
    RegisterTriviallySliceableUnversionedChunk(chunkC1);
    RegisterTriviallySliceableUnversionedChunk(chunkC2);
    RegisterTriviallySliceableUnversionedChunk(chunkC3);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC1);
    AddChunk(chunkC2);
    AddChunk(chunkC3);

    EXPECT_THROW(ChunkPool_->Finish(), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkPoolTestRandomized
    : public WithParamInterface<int>
    , public TSortedChunkPoolTest
{
public:
    TSortedChunkPoolTestRandomized() = default;

    virtual void SetUp() override final
    {
        TSortedChunkPoolTest::SetUp();
        Gen_.seed(GetParam());
    }

protected:
    std::mt19937 Gen_;
};

static constexpr int NumberOfRepeats = 15;

TEST_P(TSortedChunkPoolTestRandomized, VariousOperationsWithPoolTest)
{
    Options_.EnableKeyGuarantee = false;
    InitTables(
        {false} /* isForeign */,
        {false} /* isTeleportable */,
        {false} /* isVersioned */
    );
    Options_.PrimaryPrefixLength = 1;
    DataSizePerJob_ = 1;
    InitJobConstraints();

    const int chunkCount = 50;

    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
        RegisterTriviallySliceableUnversionedChunk(chunk);
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
    yhash_map<TChunkId, IChunkPoolInput::TCookie> chunkIdToInputCookie;
    yhash_set<TChunkId> suspendedChunks;
    yhash_set<TChunkId> resumedChunks;
    // All chunks from the IChunkPoolOutput point of view.
    yhash_map<TChunkId, IChunkPoolOutput::TCookie> chunkIdToOutputCookie;
    yhash_set<TChunkId> pendingChunks;
    yhash_set<TChunkId> startedChunks;
    yhash_set<TChunkId> completedChunks;
    yhash_map<TChunkId, TInputChunkPtr> chunkIdToChunk;

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
    TOutputStream& Cdebug = EnableDebugOutput ? Cerr : Cnull;

    while (completedChunks.size() < chunkCount) {
        EXPECT_FALSE(ChunkPool_->IsCompleted());

        // 0..0 - pool is persisted and restored;
        // 1..49 - chunk is suspended;
        // 50..79 - chunk is resumed;
        // 80..89 - chunk is extracted;
        // 90..92 - chunk is completed;
        // 93..96 - chunk is failed;
        // 97..99 - chunk is aborted.
        int eventType = dice(Gen_);
        if (eventType <= 0) {
            Cdebug << "Persisting and restoring the pool" << Endl;
            TBlobOutput output;
            TSaveContext saveContext;
            saveContext.SetOutput(&output);
            Save(saveContext, ChunkPool_);
            auto blob = output.Flush();
            ChunkPool_.reset();

            TMemoryInput input(blob.Begin(), blob.Size());
            TLoadContext loadContext;
            loadContext.SetRowBuffer(RowBuffer_);
            loadContext.SetInput(&input);
            Load(loadContext, ChunkPool_);
        } else if (eventType <= 49) {
            if (auto randomElement = chooseRandomElement(resumedChunks)) {
                const auto& chunkId = *randomElement;
                Cdebug << Format("Suspending chunk %v", chunkId) << Endl;
                ASSERT_TRUE(resumedChunks.erase(chunkId));
                ASSERT_TRUE(suspendedChunks.insert(chunkId).second);
                auto inputCookie = chunkIdToInputCookie.at(chunkId);
                auto chunk = chunkIdToChunk.at(chunkId);
                SuspendChunk(inputCookie, chunk);
            }
        } else if (eventType <= 79) {
            if (auto randomElement = chooseRandomElement(suspendedChunks)) {
                const auto& chunkId = *randomElement;
                Cdebug << Format("Resuming chunk %v", chunkId) << Endl;
                ASSERT_TRUE(suspendedChunks.erase(chunkId));
                ASSERT_TRUE(resumedChunks.insert(chunkId).second);
                auto inputCookie = chunkIdToInputCookie.at(chunkId);
                auto chunk = chunkIdToChunk.at(chunkId);
                ResumeChunk(inputCookie, chunk);
            }
        } else if (eventType <= 89) {
            if (ChunkPool_->GetPendingJobCount()) {
                auto outputCookie = ExtractCookie(TNodeId(0));
                Cdebug << Format("Extracted cookie %v...", outputCookie);
                // TODO(max42): why the following line leads to the linkage error?
                // ASSERT_NE(outputCookie, IChunkPoolOutput::NullCookie);
                // error: undefined reference to 'NYT::NScheduler::IChunkPoolOutput::NullCookie'
                auto stripeList = ChunkPool_->GetStripeList(outputCookie);
                ASSERT_TRUE(stripeList->Stripes[0]);
                const auto& stripe = stripeList->Stripes[0];
                ASSERT_TRUE(stripe->DataSlices.size() == 1);
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
        } else if (eventType <= 92) {
            if (auto randomElement = chooseRandomElement(startedChunks)) {
                const auto& chunkId = *randomElement;
                Cdebug << Format("Completed chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(startedChunks.erase(chunkId));
                ASSERT_TRUE(chunkIdToOutputCookie.erase(chunkId));
                ASSERT_TRUE(completedChunks.insert(chunkId).second);
                ChunkPool_->Completed(outputCookie, TCompletedJobSummary());
            }
        } else if (eventType <= 96) {
            if (auto randomElement = chooseRandomElement(startedChunks)) {
                const auto& chunkId = *randomElement;
                Cdebug << Format("Aborted chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(startedChunks.erase(chunkId));
                ASSERT_TRUE(chunkIdToOutputCookie.erase(chunkId));
                ASSERT_TRUE(pendingChunks.insert(chunkId).second);
                ChunkPool_->Aborted(outputCookie);
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
    TSortedChunkPoolTestRandomized,
    ::testing::Range(0, NumberOfRepeats));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NScheduler
} // namespace NYT

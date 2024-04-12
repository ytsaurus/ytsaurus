#include "chunk_pools_helpers.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/controller_agent/helpers.h>
#include <yt/yt/server/controller_agent/job_size_constraints.h>
#include <yt/yt/server/controller_agent/operation_controller.h>

#include <yt/yt/server/lib/chunk_pools/unittests/chunk_pools_helpers.h>
#include <yt/yt/server/lib/chunk_pools/unordered_chunk_pool.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/phoenix.h>

#include <util/generic/cast.h>

#include <util/stream/null.h>

#include <random>

namespace NYT::NChunkPools {
namespace {

using namespace NControllerAgent;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;

using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

//! A unit to measure all sizes in this file.
static constexpr i32 Inf32 = std::numeric_limits<i32>::max();
static constexpr i64 Inf64 = std::numeric_limits<i64>::max();

////////////////////////////////////////////////////////////////////////////////

class TUnorderedChunkPoolTest
    : public Test
{
protected:
    void SetUp() override
    {
        Options_.MinTeleportChunkSize = Inf64;
        Options_.RowBuffer = RowBuffer_;
        Options_.Logger = GetTestLogger();
        DataSizePerJob_ = Inf64;
        MaxDataSlicesPerJob_ = Inf32;
        InputSliceDataSize_ = Inf64;
        InputSliceRowCount_ = Inf64;
    }

    void InitJobConstraints()
    {
        Options_.JobSizeConstraints = CreateExplicitJobSizeConstraints(
            false /*canAdjustDataSizePerJob*/,
            IsExplicitJobCount_ /*isExplicitJobCount*/,
            JobCount_ /*jobCount*/,
            DataSizePerJob_,
            Inf64,
            MaxDataSlicesPerJob_,
            Inf64 /*maxDataSizePerJob*/,
            0 /*maxPrimaryDataWeightPerJob*/,
            InputSliceDataSize_,
            InputSliceRowCount_,
            {} /*batchRowCount*/,
            0 /*foreignSliceDataWeight*/,
            SamplingRate_);
    }

    TInputChunkPtr CreateChunk(
        int tableIndex,
        i64 size = 1_KB,
        i64 rowCount = 1000)
    {
        auto inputChunk = New<TInputChunk>();
        inputChunk->SetChunkId(MakeRandomId(EObjectType::Chunk, TCellTag(0x42)));
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

    void InitTables(std::vector<bool> isTeleportable, std::vector<bool> isVersioned)
    {
        YT_VERIFY(isTeleportable.size() == isVersioned.size() && isVersioned.size() > 0);
        for (int index = 0; index < std::ssize(isVersioned); ++index) {
            InputTables_.emplace_back(isTeleportable[index], true /*isPrimary*/, isVersioned[index]);
        }
        UnversionedTableRowCounts_.resize(InputTables_.size(), 0);
    }

    void CreateChunkPool()
    {
        ChunkPool_ = CreateUnorderedChunkPool(Options_, TInputStreamDirectory(InputTables_));
        ChunkPool_->SubscribeChunkTeleported(
            BIND([this] (TInputChunkPtr teleportChunk, std::any /*tag*/) {
                TeleportChunks_.push_back(std::move(teleportChunk));
            }));
    }

    TLegacyDataSlicePtr BuildDataSliceByChunk(const TInputChunkPtr& chunk)
    {
        auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
        dataSlice->SetInputStreamIndex(chunk->GetTableIndex());
        dataSlice->TransformToNewKeyless();
        dataSlice->Tag = chunk->GetChunkId().Parts64[0] ^ chunk->GetChunkId().Parts64[1];
        return dataSlice;
    }

    IChunkPoolInput::TCookie AddChunk(const TInputChunkPtr& chunk)
    {
        auto dataSlice = BuildDataSliceByChunk(chunk);
        ActiveChunks_.insert(chunk->GetChunkId());
        OriginalChunks_.push_back(chunk->GetChunkId());
        return ChunkPool_->Add(New<TChunkStripe>(dataSlice));
    }

    IChunkPoolInput::TCookie AddMultiChunkStripe(std::vector<TInputChunkPtr> chunks)
    {
        std::vector<TLegacyDataSlicePtr> dataSlices;
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
        YT_VERIFY(ActiveChunks_.erase(chunk->GetChunkId()));
        ChunkPool_->Suspend(cookie);
    }

    void ResumeChunk(IChunkPoolInput::TCookie cookie, const TInputChunkPtr& chunk)
    {
        auto dataSlice = BuildDataSliceByChunk(chunk);
        ActiveChunks_.insert(chunk->GetChunkId());
        return ChunkPool_->Resume(cookie);
    }

    void ExtractOutputCookiesWhilePossible()
    {
        while (ChunkPool_->GetJobCounter()->GetPending()) {
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
        TSaveContext saveContext(&output);
        Save(saveContext, ChunkPool_);
        saveContext.Finish();
        auto blob = output.Flush();
        ChunkPool_.Reset();

        TMemoryInput input(blob.Begin(), blob.Size());
        TLoadContext loadContext(&input, RowBuffer_, GetCurrentSnapshotVersion());
        Load(loadContext, ChunkPool_);
        ChunkPool_->SubscribeChunkTeleported(
            BIND([this] (TInputChunkPtr teleportChunk, std::any /*tag*/) {
                TeleportChunks_.push_back(std::move(teleportChunk));
            }));
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
        const std::vector<TChunkStripeListPtr>& stripeLists)
    {
        CheckStripeListsContainOnlyActiveChunks();
        CheckDataIntegrity(stripeLists);
    }

    // TODO(max42): extract all these weird repeating code parts into helpers already! Jeez!
    //! Check that:
    //! * The given stripe lists cover each input chunk with specified read limits without overlapping;
    //! * For each input table the input data slices follow in an ascending order with tie broken by:
    //! *** For the unversioned tables by chunk row index;
    //! *** For the versioned tables by the full key;
    void CheckDataIntegrity(const std::vector<TChunkStripeListPtr>& stripeLists)
    {
        THashMap<TInputChunkPtr, std::vector<TInputChunkSlicePtr>> chunkSlicesByInputChunk;
        THashSet<TInputChunkPtr> teleportChunksSet(TeleportChunks_.begin(), TeleportChunks_.end());

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
            if (teleportChunksSet.contains(inputChunk)) {
                continue;
            }
            TLegacyKey chunkLowerKey = inputChunk->LowerLimit() && inputChunk->LowerLimit()->HasLegacyKey()
                ? inputChunk->LowerLimit()->GetLegacyKey()
                : inputChunk->BoundaryKeys()
                ? inputChunk->BoundaryKeys()->MinKey
                : TLegacyKey();
            TLegacyKey chunkUpperKey = inputChunk->UpperLimit() && inputChunk->UpperLimit()->HasLegacyKey()
                ? inputChunk->UpperLimit()->GetLegacyKey()
                : inputChunk->BoundaryKeys()
                ? GetKeySuccessor(inputChunk->BoundaryKeys()->MaxKey, RowBuffer_)
                : TLegacyKey();
            i64 chunkLowerRowIndex = inputChunk->LowerLimit() && inputChunk->LowerLimit()->HasRowIndex()
                ? inputChunk->LowerLimit()->GetRowIndex()
                : 0;
            i64 chunkUpperRowIndex = inputChunk->UpperLimit() && inputChunk->UpperLimit()->HasRowIndex()
                ? inputChunk->UpperLimit()->GetRowIndex()
                : inputChunk->GetRowCount();

            TLegacyKey lastLowerKey;
            TLegacyKey lastUpperKey = chunkLowerKey;
            i64 lastLeftRowIndex = -1;
            i64 lastRightRowIndex = chunkLowerRowIndex;
            auto it = chunkSlicesByInputChunk.find(inputChunk);
            ASSERT_TRUE(chunkSlicesByInputChunk.end() != it);
            auto& chunkSlices = it->second;

            std::sort(chunkSlices.begin(), chunkSlices.end(), CompareChunkSlicesByLowerLimit);

            for (const auto& chunkSlice : chunkSlices) {
                TLegacyKey chunkSliceLowerKey = chunkSlice->LegacyLowerLimit().Key;
                TLegacyKey chunkSliceUpperKey = chunkSlice->LegacyUpperLimit().Key;
                i64 chunkSliceLowerRowIndex = chunkSlice->LegacyLowerLimit().RowIndex
                    ? *chunkSlice->LegacyLowerLimit().RowIndex
                    : chunkLowerRowIndex;
                i64 chunkSliceUpperRowIndex = chunkSlice->LegacyUpperLimit().RowIndex
                    ? *chunkSlice->LegacyUpperLimit().RowIndex
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
        auto unversionedDataSliceComparator = [] (const TLegacyDataSlicePtr& lhs, const TLegacyDataSlicePtr& rhs) {
            auto lhsChunk = lhs->GetSingleUnversionedChunk();
            auto rhsChunk = rhs->GetSingleUnversionedChunk();
            if (lhsChunk != rhsChunk) {
                return lhsChunk->GetTableRowIndex() < rhsChunk->GetTableRowIndex();
            } else {
                return lhs->LegacyLowerLimit().Key <= rhs->LegacyLowerLimit().Key;
            }
        };
        auto versionedDataSliceComparator = [] (const TLegacyDataSlicePtr& lhs, const TLegacyDataSlicePtr& rhs) {
            return lhs->LegacyLowerLimit().Key <= rhs->LegacyLowerLimit().Key;
        };

        for (const auto& stripeList : stripeLists) {
            for (const auto& stripe : stripeList->Stripes) {
                ASSERT_TRUE(!stripe->DataSlices.empty());
                int tableIndex = stripe->DataSlices.front()->GetTableIndex();
                if (!InputTables_[tableIndex].IsForeign()) {
                    const auto& comparator = (InputTables_[tableIndex].IsVersioned()) ? versionedDataSliceComparator : unversionedDataSliceComparator;
                    for (int index = 0; index + 1 < std::ssize(stripe->DataSlices); ++index) {
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
                            EXPECT_TRUE(ActiveChunks_.contains(chunk->GetChunkId()));
                        }
                    }
                }
            }
        }
    }

    void SplitJob(IChunkPoolOutput::TCookie cookie, int splitJobCount)
    {
        ChunkPool_->Completed(cookie, SummaryWithSplitJobCount(ChunkPool_->GetStripeList(cookie), splitJobCount));
    }

    std::vector<TChunkId> OriginalChunks_;

    IPersistentChunkPoolPtr ChunkPool_;

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

    std::optional<double> SamplingRate_;

    i64 JobCount_;

    bool IsExplicitJobCount_ = false;

    std::vector<IChunkPoolOutput::TCookie> ExtractedCookies_;

    std::mt19937 Gen_;

    std::vector<TInputChunkPtr> TeleportChunks_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TUnorderedChunkPoolTest, UnorderedMergeSimple)
{
    InitTables(
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
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
    PersistAndRestore();

    AddChunk(chunkA1);
    AddChunk(chunkB1);
    AddChunk(chunkC);
    AddChunk(chunkA2);
    AddChunk(chunkB2);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_EQ(TeleportChunks_.size(), 1u);
    EXPECT_EQ(2u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TUnorderedChunkPoolTest, OneStripe1)
{
    InitTables(
        /*isTeleprotable*/ {false},
        /*isVersion*/ {false}
    );

    DataSizePerJob_ = 2047;
    InputSliceDataSize_ = 1500;
    JobCount_ = 1;

    InitJobConstraints();

    auto chunk = CreateChunk(0, DataSizePerJob_);

    CreateChunkPool();
    PersistAndRestore();

    AddChunk(chunk);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TUnorderedChunkPoolTest, OneStripe2)
{
    InitTables(
        /*isTeleprotable*/ {false},
        /*isVersion*/ {false}
    );

    DataSizePerJob_ = 2047;
    InputSliceDataSize_ = 800;
    JobCount_ = 1;

    InitJobConstraints();

    auto chunk = CreateChunk(0, DataSizePerJob_);

    CreateChunkPool();
    PersistAndRestore();

    AddChunk(chunk);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TUnorderedChunkPoolTest, OneStripe3)
{
    InitTables(
        /*isTeleprotable*/ {false},
        /*isVersion*/ {false}
    );

    DataSizePerJob_ = 2048;
    InputSliceDataSize_ = 800;
    JobCount_ = 1;

    InitJobConstraints();

    auto chunk = CreateChunk(0, DataSizePerJob_);

    CreateChunkPool();
    PersistAndRestore();

    AddChunk(chunk);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TUnorderedChunkPoolTest, InputChunksAreSliced)
{
    InitTables(
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
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

    EXPECT_EQ(TeleportChunks_.size(), 0u);
    EXPECT_EQ(5u, stripeLists.size());

    CheckEverything(stripeLists);

    for (const auto& stripeList : stripeLists) {
        EXPECT_GE(stripeList->TotalDataWeight, DataSizePerJob_ * 0.9);
        EXPECT_LE(stripeList->TotalDataWeight, DataSizePerJob_ * 1.1);
    }
}

TEST_F(TUnorderedChunkPoolTest, InterruptionWithSuspendedChunks1)
{
    InitTables(
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );

    DataSizePerJob_ = 5_KB;
    IsExplicitJobCount_ = true;
    JobCount_ = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(0); // 2Kb.

    CreateChunkPool();

    AddChunk(chunkA);

    ChunkPool_->Finish();

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_EQ(0, ChunkPool_->Extract(TNodeId()));
    auto stripeList = ChunkPool_->GetStripeList(0);
    EXPECT_EQ(1u, stripeList->Stripes.size());
    EXPECT_TRUE(TeleportChunks_.empty());

    ChunkPool_->Suspend(0);
    ChunkPool_->Aborted(0, EAbortReason::FailedChunks);

    EXPECT_EQ(0, ChunkPool_->GetJobCounter()->GetPending());
    ChunkPool_->Resume(0);
    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_EQ(0, ChunkPool_->Extract(TNodeId()));
    ChunkPool_->Suspend(0);
    SplitJob(0 /*cookie*/, 1 /*splitJobCount*/);
    EXPECT_EQ(0, ChunkPool_->GetJobCounter()->GetPending());
    ChunkPool_->Resume(0);
    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_EQ(1, ChunkPool_->Extract(TNodeId()));
}

TEST_F(TUnorderedChunkPoolTest, InterruptionWithSuspendedChunks2)
{
    InitTables(
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );

    DataSizePerJob_ = 5_KB;
    IsExplicitJobCount_ = true;
    JobCount_ = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(0);
    auto chunkB = CreateChunk(0);

    CreateChunkPool();

    EXPECT_EQ(0, AddChunk(chunkA));
    EXPECT_EQ(1, AddChunk(chunkB));

    ChunkPool_->Finish();

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_EQ(0, ChunkPool_->Extract(TNodeId()));
    auto stripeList = ChunkPool_->GetStripeList(0);
    EXPECT_EQ(1u, stripeList->Stripes.size());
    EXPECT_TRUE(TeleportChunks_.empty());

    ChunkPool_->Suspend(0);
    ChunkPool_->Suspend(1);
    SplitJob(0, 1);
    ChunkPool_->Resume(0);
    ChunkPool_->Resume(1);

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_EQ(1, ChunkPool_->Extract(TNodeId()));
    stripeList = ChunkPool_->GetStripeList(1);
    EXPECT_EQ(1u, stripeList->Stripes.size());
}

TEST_F(TUnorderedChunkPoolTest, InterruptionWithSuspendedChunks3)
{
    InitTables(
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
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

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_EQ(0, ChunkPool_->Extract(TNodeId()));
    ChunkPool_->Suspend(0);
    SplitJob(0, 1);
    EXPECT_EQ(0, ChunkPool_->GetJobCounter()->GetPending());
    ChunkPool_->Resume(0);
    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
}

////////////////////////////////////////////////////////////////////////////////

class TUnorderedChunkPoolTestRandomized
    : public WithParamInterface<int>
    , public TUnorderedChunkPoolTest
{
public:
    TUnorderedChunkPoolTestRandomized() = default;

    void SetUp() final
    {
        TUnorderedChunkPoolTest::SetUp();
        Gen_.seed(GetParam());
    }
};

static constexpr int NumberOfRepeats = 15;

TEST_P(TUnorderedChunkPoolTestRandomized, VariousOperationsWithPoolTest)
{
    InitTables(
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );

    constexpr int chunkCount = 25;
    constexpr int maxJobLosts = 50;
    DataSizePerJob_ = 1_KB;
    IsExplicitJobCount_ = true;
    JobCount_ = chunkCount;
    InitJobConstraints();

    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(0);
    }

    CreateChunkPool();

    std::uniform_int_distribution<> dice(0, 99);

    auto chooseRandomElement = [&] (auto container) -> std::optional<typename decltype(container)::value_type> {
        if (container.empty()) {
            return std::nullopt;
        } else {
            auto it = container.begin();
            std::advance(it, std::uniform_int_distribution<>(0, container.size() - 1)(Gen_));
            return std::make_optional(*it);
        }
    };

    // All chunks from the IPersistentChunkPoolInput point of view.
    THashMap<TChunkId, IChunkPoolInput::TCookie> chunkIdToInputCookie;
    THashSet<TChunkId> suspendedChunks;
    THashSet<TChunkId> resumedChunks;
    // All chunks from the IPersistentChunkPoolOutput point of view.
    THashMap<TChunkId, IChunkPoolOutput::TCookie> chunkIdToOutputCookie;
    THashSet<TChunkId> pendingChunks;
    THashSet<TChunkId> startedChunks;
    THashSet<TChunkId> completedChunks;
    THashSet<TChunkId> lostChunks;
    THashMap<TChunkId, TInputChunkPtr> chunkIdToChunk;

    for (const auto& chunk : CreatedUnversionedChunks_) {
        auto chunkId = chunk->GetChunkId();
        chunkIdToInputCookie[chunkId] = AddChunk(chunk);
        chunkIdToChunk[chunkId] = chunk;
        resumedChunks.insert(chunkId);
        pendingChunks.insert(chunkId);
    }

    ChunkPool_->Finish();

    ASSERT_EQ(ChunkPool_->GetJobCounter()->GetPending(), chunkCount);

    // Set this to true when debugging locally. It helps a lot to understand what happens.
    constexpr bool EnableDebugOutput = false;
    IOutputStream& Cdebug = EnableDebugOutput ? Cerr : Cnull;

    int jobLosts = 0;

    while (completedChunks.size() < chunkCount) {
        YT_VERIFY(!ChunkPool_->IsCompleted());

        // 0..0 - pool is persisted and restored;
        // 1..29 - chunk is suspended;
        // 30..54 - chunk is resumed;
        // 55..59 - chunk is extracted;
        // 60..69 - chunk is completed;
        // 70..79 - chunk is failed;
        // 80..89 - chunk is lost.
        // 90..99 - chunk is aborted.
        int eventType = dice(Gen_);
        if (eventType <= 0) {
            Cdebug << "Persisting and restoring the pool" << Endl;
            PersistAndRestore();
        } else if (eventType <= 29) {
            if (auto randomElement = chooseRandomElement(resumedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Suspending chunk %v", chunkId) << Endl;
                ASSERT_TRUE(resumedChunks.erase(chunkId));
                ASSERT_TRUE(suspendedChunks.insert(chunkId).second);
                auto inputCookie = chunkIdToInputCookie.at(chunkId);
                auto chunk = chunkIdToChunk.at(chunkId);
                Cdebug << Format("Suspend cookie %v", inputCookie) << Endl;
                SuspendChunk(inputCookie, chunk);
            }
        } else if (eventType <= 54) {
            if (auto randomElement = chooseRandomElement(suspendedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Resuming chunk %v", chunkId) << Endl;
                ASSERT_TRUE(suspendedChunks.erase(chunkId));
                ASSERT_TRUE(resumedChunks.insert(chunkId).second);
                auto inputCookie = chunkIdToInputCookie.at(chunkId);
                auto chunk = chunkIdToChunk.at(chunkId);
                Cdebug << Format("Resume cookie %v", inputCookie) << Endl;
                ResumeChunk(inputCookie, chunk);
            }
        } else if (eventType <= 59) {
            if (ChunkPool_->GetJobCounter()->GetPending()) {
                auto outputCookie = ExtractCookie(TNodeId(0));
                Cdebug << Format("Extracted cookie %v...", outputCookie);
                // TODO(max42): why the following line leads to the linkage error?
                // ASSERT_NE(outputCookie, IChunkPoolOutput::NullCookie);
                // error: undefined reference to 'NYT::NScheduler::IChunkPoolOutput::NullCookie'
                auto stripeList = ChunkPool_->GetStripeList(outputCookie);
                ASSERT_TRUE(stripeList->Stripes[0]);
                const auto& stripe = stripeList->Stripes[0];
                const auto& dataSlice = stripe->DataSlices.front();
                const auto& chunk = dataSlice->GetSingleUnversionedChunk();
                auto chunkId = chunk->GetChunkId();
                Cdebug << Format(" that corresponds to a chunk %v", chunkId) << Endl;
                ASSERT_TRUE(resumedChunks.contains(chunkId));
                ASSERT_TRUE(!suspendedChunks.contains(chunkId));
                if (chunkIdToOutputCookie.contains(chunkId)) {
                    ASSERT_EQ(chunkIdToOutputCookie.at(chunkId), outputCookie);
                } else {
                    ASSERT_TRUE(chunkIdToOutputCookie.emplace(chunkId, outputCookie).second);
                }
                if (lostChunks.contains(chunkId)) {
                    ASSERT_TRUE(lostChunks.erase(chunkId));
                } else {
                    ASSERT_TRUE(pendingChunks.erase(chunkId));
                }
                ASSERT_TRUE(startedChunks.insert(chunkId).second);
            }
        } else if (eventType <= 69) {
            if (auto randomElement = chooseRandomElement(startedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Completed chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(startedChunks.erase(chunkId));
                ASSERT_TRUE(completedChunks.insert(chunkId).second);
                ChunkPool_->Completed(outputCookie, TCompletedJobSummary());
            }
        } else if (eventType <= 79) {
            if (auto randomElement = chooseRandomElement(startedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Aborted chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(startedChunks.erase(chunkId));
                ASSERT_TRUE(pendingChunks.insert(chunkId).second);
                ChunkPool_->Aborted(outputCookie, EAbortReason::Unknown);
            }
        } else if (eventType <= 89) {
            if (jobLosts >= maxJobLosts) {
                continue;
            }
            if (auto randomElement = chooseRandomElement(completedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Lost chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(completedChunks.erase(chunkId));
                ASSERT_TRUE(lostChunks.insert(chunkId).second);
                ChunkPool_->Lost(outputCookie);
                ++jobLosts;
            }
        } else { // if (eventType <= 99)
            if (auto randomElement = chooseRandomElement(startedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Failed chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(startedChunks.erase(chunkId));
                ASSERT_TRUE(pendingChunks.insert(chunkId).second);
                ChunkPool_->Failed(outputCookie);
            }
        }
    }
    ASSERT_TRUE(ChunkPool_->IsCompleted());
    ASSERT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 0);
    ASSERT_EQ(std::ssize(completedChunks), chunkCount);
    ASSERT_EQ(std::ssize(pendingChunks), 0);
    ASSERT_EQ(std::ssize(startedChunks), 0);
    ASSERT_EQ(std::ssize(lostChunks), 0);
    ASSERT_EQ(std::ssize(resumedChunks) + std::ssize(suspendedChunks), chunkCount);
}

INSTANTIATE_TEST_SUITE_P(VariousOperationsWithPoolInstantiation,
    TUnorderedChunkPoolTestRandomized,
    ::testing::Range(0, NumberOfRepeats));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkPools

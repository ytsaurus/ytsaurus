#include "chunk_pools_helpers.h"

#include <yt/yt/server/lib/chunk_pools/unittests/chunk_pools_helpers.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/unordered_chunk_pool.h>

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/blob_output.h>

#include <util/generic/xrange.h>

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

class TUnorderedChunkPoolTest
    : public TChunkPoolTestBase
{
protected:
    void SetUp() override
    {
        TChunkPoolTestBase::SetUp();

        Options_.MinTeleportChunkSize = Inf64;
        Options_.RowBuffer = RowBuffer_;
        Options_.Logger = GetTestLogger();
        DataWeightPerJob_ = Inf64;
        CompressedDataSizePerJob_ = Inf64;
        MaxDataSlicesPerJob_ = Inf32;
        MaxCompressedDataSizePerJob_ = Inf64;
        InputSliceDataWeight_ = Inf64;
        InputSliceRowCount_ = Inf64;
    }

    void InitJobConstraints()
    {
        Options_.JobSizeConstraints = CreateExplicitJobSizeConstraints(
            /*canAdjustDataSizePerJob*/ false ,
            /*isExplicitJobCount*/ IsExplicitJobCount_,
            /*jobCount*/ JobCount_,
            DataWeightPerJob_,
            /*primaryDataWeightPerJob*/ Inf64,
            CompressedDataSizePerJob_,
            /*primaryCompressedDataSizePerJob*/ Inf64,
            MaxDataSlicesPerJob_,
            /*maxDataSizePerJob*/ Inf64,
            /*maxPrimaryDataWeightPerJob*/ 0,
            MaxCompressedDataSizePerJob_,
            /*maxCompressedDataSizePerJob*/ Inf64,
            InputSliceDataWeight_,
            InputSliceRowCount_,
            /*batchRowCount*/ {},
            /*foreignSliceDataWeight*/ 0,
            SamplingRate_);
    }

    TInputChunkPtr CreateChunk(
        int tableIndex,
        i64 weight = 1_KB,
        i64 rowCount = 1000,
        i64 compressedSize = -1)
    {
        YT_VERIFY(tableIndex < std::ssize(InputTables_));
        if (compressedSize == -1) {
            compressedSize = weight;
        }
        auto inputChunk = New<TInputChunk>();
        inputChunk->SetChunkId(MakeId(
            EObjectType::Chunk,
            TCellTag(0x42),
            std::uniform_int_distribution<ui64>()(Gen_),
            std::uniform_int_distribution<ui32>()(Gen_)));
        inputChunk->SetCompressedDataSize(compressedSize);
        inputChunk->SetTotalUncompressedDataSize(weight);
        inputChunk->SetTotalDataWeight(weight);
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

    static TLegacyDataSlicePtr BuildDataSliceByChunk(const TInputChunkPtr& chunk)
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
        std::move(dataSlices.begin(), dataSlices.end(), std::back_inserter(stripe->DataSlices()));
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

    IChunkPoolOutput::TCookie ExtractCookie(TNodeId nodeId = TNodeId(0))
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
        const std::vector<TChunkStripeListPtr>& stripeLists,
        bool chunksRestored = false)
    {
        CheckStripeListsContainOnlyActiveChunks();
        CheckDataIntegrity(stripeLists, chunksRestored);
    }

    // TODO(max42): extract all these weird repeating code parts into helpers already! Jeez!
    //! Check that:
    //! * The given stripe lists cover each input chunk with specified read limits without overlapping;
    void CheckDataIntegrity(const std::vector<TChunkStripeListPtr>& stripeLists, bool chunksRestored = false)
    {
        THashMap<TInputChunkPtr, std::vector<TInputChunkSlicePtr>> chunkSlicesByInputChunkPtr;
        THashMap<TChunkId, std::vector<TInputChunkSlicePtr>> chunkSlicesByInputChunkId;
        THashSet<TInputChunkPtr> teleportChunksSet(TeleportChunks_.begin(), TeleportChunks_.end());

        // Check that data slices from each stripe are all from the same table.
        for (const auto& stripeList : stripeLists) {
            for (const auto& stripe : stripeList->Stripes()) {
                ASSERT_TRUE(!stripe->DataSlices().empty());
                int tableIndex = stripe->DataSlices().front()->GetTableIndex();

                for (const auto& dataSlice : stripe->DataSlices()) {
                    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                        const auto& inputChunk = chunkSlice->GetInputChunk();
                        EXPECT_EQ(tableIndex, inputChunk->GetTableIndex());
                        chunkSlicesByInputChunkPtr[inputChunk].emplace_back(chunkSlice);
                        if (chunksRestored) {
                            chunkSlicesByInputChunkId[inputChunk->GetChunkId()].emplace_back(chunkSlice);
                        }
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

            std::vector<TInputChunkSlicePtr> chunkSlices;
            if (chunksRestored) {
                auto it = chunkSlicesByInputChunkId.find(inputChunk->GetChunkId());
                ASSERT_TRUE(chunkSlicesByInputChunkId.end() != it);
                chunkSlices = it->second;
            } else {
                auto it = chunkSlicesByInputChunkPtr.find(inputChunk);
                ASSERT_TRUE(chunkSlicesByInputChunkPtr.end() != it);
                chunkSlices = it->second;
            }

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
    }

    void CheckStripeListsContainOnlyActiveChunks()
    {
        for (auto cookie : OutputCookies_) {
            auto stripeList = ChunkPool_->GetStripeList(cookie);
            for (const auto& stripe : stripeList->Stripes()) {
                if (stripe) {
                    for (const auto& dataSlice : stripe->DataSlices()) {
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

    // Make tests deterministic.
    struct TInputChunkHash {
        ui64 operator()(const TInputChunkPtr& chunk) const {
            return THash<TChunkId>()(chunk->GetChunkId());
        }
    };

    //! Set containing all unversioned input chunks that have ever been created.
    THashSet<TInputChunkPtr, TInputChunkHash> CreatedUnversionedChunks_;
    //! Set containing all chunks that are added to the pool without being suspended.
    THashSet<TChunkId> ActiveChunks_;

    std::vector<TInputStreamDescriptor> InputTables_;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    THashSet<IChunkPoolOutput::TCookie> OutputCookies_;

    std::vector<int> UnversionedTableRowCounts_;

    TUnorderedChunkPoolOptions Options_;

    i64 DataWeightPerJob_;

    i64 CompressedDataSizePerJob_;

    i32 MaxDataSlicesPerJob_;

    i64 MaxCompressedDataSizePerJob_;

    i64 InputSliceDataWeight_;

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
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, false, false});

    DataWeightPerJob_ = 2_KB;
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
        /*isVersion*/ {false});

    DataWeightPerJob_ = 2047;
    InputSliceDataWeight_ = 1500;
    JobCount_ = 1;

    InitJobConstraints();

    auto chunk = CreateChunk(0, DataWeightPerJob_);

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
        /*isVersion*/ {false});

    DataWeightPerJob_ = 2047;
    InputSliceDataWeight_ = 800;
    JobCount_ = 1;

    InitJobConstraints();

    auto chunk = CreateChunk(0, DataWeightPerJob_);

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
        /*isVersion*/ {false});

    DataWeightPerJob_ = 2048;
    InputSliceDataWeight_ = 800;
    JobCount_ = 1;

    InitJobConstraints();

    auto chunk = CreateChunk(0, DataWeightPerJob_);

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
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 2_KB / 5;
    IsExplicitJobCount_ = true; // TODO(max42): consider what happens with false here.
    JobCount_ = 5;
    InputSliceDataWeight_ = DataWeightPerJob_ / 10;
    InitJobConstraints();

    auto chunkA = CreateChunk(0); // 1Kb.
    auto chunkB = CreateChunk(0); // 1Kb.

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_EQ(TeleportChunks_.size(), 0u);
    EXPECT_EQ(5u, stripeLists.size());

    CheckEverything(stripeLists);

    for (int index : xrange(std::ssize(stripeLists) - 1)) {
        i64 dataWeight = stripeLists[index]->GetAggregateStatistics().DataWeight;
        EXPECT_GE(dataWeight, DataWeightPerJob_);
        EXPECT_LE(dataWeight, DataWeightPerJob_ + InputSliceDataWeight_);
    }
    EXPECT_LE(stripeLists.back()->GetAggregateStatistics().DataWeight, DataWeightPerJob_);
}

TEST_F(TUnorderedChunkPoolTest, InterruptionWithSuspendedChunks1)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 5_KB;
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
    EXPECT_EQ(1u, stripeList->Stripes().size());
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
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 5_KB;
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
    EXPECT_EQ(1u, stripeList->Stripes().size());
    EXPECT_TRUE(TeleportChunks_.empty());

    ChunkPool_->Suspend(0);
    ChunkPool_->Suspend(1);
    SplitJob(0, 1);
    ChunkPool_->Resume(0);
    ChunkPool_->Resume(1);

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_EQ(1, ChunkPool_->Extract(TNodeId()));
    stripeList = ChunkPool_->GetStripeList(1);
    EXPECT_EQ(1u, stripeList->Stripes().size());
}

TEST_F(TUnorderedChunkPoolTest, InterruptionWithSuspendedChunks3)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

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

TEST_F(TUnorderedChunkPoolTest, UnsuccessfulSplitMarksJobUnsplittable)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 2_KB;
    IsExplicitJobCount_ = false;
    JobCount_ = 1;
    InitJobConstraints();

    auto chunk = CreateChunk(0, /*size*/ 1_KB, /*rowCount*/ 1);

    CreateChunkPool();

    AddChunk(chunk);

    ChunkPool_->Finish();

    CheckUnsuccessfulSplitMarksJobUnsplittable(ChunkPool_);
}

TEST_F(TUnorderedChunkPoolTest, BuildJobsInputByMaxCompressedDataSize)
{
    InitTables(
        /*isTeleportable*/ {false, false},
        /*isVersioned*/ {false, false});

    MaxCompressedDataSizePerJob_ = 150_MB;
    DataWeightPerJob_ = 10_MB;
    InitJobConstraints();

    CreateChunkPool();

    for (int i = 0; i < 10; ++i) {
        AddChunk(CreateChunk(
            i % 2,
            /*weight*/ 1_MB,
            /*rowCount*/ 1000,
            /*compressedSize*/ 100_MB));
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_TRUE(TeleportChunks_.empty());
    EXPECT_EQ(stripeLists.size(), 10u);

    CheckEverything(stripeLists);

    for (const auto& stripeList : stripeLists) {
        const auto& statistics = stripeList->GetAggregateStatistics();

        EXPECT_GE(statistics.CompressedDataSize, static_cast<i64>(100_MB) * 0.9);
        EXPECT_LE(statistics.CompressedDataSize, static_cast<i64>(100_MB) * 1.1);

        EXPECT_GE(statistics.DataWeight, static_cast<i64>(1_MB) * 0.9);
        EXPECT_LE(statistics.DataWeight, static_cast<i64>(1_MB) * 1.1);
    }
}

TEST_F(TUnorderedChunkPoolTest, BuildJobsInputByMaxCompressedDataSizeWithChunkSlicing)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    MaxCompressedDataSizePerJob_ = 140_MB;
    InputSliceDataWeight_ = 256_KB;
    DataWeightPerJob_ = 10_MB;
    InitJobConstraints();

    CreateChunkPool();

    for (int i = 0; i < 10; ++i) {
        AddChunk(CreateChunk(
            0,
            /*weight*/ 512_KB,
            /*rowCount*/ 5000,
            /*compressedSize*/ 50_MB));
        PersistAndRestore();
    }

    ChunkPool_->Finish();

    PersistAndRestore();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_TRUE(TeleportChunks_.empty());

    EXPECT_EQ(stripeLists.size(), 4u);

    CheckEverything(stripeLists, /*chunksRestored*/ true);

    for (const auto& stripeList : stripeLists) {
        const auto& statistics = stripeList->GetAggregateStatistics();

        EXPECT_GE(statistics.CompressedDataSize, static_cast<i64>(125_MB) * 0.9);
        EXPECT_LE(statistics.CompressedDataSize, static_cast<i64>(125_MB) * 1.1);

        EXPECT_GE(statistics.DataWeight, static_cast<i64>(1_MB + 256_KB) * 0.9);
        EXPECT_LE(statistics.DataWeight, static_cast<i64>(1_MB + 256_KB) * 1.1);
    }
}

TEST_F(TUnorderedChunkPoolTest, BuildJobsInputByMaxCompressedDataSizeAndByDataWeight)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    MaxCompressedDataSizePerJob_ = 105_MB;
    DataWeightPerJob_ = 10_MB;
    InitJobConstraints();

    CreateChunkPool();

    for (int i = 0; i < 5; ++i) {
        AddChunk(CreateChunk(
            0,
            /*weight*/ 1_MB,
            /*rowCount*/ 1000,
            /*compressedSize*/ 100_MB));
    }
    for (int i = 0; i < 5; ++i) {
        AddChunk(CreateChunk(
            0,
            /*weight*/ 1_MB,
            /*rowCount*/ 1000,
            /*compressedSize*/ 10_MB));
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_TRUE(TeleportChunks_.empty());

    CheckEverything(stripeLists);

    for (const auto& stripeList : stripeLists) {
        const auto& statistics = stripeList->GetAggregateStatistics();

        EXPECT_LE(statistics.CompressedDataSize, static_cast<i64>(105_MB));
        EXPECT_LE(statistics.DataWeight, static_cast<i64>(10_MB) * 1.1);
    }
}

TEST_F(TUnorderedChunkPoolTest, BuildJobsInputByMaxCompressedDataSizeWhenDataWeightBigger)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    MaxCompressedDataSizePerJob_ = 140_MB;
    DataWeightPerJob_ = 1000_MB;
    InitJobConstraints();

    CreateChunkPool();

    for (int i = 0; i < 10; ++i) {
        AddChunk(CreateChunk(
            0,
            /*weight*/ 100_MB,
            /*rowCount*/ 5000,
            /*compressedSize*/ 50_MB));
        PersistAndRestore();
    }

    ChunkPool_->Finish();

    PersistAndRestore();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_TRUE(TeleportChunks_.empty());

    EXPECT_EQ(stripeLists.size(), 5u);

    CheckEverything(stripeLists, /*chunksRestored*/ true);

    for (const auto& stripeList : stripeLists) {
        const auto& statistics = stripeList->GetAggregateStatistics();

        EXPECT_GE(statistics.CompressedDataSize, static_cast<i64>(100_MB) * 0.9);
        EXPECT_LE(statistics.CompressedDataSize, static_cast<i64>(100_MB) * 1.1);

        EXPECT_GE(statistics.DataWeight, static_cast<i64>(200_MB) * 0.9);
        EXPECT_LE(statistics.DataWeight, static_cast<i64>(200_MB) * 1.1);
    }
}

TEST_F(TUnorderedChunkPoolTest, BuildJobsInputByCompressedDataSize)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    CompressedDataSizePerJob_ = 100_MB;
    InitJobConstraints();

    CreateChunkPool();

    for (int i = 0; i < 10; ++i) {
        AddChunk(CreateChunk(
            /*tableIndex*/ 0,
            /*weight*/ 100_MB,
            /*rowCount*/ 5000,
            /*compressedSize*/ 51_MB));
        PersistAndRestore();
    }

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetTotal(), 6);
    // One job is blocked and will become pending once chunk pool is finished.
    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetBlocked(), 1);

    ChunkPool_->Finish();

    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetTotal(), 6);
    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 6);

    PersistAndRestore();

    ExtractOutputCookiesWhilePossible();

    // Job count has decreased because each stripe has compressed size slightly
    // greater than compressed data size per job.
    EXPECT_EQ(ChunkPool_->GetJobCounter()->GetTotal(), 5);

    auto stripeLists = GetAllStripeLists();

    EXPECT_TRUE(TeleportChunks_.empty());

    EXPECT_EQ(stripeLists.size(), 5u);

    CheckEverything(stripeLists, /*chunksRestored*/ true);

    for (const auto& stripeList : stripeLists) {
        EXPECT_EQ(stripeList->GetAggregateStatistics().CompressedDataSize, 102_MBs);
    }
}

TEST_F(TUnorderedChunkPoolTest, DataWeightPerJobDoesNotAffectCompressedDataSizePerJob)
{
    // This test checks that chunk pool handles slicing by compressed data size and
    // by data weight independently. Chunk pool could use single job counter otherwise.
    //
    // +-------+---------------+
    // |       |               |
    // | Data  |               |
    // | Weight|               |
    // |       | #  _  _  _  _ | <- 250_MB
    // +-------+---------------+
    // |       | #  #  #  #  # | <- 1_GB
    // | Compr.| #  #  #  #  # |
    // | Data  | #  #  #  #  # |
    // | Size  | #  #  #  #  # |
    // +-------+---------------+
    // | Chunk | 1  2  3  4  5 |
    // +-------+---------------+
    //
    // Job count estimation:
    //  - By compressed_data_size ~ ceil(5_GB / 6_GB)   = 1
    //  - By data_weight          ~ ceil(250_MB / 1_MB) = 250
    //
    // Unordered chunk pool does not guarantee sequential ordering of input
    // chunks and may process input chunks in the following orders:
    //  1. [B][SSSS] - 2 jobs
    //  2. [SB][SSS] - 2 jobs
    //  3. [SSB][SS] - 2 jobs
    //  4. [SSSB][S] - 2 jobs
    //  5. [SSSSB]   - 1 job
    //
    // Where:
    //  - "B": data_weight = 250_MB
    //  - "S": data_weight -> 0

    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    CompressedDataSizePerJob_ = 6_GB;
    DataWeightPerJob_ = 1_MB;
    InitJobConstraints();

    CreateChunkPool();

    AddChunk(CreateChunk(
        0,
        /*weight*/ 250_MB,
        /*rowCount*/ 5000,
        /*compressedSize*/ 1_GB));
    PersistAndRestore();

    for (int i = 0; i < 4; ++i) {
        AddChunk(CreateChunk(
            0,
            /*weight*/ 10_KB,
            /*rowCount*/ 5000,
            /*compressedSize*/ 1_GB));
        PersistAndRestore();
    }

    ChunkPool_->Finish();

    PersistAndRestore();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_TRUE(TeleportChunks_.empty());

    EXPECT_LE(stripeLists.size(), 2u);

    CheckEverything(stripeLists, /*chunksRestored*/ true);
}

TEST_F(TUnorderedChunkPoolTest, CompressedDataSizePerJobDoesNotAffectDataWeightPerJob)
{
    // +-------+---------------+
    // |       | #  #  #  #  # | <- 1_GB
    // | Data  | #  #  #  #  # |
    // | Weight| #  #  #  #  # |
    // |       | #  #  #  #  # |
    // +-------+---------------+
    // |       |               |
    // | Compr.|               |
    // | Data  |               |
    // | Size  | #  _  _  _  _ | <- 250_MB
    // +-------+---------------+
    // | Chunk | 1  2  3  4  5 |
    // +-------+---------------+
    //
    // Job count estimation:
    //  - By compressed_data_size ~ ceil(250_MB / 1_MB) = 250
    //  - By data_weight          ~ ceil(5_GB / 6_GB) j = 1
    //
    // Unordered chunk pool does not guarantee sequential ordering of input
    // chunks and may process input chunks in the following orders:
    //  1. [B][SSSS] - 2 jobs
    //  2. [SB][SSS] - 2 jobs
    //  3. [SSB][SS] - 2 jobs
    //  4. [SSSB][S] - 2 jobs
    //  5. [SSSSB]   - 1 job
    //
    // Where:
    //  - "B": compressed_size = 250_MB
    //  - "S": compressed_size -> 0

    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    CompressedDataSizePerJob_ = 1_MB;
    DataWeightPerJob_ = 6_GB;
    InitJobConstraints();

    CreateChunkPool();

    AddChunk(CreateChunk(
        0,
        /*weight*/ 1_GB,
        /*rowCount*/ 5000,
        /*compressedSize*/ 250_MB));
    PersistAndRestore();

    for (int i = 0; i < 4; ++i) {
        AddChunk(CreateChunk(
            0,
            /*weight*/ 1_GB,
            /*rowCount*/ 5000,
            /*compressedSize*/ 100));
        PersistAndRestore();
    }

    ChunkPool_->Finish();

    PersistAndRestore();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_TRUE(TeleportChunks_.empty());

    EXPECT_LE(stripeLists.size(), 2u);

    CheckEverything(stripeLists, /*chunksRestored*/ true);
}

////////////////////////////////////////////////////////////////////////////////

class TUnorderedChunkPoolCountersTest
    : public TUnorderedChunkPoolTest
{
protected:
    THashMap<TInputChunkPtr, IChunkPoolInput::TCookie> FreeChunksToCookie_;

    IChunkPoolOutput::TCookie ExtractCookie()
    {
        auto cookie = TUnorderedChunkPoolTest::ExtractCookie();
        if (cookie == IChunkPoolOutput::NullCookie) {
            return cookie;
        }
        auto stripes = ChunkPool_->GetStripeList(cookie)->Stripes();
        YT_VERIFY(std::ssize(stripes) == 1);
        const auto& dataSlices = stripes.front()->DataSlices();
        for (const auto& dataSlice : dataSlices) {
            FreeChunksToCookie_.erase(dataSlice->GetSingleUnversionedChunk());
        }
        return cookie;
    }

    void CheckJobCounter(const TExpectedCounter& expected)
    {
        CheckCounter(ChunkPool_->GetJobCounter(), expected);
    }

    void CheckDataWeightCounter(const TExpectedCounter& expected)
    {
        CheckCounter(ChunkPool_->GetDataWeightCounter(), expected);
    }

    void AddChunk(i64 weight)
    {
        auto chunk = CreateChunk(
            /*tableIndex*/ 0,
            /*weight*/ weight,
            /*rowCount*/ 1);
        auto cookie = TUnorderedChunkPoolTest::AddChunk(chunk);
        FreeChunksToCookie_.insert(std::pair(std::move(chunk), cookie));
    }
};

TEST_F(TUnorderedChunkPoolCountersTest, BlockedJobCount)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 950_MB;
    InitJobConstraints();

    CreateChunkPool();

    CheckJobCounter({.Total = 0});

    AddChunk(500_MB);
    CheckJobCounter({.Total = 1, .Blocked = 1});

    AddChunk(400_MB);
    CheckJobCounter({.Total = 1, .Blocked = 1});

    TUnorderedChunkPoolTest::ExtractCookie();
    CheckJobCounter({.Total = 1, .Running = 1});

    AddChunk(900_MB);
    CheckJobCounter({.Total = 2, .Blocked = 1, .Running = 1});

    TUnorderedChunkPoolTest::ExtractCookie();
    CheckJobCounter({.Total = 2, .Running = 2});

    AddChunk(900_MB);
    CheckJobCounter({.Total = 3, .Blocked = 1, .Running = 2});

    AddChunk(400_MB);
    CheckJobCounter({.Total = 4, .Pending = 1, .Blocked = 1, .Running = 2});

    AddChunk(400_MB);
    CheckJobCounter({.Total = 4, .Pending = 1, .Blocked = 1, .Running = 2});

    TUnorderedChunkPoolTest::ExtractCookie();
    CheckJobCounter({.Total = 4, .Blocked = 1, .Running = 3});

    ChunkPool_->Finish();
    CheckJobCounter({.Total = 4, .Pending = 1, .Running = 3});

    TUnorderedChunkPoolTest::ExtractCookie();
    CheckJobCounter({.Total = 4, .Running = 4});
}

TEST_F(TUnorderedChunkPoolCountersTest, SuspendedChunks)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 950_MB;
    InitJobConstraints();

    CreateChunkPool();

    CheckJobCounter({.Total = 0});

    AddChunk(1_GB);
    CheckJobCounter({.Total = 2, .Pending = 1, .Blocked = 1});

    for (int i = 0; i < 4; ++i) {
        AddChunk(1_GB);
    }
    CheckJobCounter({.Total = 6, .Pending = 5, .Blocked = 1});

    ChunkPool_->Finish();
    CheckJobCounter({.Total = 6, .Pending = 6});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 6, .Pending = 5, .Running = 1});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 6, .Pending = 4, .Running = 2});

    auto cookie1 = FreeChunksToCookie_.begin()->second;
    ChunkPool_->Suspend(cookie1);
    CheckJobCounter({.Total = 6, .Pending = 2, .Blocked = 1, .Running = 2, .Suspended = 1});

    ChunkPool_->Resume(cookie1);
    CheckJobCounter({.Total = 6, .Pending = 4, .Running = 2});

    ChunkPool_->Suspend(cookie1);

    auto cookie2 = std::next(FreeChunksToCookie_.begin())->second;
    ChunkPool_->Suspend(cookie2);
    CheckJobCounter({.Total = 6, .Pending = 1, .Blocked = 1, .Running = 2, .Suspended = 2});

    auto cookie3 = std::next(std::next(FreeChunksToCookie_.begin()))->second;
    ChunkPool_->Suspend(cookie3);
    CheckJobCounter({.Total = 6, .Running = 2, .Suspended = 4});

    ASSERT_EQ(ExtractCookie(), IChunkPoolOutput::NullCookie);

    ChunkPool_->Resume(cookie3);

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 6, .Running = 3, .Suspended = 3});

    ASSERT_EQ(ExtractCookie(), IChunkPoolOutput::NullCookie);

    ChunkPool_->Resume(cookie2);
    CheckJobCounter({.Total = 6, .Pending = 1, .Blocked = 1, .Running = 3, .Suspended = 1});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 6, .Running = 4, .Suspended = 2});

    ChunkPool_->Resume(cookie1);
    CheckJobCounter({.Total = 6, .Pending = 2, .Running = 4});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 5, .Running = 5});

    auto stripeLists = GetAllStripeLists();

    EXPECT_EQ(std::ssize(stripeLists), 5);

    CheckEverything(stripeLists);
}

TEST_F(TUnorderedChunkPoolCountersTest, CountersWithSuspendedChunksExplicitJobCount)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 950_MB;
    IsExplicitJobCount_ = true;
    JobCount_ = 5;
    InitJobConstraints();

    CreateChunkPool();

    for (int i = 0; i < 5; ++i) {
        AddChunk(1_GB);
    }

    CheckJobCounter({.Total = 5, .Pending = 4, .Suspended = 1});

    ChunkPool_->Finish();
    CheckJobCounter({.Total = 5, .Pending = 5});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 5, .Pending = 4, .Running = 1});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 5, .Pending = 3, .Running = 2});

    auto cookie1 = FreeChunksToCookie_.begin()->second;
    ChunkPool_->Suspend(cookie1);
    CheckJobCounter({.Total = 5, .Pending = 2, .Running = 2, .Suspended = 1});

    ChunkPool_->Resume(cookie1);
    CheckJobCounter({.Total = 5, .Pending = 3, .Running = 2});

    ChunkPool_->Suspend(cookie1);

    auto cookie2 = std::next(FreeChunksToCookie_.begin())->second;
    ChunkPool_->Suspend(cookie2);
    CheckJobCounter({.Total = 5, .Pending = 1, .Running = 2, .Suspended = 2});

    auto cookie3 = std::next(std::next(FreeChunksToCookie_.begin()))->second;
    ChunkPool_->Suspend(cookie3);
    CheckJobCounter({.Total = 5, .Running = 2, .Suspended = 3});

    ASSERT_EQ(ExtractCookie(), IChunkPoolOutput::NullCookie);

    ChunkPool_->Resume(cookie3);

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 5, .Running = 3, .Suspended = 2});

    ASSERT_EQ(ExtractCookie(), IChunkPoolOutput::NullCookie);

    ChunkPool_->Resume(cookie2);
    CheckJobCounter({.Total = 5, .Pending = 1, .Running = 3, .Suspended = 1});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 5, .Running = 4, .Suspended = 1});

    ASSERT_EQ(ExtractCookie(), IChunkPoolOutput::NullCookie);

    ChunkPool_->Resume(cookie1);
    CheckJobCounter({.Total = 5, .Pending = 1, .Running = 4});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 5, .Running = 5});

    ASSERT_EQ(ExtractCookie(), IChunkPoolOutput::NullCookie);

    auto stripeLists = GetAllStripeLists();

    EXPECT_EQ(std::ssize(stripeLists), 5);

    CheckEverything(stripeLists);
}

TEST_PI(
    TUnorderedChunkPoolCountersTest,
    CheckJobCountWhenJobsAreLimitedByMaxDataSlices,
    ValuesIn({
        std::pair(0, 0),
        std::pair(1, 1),
        std::pair(9, 1),
        std::pair(10, 1),
        std::pair(11, 2),
        std::pair(15, 2),
        std::pair(19, 2),
        std::pair(20, 2),
        std::pair(21, 3),
        std::pair(90, 9),
        std::pair(91, 10),
        std::pair(100, 10),
        std::pair(101, 11),
    }),
    std::pair<int, int>)
{
    auto [sliceCount, expectedJobCount] = GetParam();

    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 1_GB;
    MaxDataSlicesPerJob_ = 10;

    InitJobConstraints();

    CreateChunkPool();

    CheckJobCounter({.Total = 0});

    for (int i = 0; i < sliceCount; ++i) {
        AddChunk(1_MB);

        int jobCount = DivCeil(i + 1, MaxDataSlicesPerJob_);

        CheckJobCounter({
            .Total = jobCount,
            .Pending = jobCount - 1,
            .Blocked = 1,
        });
    }

    ChunkPool_->Finish();
    CheckJobCounter({.Total = expectedJobCount, .Pending = expectedJobCount});

    for (int i = 0; i < expectedJobCount; ++i) {
        ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
        CheckJobCounter({
            .Total = expectedJobCount,
            .Pending = expectedJobCount - i - 1,
            .Running = i + 1,
        });
    }

    ASSERT_EQ(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = expectedJobCount, .Running = expectedJobCount});

    auto stripeLists = GetAllStripeLists();
    EXPECT_EQ(std::ssize(stripeLists), expectedJobCount);
    CheckEverything(stripeLists);
}

TEST_F(TUnorderedChunkPoolCountersTest, ExtractedJobsThatLimitedByMaxDataSlicesFromUnfinishedPool)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 1_GB;
    MaxDataSlicesPerJob_ = 2;

    InitJobConstraints();

    CreateChunkPool();

    CheckJobCounter({.Total = 0});
    for (int i = 0; i < MaxDataSlicesPerJob_; ++i) {
        AddChunk(1_MB);
        CheckJobCounter({.Total = 1, .Blocked = 1});
    }
    AddChunk(1_MB);

    CheckJobCounter({.Total = 2, .Pending = 1, .Blocked = 1});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 2, .Blocked = 1, .Running = 1});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 2, .Running = 2});

    AddChunk(1_MB);
    CheckJobCounter({.Total = 3, .Blocked = 1, .Running = 2});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 3, .Running = 3});

    AddChunk(1_MB);
    CheckJobCounter({.Total = 4, .Blocked = 1, .Running = 3});

    AddChunk(1_MB);
    CheckJobCounter({.Total = 4, .Blocked = 1, .Running = 3});

    ChunkPool_->Finish();
    CheckJobCounter({.Total = 4, .Pending = 1, .Running = 3});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 4, .Running = 4});

    auto stripeLists = GetAllStripeLists();
    CheckEverything(stripeLists);
}

TEST_F(TUnorderedChunkPoolCountersTest, SingleRunningJobCounterRemainsAfterFinishingPool)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 1_GB;
    MaxDataSlicesPerJob_ = 2;

    InitJobConstraints();

    CreateChunkPool();

    AddChunk(1_MB);
    CheckJobCounter({.Total = 1, .Blocked = 1});

    ASSERT_NE(ExtractCookie(), IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 1, .Running = 1});

    ChunkPool_->Finish();
    CheckJobCounter({.Total = 1, .Running = 1});

    auto stripeLists = GetAllStripeLists();
    CheckEverything(stripeLists);
}

TEST_F(TUnorderedChunkPoolCountersTest, JobCounterWithLostJob)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 1_GB;
    InitJobConstraints();

    CreateChunkPool();

    CheckJobCounter({.Total = 0});

    AddChunk(500_MB);

    ChunkPool_->Finish();
    CheckJobCounter({.Total = 1, .Pending = 1});
    CheckDataWeightCounter({.Total = 500_MB, .Pending = 500_MB});

    auto cookie = ExtractCookie();
    ASSERT_NE(cookie, IChunkPoolOutput::NullCookie);

    TCompletedJobSummary summary{};
    ChunkPool_->Completed(cookie, summary);

    ASSERT_TRUE(ChunkPool_->IsCompleted());
    CheckJobCounter({.Total = 1, .Completed = 1});
    CheckDataWeightCounter({.Total = 500_MB, .Completed = 500_MB});

    ChunkPool_->Lost(cookie);
    ASSERT_FALSE(ChunkPool_->IsCompleted());
    CheckJobCounter({.Total = 1, .Pending = 1, .Lost = 1});
    CheckDataWeightCounter({.Total = 500_MB, .Pending = 500_MB, .Lost = 500_MB});

    cookie = ExtractCookie();
    ASSERT_NE(cookie, IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 1, .Running = 1, .Lost = 1});
    CheckDataWeightCounter({.Total = 500_MB, .Running = 500_MB, .Lost = 500_MB});

    ChunkPool_->Completed(cookie, summary);
    ASSERT_TRUE(ChunkPool_->IsCompleted());
    CheckJobCounter({.Total = 1, .Completed = 1, .Lost = 1});
    CheckDataWeightCounter({.Total = 500_MB, .Completed = 500_MB, .Lost = 500_MB});
}

TEST_F(TUnorderedChunkPoolCountersTest, FailedAndAbortedJobs)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 400_MB;
    InitJobConstraints();

    CreateChunkPool();

    CheckJobCounter({.Total = 0});
    CheckDataWeightCounter({.Total = 0_MB});

    AddChunk(500_MB);
    CheckJobCounter({.Total = 2, .Pending = 1, .Blocked = 1});
    CheckDataWeightCounter({.Total = 500_MB, .Pending = 500_MB});

    AddChunk(600_MB);
    CheckJobCounter({.Total = 3, .Pending = 2, .Blocked = 1});
    CheckDataWeightCounter({.Total = 1100_MB, .Pending = 1100_MB});

    ChunkPool_->Finish();
    CheckJobCounter({.Total = 3, .Pending = 3});
    CheckDataWeightCounter({.Total = 1100_MB, .Pending = 1100_MB});

    auto cookie1 = ExtractCookie();
    ASSERT_NE(cookie1, IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 3, .Pending = 2, .Running = 1});
    CheckDataWeightCounter({.Total = 1100_MB, .Pending = 600_MB, .Running = 500_MB});

    auto cookie2 = ExtractCookie();
    ASSERT_NE(cookie2, IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 2, .Running = 2});
    CheckDataWeightCounter({.Total = 1100_MB, .Running = 1100_MB});

    ChunkPool_->Failed(cookie1);
    ASSERT_FALSE(ChunkPool_->IsCompleted());
    CheckJobCounter({.Total = 2, .Pending = 1, .Running = 1, .Failed = 1});
    CheckDataWeightCounter({.Total = 1100_MB, .Pending = 500_MB, .Running = 600_MB, .Failed = 500_MB});

    ChunkPool_->Aborted(cookie2, EAbortReason::Scheduler);
    ASSERT_FALSE(ChunkPool_->IsCompleted());
    CheckJobCounter({.Total = 2, .Pending = 2, .Failed = 1, .Aborted = 1});
    CheckDataWeightCounter({.Total = 1100_MB, .Pending = 1100_MB, .Failed = 500_MB, .Aborted = 600_MB});

    auto cookie3 = ExtractCookie();
    ASSERT_NE(cookie3, IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 2, .Pending = 1, .Running = 1, .Failed = 1, .Aborted = 1});
    CheckDataWeightCounter({.Total = 1100_MB, .Pending = 500_MB, .Running = 600_MB, .Failed = 500_MB, .Aborted = 600_MB});

    auto cookie4 = ExtractCookie();
    ASSERT_NE(cookie4, IChunkPoolOutput::NullCookie);
    CheckJobCounter({.Total = 2, .Running = 2, .Failed = 1, .Aborted = 1});
    CheckDataWeightCounter({.Total = 1100_MB, .Running = 1100_MB, .Failed = 500_MB, .Aborted = 600_MB});

    TCompletedJobSummary summary{};
    ChunkPool_->Completed(cookie3, summary);
    ASSERT_FALSE(ChunkPool_->IsCompleted());
    CheckJobCounter({.Total = 2, .Running = 1, .Completed = 1, .Failed = 1, .Aborted = 1});
    CheckDataWeightCounter({.Total = 1100_MB, .Running = 500_MB, .Completed = 600_MB, .Failed = 500_MB, .Aborted = 600_MB});

    ChunkPool_->Completed(cookie4, summary);
    ASSERT_TRUE(ChunkPool_->IsCompleted());
    CheckJobCounter({.Total = 2, .Completed = 2, .Failed = 1, .Aborted = 1});
    CheckDataWeightCounter({.Total = 1100_MB, .Completed = 1100_MB, .Failed = 500_MB, .Aborted = 600_MB});

    auto stripeLists = GetAllStripeLists();
    EXPECT_EQ(std::ssize(stripeLists), 2);
    CheckEverything(stripeLists);
}

////////////////////////////////////////////////////////////////////////////////

class TUnorderedChunkPoolTestRandomized
    : public WithParamInterface<int>
    , public TUnorderedChunkPoolTest
{
public:
    void SetUp() final
    {
        TUnorderedChunkPoolTest::SetUp();
        Gen_.seed(GetParam());
    }
};

TEST_P(TUnorderedChunkPoolTestRandomized, VariousOperationsWithPoolTest)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    auto generateSize = [&] () -> i64 {
        auto baseSizes = std::to_array({1_KB, 1_MB, 100_MB, 1_GB, 512_GB, 1_TB, 10_TB});
        i64 baseSize = baseSizes[std::uniform_int_distribution<i64>(0, std::ssize(baseSizes) - 1)(Gen_)];
        return std::lognormal_distribution<double>()(Gen_) * baseSize;
    };

    auto generateRowCount = [&] () -> i64 {
        auto baseCounts = std::to_array<i64>({100, 1'000, 1'000'000, 1'000'000'000, 1'000'000'000'000});
        i64 baseCount = baseCounts[std::uniform_int_distribution<i64>(0, std::ssize(baseCounts) - 1)(Gen_)];
        return std::max<i64>(std::lognormal_distribution<double>()(Gen_) * baseCount, 1);
    };

    auto generateChunkCount = [&] () -> i64 {
        auto baseCounts = std::to_array<i64>({1, 5, 10, 30});
        i64 baseCount = baseCounts[std::uniform_int_distribution<i64>(0, std::ssize(baseCounts) - 1)(Gen_)];
        return std::min(std::max<i64>(std::lognormal_distribution<double>()(Gen_) * baseCount, 1), 30l);
    };

    constexpr int approximateMaxJobCount = 20;
    constexpr int approximateMaxSlicesCount = 30;

    int chunkCount = generateChunkCount();
    if (std::uniform_int_distribution(0, 10)(Gen_) == 0) {
        chunkCount = 0;
    }
    Cdebug << Format("Chunk count = %v...", chunkCount) << Endl;

    int maxJobLosts = std::uniform_int_distribution(0, chunkCount)(Gen_);
    i64 totalDataWeight = 0;
    i64 totalCompressedDataSize = 0;
    i64 totalRowCount = 0;
    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(
            /*tableIndex*/ 0,
            /*dataWeight*/ generateSize(),
            /*tableIndex*/ generateRowCount());
        totalDataWeight += chunk->GetDataWeight();
        totalCompressedDataSize += chunk->GetCompressedDataSize();
        totalRowCount += chunk->GetRowCount();
    }

    // Don't build too many jobs.
    auto normalizeValue = [&] (i64 value, i64 nominator, i64 maxValue) {
        while (nominator / value > maxValue) {
            value *= 10;
            value += std::uniform_int_distribution<int>(0, 9)(Gen_);
        }
        return value;
    };

    DataWeightPerJob_ = normalizeValue(generateSize(), totalDataWeight, approximateMaxJobCount);
    CompressedDataSizePerJob_ = normalizeValue(generateSize(), totalCompressedDataSize, approximateMaxJobCount);
    MaxCompressedDataSizePerJob_ = normalizeValue(generateSize(), totalCompressedDataSize, approximateMaxJobCount);
    InputSliceRowCount_ = normalizeValue(generateRowCount(), totalRowCount, approximateMaxSlicesCount);
    InputSliceDataWeight_ = normalizeValue(generateSize(), totalDataWeight, approximateMaxSlicesCount);

    IsExplicitJobCount_ = std::uniform_int_distribution<int>(0, 1)(Gen_);
    if (IsExplicitJobCount_ && std::uniform_int_distribution<int>(0, 1)(Gen_) || chunkCount == 0) {
        JobCount_ = chunkCount;
    } else {
        JobCount_ = std::uniform_int_distribution(1, 3 * chunkCount)(Gen_);
    }

    Options_.BuildFirstJobOnFinishedInput = std::bernoulli_distribution(0.3)(Gen_);

    InitJobConstraints();

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
    THashMap<IChunkPoolOutput::TCookie, THashSet<TChunkId>> outputCookiesToChunkIds;
    THashSet<IChunkPoolOutput::TCookie> pendingCookies;
    THashSet<IChunkPoolOutput::TCookie> startedCookies;
    THashSet<IChunkPoolOutput::TCookie> completedCookies;
    THashSet<IChunkPoolOutput::TCookie> lostCookies;
    THashMap<TChunkId, TInputChunkPtr> chunkIdToChunk;

    for (const auto& chunk : CreatedUnversionedChunks_) {
        auto chunkId = chunk->GetChunkId();
        chunkIdToInputCookie[chunkId] = AddChunk(chunk);
        chunkIdToChunk[chunkId] = chunk;
        resumedChunks.insert(chunkId);
    }

    ChunkPool_->Finish();

    i64 estimatedJobCount = ChunkPool_->GetJobCounter()->GetTotal();
    Cdebug << Format("Estimated job count = %v...", estimatedJobCount) << Endl;

    if (IsExplicitJobCount_) {
        i64 pendingJobCount = ChunkPool_->GetJobCounter()->GetPending();
        if (!Options_.BuildFirstJobOnFinishedInput || JobCount_ <= chunkCount) {
            ASSERT_EQ(pendingJobCount, JobCount_);
        } else {
            ASSERT_LE(pendingJobCount, JobCount_);
        }
    }

    int jobLosts = 0;

    while (!ChunkPool_->IsCompleted()) {
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

                ChunkPool_->GetJobCounter()->GetSuspended();
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
            if (ChunkPool_->GetJobCounter()->GetPending() + ChunkPool_->GetJobCounter()->GetBlocked()) {
                auto outputCookie = ExtractCookie(TNodeId(0));
                Cdebug << Format("Extracted cookie %v...", outputCookie) << Endl;
                // TODO(max42): why the following line leads to the linkage error?
                // ASSERT_NE(outputCookie, IChunkPoolOutput::NullCookie);
                // error: undefined reference to 'NYT::NScheduler::IChunkPoolOutput::NullCookie'
                auto stripeList = ChunkPool_->GetStripeList(outputCookie);
                ASSERT_EQ(std::ssize(stripeList->Stripes()), 1);
                const auto& stripe = stripeList->Stripes()[0];
                ASSERT_TRUE(stripe);

                THashSet<TChunkId> inputChunks;
                for (const auto& dataSlice : stripe->DataSlices()) {
                    const auto& chunk = dataSlice->GetSingleUnversionedChunk();
                    inputChunks.insert(chunk->GetChunkId());
                }

                Cdebug << Format(" that corresponds to chunks %v", inputChunks) << Endl;

                if (outputCookiesToChunkIds.contains(outputCookie)) {
                    ASSERT_EQ(outputCookiesToChunkIds[outputCookie], inputChunks);
                } else {
                    outputCookiesToChunkIds[outputCookie] = inputChunks;
                }

                for (const auto& chunkId : inputChunks) {
                    ASSERT_TRUE(resumedChunks.contains(chunkId));
                    ASSERT_TRUE(!suspendedChunks.contains(chunkId));
                }
                if (lostCookies.contains(outputCookie)) {
                    ASSERT_TRUE(lostCookies.erase(outputCookie));
                } else {
                    pendingCookies.erase(outputCookie);
                }
                ASSERT_TRUE(startedCookies.insert(outputCookie).second);
            }
        } else if (eventType <= 69) {
            if (auto randomElement = chooseRandomElement(startedCookies)) {
                auto outputCookie = *randomElement;
                Cdebug << Format("Completed job with output cookie %v", outputCookie) << Endl;
                ASSERT_TRUE(startedCookies.erase(outputCookie));
                ASSERT_TRUE(completedCookies.insert(outputCookie).second);
                ChunkPool_->Completed(outputCookie, TCompletedJobSummary());
            }
        } else if (eventType <= 79) {
            if (auto randomElement = chooseRandomElement(startedCookies)) {
                auto outputCookie = *randomElement;
                Cdebug << Format("Aborted job with output cookie %v", outputCookie) << Endl;
                ASSERT_TRUE(startedCookies.erase(outputCookie));
                ASSERT_TRUE(pendingCookies.insert(outputCookie).second);
                ChunkPool_->Aborted(outputCookie, EAbortReason::Unknown);
            }
        } else if (eventType <= 89) {
            if (jobLosts >= maxJobLosts) {
                continue;
            }
            if (auto randomElement = chooseRandomElement(completedCookies)) {
                auto outputCookie = *randomElement;
                Cdebug << Format("Lost job with output cookie %v", outputCookie) << Endl;
                ASSERT_TRUE(completedCookies.erase(outputCookie));
                ASSERT_TRUE(lostCookies.insert(outputCookie).second);
                ChunkPool_->Lost(outputCookie);
                ++jobLosts;
            }
        } else { // if (eventType <= 99)
            if (auto randomElement = chooseRandomElement(startedCookies)) {
                auto chunkId = *randomElement;
                auto outputCookie = *randomElement;
                Cdebug << Format("Failed job with output cookie %v", chunkId) << Endl;
                ASSERT_TRUE(startedCookies.erase(outputCookie));
                ASSERT_TRUE(pendingCookies.insert(outputCookie).second);
                ChunkPool_->Failed(outputCookie);
            }
        }
    }
    ASSERT_TRUE(ChunkPool_->IsCompleted());
    ASSERT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 0);
    if (IsExplicitJobCount_) {
        if (JobCount_ <= chunkCount) {
            ASSERT_EQ(std::ssize(completedCookies), JobCount_);
        } else {
            ASSERT_LE(std::ssize(completedCookies), JobCount_);
        }
    }
    ASSERT_EQ(std::ssize(pendingCookies), 0);
    ASSERT_EQ(std::ssize(startedCookies), 0);
    ASSERT_EQ(std::ssize(lostCookies), 0);
    ASSERT_EQ(std::ssize(resumedChunks) + std::ssize(suspendedChunks), chunkCount);

    i64 actualTotalRowCount = 0;
    for (auto outputCookie : completedCookies) {
        auto stripeList = ChunkPool_->GetStripeList(outputCookie);
        actualTotalRowCount += stripeList->GetAggregateStatistics().RowCount;
    }
    ASSERT_EQ(actualTotalRowCount, totalRowCount);

    i64 actualJobCount = ChunkPool_->GetJobCounter()->GetTotal();
    if (Options_.BuildFirstJobOnFinishedInput) {
        if (estimatedJobCount == 0) {
            EXPECT_EQ(actualJobCount, 0);
        } else if (estimatedJobCount == 1) {
            EXPECT_EQ(actualJobCount, 1);
        } else {
            EXPECT_GT(actualJobCount, 1);
        }
    }
}

static constexpr int NumberOfRepeats = 500;

INSTANTIATE_TEST_SUITE_P(VariousOperationsWithPoolInstantiation,
    TUnorderedChunkPoolTestRandomized,
    Range(0, NumberOfRepeats));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkPools

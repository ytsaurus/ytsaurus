#include "chunk_pools_helpers.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/controller_agent/helpers.h>
#include <yt/yt/server/controller_agent/job_size_constraints.h>
#include <yt/yt/server/controller_agent/operation_controller.h>

#include <yt/yt/server/lib/chunk_pools/unittests/chunk_pools_helpers.h>

#include <yt/yt/server/lib/chunk_pools/ordered_chunk_pool.h>

#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/ytlib/chunk_pools/output_order.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/blob_output.h>

#include <library/cpp/iterator/zip.h>

#include <util/stream/null.h>

#include <random>

namespace NYT::NChunkPools {
namespace {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;

using NControllerAgent::TCompletedJobSummary;

using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

//! A unit to measure all sizes in this file.
static constexpr i32 Inf32 = std::numeric_limits<i32>::max();
static constexpr i64 Inf64 = std::numeric_limits<i64>::max();

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkPoolTestBase
    : public Test
{
protected:
    void SetUp() override
    {
        Options_.MinTeleportChunkSize = Inf64;
        Options_.MaxTotalSliceCount = Inf64;
        Options_.ShouldSliceByRowIndices = true;
        Options_.Logger = GetTestLogger();
        DataWeightPerJob_ = Inf64;
        MaxDataSlicesPerJob_ = Inf32;
        InputSliceDataWeight_ = Inf64;
        InputSliceRowCount_ = Inf64;
        MaxCompressedDataSizePerJob_ = Inf64;
    }

    void InitJobConstraints()
    {
        Options_.JobSizeConstraints = CreateExplicitJobSizeConstraints(
            /*canAdjustDataSizePerJob*/ false,
            /*isExplicitJobCount*/ static_cast<bool>(ExplicitJobCount_),
            /*jobCount*/ ExplicitJobCount_.value_or(0),
            DataWeightPerJob_,
            Inf64,
            MaxDataSlicesPerJob_,
            /*maxDataSizePerJob_*/ 0,
            /*maxPrimaryDataWeightPerJob*/ 0,
            /*maxCompressedDataSizePerJob*/ MaxCompressedDataSizePerJob_,
            InputSliceDataWeight_,
            InputSliceRowCount_,
            BatchRowCount_,
            /*foreignSliceDataWeight*/ 0,
            SamplingRate_);
    }

    TInputChunkPtr CreateChunk(
        int tableIndex,
        i64 dataWeight = 1_KB,
        i64 rowCount = 1000,
        std::optional<i64> compressedDataSize = std::nullopt)
    {
        if (!compressedDataSize.has_value()) {
            compressedDataSize = dataWeight;
        }
        auto inputChunk = New<TInputChunk>();
        inputChunk->SetChunkId(MakeRandomId(EObjectType::Chunk, TCellTag(0x42)));
        inputChunk->SetCompressedDataSize(*compressedDataSize);
        inputChunk->SetTotalUncompressedDataSize(dataWeight);
        inputChunk->SetTotalDataWeight(dataWeight);
        inputChunk->SetTableIndex(tableIndex);
        inputChunk->SetTableRowIndex(UnversionedTableRowCounts_[tableIndex]);
        UnversionedTableRowCounts_[tableIndex] += rowCount;
        if (!InputTables_[tableIndex].IsVersioned()) {
            CreatedUnversionedPrimaryChunks_.insert(inputChunk);
        }
        inputChunk->SetTotalRowCount(rowCount);
        return inputChunk;
    }

    void InitTables(std::vector<bool> isTeleportable, std::vector<bool> isVersioned)
    {
        YT_VERIFY(isTeleportable.size() == isVersioned.size() && isVersioned.size() > 0);
        for (int index = 0; index < std::ssize(isVersioned); ++index) {
            InputTables_.emplace_back(isTeleportable[index], /*isPrimary*/ true, isVersioned[index]);
        }
        UnversionedTableRowCounts_.resize(InputTables_.size(), 0);
        OriginalChunks_.resize(InputTables_.size());
    }

    void CreateChunkPool(bool useGenericInputStreamDirectory = false)
    {
        ChunkPool_ = CreateOrderedChunkPool(
            Options_,
            useGenericInputStreamDirectory ? IntermediateInputStreamDirectory : TInputStreamDirectory(InputTables_));
        ChunkPool_->SubscribeChunkTeleported(
            BIND([this] (TInputChunkPtr teleportChunk, std::any /*tag*/ ) {
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
        MaxChunkRowDataWeight_ = std::max(MaxChunkRowDataWeight_, DivCeil(chunk->GetDataWeight(), chunk->GetRowCount()));

        auto dataSlice = BuildDataSliceByChunk(chunk);
        ActiveChunks_.insert(chunk->GetChunkId());
        YT_VERIFY(chunk->GetTableIndex() < std::ssize(OriginalChunks_) );
        OriginalChunks_[chunk->GetTableIndex()].push_back(chunk->GetChunkId());
        return ChunkPool_->Add(New<TChunkStripe>(dataSlice));
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

    std::vector<TChunkStripeListPtr> GetAllStripeListsByOutputOrder()
    {
        auto outputOrder = ChunkPool_->GetOutputOrder();
        YT_VERIFY(outputOrder);
        std::vector<TChunkStripeListPtr> stripeLists;
        for (const auto& entry : outputOrder->ToEntryVector()) {
            if (entry.IsTeleportChunk()) {
                continue;
            }
            stripeLists.push_back(ChunkPool_->GetStripeList(entry.GetCookie()));
        }
        return stripeLists;
    }

    //! Perform all the correctness checks over the given result of ordered chunk pool invocation
    //! (without any suspends nor job interruptions).
    void CheckEverything(
        const std::vector<TChunkStripeListPtr>& stripeLists)
    {
        CheckStripeListsContainOnlyActiveChunks();
        CheckSlicesFollowInOriginalOrder(stripeLists);
    }

    void CheckStripeListsContainOnlyActiveChunks()
    {
        for (auto cookie : OutputCookies_) {
            auto stripeList = ChunkPool_->GetStripeList(cookie);
            for (const auto& stripe : stripeList->Stripes) {
                YT_VERIFY(stripe);
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

    void CheckSlicesFollowInOriginalOrder(const std::vector<TChunkStripeListPtr>& stripeLists)
    {
        std::vector<int> chunkIndices(InputTables_.size());
        for (const auto& stripeList : stripeLists) {
            for (const auto& stripe : stripeList->Stripes) {
                ASSERT_LE(stripe->GetTableIndex(), std::ssize(OriginalChunks_));
                const auto& chunks = OriginalChunks_[stripe->GetTableIndex()];
                int& chunkIndex = chunkIndices[stripe->GetTableIndex()];
                for (const auto& dataSlice : stripe->DataSlices) {
                    if (dataSlice->Type != EDataSourceType::UnversionedTable) {
                        continue;
                    }

                    while (
                        chunkIndex < std::ssize(chunks) &&
                        dataSlice->GetSingleUnversionedChunk()->GetChunkId() != chunks[chunkIndex])
                    {
                        ++chunkIndex;
                    }
                    EXPECT_NE(chunkIndex, std::ssize(chunks));
                }
            }
        }
    }

    void PrintEntry(const TOutputOrder::TEntry entry)
    {
        if (entry.IsTeleportChunk()) {
            Cerr << "T " << ToString(entry.GetTeleportChunk()->GetChunkId()) << Endl;
        } else {
            Cerr << "C ";
            auto stripeList = ChunkPool_->GetStripeList(entry.IsCookie());
            for (const auto& dataSlice : stripeList->Stripes[0]->DataSlices) {
                Cerr << ToString(dataSlice->GetSingleUnversionedChunk()->GetChunkId()) << " ";
            }
            Cerr << Endl;
        }
    }

    void SplitJob(IChunkPoolOutput::TCookie cookie, int splitJobCount)
    {
        ChunkPool_->Completed(cookie, SummaryWithSplitJobCount(ChunkPool_->GetStripeList(cookie), splitJobCount));
    }

    static void ExpectEntryIsTeleportChunk(const TOutputOrder::TEntry& entry, TInputChunkPtr chunk)
    {
        EXPECT_TRUE(entry.IsTeleportChunk());
        EXPECT_EQ(entry.GetTeleportChunk()->GetChunkId(), chunk->GetChunkId());
    }

    void ExpectEntryIsCookie(const TOutputOrder::TEntry& entry, std::vector<TInputChunkPtr> chunks)
    {
        EXPECT_TRUE(entry.IsCookie());
        auto stripeList = ChunkPool_->GetStripeList(entry.GetCookie());
        EXPECT_EQ(stripeList->Stripes.size(), 1u);
        EXPECT_EQ(stripeList->Stripes[0]->DataSlices.size(), chunks.size());
        for (int index = 0; index < std::ssize(stripeList->Stripes[0]->DataSlices); ++index) {
            EXPECT_EQ(stripeList->Stripes[0]->DataSlices[index]->GetSingleUnversionedChunk()->GetChunkId(), chunks[index]->GetChunkId());
        }
    }

    void CheckEntryForBatchRowCountAcceptability(const TOutputOrder::TEntry& entry)
    {
        EXPECT_TRUE(entry.IsCookie());

        auto stripeList = ChunkPool_->GetStripeList(entry.GetCookie());
        EXPECT_TRUE(stripeList->TotalRowCount % *BatchRowCount_ == 0);
        EXPECT_LE(std::abs(stripeList->TotalDataWeight - DataWeightPerJob_), *BatchRowCount_ * MaxChunkRowDataWeight_);
    }

    void CheckBatchRowCount()
    {
        ASSERT_TRUE(Options_.BuildOutputOrder);

        auto order = ChunkPool_->GetOutputOrder();
        ASSERT_TRUE(order);

        auto entries = order->ToEntryVector();
        for (int index = 0; index + 1 < std::ssize(entries); ++index) {
            CheckEntryForBatchRowCountAcceptability(entries[index]);
        }
    }

    void CheckExplicitRowCounts(std::vector<i64> rowCounts)
    {
        ASSERT_TRUE(Options_.BuildOutputOrder);

        auto order = ChunkPool_->GetOutputOrder();
        ASSERT_TRUE(order);

        auto entries = order->ToEntryVector();
        EXPECT_EQ(std::ssize(entries), std::ssize(rowCounts));

        for (const auto& [entry, rowCount] : Zip(entries, rowCounts)) {
            if (entry.IsTeleportChunk()) {
                EXPECT_EQ(entry.GetTeleportChunk()->GetRowCount(), rowCount);
            } else {
                EXPECT_TRUE(entry.IsCookie());
                EXPECT_EQ(ChunkPool_->GetStripeList(entry.GetCookie())->TotalRowCount, rowCount);
            }
        }
    }

    std::vector<std::vector<TChunkId>> OriginalChunks_;

    IPersistentChunkPoolPtr ChunkPool_;

    //! Set containing all unversioned primary input chunks that have ever been created.
    THashSet<TInputChunkPtr> CreatedUnversionedPrimaryChunks_;
    //! Set containing all chunks that are added to the pool without being suspended.
    THashSet<TChunkId> ActiveChunks_;

    i64 MaxChunkRowDataWeight_ = 0;

    std::vector<TInputStreamDescriptor> InputTables_;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    THashSet<IChunkPoolOutput::TCookie> OutputCookies_;

    std::vector<int> UnversionedTableRowCounts_;

    TOrderedChunkPoolOptions Options_;

    i64 DataWeightPerJob_;

    i32 MaxDataSlicesPerJob_;

    i64 MaxCompressedDataSizePerJob_;

    i64 InputSliceDataWeight_;

    i64 InputSliceRowCount_;

    std::optional<i64> BatchRowCount_;

    std::optional<i32> ExplicitJobCount_;

    std::optional<double> SamplingRate_;

    std::mt19937 Gen_;

    std::vector<TInputChunkPtr> TeleportChunks_;
};

// COMPAT(apollo1321): Remove in 25.2 release.
class TOrderedChunkPoolTest
    : public TOrderedChunkPoolTestBase
    , public WithParamInterface<bool>
{
    void SetUp() override
    {
        TOrderedChunkPoolTestBase::SetUp();
        Options_.UseNewSlicingImplementation = GetParam();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, OrderedMergeSimple)
{
    InitTables(
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, false, false});

    DataWeightPerJob_ = 2_KB;

    InitJobConstraints();

    auto chunkA1 = CreateChunk(0);
    auto chunkA2 = CreateChunk(0);
    auto chunkB = CreateChunk(1);
    auto chunkC = CreateChunk(2);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(2u, stripeLists.size());

    CheckEverything(stripeLists);
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, LargeChunksPreciseSlicing)
{
    InitTables(
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, false, false});

    DataWeightPerJob_ = 2_KB;

    InitJobConstraints();

    auto chunkA1 = CreateChunk(0, 10_KB);
    auto chunkA2 = CreateChunk(0, 22_KB);
    auto chunkB = CreateChunk(1, 23_KB);
    auto chunkC = CreateChunk(2, 3_KB);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(29u, stripeLists.size());

    CheckEverything(stripeLists);
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, BatchRowCountBasic)
{
    InitTables(
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, false, false});

    Options_.BuildOutputOrder = true;
    // This should have no effect!
    Options_.MinTeleportChunkSize = 2_KB;

    // Nor this!
    SamplingRate_ = 0.3;
    BatchRowCount_ = 42;
    DataWeightPerJob_ = 2_KB;

    InitJobConstraints();

    auto chunkA1 = CreateChunk(0, 10_KB, 1234);
    auto chunkA2 = CreateChunk(0, 22_KB, 2435);
    auto chunkB = CreateChunk(1, 23_KB, 3434);
    auto chunkC = CreateChunk(2, 3_KB, 333);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    CheckBatchRowCount();
    CheckEverything(GetAllStripeLists());
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, BatchRowCountDoesNotFailWithVersionedChunks)
{
    InitTables(
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, true, false});

    Options_.BuildOutputOrder = true;
    // This should have no effect!
    Options_.MinTeleportChunkSize = 2_KB;
    // Nor this!
    SamplingRate_ = 0.3;
    BatchRowCount_ = 42;
    DataWeightPerJob_ = 2_KB;

    InitJobConstraints();

    auto chunkA1 = CreateChunk(0, 10_KB, 1234);
    auto chunkA2 = CreateChunk(0, 22_KB, 2435);
    auto chunkB = CreateChunk(1, 23_KB, 3434);
    auto chunkC = CreateChunk(2, 3_KB, 333);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    CheckEverything(GetAllStripeLists());
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, BatchRowCountBigBatchesSmallDataSizePerJob)
{
    InitTables(
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, false, false});

    Options_.BuildOutputOrder = true;
    // This should have no effect!
    Options_.MinTeleportChunkSize = 2_KB;
    BatchRowCount_ = 20;
    DataWeightPerJob_ = 2_KB;

    InitJobConstraints();

    auto chunkA1 = CreateChunk(0, 10_KB, 10);
    auto chunkA2 = CreateChunk(0, 30_KB, 5);
    auto chunkB = CreateChunk(1, 60_KB, 30);
    auto chunkC = CreateChunk(2, 3_KB, 6);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    CheckBatchRowCount();

    auto stripeLists = GetAllStripeLists();
    EXPECT_EQ(std::ssize(stripeLists), 3);
    CheckExplicitRowCounts({20, 20, 11});

    CheckEverything(GetAllStripeLists());
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, OrderedMergeOrderedOutput)
{
    InitTables(
        /*isTeleportable*/ {true, true, true},
        /*isVersioned*/ {false, false, false});

    Options_.BuildOutputOrder = true;
    Options_.MinTeleportChunkSize = 2_KB;
    DataWeightPerJob_ = 2_KB;

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
    ASSERT_EQ(entries.size(), 8u);
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
    ASSERT_EQ(entries.size(), 10u);

    SplitJob(originalEntries[0].GetCookie(), 10);
    entries = order->ToEntryVector();
    ASSERT_EQ(entries.size(), 11u);

    SplitJob(originalEntries[7].GetCookie(), 10);
    entries = order->ToEntryVector();
    ASSERT_EQ(entries.size(), 12u);

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

TEST_P(TOrderedChunkPoolTest, OrderedMergeSliceLargeChunks)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 2_KB;
    InputSliceDataWeight_ = 2_KB;
    InputSliceRowCount_ = 100;

    InitJobConstraints();

    auto chunkA = CreateChunk(0, 20_KB, /*rowCount*/ 1000);

    CreateChunkPool();

    AddChunk(chunkA);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_LE(9u, stripeLists.size());
    EXPECT_LE(stripeLists.size(), 11u);

    CheckEverything(stripeLists);
}

////////////////////////////////////////////////////////////////////////////////

TEST_P(TOrderedChunkPoolTest, ExplicitSingleJob)
{
    InitTables(
        /*isTeleportable*/ {true},
        /*isVersioned*/ {false});

    ExplicitJobCount_ = 1;
    DataWeightPerJob_ = 1_KB;
    MaxDataSlicesPerJob_ = 1;
    InputSliceDataWeight_ = 2_KB;
    InputSliceRowCount_ = 100;

    InitJobConstraints();

    // We have many data slices, large data weight and teleportable chunks.
    // So many reasons to create two jobs.
    auto chunkA = CreateChunk(0, 10_KB, /*rowCount*/ 1000);
    auto chunkB = CreateChunk(0, 10_KB, /*rowCount*/ 1000);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(stripeLists.size(), 1u);

    CheckEverything(stripeLists);
}

TEST_P(TOrderedChunkPoolTest, UnsuccessfulSplitMarksJobUnsplittable)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});

    DataWeightPerJob_ = 2_KB;
    InitJobConstraints();

    auto chunk = CreateChunk(
        0,
        /*dataWeight*/ 1_KB,
        /*rowCount*/ 1);

    CreateChunkPool();

    AddChunk(chunk);

    ChunkPool_->Finish();

    CheckUnsuccessfulSplitMarksJobUnsplittable(ChunkPool_);
}

INSTANTIATE_TEST_SUITE_P(BasicTests,
    TOrderedChunkPoolTest,
    Values(/*useNewSlicingImplementation*/ false, /*useNewSlicingImplementation*/ true));

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkPoolTestRandomized
    : public WithParamInterface<std::tuple<int, bool>>
    , public TOrderedChunkPoolTestBase
{
public:
    void SetUp() final
    {
        TOrderedChunkPoolTestBase::SetUp();
        Gen_.seed(std::get<0>(GetParam()));
        Options_.UseNewSlicingImplementation = std::get<1>(GetParam());
    }
};

static constexpr int NumberOfRepeats = 15;

TEST_P(TOrderedChunkPoolTestRandomized, VariousOperationsWithPoolTest)
{
    InitTables(
        /*isTeleportable*/ {false},
        /*isVersioned*/ {false});
    DataWeightPerJob_ = 1_KB;
    InitJobConstraints();

    constexpr int chunkCount = 25;
    constexpr int maxJobLosts = 50;

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

    for (const auto& chunk : CreatedUnversionedPrimaryChunks_) {
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
        EXPECT_FALSE(ChunkPool_->IsCompleted());

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
    TOrderedChunkPoolTestRandomized,
    Combine(Range(0, NumberOfRepeats), Values(/*useNewSlicingImplementation*/ false, /*useNewSlicingImplementation*/ true)));

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkPoolJobSizesTestRandomized
    : public WithParamInterface<int>
    , public TOrderedChunkPoolTestBase
{
public:
    void SetUp() final
    {
        TOrderedChunkPoolTestBase::SetUp();
        Gen_.seed(GetParam());
    }
};

TEST_P(TOrderedChunkPoolJobSizesTestRandomized, BuildJobsInputByCompressedDataSizeAndDataWeight)
{
    int tableCount = std::uniform_int_distribution<int>(1, 3)(Gen_);

    std::vector<bool> isTeleportable(tableCount);
    for (bool& value : isTeleportable) {
        value = std::uniform_int_distribution<int>(0, 1)(Gen_);
    }

    InitTables(
        /*isTeleportable*/ isTeleportable,
        /*isVersioned*/ std::vector<bool>(tableCount));

    auto generateSize = [&] () -> i64 {
        auto baseSizes = std::to_array({1_KB, 1_MB, 100_MB, 1_GB, 512_GB, 1_TB, 10_TB});
        i64 baseSize = baseSizes[std::uniform_int_distribution<i64>(0, std::ssize(baseSizes) - 1)(Gen_)];
        return std::lognormal_distribution<double>()(Gen_) * baseSize;
    };

    auto generateRowCount = [&] () -> i64 {
        auto baseCounts = std::to_array<i64>({100, 1'000, 1'000'000, 1'000'000'000, 1'000'000'000'000});
        i64 baseCount = baseCounts[std::uniform_int_distribution<i64>(0, std::ssize(baseCounts) - 1)(Gen_)];
        return std::lognormal_distribution<double>()(Gen_) * baseCount;
    };

    auto generateTableIndex = [&] {
        return std::uniform_int_distribution(0, tableCount - 1)(Gen_);
    };

    Options_.BuildOutputOrder = true;

    InputSliceRowCount_ = generateRowCount();
    InputSliceDataWeight_ = generateSize();
    if (std::uniform_int_distribution(0, 1)(Gen_) == 0) {
        BatchRowCount_ = std::uniform_int_distribution(10, 1000)(Gen_);
    }
    Options_.MinTeleportChunkSize = generateSize();
    MaxDataSlicesPerJob_ = std::uniform_int_distribution(1, 1000)(Gen_);

    std::vector<TInputChunkPtr> chunks;

    int chunkCount = std::uniform_int_distribution(1, 100)(Gen_);
    i64 totalDataWeight = 0;
    i64 totalCompressedDataSize = 0;
    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(
            /*tableIndex*/ generateTableIndex(),
            /*dataWeight*/ generateSize(),
            /*tableIndex*/ generateRowCount());
        totalDataWeight += chunk->GetDataWeight();
        totalCompressedDataSize += chunk->GetCompressedDataSize();
        chunks.push_back(std::move(chunk));
    };

    DataWeightPerJob_ = std::max<i64>(generateSize(), 1);
    MaxCompressedDataSizePerJob_ = std::max<i64>(generateSize(), 1);

    // Don't build too many jobs.
    while (totalDataWeight / DataWeightPerJob_ > 50) {
        DataWeightPerJob_ *= 10;
        DataWeightPerJob_ += std::uniform_int_distribution<int>(0, 9)(Gen_);
    }
    while (totalCompressedDataSize / MaxCompressedDataSizePerJob_ > 50) {
        MaxCompressedDataSizePerJob_ *= 10;
        MaxCompressedDataSizePerJob_ += std::uniform_int_distribution<int>(0, 9)(Gen_);
    }

    InitJobConstraints();

    CreateChunkPool();

    for (auto& chunk : chunks) {
        AddChunk(std::move(chunk));
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    CheckEverything(GetAllStripeListsByOutputOrder());

    auto outputOrder = ChunkPool_->GetOutputOrder();
    YT_VERIFY(outputOrder);

    for (const auto& entry : outputOrder->ToEntryVector()) {
        if (entry.IsTeleportChunk()) {
            ASSERT_GE(entry.GetTeleportChunk()->GetDataWeight(), Options_.MinTeleportChunkSize);
            continue;
        }

        const auto& stripeList = ChunkPool_->GetStripeList(entry.GetCookie());

        i64 sliceCount = 0;
        for (const auto& stripe : stripeList->Stripes) {
            sliceCount += std::ssize(stripe->DataSlices);
        }

        if (sliceCount > 1) {
            ASSERT_LE(stripeList->GetAggregateStatistics().CompressedDataSize, MaxCompressedDataSizePerJob_);
        }

        ASSERT_LE(sliceCount, MaxDataSlicesPerJob_);

        if (!BatchRowCount_.has_value()) {
            if (sliceCount > 1)  {
                i64 maxDataWeightSlice = 0;
                for (const auto& stripe : stripeList->Stripes) {
                    for (const auto& slice : stripe->DataSlices) {
                        maxDataWeightSlice = std::max(maxDataWeightSlice, slice->GetDataWeight());
                    }
                }

                EXPECT_LT(
                    stripeList->TotalDataWeight,
                    DataWeightPerJob_ + maxDataWeightSlice);
            }
        } else if (tableCount == 1 && sliceCount < MaxDataSlicesPerJob_) {
            // Stripes are sorted by table index internally, so we can't check data
            // weight guarantee when batch row count is set and table count is more than 1.

            int rowsLeft = *BatchRowCount_;
            i64 lastRowBatchDataWeight = 0;
            int stripeIndex = std::ssize(stripeList->Stripes) - 1;
            while (stripeIndex >= 0 && rowsLeft > 0) {
                int sliceIndex = std::ssize(stripeList->Stripes[stripeIndex]->DataSlices) - 1;
                while (sliceIndex >= 0 && rowsLeft > 0) {
                    const auto& slice = stripeList->Stripes[stripeIndex]->DataSlices[sliceIndex];
                    i64 currentSliceRowCount = std::min<i64>(slice->GetRowCount(), rowsLeft);

                    lastRowBatchDataWeight += std::ceil(static_cast<double>(slice->GetDataWeight()) / slice->GetRowCount()) * currentSliceRowCount;

                    rowsLeft -= currentSliceRowCount;
                    --sliceIndex;
                }
                --stripeIndex;
            }

            EXPECT_LE(stripeList->TotalDataWeight, DataWeightPerJob_ + lastRowBatchDataWeight);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(BuildJobsInputByCompressedDataSizeAndDataWeight,
    TOrderedChunkPoolJobSizesTestRandomized,
    Range(/*start*/ 0, /*end*/ 10000));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkPools

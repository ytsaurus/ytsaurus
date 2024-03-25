#include "chunk_pools_helpers.h"

#include <yt/yt/server/lib/chunk_pools/mock/chunk_slice_fetcher.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/controller_agent/helpers.h>
#include <yt/yt/server/controller_agent/job_size_constraints.h>
#include <yt/yt/server/controller_agent/operation_controller.h>

#include <yt/yt/server/lib/chunk_pools/input_chunk_mapping.h>
#include <yt/yt/server/lib/chunk_pools/multi_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/legacy_sorted_chunk_pool.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/phoenix.h>

#include <util/generic/cast.h>
#include <util/generic/size_literals.h>

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

using NControllerAgent::TCompletedJobSummary;

using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

//! A unit to measure all sizes in this file.
static constexpr i32 Inf32 = std::numeric_limits<i32>::max();
static constexpr i64 Inf64 = std::numeric_limits<i64>::max();

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkPoolTest
    : public Test
{
protected:
    void SetUp() override
    {
        Options_.RowBuffer = RowBuffer_;
        Options_.MinTeleportChunkSize = Inf64;
        Options_.SliceForeignChunks = true;
        Options_.SortedJobOptions.MaxTotalSliceCount = Inf64;
        Options_.UseNewJobBuilder = false;
        Options_.ReturnNewDataSlices = false;
        Options_.Logger = GetTestLogger();
        DataSizePerJob_ = Inf64;
        MaxBuildRetryCount_ = 1;
        MaxDataSlicesPerJob_ = Inf32;
        MaxDataWeightPerJob_ = Inf64;
        MaxPrimaryDataWeightPerJob_ = Inf64;
        InputSliceDataWeight_ = Inf64;
    }

    void InitJobConstraints()
    {
        Options_.JobSizeConstraints = CreateExplicitJobSizeConstraints(
            false /*canAdjustDataWeightPerJob*/,
            false /*isExplicitJobCount*/,
            0 /*jobCount*/,
            DataSizePerJob_,
            PrimaryDataWeightPerJob_,
            MaxDataSlicesPerJob_,
            MaxDataWeightPerJob_,
            MaxPrimaryDataWeightPerJob_,
            InputSliceDataWeight_,
            Inf64 /*inputSliceRowCount*/,
            {} /*batchRowCount*/,
            InputSliceDataWeight_,
            SamplingRate_,
            SamplingDataWeightPerJob_,
            SamplingPrimaryDataWeightPerJob_,
            MaxBuildRetryCount_);
    }

    struct TMockChunkSliceFetcherBuilder
    {
        TMockChunkSliceFetcherBuilder(TSortedChunkPoolTest* owner)
            : Owner(owner)
        { }

        TStrictMockChunkSliceFetcherPtr Build()
        {
            Expectation expectation = EXPECT_CALL(*ChunkSliceFetcher, Fetch())
                .After(AllChunksAreAdded)
                .WillOnce(Return(VoidFuture));

            EXPECT_CALL(*ChunkSliceFetcher, GetChunkSlices())
                .After(expectation)
                .WillOnce(ReturnPointee(&ChunkSlices));

            return ChunkSliceFetcher;
        }

        void RegisterSliceableUnversionedChunk(const TInputChunkPtr& chunk, std::vector<TInputChunkSlicePtr> slices)
        {
            ChunkSlices.insert(ChunkSlices.end(), slices.begin(), slices.end());
            AllChunksAreAdded += EXPECT_CALL(
                *ChunkSliceFetcher,
                AddDataSliceForSlicing(
                    Property(
                        &TLegacyDataSlicePtr::Get,
                        Pointee(
                            Property(
                                &TLegacyDataSlice::GetSingleUnversionedChunk,
                                Eq(chunk)))),
                    _,
                    _,
                    _));
        }

        void RegisterTriviallySliceableUnversionedChunk(const TInputChunkPtr& chunk)
        {
            auto chunkSlices = Owner->SliceUnversionedChunk(chunk, {}, {chunk->GetCompressedDataSize()});
            RegisterSliceableUnversionedChunk(chunk, std::move(chunkSlices));
        }

        ExpectationSet AllChunksAreAdded;
        TStrictMockChunkSliceFetcherPtr ChunkSliceFetcher = New<StrictMock<TMockChunkSliceFetcher>>();
        std::vector<TInputChunkSlicePtr> ChunkSlices;
        TSortedChunkPoolTest* Owner;
    };

    std::vector<TMockChunkSliceFetcherBuilder> MockBuilders_;
    std::vector<TStrictMockChunkSliceFetcherPtr> Fetchers_;

    IChunkSliceFetcherFactoryPtr BuildMockChunkSliceFetcherFactory()
    {
        YT_VERIFY(Fetchers_.empty());
        for (auto& mockBuilder : MockBuilders_) {
            Fetchers_.emplace_back(mockBuilder.Build());
        }
        return New<TMockChunkSliceFetcherFactory>(&Fetchers_);
    }

    void PrepareNewMock()
    {
        MockBuilders_.emplace_back(this);
    }

    TMockChunkSliceFetcherBuilder& CurrentMock()
    {
        YT_VERIFY(!MockBuilders_.empty());
        return MockBuilders_.back();
    }

    // In this test we will only deal with integral rows as
    // all the logic inside sorted chunk pool does not depend on
    // actual type of values in keys.
    TLegacyKey BuildRow(std::vector<i64> values)
    {
        auto row = RowBuffer_->AllocateUnversioned(values.size());
        for (int index = 0; index < std::ssize(values); ++index) {
            row[index] = MakeUnversionedInt64Value(values[index], index);
        }
        return row;
    }

    TInputChunkPtr CreateChunk(
        const TLegacyKey& minBoundaryKey,
        const TLegacyKey& maxBoundaryKey,
        int tableIndex,
        i64 size = 1_KB,
        const TLegacyKey& lowerLimit = TLegacyKey(),
        const TLegacyKey& upperLimit = TLegacyKey(),
        i64 rowCount = 1000)
    {
        auto inputChunk = New<TInputChunk>();
        inputChunk->SetChunkId(MakeRandomId(EObjectType::Chunk, TCellTag(0x42)));
        inputChunk->SetCompressedDataSize(size);
        inputChunk->SetTotalUncompressedDataSize(size);
        inputChunk->SetTotalDataWeight(size);
        inputChunk->BoundaryKeys() = std::make_unique<TOwningBoundaryKeys>(TOwningBoundaryKeys {
            TLegacyOwningKey(minBoundaryKey),
            TLegacyOwningKey(maxBoundaryKey)
        });
        inputChunk->SetTableIndex(tableIndex);
        inputChunk->SetTableRowIndex(UnversionedTableRowCounts_[tableIndex]);
        UnversionedTableRowCounts_[tableIndex] += rowCount;
        if (lowerLimit) {
            inputChunk->LowerLimit() = std::make_unique<TLegacyReadLimit>(TLegacyOwningKey(lowerLimit));
        }
        if (upperLimit) {
            inputChunk->UpperLimit() = std::make_unique<TLegacyReadLimit>(TLegacyOwningKey(upperLimit));
        }
        const auto& inputTable = InputTables_[tableIndex];
        if (!inputTable.IsVersioned()) {
            if (inputTable.IsForeign()) {
                CreatedUnversionedForeignChunks_.insert(inputChunk);
            } else {
                CreatedUnversionedPrimaryChunks_.insert(inputChunk);
            }
        }
        inputChunk->SetTotalRowCount(rowCount);
        return inputChunk;
    }

    TInputChunkPtr CopyChunk(const TInputChunkPtr& chunk)
    {
        auto chunkCopy = New<TInputChunk>();
        chunkCopy->SetChunkId(chunk->GetChunkId());
        chunkCopy->SetCompressedDataSize(chunk->GetCompressedDataSize());
        chunkCopy->BoundaryKeys() = std::make_unique<TOwningBoundaryKeys>(*chunk->BoundaryKeys());
        int tableIndex = chunk->GetTableIndex();
        chunkCopy->SetTableIndex(tableIndex);
        chunkCopy->SetTableRowIndex(chunk->GetTableRowIndex());
        chunkCopy->SetTotalRowCount(chunk->GetRowCount());
        if (chunk->LowerLimit()) {
            chunkCopy->LowerLimit() = std::make_unique<TLegacyReadLimit>(*chunk->LowerLimit());
        }
        if (chunk->UpperLimit()) {
            chunkCopy->UpperLimit() = std::make_unique<TLegacyReadLimit>(*chunk->UpperLimit());
        }
        const auto& inputTable = InputTables_[tableIndex];
        if (!inputTable.IsVersioned()) {
            if (inputTable.IsForeign()) {
                CreatedUnversionedForeignChunks_.insert(chunkCopy);
            } else {
                CreatedUnversionedPrimaryChunks_.insert(chunkCopy);
            }
        }
        return chunkCopy;
    }

    void InitTables(std::vector<bool> isForeign, std::vector<bool> isTeleportable, std::vector<bool> isVersioned)
    {
        YT_VERIFY(isForeign.size() == isTeleportable.size() && isTeleportable.size() == isVersioned.size() && isForeign.size() > 0u);
        for (int index = 0; index < std::ssize(isForeign); ++index) {
            InputTables_.emplace_back(isTeleportable[index], !isForeign[index] /*isPrimary*/, isVersioned[index]);
        }
        UnversionedTableRowCounts_.resize(InputTables_.size(), 0);
    }

    std::vector<TInputChunkSlicePtr> SliceUnversionedChunk(
        TInputChunkPtr chunk,
        std::vector<TLegacyKey> internalPoints,
        std::vector<i64> sliceSizes = std::vector<i64>(),
        std::vector<i64> sliceRowCounts = std::vector<i64>())
    {
        if (sliceSizes.empty()) {
            sliceSizes.assign(internalPoints.size() + 1, chunk->GetUncompressedDataSize() / (internalPoints.size() + 1));
            // Fix the first size to fix the error because of integer division.
            sliceSizes[0] += chunk->GetUncompressedDataSize() - (internalPoints.size() + 1) * sliceSizes[0];
        } else {
            YT_VERIFY(internalPoints.size() + 1 == sliceSizes.size());
        }
        if (sliceRowCounts.empty()) {
            sliceRowCounts.assign(internalPoints.size() + 1, chunk->GetRowCount() / (internalPoints.size() + 1));
            sliceRowCounts[0] += chunk->GetRowCount() - (internalPoints.size() + 1) * sliceRowCounts[0];
        } else {
            YT_VERIFY(internalPoints.size() + 1 == sliceSizes.size());
        }

        YT_VERIFY(!InputTables_[chunk->GetTableIndex()].IsVersioned());

        TLegacyKey lastKey = chunk->LowerLimit() ? chunk->LowerLimit()->GetLegacyKey() : chunk->BoundaryKeys()->MinKey;
        i64 currentRow = 0;
        std::vector<TInputChunkSlicePtr> slices;
        for (int index = 0; index <= std::ssize(internalPoints); ++index) {
            TLegacyKey upperLimit = index < std::ssize(internalPoints)
                ? GetKeySuccessor(internalPoints[index], RowBuffer_)
                : (chunk->UpperLimit()
                ? chunk->UpperLimit()->GetLegacyKey()
                : GetKeySuccessor(chunk->BoundaryKeys()->MaxKey, RowBuffer_));
            slices.emplace_back(New<TInputChunkSlice>(chunk, lastKey, upperLimit));
            if (!internalPoints.empty()) {
                slices.back()
                    ->LegacyLowerLimit().RowIndex = currentRow;
                currentRow += sliceRowCounts[index];
                slices.back()
                    ->LegacyUpperLimit().RowIndex = currentRow;
                slices.back()->OverrideSize(sliceRowCounts[index], sliceSizes[index]);
            }
            lastKey = upperLimit;
        }
        return slices;
    }

    void CreateChunkPool(bool useGenericInputStreamDirectory = false)
    {
        ChunkPool_ = CreateLegacySortedChunkPool(
            Options_,
            !MockBuilders_.empty() ? BuildMockChunkSliceFetcherFactory() : nullptr,
            useGenericInputStreamDirectory ? IntermediateInputStreamDirectory : TInputStreamDirectory(InputTables_));
        ChunkPool_->SubscribeChunkTeleported(
            BIND([this] (TInputChunkPtr teleportChunk, std::any /*tag*/) {
                TeleportChunks_.push_back(std::move(teleportChunk));
            }));
    }

    IChunkPoolInput::TCookie AddMultiChunkStripe(std::vector<TInputChunkPtr> chunks)
    {
        std::vector<TLegacyDataSlicePtr> dataSlices;
        for (const auto& chunk : chunks) {
            auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
            dataSlice->SetInputStreamIndex(dataSlice->GetTableIndex());
            InferLimitsFromBoundaryKeys(dataSlice, RowBuffer_);
            dataSlices.emplace_back(std::move(dataSlice));
        }
        auto stripe = New<TChunkStripe>();
        std::move(dataSlices.begin(), dataSlices.end(), std::back_inserter(stripe->DataSlices));
        return ChunkPool_->Add(stripe);
    }

    TChunkStripePtr CreateStripe(const std::vector<TInputChunkPtr>& chunks)
    {
        auto stripe = New<TChunkStripe>();
        for (const auto& chunk : chunks) {
            auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
            dataSlice->SetInputStreamIndex(dataSlice->GetTableIndex());
            ActiveChunks_.insert(chunk->GetChunkId());
            InferLimitsFromBoundaryKeys(dataSlice, RowBuffer_);
            stripe->DataSlices.emplace_back(std::move(dataSlice));
        }
        return stripe;
    }

    IChunkPoolInput::TCookie AddChunk(const TInputChunkPtr& chunk)
    {
        auto stripe = CreateStripe({chunk});
        auto cookie = ChunkPool_->Add(std::move(stripe));
        InputCookieToChunkId_[cookie] = chunk->GetChunkId();
        return cookie;
    }

    void SuspendChunk(IChunkPoolInput::TCookie cookie)
    {
        auto chunkId = InputCookieToChunkId_[cookie];
        YT_VERIFY(chunkId);
        YT_VERIFY(ActiveChunks_.erase(chunkId));
        ChunkPool_->Suspend(cookie);
    }

    void ResumeChunk(IChunkPoolInput::TCookie cookie)
    {
        auto chunkId = InputCookieToChunkId_[cookie];
        YT_VERIFY(chunkId);
        ActiveChunks_.insert(chunkId);
        ChunkPool_->Resume(cookie);
    }

    void ResetChunk(IChunkPoolInput::TCookie cookie, const TInputChunkPtr& chunk)
    {
        const auto& oldChunkId = InputCookieToChunkId_[cookie];
        YT_VERIFY(oldChunkId);
        auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
        // TODO(max42): what is this?
        dataSlice->SetInputStreamIndex(chunk->GetTableIndex());
        InferLimitsFromBoundaryKeys(dataSlice, RowBuffer_);
        ChunkPool_->Reset(cookie, New<TChunkStripe>(dataSlice), IdentityChunkMapping);
        InputCookieToChunkId_[cookie] = chunk->GetChunkId();
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
        Save(saveContext, MultiChunkPool_);
        Save(saveContext, UnderlyingPools__);
        saveContext.Finish();
        auto blob = output.Flush();
        ChunkPool_.Reset();

        TMemoryInput input(blob.Begin(), blob.Size());
        TLoadContext loadContext(&input, RowBuffer_, GetCurrentSnapshotVersion());
        Load(loadContext, ChunkPool_);
        Load(loadContext, MultiChunkPool_);
        Load(loadContext, UnderlyingPools__);
        ChunkPool_->SubscribeChunkTeleported(
            BIND([this] (TInputChunkPtr teleportChunk, std::any /*tag*/) {
                TeleportChunks_.push_back(std::move(teleportChunk));
            }));
    }

    std::vector<TChunkStripeListPtr> GetAllStripeLists()
    {
        std::vector<TChunkStripeListPtr> stripeLists;
        auto sortedExtractedCookies = ExtractedCookies_;
        std::sort(sortedExtractedCookies.begin(), sortedExtractedCookies.end());
        for (auto cookie : sortedExtractedCookies) {
            if (cookie != IChunkPoolOutput::NullCookie) {
                stripeLists.emplace_back(ChunkPool_->GetStripeList(cookie));
            }
        }
        return stripeLists;
    }

    TChunkStripePtr GetStripeByTableIndex(const TChunkStripeListPtr& stripeList, int tableIndex)
    {
        for (auto& stripe : stripeList->Stripes) {
            if (stripe->DataSlices.front()->GetTableIndex() == tableIndex) {
                return stripe;
            }
        }

        YT_ABORT();
    }

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
        for (const auto& inputChunk : CreatedUnversionedPrimaryChunks_) {
            if (teleportChunksSet.contains(inputChunk)) {
                continue;
            }
            TLegacyKey chunkLowerKey = inputChunk->LowerLimit() && inputChunk->LowerLimit()->HasLegacyKey()
                ? inputChunk->LowerLimit()->GetLegacyKey()
                : inputChunk->BoundaryKeys()->MinKey;
            TLegacyKey chunkUpperKey = inputChunk->UpperLimit() && inputChunk->UpperLimit()->HasLegacyKey()
                ? inputChunk->UpperLimit()->GetLegacyKey()
                : GetKeySuccessor(inputChunk->BoundaryKeys()->MaxKey, RowBuffer_);
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

    //! Check that:
    //! * All teleport chunks satisfy the Options_.MinTeleportChunkSize constraint;
    //! * All stripe lists have no more than Options_.MaxDataSlicesPerJob + InputTables_.size() - 1 data slices in total.
    //! Unfortunately we cannot check the Options_.MaxPrimaryDataSizePerJob constraint satisfaction as this is not an absolute
    //! restriction, but only a best-effort bound.
    void TryCheckJobConstraintsSatisfaction(
        const std::vector<TChunkStripeListPtr>& stripeLists)
    {
        for (const auto& teleportChunk : TeleportChunks_) {
            EXPECT_TRUE(teleportChunk->IsLargeCompleteChunk(Options_.MinTeleportChunkSize));
        }

        for (const auto& stripeList : stripeLists) {
            int dataSlicesTotalNumber = 0;
            for (const auto& stripe : stripeList->Stripes) {
                if (stripe) {
                    dataSlicesTotalNumber += stripe->DataSlices.size();
                }
            }
            EXPECT_LE(dataSlicesTotalNumber, MaxDataSlicesPerJob_ + std::ssize(InputTables_) - 1);
        }
    }

    //! Check that jobs do not overlap by keys. Applicable only when Options_.SortedJobOptions.EnableKeyGuarantee is true.
    void CheckKeyGuarantee(const std::vector<TChunkStripeListPtr>& stripeLists)
    {
        TLegacyKey lastUpperKey;
        for (const auto& stripeList : stripeLists) {
            TLegacyKey lowerKey = MaxKey();
            TLegacyKey upperKey = MinKey();
            for (const auto& stripe : stripeList->Stripes) {
                for (const auto& dataSlice : stripe->DataSlices) {
                    if (lowerKey > dataSlice->LegacyLowerLimit().Key) {
                        lowerKey = dataSlice->LegacyLowerLimit().Key;
                    }
                    if (upperKey < dataSlice->LegacyUpperLimit().Key) {
                        upperKey = dataSlice->LegacyUpperLimit().Key;
                    }
                }
            }
            EXPECT_LE(lastUpperKey, lowerKey);
            lastUpperKey = upperKey;
        }
    }

    //! Find all teleport chunks naively (in quadratic time) and check that chunk pool detected exactly
    //! the same chunks.
    void CheckTeleportChunks()
    {
        // TODO(max42): implement a naive procedure for finding the teleport chunks and compare
        // its result with `teleportChunks`.
    }

    //! Check the correctness of joined data (in quadratic time).
    void CheckCorrectnessOfJoin(const std::vector<TChunkStripeListPtr>& /*stripeLists*/)
    {
        // TODO(max42): implement a naive procedure here.
    }

    //! Perform all the correctness checks over the given result of sorted chunk pool invocation
    //! (without any suspends nor job interruptions).
    void CheckEverything(
        const std::vector<TChunkStripeListPtr>& stripeLists)
    {
        CheckDataIntegrity(stripeLists);
        TryCheckJobConstraintsSatisfaction(stripeLists);
        CheckTeleportChunks();
        CheckStripeListsContainOnlyActiveChunks();
        CheckForeignStripesAreMarkedAsForeign();
        if (Options_.SortedJobOptions.EnableKeyGuarantee) {
            CheckKeyGuarantee(stripeLists);
        }
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
                            EXPECT_TRUE(ActiveChunks_.contains(chunk->GetChunkId()));
                        }
                    }
                }
            }
        }
    }

    IPersistentChunkPoolPtr ChunkPool_;
    IMultiChunkPoolPtr MultiChunkPool_;

    std::vector<IPersistentChunkPoolPtr> UnderlyingPools__;

    //! Set containing all unversioned primary input chunks that have ever been created.
    THashSet<TInputChunkPtr> CreatedUnversionedPrimaryChunks_;
    //! Set containing all unversioned foreign input chunks that have ever been created.
    THashSet<TInputChunkPtr> CreatedUnversionedForeignChunks_;
    //! Set containing all chunks that are added to the pool without being suspended.
    THashSet<TChunkId> ActiveChunks_;

    THashMap<IChunkPoolInput::TCookie, TChunkId> InputCookieToChunkId_;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    std::vector<TInputStreamDescriptor> InputTables_;

    THashSet<IChunkPoolOutput::TCookie> OutputCookies_;

    std::vector<int> UnversionedTableRowCounts_;

    TSortedChunkPoolOptions Options_;

    i64 DataSizePerJob_;
    i64 PrimaryDataWeightPerJob_ = std::numeric_limits<i64>::max();

    i64 MaxDataWeightPerJob_;
    i64 MaxPrimaryDataWeightPerJob_;

    i64 MaxBuildRetryCount_;

    i32 MaxDataSlicesPerJob_;

    i64 InputSliceDataWeight_;

    std::optional<double> SamplingRate_;
    i64 SamplingDataWeightPerJob_ = Inf64;
    i64 SamplingPrimaryDataWeightPerJob_ = Inf64;

    std::vector<IChunkPoolOutput::TCookie> ExtractedCookies_;

    std::mt19937 Gen_;

    THashMap<TChunkId, int> ChunkIdToUnderlyingPoolIndex_;

    std::vector<TInputChunkPtr> TeleportChunks_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedChunkPoolTest, SortedMergeTeleports1)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false, false} /*isForeign*/,
        {true, true, true, true} /*isTeleportable*/,
        {false, false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({0, 10}), BuildRow({1, 11}), 0);
    auto chunkB = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 10}), BuildRow({1, 13}), 2);
    auto chunkD = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 3, 1_KB, BuildRow({1, 13}), BuildRow({1, 17}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkD);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);
    AddChunk(chunkD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, UnorderedElementsAreArray({chunkA, chunkB, chunkC}));
    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, SortedMergeTeleports2)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false, false} /*isForeign*/,
        {false, true, true, true} /*isTeleportable*/,
        {false, false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({0, 10}), BuildRow({1, 11}), 0);
    auto chunkB = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 10}), BuildRow({1, 13}), 2);
    auto chunkD = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 3, 1_KB, BuildRow({1, 13}), BuildRow({1, 17}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkD);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);
    AddChunk(chunkD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, UnorderedElementsAreArray({chunkB, chunkC}));
    // Non-teleportable chunks are separated with teleportable ones, so there should be two separate jobs.
    EXPECT_EQ(2u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, SortedMergeTeleports3)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({0, 10}), BuildRow({1, 11}), 0);
    auto chunkB = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 10}), BuildRow({1, 13}), 2);

    CreateChunkPool();
    PersistAndRestore();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, UnorderedElementsAreArray({chunkA, chunkB, chunkC}));
    EXPECT_EQ(0u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, SortedMergeTeleports4)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 2;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({0, 10}), BuildRow({1, 11}), 0);
    auto chunkB = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 10}), BuildRow({1, 13}), 2);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkC);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

// NB(max42): completely getting into this test may take several hours of your life.
// Double-think before reading it :)
TEST_F(TSortedChunkPoolTest, SortedMergeAllKindOfTeleports)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 3;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

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
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA3);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB3);

    // No (one contained in another).
    // [====]__
    // _[==]___
    auto chunkA4 = CreateChunk(BuildRow({4, 1, 0}), BuildRow({4, 1, 3}), 0);
    auto chunkB4 = CreateChunk(BuildRow({4, 1, 1}), BuildRow({4, 1, 2}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA4);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB4);

    // No (single_key one contained in another).
    // [====]__
    // __[]____
    auto chunkA5 = CreateChunk(BuildRow({5, 1, 0}), BuildRow({5, 1, 3}), 0);
    auto chunkB5 = CreateChunk(BuildRow({5, 1, 1}), BuildRow({5, 1, 1}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA5);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB5);

    // No (they coincide).
    // [===]__
    // [===]__
    auto chunkA6 = CreateChunk(BuildRow({6, 1, 0}), BuildRow({6, 1, 3}), 0);
    auto chunkB6 = CreateChunk(BuildRow({6, 1, 0}), BuildRow({6, 1, 3}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA6);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB6);

    // No (one covers another).
    // [===]__
    // [====]_
    auto chunkA7 = CreateChunk(BuildRow({7, 1, 0}), BuildRow({7, 1, 3}), 0);
    auto chunkB7 = CreateChunk(BuildRow({7, 1, 0}), BuildRow({7, 1, 4}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA7);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB7);

    // No (one covers another).
    // _[===]__
    // [====]__
    auto chunkA8 = CreateChunk(BuildRow({8, 1, 0}), BuildRow({8, 1, 4}), 0);
    auto chunkB8 = CreateChunk(BuildRow({8, 1, 1}), BuildRow({8, 1, 4}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA8);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB8);

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
    auto chunkB12 = CreateChunk(BuildRow({12, 1, 2}), BuildRow({12, 1, 8}), 1, 1_KB, BuildRow({12, 1, 5}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB12);

    // Yes/No (non-trivial lower limit coinciding with max key).
    // _[==]_____
    // ___=[====]
    auto chunkA13 = CreateChunk(BuildRow({13, 1, 0}), BuildRow({13, 1, 4}), 0);
    auto chunkB13 = CreateChunk(BuildRow({13, 1, 2}), BuildRow({13, 1, 8}), 1, 1_KB, BuildRow({13, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB13);

    // No/No (they partially intersect with each other).
    // _[===]____
    // ___=[===]_
    auto chunkA14 = CreateChunk(BuildRow({14, 1, 0}), BuildRow({14, 1, 4}), 0);
    auto chunkB14 = CreateChunk(BuildRow({14, 1, 2}), BuildRow({14, 1, 8}), 1, 1_KB, BuildRow({14, 1, 3}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA14);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB14);

    // Yes/No (second one is de-facto single-key coinciding with the max-key of the first one).
    // _[===]____
    // ___=[]____
    auto chunkA15 = CreateChunk(BuildRow({15, 1, 0}), BuildRow({15, 1, 4}), 0);
    auto chunkB15 = CreateChunk(BuildRow({15, 1, 2}), BuildRow({15, 1, 4}), 1, 1_KB, BuildRow({15, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB15);

    // Yes/No (non-trivial upper limit).
    // ______[===]_
    // _[==)===____
    auto chunkA16 = CreateChunk(BuildRow({16, 1, 4}), BuildRow({16, 1, 8}), 0);
    auto chunkB16 = CreateChunk(BuildRow({16, 1, 0}), BuildRow({16, 1, 6}), 1, 1_KB, TLegacyKey(), BuildRow({16, 1, 3}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB16);

    // Yes/No (non-trivial upper limit).
    // ____[===]_
    // _[==)===__
    auto chunkA17 = CreateChunk(BuildRow({17, 1, 4}), BuildRow({17, 1, 8}), 0);
    auto chunkB17 = CreateChunk(BuildRow({17, 1, 0}), BuildRow({17, 1, 6}), 1, 1_KB, TLegacyKey(), BuildRow({17, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB17);

    // No/No (non-trivial upper limit).
    // ____[===]_
    // _[====)=__
    auto chunkA18 = CreateChunk(BuildRow({18, 1, 4}), BuildRow({18, 1, 8}), 0);
    auto chunkB18 = CreateChunk(BuildRow({18, 1, 0}), BuildRow({18, 1, 6}), 1, 1_KB, TLegacyKey(), BuildRow({18, 1, 5}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA18);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB18);

    // Yes/No (first one is single-key touching the second one with non-trivial lower limit).
    // __[]_______
    // ===[==)____
    auto chunkA19 = CreateChunk(BuildRow({19, 1, 4}), BuildRow({19, 1, 4}), 0);
    auto chunkB19 = CreateChunk(BuildRow({19, 1, 0}), BuildRow({19, 1, 6}), 1, 1_KB, BuildRow({19, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB19);

    // Yes/No (first one is single-key touching the second one with non-trivial upper limit).
    // _____[]___
    // ___[==)===_
    auto chunkA20 = CreateChunk(BuildRow({20, 1, 4}), BuildRow({20, 1, 4}), 0);
    auto chunkB20 = CreateChunk(BuildRow({20, 1, 0}), BuildRow({20, 1, 6}), 1, 1_KB, TLegacyKey(), BuildRow({20, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB20);

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
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA23);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB23);

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
    auto chunkB27 = CreateChunk(BuildRow({27, 1, 1, 42}), BuildRow({27, 1, 2, 42}), 1, 1_KB, TLegacyKey(), BuildRow({27, 1, 1, 46}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB27);

    // No/No (after shortening chunks will be intersecting).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // _________[========)======______________________________

    auto chunkA28 = CreateChunk(BuildRow({28, 1, 1, 42}), BuildRow({28, 1, 3, 42}), 0);
    auto chunkB28 = CreateChunk(BuildRow({28, 1, 1, 42}), BuildRow({28, 1, 3, 42}), 1, 1_KB, TLegacyKey(), BuildRow({28, 1, 2, 46}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA28);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB28);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with and without read limits, read limits shorter than PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // No/No (after shortening one chunks will be intersecting).
    //                0              ||              1              ||              2               <- 1-st component is shown here
    //   ...  |  0,0  |  0,1  |  ... || ...  |  1,0  |  1,1  |  ... || ...  |  2,0  |  2,1  |  ...  <- 1-st and 2-nd component are shown here
    // _____________________==========[======================]______________________________________
    // _______________[==============================]______________________________________________

    auto chunkA29 = CreateChunk(BuildRow({29, 0, 1}), BuildRow({29, 1, 1}), 0, 1_KB, BuildRow({29, 1}));
    auto chunkB29 = CreateChunk(BuildRow({29, 0, 1}), BuildRow({29, 1, 0}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA29);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB29);

    // No/Yes (after shortening one chunks will be intersecting).
    //                0              ||              1              ||              2               <- 1-st component is shown here
    //   ...  |  0,0  |  0,1  |  ... || ...  |  1,0  |  1,1  |  ... || ...  |  2,0  |  2,1  |  ...  <- 1-st and 2-nd component are shown here
    // ______________________________________[========================)==============________________
    // _____________________________________________________________________[=======]________________

    auto chunkA30 = CreateChunk(BuildRow({30, 1, 0}), BuildRow({30, 2, 1}), 0, 1_KB, TLegacyKey(), BuildRow({30, 2}));
    auto chunkB30 = CreateChunk(BuildRow({30, 2, 0}), BuildRow({30, 2, 0}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA30);

    CreateChunkPool();

    for (const auto& unversionedInputChunk : CreatedUnversionedPrimaryChunks_) {
        AddChunk(unversionedInputChunk);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, UnorderedElementsAreArray({
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

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, SortedMergeSimple)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({3}), 2);
    auto chunkBSlices = SliceUnversionedChunk(chunkB, {BuildRow({3}), BuildRow({6})}, {1_KB / 4, 1_KB / 2, 1_KB / 4});
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);
    CurrentMock().RegisterSliceableUnversionedChunk(chunkB, chunkBSlices);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkC);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, SortedMergeWithPersistBeforeFinish)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({3}), 2);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    PersistAndRestore();

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());
    EXPECT_EQ(3u, stripeLists.front()->Stripes.size());
}

TEST_F(TSortedChunkPoolTest, SortedMergeSimpleWithGenericInputStreamDirectory)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({3}), 2);
    auto chunkBSlices = SliceUnversionedChunk(chunkB, {BuildRow({3}), BuildRow({6})}, {1_KB / 4, 1_KB / 2, 1_KB / 4});
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);
    CurrentMock().RegisterSliceableUnversionedChunk(chunkB, chunkBSlices);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkC);

    CreateChunkPool(true /*useGenericInputStreamDirectory*/);

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, SlicingManiacs1)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    MaxDataSlicesPerJob_ = 3;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({5}), 0);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);
    std::vector<TInputChunkPtr> maniacChunksB;
    for (int i = 0; i < 100; i++) {
        maniacChunksB.emplace_back(CreateChunk(BuildRow({3}), BuildRow({3}), 1));
        CurrentMock().RegisterTriviallySliceableUnversionedChunk(maniacChunksB.back());
    }

    CreateChunkPool();

    AddChunk(chunkA);
    for (const auto& chunkB : maniacChunksB) {
        AddChunk(chunkB);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    // In an ideal world we would've split all this stuff into (100 + 2) / 3 == 34 jobs.
    // Since our implementation is not perfect, we ensure that there is at least 34 jobs
    // and at most 100 / 2 + 2
    EXPECT_LE((100 + 2) / 3, std::ssize(stripeLists));
    EXPECT_LE(std::ssize(stripeLists), 100 / 2 + 2);

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, SlicingManiacs2)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
        );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    MaxDataSlicesPerJob_ = 3;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({5}), 0);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);
    std::vector<TInputChunkPtr> maniacChunksB;
    for (int i = 0; i < 100; i++) {
        maniacChunksB.emplace_back(CreateChunk(BuildRow({3}), BuildRow({3}), 1));
        CurrentMock().RegisterTriviallySliceableUnversionedChunk(maniacChunksB.back());
    }

    auto chunkBTail = CreateChunk(BuildRow({3}), BuildRow({4}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkBTail);

    CreateChunkPool();

    AddChunk(chunkA);
    for (const auto& chunkB : maniacChunksB) {
        AddChunk(chunkB);
    }
    AddChunk(chunkBTail);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    // In an ideal world we would've split all this stuff into (100 + 2) / 3 == 34 jobs.
    // Since our implementation is not perfect, we ensure that there is at least 34 jobs
    // and at most 100 / 2 + 2
    EXPECT_LE((100 + 2) / 3, std::ssize(stripeLists));
    EXPECT_LE(std::ssize(stripeLists), 100 / 2 + 2);

    CheckEverything(stripeLists);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedChunkPoolTest, SortedReduceSimple)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    MaxDataSlicesPerJob_ = 1;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({0, 1}), BuildRow({2, 2}), 0);
    auto chunkB = CreateChunk(BuildRow({2, 6}), BuildRow({5, 8}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    ASSERT_EQ(2u, stripeLists.size());
    // At least one stripe list should be responsible for the shared key {2}.
    EXPECT_TRUE(
        (stripeLists[0]->Stripes[0] && stripeLists[0]->Stripes[1]) ||
        (stripeLists[0]->Stripes[1] && stripeLists[1]->Stripes[1]));

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, ReturnNewDataSlices)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    Options_.ReturnNewDataSlices = true;
    MaxDataSlicesPerJob_ = 1;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({0}), BuildRow({2}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({5}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    ASSERT_EQ(2u, stripeLists.size());

    // At least one stripe list should be responsible for the shared key {2}.
    bool anyCovers2 = false;
    for (const auto& stripeList : stripeLists) {
        if (stripeList->Stripes.size() == 2u) {
            anyCovers2 = true;
        }
    }
    EXPECT_TRUE(anyCovers2);

    for (const auto& stripeList : stripeLists) {
        for (const auto& stripe : stripeList->Stripes) {
            for (const auto& dataSlice : stripe->DataSlices) {
                EXPECT_TRUE(!dataSlice->IsLegacy);
            }
        }
    }
}

TEST_F(TSortedChunkPoolTest, SortedReduceManiacs)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({0, 1}), BuildRow({2, 9}), 0);
    auto chunkB = CreateChunk(BuildRow({2, 6}), BuildRow({2, 8}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, SortedReduceAllKindOfTeleports)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false, false, true} /*isForeign*/,
        {true, true, false} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 3;
    Options_.SortedJobOptions.ForeignPrefixLength = 3;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

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
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA2);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB2);

    // No (they partially intersect).
    // [===]__
    // __[===]
    auto chunkA3 = CreateChunk(BuildRow({3, 1, 0}), BuildRow({3, 1, 2}), 0);
    auto chunkB3 = CreateChunk(BuildRow({3, 1, 1}), BuildRow({3, 1, 4}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA3);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB3);

    // No (one contained in another).
    // [====]__
    // _[==]___
    auto chunkA4 = CreateChunk(BuildRow({4, 1, 0}), BuildRow({4, 1, 3}), 0);
    auto chunkB4 = CreateChunk(BuildRow({4, 1, 1}), BuildRow({4, 1, 2}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA4);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB4);

    // No (single_key one contained in another).
    // [====]__
    // __[]____
    auto chunkA5 = CreateChunk(BuildRow({5, 1, 0}), BuildRow({5, 1, 3}), 0);
    auto chunkB5 = CreateChunk(BuildRow({5, 1, 1}), BuildRow({5, 1, 1}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA5);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB5);

    // No (they coincide).
    // [===]__
    // [===]__
    auto chunkA6 = CreateChunk(BuildRow({6, 1, 0}), BuildRow({6, 1, 3}), 0);
    auto chunkB6 = CreateChunk(BuildRow({6, 1, 0}), BuildRow({6, 1, 3}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA6);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB6);

    // No (one covers another).
    // [===]__
    // [====]_
    auto chunkA7 = CreateChunk(BuildRow({7, 1, 0}), BuildRow({7, 1, 3}), 0);
    auto chunkB7 = CreateChunk(BuildRow({7, 1, 0}), BuildRow({7, 1, 4}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA7);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB7);

    // No (one covers another).
    // _[===]__
    // [====]__
    auto chunkA8 = CreateChunk(BuildRow({8, 1, 0}), BuildRow({8, 1, 4}), 0);
    auto chunkB8 = CreateChunk(BuildRow({8, 1, 1}), BuildRow({8, 1, 4}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA8);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB8);

    // No (single-key is located exactly at the max boundary key of another).
    // [===]__
    // ___[]__
    auto chunkA9 = CreateChunk(BuildRow({9, 1, 0}), BuildRow({9, 1, 4}), 0);
    auto chunkB9 = CreateChunk(BuildRow({9, 1, 4}), BuildRow({9, 1, 4}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA9);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB9);

    // No (single-key is located exactly at the min boundary key of another).
    // [===]__
    // []_____
    auto chunkA10 = CreateChunk(BuildRow({10, 1, 0}), BuildRow({10, 1, 4}), 0);
    auto chunkB10 = CreateChunk(BuildRow({10, 1, 0}), BuildRow({10, 1, 0}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA10);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB10);

    // No (single-key chunks coincide).
    // _[]___
    // _[]___
    auto chunkA11 = CreateChunk(BuildRow({11, 1, 4}), BuildRow({11, 1, 4}), 0);
    auto chunkB11 = CreateChunk(BuildRow({11, 1, 4}), BuildRow({11, 1, 4}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA11);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB11);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with read limits, keys of length exactly PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // Yes/No (non-trivial lower limit).
    // NB: chunkB12 may not be teleported because it has non-trivial read limits.
    // _[==]_____
    // ___===[==]
    auto chunkA12 = CreateChunk(BuildRow({12, 1, 0}), BuildRow({12, 1, 4}), 0);
    auto chunkB12 = CreateChunk(BuildRow({12, 1, 2}), BuildRow({12, 1, 8}), 1, 1_KB, BuildRow({12, 1, 5}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB12);

    // No/No (non-trivial lower limit coinciding with max key).
    // _[==]_____
    // ___=[====]
    auto chunkA13 = CreateChunk(BuildRow({13, 1, 0}), BuildRow({13, 1, 4}), 0);
    auto chunkB13 = CreateChunk(BuildRow({13, 1, 2}), BuildRow({13, 1, 8}), 1, 1_KB, BuildRow({13, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA13);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB13);

    // No/No (they partially intersect with each other).
    // _[===]____
    // ___=[===]_
    auto chunkA14 = CreateChunk(BuildRow({14, 1, 0}), BuildRow({14, 1, 4}), 0);
    auto chunkB14 = CreateChunk(BuildRow({14, 1, 2}), BuildRow({14, 1, 8}), 1, 1_KB, BuildRow({14, 1, 3}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA14);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB14);

    // No/No (second one is de-facto single-key coinciding with the max-key of the first one).
    // _[===]____
    // ___=[]____
    auto chunkA15 = CreateChunk(BuildRow({15, 1, 0}), BuildRow({15, 1, 4}), 0);
    auto chunkB15 = CreateChunk(BuildRow({15, 1, 2}), BuildRow({15, 1, 4}), 1, 1_KB, BuildRow({15, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA15);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB15);

    // Yes/No (non-trivial upper limit).
    // ______[===]_
    // _[==)===____
    auto chunkA16 = CreateChunk(BuildRow({16, 1, 4}), BuildRow({16, 1, 8}), 0);
    auto chunkB16 = CreateChunk(BuildRow({16, 1, 0}), BuildRow({16, 1, 6}), 1, 1_KB, TLegacyKey(), BuildRow({16, 1, 3}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB16);

    // Yes/No (non-trivial upper limit).
    // ____[===]_
    // _[==)===__
    auto chunkA17 = CreateChunk(BuildRow({17, 1, 4}), BuildRow({17, 1, 8}), 0);
    auto chunkB17 = CreateChunk(BuildRow({17, 1, 0}), BuildRow({17, 1, 6}), 1, 1_KB, TLegacyKey(), BuildRow({17, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB17);

    // No/No (non-trivial upper limit).
    // ____[===]_
    // _[====)=__
    auto chunkA18 = CreateChunk(BuildRow({18, 1, 4}), BuildRow({18, 1, 8}), 0);
    auto chunkB18 = CreateChunk(BuildRow({18, 1, 0}), BuildRow({18, 1, 6}), 1, 1_KB, TLegacyKey(), BuildRow({18, 1, 5}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA18);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB18);

    // No/No (first one is single-key touching the second one with non-trivial lower limit).
    // __[]_______
    // ===[==)____
    auto chunkA19 = CreateChunk(BuildRow({19, 1, 4}), BuildRow({19, 1, 4}), 0);
    auto chunkB19 = CreateChunk(BuildRow({19, 1, 0}), BuildRow({19, 1, 6}), 1, 1_KB, BuildRow({19, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA19);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB19);

    // Yes/No (first one is single-key touching the second one with non-trivial upper limit).
    // _____[]___
    // ___[==)===_
    auto chunkA20 = CreateChunk(BuildRow({20, 1, 4}), BuildRow({20, 1, 4}), 0);
    auto chunkB20 = CreateChunk(BuildRow({20, 1, 0}), BuildRow({20, 1, 6}), 1, 1_KB, TLegacyKey(), BuildRow({20, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB20);

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
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA22);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB22);

    // No (after shortening chunks will be intersecting).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // __________________[================]___________________

    auto chunkA23 = CreateChunk(BuildRow({23, 1, 1, 42}), BuildRow({23, 1, 3, 42}), 0);
    auto chunkB23 = CreateChunk(BuildRow({23, 1, 2, 42}), BuildRow({23, 1, 4, 46}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA23);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB23);

    // No (after shortening one of the chunks will be single-key touching the max-key).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // __________________________[==]_________________________

    auto chunkA24 = CreateChunk(BuildRow({24, 1, 1, 42}), BuildRow({24, 1, 3, 42}), 0);
    auto chunkB24 = CreateChunk(BuildRow({24, 1, 3, 42}), BuildRow({24, 1, 4, 42}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA24);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB24);

    // No (after shortening one of the chunks will be single-key touching the min-key).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // _________[==]__________________________________________

    auto chunkA25 = CreateChunk(BuildRow({25, 1, 1, 42}), BuildRow({25, 1, 3, 42}), 0);
    auto chunkB25 = CreateChunk(BuildRow({25, 1, 1, 42}), BuildRow({25, 1, 1, 42}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA25);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB25);

    // No (after shortening both chunks will be coinciding and single-key).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ________________[==]___________________________________
    // _________________[===]_________________________________

    auto chunkA26 = CreateChunk(BuildRow({26, 1, 2, 42}), BuildRow({26, 1, 2, 42}), 0);
    auto chunkB26 = CreateChunk(BuildRow({26, 1, 2, 42}), BuildRow({26, 1, 2, 42}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA26);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB26);

    // No/No (after shortening one of the chunks will be single-key touching the min-key with non-trivial read limits).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // _________[==)======____________________________________

    auto chunkA27 = CreateChunk(BuildRow({27, 1, 1, 42}), BuildRow({27, 1, 3, 42}), 0);
    auto chunkB27 = CreateChunk(BuildRow({27, 1, 1, 42}), BuildRow({27, 1, 2, 42}), 1, 1_KB, TLegacyKey(), BuildRow({27, 1, 1, 46}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA27);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB27);

    // No/No (after shortening chunks will be intersecting).
    //    0   |   1   |   2   |   3   |   4   |   5   |   6
    // ___________[===============]___________________________
    // _________[========)======______________________________

    auto chunkA28 = CreateChunk(BuildRow({28, 1, 1, 42}), BuildRow({28, 1, 3, 42}), 0);
    auto chunkB28 = CreateChunk(BuildRow({28, 1, 1, 42}), BuildRow({28, 1, 3, 42}), 1, 1_KB, TLegacyKey(), BuildRow({28, 1, 2, 46}));
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA28);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB28);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with and without read limits, read limits shorter than PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // No/No (after shortening one chunks will be intersecting).
    //                0              ||              1              ||              2               <- 1-st component is shown here
    //   ...  |  0,0  |  0,1  |  ... || ...  |  1,0  |  1,1  |  ... || ...  |  2,0  |  2,1  |  ...  <- 1-st and 2-nd component are shown here
    // _____________________==========[======================]______________________________________
    // _______________[==============================]______________________________________________

    auto chunkA29 = CreateChunk(BuildRow({29, 0, 1}), BuildRow({29, 1, 1}), 0, 1_KB, BuildRow({29, 1}));
    auto chunkB29 = CreateChunk(BuildRow({29, 0, 1}), BuildRow({29, 1, 0}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA29);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB29);

    // No/No (after shortening one chunks will be intersecting).
    //                0              ||              1              ||              2               <- 1-st component is shown here
    //   ...  |  0,0  |  0,1  |  ... || ...  |  1,0  |  1,1  |  ... || ...  |  2,0  |  2,1  |  ...  <- 1-st and 2-nd component are shown here
    // ______________________________________[========================)==============________________
    // _____________________________________________________________________[=======]________________

    auto chunkA30 = CreateChunk(BuildRow({30, 1, 0}), BuildRow({30, 2, 1}), 0, 1_KB, TLegacyKey(), BuildRow({30, 2}));
    auto chunkB30 = CreateChunk(BuildRow({30, 2, 0}), BuildRow({30, 2, 0}), 1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA30);

    // Yes (primary and foreign chunks are non-intersecting).
    // [==]_____
    // _____[==]
    auto chunkA31 = CreateChunk(BuildRow({31, 1, 0}), BuildRow({31, 1, 2}), 0);
    auto chunkC31 = CreateChunk(BuildRow({31, 1, 3}), BuildRow({31, 1, 5}), 2);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkC31);

    // No (primary and foreign chunk share only one boundary key).
    // [==]___
    // ___[==]
    auto chunkA32 = CreateChunk(BuildRow({32, 1, 0}), BuildRow({32, 1, 2}), 0);
    auto chunkC32 = CreateChunk(BuildRow({32, 1, 2}), BuildRow({32, 1, 4}), 2);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA32);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkC32);

    // No (single-key primary and foreign chunks coincide).
    // _[]___
    // _[]___
    auto chunkA33 = CreateChunk(BuildRow({33, 1, 4}), BuildRow({33, 1, 4}), 0);
    auto chunkC33 = CreateChunk(BuildRow({33, 1, 4}), BuildRow({33, 1, 4}), 2);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA33);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkC33);

    CreateChunkPool();

    for (const auto& chunk : CreatedUnversionedPrimaryChunks_) {
        AddChunk(chunk);
    }
    for (const auto& chunk : CreatedUnversionedForeignChunks_) {
        AddChunk(chunk);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, UnorderedElementsAreArray({
        chunkA1, chunkB1,
        chunkA12,
        chunkA16,
        chunkA17,
        chunkA20,
        chunkA21, chunkB21,
        chunkB30,
        chunkA31,
    }));

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, SortedReduceWithJoin)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {true, true, false, false} /*isForeign*/,
        {false, false, false, false} /*isTeleportable*/,
        {false, false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 2;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1, 21}), BuildRow({4, 24}), 0);
    auto chunkB = CreateChunk(BuildRow({2, 62}), BuildRow({4, 64}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 101, 11}), BuildRow({4, 402, 18}), 2);
    auto chunkD = CreateChunk(BuildRow({1, 102, 42}), BuildRow({4, 402, 48}), 3);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkC);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkD);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);
    AddChunk(chunkD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, JoinReduce)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {true, true, false, false} /*isForeign*/,
        {false, false, false, false} /*isTeleportable*/,
        {false, false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 2;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1, 21}), BuildRow({4, 24}), 0);
    auto chunkB = CreateChunk(BuildRow({2, 62}), BuildRow({4, 64}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 101, 11}), BuildRow({4, 402, 18}), 2);
    auto chunkD = CreateChunk(BuildRow({1, 102, 42}), BuildRow({4, 402, 48}), 3);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkC);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkD);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);
    AddChunk(chunkD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, ManiacIsSliced)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    MaxDataSlicesPerJob_ = 1;
    InputSliceDataWeight_ = 10;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1, 2}), BuildRow({1, 42}), 0);
    chunkA->SetTotalRowCount(10000);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);

    CreateChunkPool();

    AddChunk(chunkA);

    ChunkPool_->Finish();
    EXPECT_GE(ChunkPool_->GetJobCounter()->GetPending(), 100 / 2);
}

TEST_F(TSortedChunkPoolTest, MaxTotalSliceCountExceeded)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {false, false, false} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.SortedJobOptions.MaxTotalSliceCount = 6;
    DataSizePerJob_ = 1000;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({1}), BuildRow({3}), 1);
    auto chunkC1 = CreateChunk(BuildRow({1}), BuildRow({1}), 2);
    auto chunkC2 = CreateChunk(BuildRow({2}), BuildRow({2}), 2);
    auto chunkC3 = CreateChunk(BuildRow({3}), BuildRow({3}), 2);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC1);
    AddChunk(chunkC2);
    AddChunk(chunkC3);

    EXPECT_THROW(ChunkPool_->Finish(), std::exception);
}

TEST_F(TSortedChunkPoolTest, MaxTotalSliceCountRetries)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {false, false, false} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.SortedJobOptions.MaxTotalSliceCount = 6;
    MaxBuildRetryCount_ = 5;
    DataSizePerJob_ = 1000;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({1}), BuildRow({3}), 1);
    auto chunkC1 = CreateChunk(BuildRow({1}), BuildRow({1}), 2);
    auto chunkC2 = CreateChunk(BuildRow({2}), BuildRow({2}), 2);
    auto chunkC3 = CreateChunk(BuildRow({3}), BuildRow({3}), 2);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC1);
    AddChunk(chunkC2);
    AddChunk(chunkC3);

    ChunkPool_->Finish();
}

TEST_F(TSortedChunkPoolTest, TestJobInterruption)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false, true} /*isForeign*/,
        {false, false, false, false} /*isTeleportable*/,
        {false, false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({20}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({42}), 1);
    auto chunkC = CreateChunk(BuildRow({10}), BuildRow({12}), 2);
    auto chunkD = CreateChunk(BuildRow({1}), BuildRow({42}), 3);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkC);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkD);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);
    AddChunk(chunkD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    ASSERT_EQ(stripeLists.size(), 1u);
    ASSERT_EQ(ExtractedCookies_.size(), 1u);

    const auto& stripeList = stripeLists[0];
    std::vector<TLegacyDataSlicePtr> unreadDataSlices = {
        CreateInputDataSlice(GetStripeByTableIndex(stripeList, 0)->DataSlices.front(), BuildRow({13})),
        CreateInputDataSlice(GetStripeByTableIndex(stripeList, 1)->DataSlices.front(), BuildRow({14})),
    };
    TCompletedJobSummary jobSummary;
    jobSummary.InterruptReason = EInterruptReason::Preemption;
    jobSummary.UnreadInputDataSlices = unreadDataSlices;
    ChunkPool_->Completed(ExtractedCookies_.front(), jobSummary);

    ExtractOutputCookiesWhilePossible();
    ASSERT_EQ(ExtractedCookies_.size(), 2u);
    auto newStripeList = ChunkPool_->GetStripeList(ExtractedCookies_.back());
    ASSERT_EQ(newStripeList->Stripes.size(), 3u);
    ASSERT_EQ(GetStripeByTableIndex(newStripeList, 0)->DataSlices.size(), 1u);
    ASSERT_EQ(GetStripeByTableIndex(newStripeList, 0)->DataSlices
                .front()
                ->LegacyLowerLimit().Key, BuildRow({13}));
    ASSERT_EQ(GetStripeByTableIndex(newStripeList, 1)->DataSlices.size(), 1u);
    ASSERT_EQ(GetStripeByTableIndex(newStripeList, 1)->DataSlices
                .front()
                ->LegacyLowerLimit().Key, BuildRow({14}));
    ASSERT_EQ(GetStripeByTableIndex(newStripeList, 3)->DataSlices.size(), 1u);
}

TEST_F(TSortedChunkPoolTest, TestJobSplitSimple)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    DataSizePerJob_ = Inf64;
    InitJobConstraints();
    PrepareNewMock();

    const int chunkCount = 100;
    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
        CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunk);
    }

    CreateChunkPool();

    for (const auto& chunk : CreatedUnversionedPrimaryChunks_) {
        AddChunk(chunk);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();
    TCompletedJobSummary jobSummary;
    jobSummary.InterruptReason = EInterruptReason::JobSplit;
    jobSummary.UnreadInputDataSlices = std::vector<TLegacyDataSlicePtr>(
        stripeLists[0]->Stripes[0]->DataSlices.begin(),
        stripeLists[0]->Stripes[0]->DataSlices.end());
    jobSummary.SplitJobCount = 10;
    ChunkPool_->Completed(*OutputCookies_.begin(), jobSummary);

    OutputCookies_.clear();

    ExtractOutputCookiesWhilePossible();
    stripeLists = GetAllStripeLists();
    ASSERT_LE(8u, stripeLists.size());
    ASSERT_LE(stripeLists.size(), 12u);
}

TEST_F(TSortedChunkPoolTest, TestJobSplitWithForeign)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, true} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.SortedJobOptions.ForeignPrefixLength = 1;
    DataSizePerJob_ = Inf64;
    InitJobConstraints();
    PrepareNewMock();

    std::vector<TInputChunkPtr> allChunks;
    const int chunkCount = 100;
    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
        CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunk);
        allChunks.emplace_back(std::move(chunk));
    }

    const int foreignChunkCount = 5;

    for (int index = 0; index < foreignChunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({index * 40}), BuildRow({index * 40 + 39}), 1);
        CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunk);
        allChunks.emplace_back(std::move(chunk));
    }

    CreateChunkPool();

    for (const auto& chunk : allChunks) {
        AddChunk(std::move(chunk));
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();
    TCompletedJobSummary jobSummary;
    jobSummary.InterruptReason = EInterruptReason::JobSplit;
    std::vector<TLegacyDataSlicePtr> unreadSlices;
    unreadSlices.insert(
        unreadSlices.end(),
        GetStripeByTableIndex(stripeLists[0], 0)->DataSlices.begin(),
        GetStripeByTableIndex(stripeLists[0], 0)->DataSlices.end());
    jobSummary.SplitJobCount = 10;
    jobSummary.UnreadInputDataSlices = std::move(unreadSlices);
    ChunkPool_->Completed(*OutputCookies_.begin(), jobSummary);

    OutputCookies_.clear();
    ExtractedCookies_.clear();

    ExtractOutputCookiesWhilePossible();
    stripeLists = GetAllStripeLists();
    ASSERT_LE(8u, stripeLists.size());
    ASSERT_LE(stripeLists.size(), 12u);

    for (const auto& stripeList : stripeLists) {
        ASSERT_EQ(stripeList->Stripes.size(), 2u);
        ASSERT_LE(GetStripeByTableIndex(stripeList, 1)->DataSlices.size(), 2u);
    }
}

TEST_F(TSortedChunkPoolTest, TestJobSplitStripeSuspension)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, true} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.SortedJobOptions.ForeignPrefixLength = 1;
    DataSizePerJob_ = Inf64;
    InitJobConstraints();
    PrepareNewMock();

    std::vector<TInputChunkPtr> allChunks;
    const int chunkCount = 100;
    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
        CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunk);
        allChunks.emplace_back(std::move(chunk));
    }

    const int foreignChunkCount = 5;

    for (int index = 0; index < foreignChunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({index * 40}), BuildRow({index * 40 + 39}), 1);
        CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunk);
        allChunks.emplace_back(std::move(chunk));
    }

    CreateChunkPool();

    for (const auto& chunk : allChunks) {
        AddChunk(std::move(chunk));
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();
    TCompletedJobSummary jobSummary;
    jobSummary.InterruptReason = EInterruptReason::JobSplit;
    std::vector<TLegacyDataSlicePtr> unreadSlices;
    unreadSlices.insert(
        unreadSlices.end(),
        GetStripeByTableIndex(stripeLists[0], 0)->DataSlices.begin(),
        GetStripeByTableIndex(stripeLists[0], 0)->DataSlices.end());
    jobSummary.SplitJobCount = 10;
    jobSummary.UnreadInputDataSlices = std::move(unreadSlices);
    ChunkPool_->Completed(*OutputCookies_.begin(), jobSummary);

    OutputCookies_.clear();

    int pendingJobCount = ChunkPool_->GetJobCounter()->GetPending();
    ASSERT_LE(8, pendingJobCount);
    ASSERT_LE(pendingJobCount, 12);
    SuspendChunk(0);
    ASSERT_EQ(ChunkPool_->GetJobCounter()->GetPending(), pendingJobCount - 1);
    for (int cookie = chunkCount; cookie < chunkCount + foreignChunkCount; ++cookie) {
        SuspendChunk(cookie);
    }
    ASSERT_EQ(0, ChunkPool_->GetJobCounter()->GetPending());
}

TEST_F(TSortedChunkPoolTest, TestCorrectOrderInsideStripe)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    DataSizePerJob_ = Inf64;
    InitJobConstraints();
    PrepareNewMock();

    auto chunk = CreateChunk(BuildRow({10}), BuildRow({20}), 0);
    std::vector<TInputChunkSlicePtr> slices;
    for (int index = 0; index < 100; ++index) {
        slices.emplace_back(New<TInputChunkSlice>(chunk, 0 /*partIndex*/, 10 * index, 10 * (index + 1), 1_KB));
        slices.back()
            ->LegacyLowerLimit().Key = BuildRow({10});
        slices.back()
            ->LegacyUpperLimit().Key = BuildRow({20});
    }
    shuffle(slices.begin(), slices.end(), Gen_);

    CurrentMock().RegisterSliceableUnversionedChunk(chunk, slices);

    CreateChunkPool();

    AddChunk(chunk);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    ASSERT_EQ(ExtractedCookies_.size(), 1u);
    auto stripeList = ChunkPool_->GetStripeList(ExtractedCookies_.back());
    ASSERT_EQ(stripeList->Stripes.size(), 1u);
    const auto& stripe = stripeList->Stripes.front();
    ASSERT_EQ(stripe->DataSlices.size(), 100u);
    for (int index = 0; index + 1 < std::ssize(stripe->DataSlices); ++index) {
        ASSERT_EQ(*stripe->DataSlices[index]->LegacyUpperLimit().RowIndex, *stripe->DataSlices[index + 1]->LegacyLowerLimit().RowIndex);
    }
}

TEST_F(TSortedChunkPoolTest, TestTrickyCase)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false},
        {false},
        {false}
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({100}), BuildRow({100}), 0, 12_KB);
    auto chunkB = CreateChunk(BuildRow({100}), BuildRow({200}), 0, 3_KB);
    auto chunkASlices = SliceUnversionedChunk(chunkA, {BuildRow({100})}, {9_KB, 3_KB}, {500, 500});
    chunkASlices[1]->LegacyLowerLimit().Key = BuildRow({100});
    CurrentMock().RegisterSliceableUnversionedChunk(chunkA, chunkASlices);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();
    ASSERT_EQ(stripeLists.size(), 2u);
    ASSERT_EQ(stripeLists[0]->Stripes.size(), 1u);
    ASSERT_EQ(stripeLists[1]->Stripes.size(), 1u);
    std::vector<TInputChunkPtr> chunkSequence;
    for (const auto& dataSlice : stripeLists[0]->Stripes[0]->DataSlices) {
        chunkSequence.push_back(dataSlice->GetSingleUnversionedChunk());
    }
    for (const auto& dataSlice : stripeLists[1]->Stripes[0]->DataSlices) {
        chunkSequence.push_back(dataSlice->GetSingleUnversionedChunk());
    }
    chunkSequence.erase(std::unique(chunkSequence.begin(), chunkSequence.end()), chunkSequence.end());
    ASSERT_EQ(chunkSequence.size(), 2u);

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, TestTrickyCase2)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false},
        {false},
        {false}
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({100}), BuildRow({100}), 0, 12_KB);
    auto chunkB = CreateChunk(BuildRow({100}), BuildRow({100}), 0, 1_KB / 10);
    auto chunkC = CreateChunk(BuildRow({100}), BuildRow({200}), 0, 3_KB);
    auto chunkASlices = SliceUnversionedChunk(chunkA, {BuildRow({100})}, {9_KB, 3_KB}, {500, 500});
    chunkASlices[1]->LegacyLowerLimit().Key = BuildRow({100});
    CurrentMock().RegisterSliceableUnversionedChunk(chunkA, chunkASlices);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkC);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();
    ASSERT_EQ(stripeLists.size(), 2u);
    ASSERT_EQ(stripeLists[0]->Stripes.size(), 1u);
    ASSERT_EQ(stripeLists[1]->Stripes.size(), 1u);
    std::vector<TInputChunkPtr> chunkSequence;
    for (const auto& dataSlice : stripeLists[0]->Stripes[0]->DataSlices) {
        chunkSequence.push_back(dataSlice->GetSingleUnversionedChunk());
    }
    for (const auto& dataSlice : stripeLists[1]->Stripes[0]->DataSlices) {
        chunkSequence.push_back(dataSlice->GetSingleUnversionedChunk());
    }
    chunkSequence.erase(std::unique(chunkSequence.begin(), chunkSequence.end()), chunkSequence.end());
    ASSERT_EQ(chunkSequence.size(), 3u);

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, TestTrickyCase3)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {true, false, false},
        {false, false, false},
        {false, false, false}
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 2;
    Options_.SortedJobOptions.ForeignPrefixLength = 1;
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({2}), BuildRow({2}), 0, 1_KB / 10);
    auto chunkB = CreateChunk(BuildRow({1, 0}), BuildRow({5, 0}), 1, 100_KB);
    auto chunkC = CreateChunk(BuildRow({2, 1}), BuildRow({2, 2}), 2, 3_KB);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, TestTrickyCase4)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false},
        {false, false, false},
        {false, false, false}
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 3;
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA1 = CreateChunk(BuildRow({133, 1, 1}), BuildRow({133, 3, 3}), 0, 8_KB);
    auto chunkA2 = CreateChunk(BuildRow({133, 3, 3}), BuildRow({133, 3, 3}), 0, 8_KB);
    auto chunkA3 = CreateChunk(BuildRow({133, 5, 5}), BuildRow({133, 9, 9}), 0, 8_KB);
    auto chunkB = CreateChunk(
        BuildRow({0, 0, 0}),
        BuildRow({200, 200, 200}),
        1,
        1_KB,
        BuildRow({133}), BuildRow({133, 1000 /* equivalent to <max> */}));

    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA1);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA2);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkA3);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB);

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkA3);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();

    CheckEverything(stripeLists);

    EXPECT_TRUE(TeleportChunks_.empty());
    ASSERT_EQ(stripeLists.size(), 2u);

    // We assert that chunkB should become split up into two slices that go to each of the jobs.
    for (const auto& stripeList : stripeLists) {
        EXPECT_EQ(stripeList->Stripes.size(), 2u);
    }
}

TEST_F(TSortedChunkPoolTest, TestNoChunkSliceFetcher)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({3}), 2);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolTest, TestStripeListStatisticsAreSet)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({3}), 2);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());

    EXPECT_GT(stripeLists[0]->TotalChunkCount, 0);
    EXPECT_GT(stripeLists[0]->TotalRowCount, 0);
    EXPECT_GT(stripeLists[0]->TotalDataWeight, 0);
}

TEST_F(TSortedChunkPoolTest, TestSeveralSlicesInInputStripe)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    InitJobConstraints();

    auto chunkAA = CreateChunk(BuildRow({1}), BuildRow({1}), 0);
    auto chunkAB = CreateChunk(BuildRow({2}), BuildRow({2}), 0);
    auto chunkBA = CreateChunk(BuildRow({3}), BuildRow({3}), 1);
    auto chunkBB = CreateChunk(BuildRow({4}), BuildRow({4}), 1);

    CreateChunkPool();

    AddMultiChunkStripe({chunkAA, chunkAB});
    AddMultiChunkStripe({chunkBA, chunkBB});

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());
    EXPECT_EQ(2u, stripeLists[0]->Stripes.size());
    EXPECT_EQ(2u, stripeLists[0]->Stripes[0]->DataSlices.size());
    EXPECT_EQ(2u, stripeLists[0]->Stripes[1]->DataSlices.size());
}

TEST_F(TSortedChunkPoolTest, TestPivotKeys1)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    InitJobConstraints();

    auto chunkA1 = CreateChunk(BuildRow({2}), BuildRow({2}), 0);
    auto chunkA2 = CreateChunk(BuildRow({3}), BuildRow({14}), 0);
    auto chunkB1 = CreateChunk(BuildRow({0}), BuildRow({1}), 1);
    auto chunkB2 = CreateChunk(BuildRow({8}), BuildRow({20}), 1);

    Options_.SortedJobOptions.PivotKeys = std::vector<TLegacyKey>{BuildRow({2}), BuildRow({5}), BuildRow({8})};

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkB1);
    AddChunk(chunkB2);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(4u, stripeLists.size());
    EXPECT_EQ(1u, stripeLists[0]->Stripes.size());
    EXPECT_EQ(1u, stripeLists[0]->Stripes[0]->DataSlices.size());
    EXPECT_EQ(1u, stripeLists[1]->Stripes.size());
    EXPECT_EQ(2u, stripeLists[1]->Stripes[0]->DataSlices.size());
    EXPECT_EQ(1u, stripeLists[2]->Stripes.size());
    EXPECT_EQ(1u, stripeLists[2]->Stripes[0]->DataSlices.size());
    EXPECT_EQ(2u, stripeLists[3]->Stripes.size());
    EXPECT_EQ(1u, stripeLists[3]->Stripes[0]->DataSlices.size());
    EXPECT_EQ(1u, stripeLists[3]->Stripes[1]->DataSlices.size());
}

TEST_F(TSortedChunkPoolTest, TestPivotKeys2)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({2}), BuildRow({5}), 0);
    Options_.SortedJobOptions.PivotKeys = std::vector<TLegacyKey>{BuildRow({2}), BuildRow({3}), BuildRow({4}), BuildRow({5})};

    CreateChunkPool();

    AddChunk(chunkA);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(4u, stripeLists.size());
    EXPECT_EQ(1u, stripeLists[0]->Stripes.size());
    EXPECT_EQ(1u, stripeLists[0]->Stripes[0]->DataSlices.size());
    EXPECT_EQ(BuildRow({2}), stripeLists[0]->Stripes[0]->DataSlices[0]->LegacyLowerLimit().Key);
    EXPECT_EQ(1u, stripeLists[1]->Stripes.size());
    EXPECT_EQ(1u, stripeLists[2]->Stripes.size());
    EXPECT_EQ(1u, stripeLists[3]->Stripes.size());
}


TEST_F(TSortedChunkPoolTest, SuspendFinishResumeTest)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({1}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({2}), 0);
    auto chunkC = CreateChunk(BuildRow({3}), BuildRow({3}), 0);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    SuspendChunk(0);
    SuspendChunk(2);

    ChunkPool_->Finish();

    ResumeChunk(0);
    ResumeChunk(2);

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());
    EXPECT_EQ(1u, stripeLists[0]->Stripes.size());
    EXPECT_EQ(3u, stripeLists[0]->Stripes[0]->DataSlices.size());
}

TEST_F(TSortedChunkPoolTest, SliceByPrimaryDataSize)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {true, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    DataSizePerJob_ = 10_KB;
    PrimaryDataWeightPerJob_ = 1_KB;
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    InitJobConstraints();

    std::vector<TInputChunkPtr> chunks;
    for (int index = 0; index < 100; ++index) {
        chunks.push_back(CreateChunk(BuildRow({10 * index}), BuildRow({10 * index + 9}), 0, 1_KB));
    }
    for (int index = 0; index < 10; ++index) {
        chunks.push_back(CreateChunk(BuildRow({10 * 42 + index}), BuildRow({10 * 42 + index}), 1, 1_KB));
    }

    CreateChunkPool();

    for (const auto& chunk : chunks) {
        AddChunk(chunk);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_LE(5u, stripeLists.size());
    EXPECT_GE(20u, stripeLists.size());
}

TEST_F(TSortedChunkPoolTest, ExtractByDataSize)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    DataSizePerJob_ = 1;
    auto chunkA1 = CreateChunk(BuildRow({0}), BuildRow({5}), 0, 10_KB);
    auto chunkA2 = CreateChunk(BuildRow({6}), BuildRow({10}), 0, 5_KB);
    auto chunkA3 = CreateChunk(BuildRow({11}), BuildRow({15}), 0, 15_KB);

    InitJobConstraints();

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkA2);
    AddChunk(chunkA3);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(stripeLists.size(), 3u);
    std::vector<i64> stripeListDataSizes;
    for (auto cookie : ExtractedCookies_) {
        stripeListDataSizes.emplace_back(ChunkPool_->GetStripeList(cookie)->TotalDataWeight);
    }
    EXPECT_GT(stripeListDataSizes[0], stripeListDataSizes[1]);
    EXPECT_GT(stripeListDataSizes[1], stripeListDataSizes[2]);
}

TEST_F(TSortedChunkPoolTest, MaximumDataWeightPerJobViolation)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    MaxDataWeightPerJob_ = 10_KB;
    InitTables(
        {false, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    DataSizePerJob_ = 5_KB;
    auto chunkA1 = CreateChunk(BuildRow({0}), BuildRow({5}), 0, 7_KB);
    auto chunkB1 = CreateChunk(BuildRow({3}), BuildRow({8}), 0, 7_KB);

    InitJobConstraints();

    CreateChunkPool();

    AddChunk(chunkA1);
    AddChunk(chunkB1);

    EXPECT_THROW(ChunkPool_->Finish(), std::exception);
}

TEST_F(TSortedChunkPoolTest, CartesianProductViaJoinReduce)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, true} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.SortedJobOptions.ForeignPrefixLength = 1;
    DataSizePerJob_ = Inf64;
    PrimaryDataWeightPerJob_ = 10_KB;
    std::vector<TInputChunkPtr> chunks;
    for (int index = 0; index < 1000; ++index) {
        chunks.emplace_back(CreateChunk(BuildRow({0}), BuildRow({0}), 0, 1_KB));
        chunks.emplace_back(CreateChunk(BuildRow({0}), BuildRow({0}), 1, 1_KB));
    }

    InitJobConstraints();

    CreateChunkPool();

    for (const auto& chunk : chunks) {
        AddChunk(chunk);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_GE(stripeLists.size(), 90u);
    EXPECT_LE(stripeLists.size(), 110u);
}

TEST_F(TSortedChunkPoolTest, ResetBeforeFinish)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC1 = CreateChunk(BuildRow({0}), BuildRow({2}), 2);
    auto chunkC2 = CreateChunk(BuildRow({2}), BuildRow({3}), 2);
    auto chunkC1Replayed = CreateChunk(BuildRow({0}), BuildRow({1}), 2);
    auto chunkC2Replayed = CreateChunk(BuildRow({1}), BuildRow({3}), 2);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    auto stripeC = CreateStripe({chunkC1, chunkC2});
    auto cookie = ChunkPool_->Add(stripeC);
    auto stripeCReplayed = CreateStripe({chunkC1Replayed, chunkC2Replayed});
    ChunkPool_->Reset(cookie, stripeCReplayed, IdentityChunkMapping);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_EQ(TeleportChunks_, std::vector<TInputChunkPtr>{chunkC1Replayed});
    EXPECT_EQ(1u, stripeLists.size());
}

TEST_F(TSortedChunkPoolTest, TeleportChunkAndShortReadLimits)
{
    // YT-8836.
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 2;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkALeft = CreateChunk(BuildRow({1, 0}), BuildRow({10, 0}), 0, 1_KB, TLegacyKey(), BuildRow({4}));
    auto chunkARight = CreateChunk(BuildRow({1, 0}), BuildRow({10, 0}), 0, 1_KB, BuildRow({5}), TLegacyKey());
    auto chunkB = CreateChunk(BuildRow({4, 2}), BuildRow({4, 2}), 1);

    CreateChunkPool();

    AddChunk(chunkALeft);
    AddChunk(chunkARight);
    AddChunk(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_EQ(1u, TeleportChunks_.size());
    EXPECT_EQ(2u, stripeLists.size());
    EXPECT_EQ(1u, stripeLists[0]->Stripes.size());
    EXPECT_EQ(1u, stripeLists[1]->Stripes.size());
}

TEST_F(TSortedChunkPoolTest, Sampling)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    DataSizePerJob_ = 1;
    SamplingRate_ = 0.5;
    SamplingDataWeightPerJob_ = DataSizePerJob_;
    InitJobConstraints();

    CreateChunkPool();

    TInputChunkPtr chunk42;
    for (int index = 0; index < 100; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
        auto cookie = AddChunk(chunk);
        if (cookie == 42) {
            chunk42 = chunk;
        }
    }

    ChunkPool_->Finish();

    Cerr << "Pending job count: " << ChunkPool_->GetJobCounter()->GetPending() << Endl;
    EXPECT_LE(40, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_GE(60, ChunkPool_->GetJobCounter()->GetPending());

    ResetChunk(42, chunk42);

    Cerr << "Pending job count: " << ChunkPool_->GetJobCounter()->GetPending() << Endl;
    EXPECT_LE(40, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_GE(60, ChunkPool_->GetJobCounter()->GetPending());
}

TEST_F(TSortedChunkPoolTest, SamplingWithEnlarging)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    DataSizePerJob_ = 10_KB;
    SamplingRate_ = 0.5;
    SamplingDataWeightPerJob_ = 1;
    InitJobConstraints();

    CreateChunkPool();

    TInputChunkPtr chunk42;
    for (int index = 0; index < 100; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
        auto cookie = AddChunk(chunk);
        if (cookie == 42) {
            chunk42 = chunk;
        }
    }

    ChunkPool_->Finish();

    Cerr << "Pending job count: " << ChunkPool_->GetJobCounter()->GetPending() << Endl;
    EXPECT_LE(3, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_GE(7, ChunkPool_->GetJobCounter()->GetPending());

    ResetChunk(42, chunk42);

    Cerr << "Pending job count: " << ChunkPool_->GetJobCounter()->GetPending() << Endl;
    EXPECT_LE(3, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_GE(7, ChunkPool_->GetJobCounter()->GetPending());
}

TEST_F(TSortedChunkPoolTest, EnlargingWithTeleportation)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {true, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = 0;
    DataSizePerJob_ = 10_KB;
    SamplingRate_ = 1.0;
    SamplingDataWeightPerJob_ = 10_KB;
    InitJobConstraints();

    CreateChunkPool();

    AddChunk(CreateChunk(BuildRow({5}), BuildRow({5}), 0));
    AddChunk(CreateChunk(BuildRow({0}), BuildRow({1}), 1));
    AddChunk(CreateChunk(BuildRow({8}), BuildRow({9}), 1));

    ChunkPool_->Finish();

    EXPECT_EQ(1u, TeleportChunks_.size());
    EXPECT_EQ(2u, ChunkPool_->GetJobCounter()->GetPending());
}

// YT-9791
TEST_F(TSortedChunkPoolTest, TrickySliceSortOrder)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();

    PrepareNewMock();
    CreateChunkPool();

    auto chunk = CreateChunk(BuildRow({0xA}), BuildRow({0xD}), 0);
    auto chunkSlice1 = CreateInputChunkSlice(chunk);
    chunkSlice1->LegacyLowerLimit().RowIndex = 0;
    chunkSlice1->LegacyUpperLimit().RowIndex = 20;
    chunkSlice1->LegacyLowerLimit().Key = BuildRow({0xA});
    chunkSlice1->LegacyUpperLimit().Key = BuildRow({0xB});
    auto chunkSlice2 = CreateInputChunkSlice(chunk);
    chunkSlice2->LegacyLowerLimit().RowIndex = 0;
    chunkSlice2->LegacyUpperLimit().RowIndex = 20;
    chunkSlice2->LegacyLowerLimit().Key = BuildRow({0xB});
    chunkSlice2->LegacyUpperLimit().Key = BuildRow({0xC});

    CurrentMock().RegisterSliceableUnversionedChunk(chunk, {chunkSlice1, chunkSlice2});

    AddChunk(chunk);

    ChunkPool_->Finish();

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    auto outputCookie = ChunkPool_->Extract(NNodeTrackerClient::TNodeId(0));
    auto stripeList = ChunkPool_->GetStripeList(outputCookie);
    EXPECT_EQ(1u, stripeList->Stripes.size());
    EXPECT_EQ(2u, stripeList->Stripes[0]->DataSlices.size());
    EXPECT_EQ(BuildRow({0xB}), stripeList->Stripes[0]->DataSlices[0]->LegacyUpperLimit().Key);

    std::vector<TLegacyDataSlicePtr> unreadDataSlices = {
        CreateInputDataSlice(stripeList->Stripes[0]->DataSlices[0]),
        CreateInputDataSlice(stripeList->Stripes[0]->DataSlices[1]),
    };
    unreadDataSlices[0]->LegacyLowerLimit().RowIndex = 15;
    TCompletedJobSummary jobSummary;
    jobSummary.InterruptReason = EInterruptReason::Preemption;
    jobSummary.UnreadInputDataSlices = unreadDataSlices;
    ChunkPool_->Completed(outputCookie, jobSummary);

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    outputCookie = ChunkPool_->Extract(NNodeTrackerClient::TNodeId(0));
    stripeList = ChunkPool_->GetStripeList(outputCookie);
    EXPECT_EQ(1u, stripeList->Stripes.size());
    EXPECT_EQ(2u, stripeList->Stripes[0]->DataSlices.size());
    EXPECT_EQ(BuildRow({0xB}), stripeList->Stripes[0]->DataSlices[0]->LegacyUpperLimit().Key);
}

// YTADMINREQ-19334
TEST_F(TSortedChunkPoolTest, TrickySliceSortOrder2)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();

    PrepareNewMock();
    CreateChunkPool();

    auto chunk = CreateChunk(BuildRow({0xA}), BuildRow({0xD}), 0);
    auto chunkSlice1 = CreateInputChunkSlice(chunk);
    chunkSlice1->LegacyLowerLimit().RowIndex = 0;
    chunkSlice1->LegacyUpperLimit().RowIndex = 10;
    chunkSlice1->LegacyLowerLimit().Key = BuildRow({0xA});
    chunkSlice1->LegacyUpperLimit().Key = BuildRow({0xD});
    auto chunkSlice2 = CreateInputChunkSlice(chunk);
    chunkSlice2->LegacyLowerLimit().RowIndex = 10;
    chunkSlice2->LegacyUpperLimit().RowIndex = 20;
    chunkSlice2->LegacyLowerLimit().Key = BuildRow({0xA});
    chunkSlice2->LegacyUpperLimit().Key = BuildRow({0xD});

    CurrentMock().RegisterSliceableUnversionedChunk(chunk, {chunkSlice1, chunkSlice2});

    AddChunk(chunk);

    ChunkPool_->Finish();

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    auto outputCookie = ChunkPool_->Extract(NNodeTrackerClient::TNodeId(0));
    auto stripeList = ChunkPool_->GetStripeList(outputCookie);
    EXPECT_EQ(1u, stripeList->Stripes.size());
    EXPECT_EQ(2u, stripeList->Stripes[0]->DataSlices.size());
    EXPECT_EQ(10u, stripeList->Stripes[0]->DataSlices[0]->LegacyUpperLimit().RowIndex);

    std::vector<TLegacyDataSlicePtr> unreadDataSlices = {
        CreateInputDataSlice(stripeList->Stripes[0]->DataSlices[0]),
        CreateInputDataSlice(stripeList->Stripes[0]->DataSlices[1]),
    };
    unreadDataSlices[0]->LegacyLowerLimit().RowIndex = 5;
    unreadDataSlices[0]->LegacyLowerLimit().Key = BuildRow({0xB});
    TCompletedJobSummary jobSummary;
    jobSummary.InterruptReason = EInterruptReason::Preemption;
    jobSummary.UnreadInputDataSlices = unreadDataSlices;
    ChunkPool_->Completed(outputCookie, jobSummary);

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    outputCookie = ChunkPool_->Extract(NNodeTrackerClient::TNodeId(0));
    stripeList = ChunkPool_->GetStripeList(outputCookie);
    EXPECT_EQ(1u, stripeList->Stripes.size());
    EXPECT_EQ(2u, stripeList->Stripes[0]->DataSlices.size());
    EXPECT_EQ(BuildRow({0xB}), stripeList->Stripes[0]->DataSlices[0]->LegacyLowerLimit().Key);
    EXPECT_EQ(5, stripeList->Stripes[0]->DataSlices[0]->LegacyLowerLimit().RowIndex);
}

TEST_F(TSortedChunkPoolTest, JoinReduceForeignChunkSlicing)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {true, false, false} /*isForeign*/,
        {false, false, false} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 2;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1, 21}), BuildRow({4, 24}), 0);
    auto chunkASlices = SliceUnversionedChunk(chunkA, {BuildRow({3, 22})}, {1_KB / 2, 1_KB / 2});
    CurrentMock().RegisterSliceableUnversionedChunk(chunkA, chunkASlices);
    auto chunkB = CreateChunk(BuildRow({1, 101, 11}), BuildRow({4, 402, 18}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 102, 42}), BuildRow({4, 402, 48}), 2);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkB);
    CurrentMock().RegisterTriviallySliceableUnversionedChunk(chunkC);

    CreateChunkPool();

    AddChunk(chunkA);
    AddChunk(chunkB);
    AddChunk(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    CheckEverything(stripeLists);
    EXPECT_EQ(2u, stripeLists[0]->Stripes[0]->DataSlices.size());
}

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkPoolTestRandomized
    : public WithParamInterface<std::tuple<int, bool>>
    , public TSortedChunkPoolTest
{
public:
    TSortedChunkPoolTestRandomized() = default;

    void SetUp() final
    {
        TSortedChunkPoolTest::SetUp();
        Gen_.seed(get<0>(GetParam()));
    }
};

static constexpr int NumberOfRepeats = 100;

TEST_P(TSortedChunkPoolTestRandomized, JobDataWeightDistribution)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    const int TableCount = 10;
    InitTables(
        std::vector<bool>(TableCount, false) /*isForeign*/,
        std::vector<bool>(TableCount, false) /*isTeleportable*/,
        std::vector<bool>(TableCount, false) /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    Options_.MinTeleportChunkSize = Inf32;

    const int ChunkCount = 50;
    const int MaxEndpoint = 50000;

    i64 totalDataWeight = 0;

    std::vector<TInputChunkPtr> chunks;

    auto initTable = [&, this] (int tableIndex) {
        std::vector<int> endpoints;
        for (int i = 0; i < 2 * ChunkCount; ++i) {
            endpoints.push_back(std::uniform_int_distribution<>(0, MaxEndpoint - 2 * ChunkCount + 1)(Gen_));
        }
        std::sort(endpoints.begin(), endpoints.end());
        for (int i = 0; i < 2 * ChunkCount; ++i) {
            endpoints[i] += i;
        }
        for (int i = 0; i < ChunkCount; ++i) {
            int minKey = endpoints[2 * i];
            int maxKey = endpoints[2 * i + 1];
            int dataWeight = (maxKey - minKey) * 1_KB;
            totalDataWeight += dataWeight;
            auto chunk = CreateChunk(BuildRow({minKey}), BuildRow({maxKey}), tableIndex, dataWeight);
            chunks.emplace_back(std::move(chunk));
        }
    };

    for (int tableIndex = 0; tableIndex < TableCount; ++tableIndex) {
        initTable(tableIndex);
    }

    const int JobCount = 100;

    DataSizePerJob_ = totalDataWeight / JobCount;
    InitJobConstraints();

    CreateChunkPool();

    for (auto& chunk : chunks) {
        AddChunk(std::move(chunk));
    }

    ChunkPool_->Finish();

    Cerr << "Pool created " << ChunkPool_->GetJobCounter()->GetPending() << " jobs" << Endl;

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    Cerr << "Job data weights: [";
    for (const auto& stripeList : stripeLists) {
        Cerr << stripeList->TotalDataWeight << ", ";
    }
    Cerr << "]" << Endl;
}

TEST_P(TSortedChunkPoolTestRandomized, VariousOperationsWithPoolTest)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    Options_.SortedJobOptions.PrimaryPrefixLength = 1;
    DataSizePerJob_ = 1_KB;
    InitJobConstraints();

    constexpr int maxChunkCount = 50;
    constexpr int maxUnderlyingPoolCount = 5;
    constexpr int maxJobLosts = 25;
    constexpr int maxInvalidationCount = 5;

    int chunkCount = std::uniform_int_distribution<>(0, maxChunkCount)(Gen_);

    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
    }

    bool useMultiPool = get<1>(GetParam());
    int underlyingPoolCount = 0;
    std::vector<IPersistentChunkPoolPtr> UnderlyingPools_;
    THashSet<int> pendingUnderlyingPoolIndexes;

    if (useMultiPool) {
        // Multi pool created of several sorted subpools.
        underlyingPoolCount = std::uniform_int_distribution<>(2, maxUnderlyingPoolCount)(Gen_);
        UnderlyingPools_.reserve(underlyingPoolCount);
        for (int poolIndex = 0; poolIndex < underlyingPoolCount; ++poolIndex) {
            UnderlyingPools_.push_back(CreateLegacySortedChunkPool(
                Options_,
                nullptr,
                TInputStreamDirectory(InputTables_)));
            if (poolIndex != 0) {
                pendingUnderlyingPoolIndexes.insert(poolIndex);
            }
        }
        MultiChunkPool_ = CreateMultiChunkPool({UnderlyingPools_[0]});
        ChunkPool_ = MultiChunkPool_;
        ChunkPool_->SubscribeChunkTeleported(
            BIND([this] (TInputChunkPtr teleportChunk, std::any /*tag*/) {
                TeleportChunks_.push_back(std::move(teleportChunk));
            }));
    } else {
        CreateChunkPool();
    }

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

    // All stuff from the IPersistentChunkPoolInput point of view.
    THashMap<TChunkId, IChunkPoolInput::TCookie> chunkIdToInputCookie;
    THashSet<IChunkPoolInput::TCookie> suspendedCookies;
    THashSet<IChunkPoolInput::TCookie> resumedCookies;
    THashSet<TChunkId> suspendedChunks;
    THashSet<TChunkId> resumedChunks;
    // All stuff from the IPersistentChunkPoolOutput point of view.
    THashMap<TChunkId, IChunkPoolOutput::TCookie> chunkIdToOutputCookie;
    THashSet<TChunkId> pendingChunks;
    THashSet<TChunkId> startedChunks;
    THashSet<TChunkId> completedChunks;
    THashSet<TChunkId> lostChunks;
    THashMap<TChunkId, TInputChunkPtr> chunkIdToChunk;

    std::vector<std::vector<TChunkStripePtr>> stripesByPoolIndex;
    if (useMultiPool) {
        stripesByPoolIndex.resize(underlyingPoolCount);
    } else {
        stripesByPoolIndex.resize(1);
    }

    THashMap<TChunkStripePtr, TChunkId> stripeToChunkId;

    for (const auto& chunk : CreatedUnversionedPrimaryChunks_) {
        auto chunkId = chunk->GetChunkId();
        chunkIdToChunk[chunkId] = chunk;
        auto stripe = CreateStripe({chunk});
        YT_VERIFY(stripeToChunkId.emplace(stripe, chunkId).second);
        if (useMultiPool) {
            stripe->PartitionTag = std::uniform_int_distribution<>(0, underlyingPoolCount - 1)(Gen_);
            ChunkIdToUnderlyingPoolIndex_[chunkId] = *stripe->PartitionTag;
            stripesByPoolIndex[*stripe->PartitionTag].push_back(stripe);
        } else {
            stripesByPoolIndex[0].push_back(stripe);
        }
    }

    auto registerStripe = [&] (TChunkStripePtr stripe) {
        auto chunkId = GetOrCrash(stripeToChunkId, stripe);
        auto cookie = ChunkPool_->Add(stripe);
        chunkIdToInputCookie[chunkId] = cookie;
        resumedCookies.insert(cookie);
        resumedChunks.insert(chunkId);
        pendingChunks.insert(chunkId);
        InputCookieToChunkId_[cookie] = chunkId;
    };

    for (const auto& stripe : stripesByPoolIndex[0]) {
        registerStripe(stripe);
    }

    if (useMultiPool) {
        MultiChunkPool_->FinishPool(0);
    } else {
        ChunkPool_->Finish();
    }

    ASSERT_EQ(ChunkPool_->GetJobCounter()->GetPending(), std::ssize(stripesByPoolIndex[0]));

    // Set this to true when debugging locally. It helps a lot to understand what happens.
    constexpr bool EnableDebugOutput = false;
    IOutputStream& Cdebug = EnableDebugOutput ? Cerr : Cnull;

    int invalidationCount = 0;

    auto invalidate = [&] (std::optional<int> underlyingPoolIndex) {
        std::vector<TChunkId> toDeleteInStarted;
        for (const auto& chunkId : startedChunks) {
            if (!underlyingPoolIndex || GetOrCrash(ChunkIdToUnderlyingPoolIndex_, chunkId) == underlyingPoolIndex) {
                pendingChunks.insert(chunkId);
                toDeleteInStarted.push_back(chunkId);
            }
        }

        std::vector<TChunkId> toDeleteInCompleted;
        for (const auto& chunkId : completedChunks) {
            if (!underlyingPoolIndex || GetOrCrash(ChunkIdToUnderlyingPoolIndex_, chunkId) == underlyingPoolIndex) {
                pendingChunks.insert(chunkId);
                toDeleteInCompleted.push_back(chunkId);
            }
        }

        std::vector<TChunkId> toDeleteInChunkIdToInputCookie;
        for (const auto& [chunkId, cookie] : chunkIdToInputCookie) {
            if (!underlyingPoolIndex || GetOrCrash(ChunkIdToUnderlyingPoolIndex_, chunkId) == underlyingPoolIndex) {
                toDeleteInChunkIdToInputCookie.push_back(chunkId);
            }
        }

        std::vector<TChunkId> toDeleteInChunkIdToOutputCookie;
        for (const auto& [chunkId, cookie] : chunkIdToOutputCookie) {
            if (!underlyingPoolIndex || GetOrCrash(ChunkIdToUnderlyingPoolIndex_, chunkId) == underlyingPoolIndex) {
                toDeleteInChunkIdToOutputCookie.push_back(chunkId);
            }
        }

        for (const auto& chunkId : toDeleteInCompleted) {
            Cdebug << Format("Deleting chunk %v from completed", chunkId) << Endl;
            YT_VERIFY(completedChunks.erase(chunkId));
        }

        for (const auto& chunkId : toDeleteInChunkIdToInputCookie) {
            Cdebug << Format("Deleting chunk %v from chunk id to input cookie map", chunkId) << Endl;
            YT_VERIFY(chunkIdToInputCookie.erase(chunkId));
        }

        ++invalidationCount;
        Cdebug << "Invalidating pool" << Endl;
        for (const auto& chunkId : toDeleteInStarted) {
            Cdebug << Format("Aborted chunk %v due to invalidation", chunkId) << Endl;
            auto outputCookie = chunkIdToOutputCookie.at(chunkId);
            ChunkPool_->Aborted(outputCookie, EAbortReason::Unknown);
        }

        for (const auto& chunkId : toDeleteInStarted) {
            Cdebug << Format("Deleting chunk %v from started", chunkId) << Endl;
            YT_VERIFY(startedChunks.erase(chunkId));
        }

        for (const auto& chunkId : toDeleteInChunkIdToOutputCookie) {
            Cdebug << Format("Deleting chunk %v from chunk id to output cookie map", chunkId) << Endl;
            YT_VERIFY(chunkIdToOutputCookie.erase(chunkId));
        }
    };

    int jobLosts = 0;

    while (std::ssize(completedChunks) < chunkCount) {
        EXPECT_FALSE(ChunkPool_->IsCompleted());

        // 0..0 - pool is persisted and restored;
        // 1..19 - chunk is suspended;
        // 20..39 - chunk is resumed;
        // 40..49 - chunk is reset;
        // 50..59 - chunk is extracted;
        // 60..69 - chunk is completed;
        // 70..79 - chunk is failed;
        // 80..89 - chunk is lost.
        // 90..97 - chunk is aborted.
        // 98..99 - add new pool to multi pool if multi pool is used.
        int eventType = dice(Gen_);
        if (eventType <= 0) {
            Cdebug << "Persisting and restoring the pool" << Endl;
            PersistAndRestore();
        } else if (eventType <= 19) {
            if (auto randomElement = chooseRandomElement(resumedCookies)) {
                auto cookie = *randomElement;
                Cdebug << Format("Suspending cookie %v", cookie);
                auto chunkId = InputCookieToChunkId_[cookie];
                YT_VERIFY(chunkId);
                Cdebug << Format(" that corresponds to a chunk %v", chunkId) << Endl;
                ASSERT_TRUE(resumedCookies.erase(cookie));
                ASSERT_TRUE(suspendedCookies.insert(cookie).second);
                ASSERT_TRUE(resumedChunks.erase(chunkId));
                ASSERT_TRUE(suspendedChunks.insert(chunkId).second);
                SuspendChunk(cookie);
            }
        } else if (eventType <= 39) {
            if (auto randomElement = chooseRandomElement(suspendedCookies)) {
                auto cookie = *randomElement;
                Cdebug << Format("Resuming cookie %v", cookie);
                auto chunkId = InputCookieToChunkId_[cookie];
                YT_VERIFY(chunkId);
                Cdebug << Format(" that corresponds to a chunk %v", chunkId) << Endl;
                ASSERT_TRUE(suspendedCookies.erase(cookie));
                ASSERT_TRUE(resumedCookies.insert(cookie).second);
                ASSERT_TRUE(suspendedChunks.erase(chunkId));
                ASSERT_TRUE(resumedChunks.insert(chunkId).second);
                ResumeChunk(cookie);
            }
        } else if (eventType <= 49 && invalidationCount < maxInvalidationCount && std::ssize(completedChunks) > chunkCount / 2) {
            if (auto randomElement = chooseRandomElement(suspendedCookies)) {
                auto cookie = *randomElement;
                Cdebug << Format("Resetting cookie %v", cookie);
                auto chunkId = InputCookieToChunkId_[cookie];
                YT_VERIFY(chunkId);
                Cdebug << Format(" that corresponds to a chunk %v", chunkId) << Endl;
                // TODO(max42): reset to something different.
                const auto& chunk = chunkIdToChunk.at(chunkId);
                ResetChunk(cookie, chunk);
                invalidate(useMultiPool ? std::make_optional(ChunkIdToUnderlyingPoolIndex_[chunkId]) : std::nullopt);
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
        } else if (eventType <= 97) {
            if (auto randomElement = chooseRandomElement(startedChunks)) {
                auto chunkId = *randomElement;
                Cdebug << Format("Failed chunk %v", chunkId) << Endl;
                auto outputCookie = chunkIdToOutputCookie.at(chunkId);
                ASSERT_TRUE(startedChunks.erase(chunkId));
                ASSERT_TRUE(pendingChunks.insert(chunkId).second);
                ChunkPool_->Failed(outputCookie);
            }
        } else /* eventType <= 99 */ {
            if (useMultiPool && !pendingUnderlyingPoolIndexes.empty()) {
                auto poolIndex = *chooseRandomElement(pendingUnderlyingPoolIndexes);
                pendingUnderlyingPoolIndexes.erase(poolIndex);
                Cdebug << Format("Adding pool %v to multi pool", poolIndex) << Endl;
                MultiChunkPool_->AddPool(UnderlyingPools_[poolIndex], poolIndex);
                for (const auto& stripe : stripesByPoolIndex[poolIndex]) {
                    registerStripe(stripe);
                }
                MultiChunkPool_->FinishPool(poolIndex);
            }
        }
    }
    if (useMultiPool) {
        // Add empty underlying pools.
        for (auto poolIndex : pendingUnderlyingPoolIndexes) {
            MultiChunkPool_->AddPool(UnderlyingPools_[poolIndex], poolIndex);
            MultiChunkPool_->FinishPool(poolIndex);
        }
        MultiChunkPool_->Finalize();
    }

    ASSERT_TRUE(ChunkPool_->IsCompleted());
    ASSERT_EQ(ChunkPool_->GetJobCounter()->GetPending(), 0);
    ASSERT_EQ(ChunkPool_->GetDataWeightCounter()->GetTotal(), 1024 * chunkCount);
    ASSERT_EQ(ChunkPool_->GetRowCounter()->GetTotal(), 1000 * chunkCount);
    ASSERT_EQ(ChunkPool_->GetDataSliceCounter()->GetTotal(), chunkCount);
    ASSERT_EQ(std::ssize(completedChunks), chunkCount);
    ASSERT_EQ(std::ssize(pendingChunks), 0);
    ASSERT_EQ(std::ssize(startedChunks), 0);
    ASSERT_EQ(std::ssize(lostChunks), 0);
    ASSERT_EQ(std::ssize(resumedChunks) + std::ssize(suspendedChunks), chunkCount);
    ASSERT_EQ(std::ssize(resumedChunks), std::ssize(resumedCookies));
    ASSERT_EQ(std::ssize(suspendedChunks), std::ssize(suspendedCookies));
}

INSTANTIATE_TEST_SUITE_P(Instantiation200,
    TSortedChunkPoolTestRandomized,
    ::testing::Combine(::testing::Range(0, NumberOfRepeats), ::testing::Bool()));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkPools

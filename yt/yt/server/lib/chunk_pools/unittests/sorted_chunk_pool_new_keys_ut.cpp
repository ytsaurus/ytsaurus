#include "chunk_pools_helpers.h"

#include <yt/yt/server/lib/chunk_pools/mock/chunk_slice_fetcher.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/controller_agent/helpers.h>
#include <yt/yt/server/controller_agent/job_size_constraints.h>
#include <yt/yt/server/controller_agent/operation_controller.h>

#include <yt/yt/server/lib/chunk_pools/input_chunk_mapping.h>
#include <yt/yt/server/lib/chunk_pools/multi_chunk_pool.h>
#include <yt/yt/server/lib/chunk_pools/new_sorted_chunk_pool.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/blob_output.h>
#include <yt/yt/core/misc/phoenix.h>

#include <library/cpp/iterator/functools.h>

#include <util/generic/cast.h>
#include <util/generic/size_literals.h>

#include <util/stream/null.h>

#include <util/system/env.h>

#include <random>

namespace NYT::NChunkPools {
namespace {

using namespace NControllerAgent;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NTableClient;

using NControllerAgent::TCompletedJobSummary;

using namespace ::testing;

////////////////////////////////////////////////////////////////////////////////

static constexpr i32 Inf32 = std::numeric_limits<i32>::max() / 4;
static constexpr i64 Inf64 = std::numeric_limits<i64>::max() / 4;

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkPoolNewKeysTest
    : public Test
{
protected:
    void SetUp() override
    {
        ChunkPool_ = nullptr;
        MultiChunkPool_ = nullptr;
        UnderlyingPools_.clear();
        CreatedUnversionedDataSlices_.clear();
        CreatedUnversionedPrimaryDataSlices_.clear();
        CreatedUnversionedForeignDataSlices_.clear();
        ActiveChunks_.clear();
        InputCookieToChunkIds_.clear();
        RowBuffer_ = New<TRowBuffer>();
        InputTables_.clear();
        OutputCookies_.clear();
        UnversionedTableRowCounts_.clear();
        Options_ = TSortedChunkPoolOptions();
        PrimaryDataWeightPerJob_ = std::numeric_limits<i64>::max() / 4;
        SamplingRate_ = std::nullopt;
        SamplingDataWeightPerJob_ = Inf64;
        SamplingPrimaryDataWeightPerJob_ = Inf64;
        ExtractedCookies_.clear();
        ChunkIdToUnderlyingPoolIndex_.clear();
        TeleportChunks_.clear();
        PrimaryComparator_ = TComparator();
        ForeignComparator_ = TComparator();
        MockBuilders_.clear();
        Fetchers_.clear();

        Options_.MinTeleportChunkSize = Inf64;
        Options_.SliceForeignChunks = true;
        Options_.SortedJobOptions.MaxTotalSliceCount = Inf64;
        Options_.UseNewJobBuilder = true;
        Options_.Logger = GetTestLogger();
        Options_.RowBuffer = RowBuffer_;
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
            0 /*batchRowCount*/,
            InputSliceDataWeight_,
            SamplingRate_,
            SamplingDataWeightPerJob_,
            SamplingPrimaryDataWeightPerJob_,
            MaxBuildRetryCount_);
    }

    struct TMockChunkSliceFetcherBuilder
    {
        TMockChunkSliceFetcherBuilder(TSortedChunkPoolNewKeysTest* owner)
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

        void RegisterSliceableUnversionedDataSlice(const TLegacyDataSlicePtr& dataSlice, std::vector<TInputChunkSlicePtr> slices)
        {
            for (const auto& slice : slices) {
                YT_VERIFY(!slice->IsLegacy);
            }
            auto chunk = dataSlice->GetSingleUnversionedChunk();
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

        void RegisterTriviallySliceableUnversionedDataSlice(const TLegacyDataSlicePtr& dataSlice)
        {
            auto chunkSlices = Owner->SliceUnversionedDataSlice(
                dataSlice,
                {},
                {dataSlice->GetSingleUnversionedChunk()->GetDataWeight()});
            RegisterSliceableUnversionedDataSlice(dataSlice, std::move(chunkSlices));
        }

        ExpectationSet AllChunksAreAdded;
        TStrictMockChunkSliceFetcherPtr ChunkSliceFetcher = New<StrictMock<TMockChunkSliceFetcher>>();
        std::vector<TInputChunkSlicePtr> ChunkSlices;
        TSortedChunkPoolNewKeysTest* Owner;
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

    //! Helper for building key bound. #boolOperator must be one of
    //! {"<", "<=", ">", ">="}.
    TKeyBound BuildBound(const char* boolOperator, std::vector<i64> values)
    {
        return TKeyBound::FromRow(
            BuildRow(values),
            /*isInclusive*/ boolOperator[1] == '=',
            /*isUpper*/ boolOperator[0] == '<');
    }

    TInputChunkPtr CreateChunk(
        const TLegacyKey& minBoundaryKey,
        const TLegacyKey& maxBoundaryKey,
        int tableIndex,
        i64 size = 1_KB,
        i64 rowCount = 1000)
    {
        auto prefixLength = InputTables_[tableIndex].IsForeign()
            ? ForeignComparator_.GetLength()
            : PrimaryComparator_.GetLength();
        YT_VERIFY(static_cast<int>(minBoundaryKey.GetCount()) == prefixLength);
        YT_VERIFY(static_cast<int>(maxBoundaryKey.GetCount()) == prefixLength);

        auto inputChunk = New<TInputChunk>();
        static int ChunkIndex = 1;
        inputChunk->SetChunkId(TChunkId(ChunkIndex++, 0));
        inputChunk->SetCompressedDataSize(size);
        inputChunk->SetTotalUncompressedDataSize(size);
        inputChunk->SetTotalDataWeight(size);

        // NB: YT-7358. We behave similarly to controller here by not employing
        // too long boundary keys. Boundary keys should correspond to the prefix length
        // of a corresponding input stream (primary/foreign).

        TLegacyOwningKey owningMinBoundaryKey;
        TLegacyOwningKey owningMaxBoundaryKey;

        owningMinBoundaryKey = TLegacyOwningKey(minBoundaryKey.FirstNElements(prefixLength));
        owningMaxBoundaryKey = TLegacyOwningKey(maxBoundaryKey.FirstNElements(prefixLength));

        inputChunk->BoundaryKeys() = std::make_unique<TOwningBoundaryKeys>(TOwningBoundaryKeys {
            std::move(owningMinBoundaryKey),
            std::move(owningMaxBoundaryKey)
        });
        inputChunk->SetTableIndex(tableIndex);
        inputChunk->SetTableRowIndex(UnversionedTableRowCounts_[tableIndex]);
        UnversionedTableRowCounts_[tableIndex] += rowCount;

        // inputChunk->{Lower,Upper}Limit() are not used in new sorted pool.
        // XXX(max42): double-check that.

        inputChunk->SetTotalRowCount(rowCount);
        return inputChunk;
    }

    void InitTables(std::vector<bool> isForeign, std::vector<bool> isTeleportable, std::vector<bool> isVersioned)
    {
        YT_VERIFY(isForeign.size() == isTeleportable.size() && isTeleportable.size() == isVersioned.size() && isForeign.size() > 0u);
        for (int index = 0; index < std::ssize(isForeign); ++index) {
            InputTables_.emplace_back(isTeleportable[index], !isForeign[index] /*isPrimary*/, isVersioned[index]);
        }
        UnversionedTableRowCounts_.resize(InputTables_.size(), 0);
    }

    std::vector<TInputChunkSlicePtr> SliceUnversionedDataSlice(
        TLegacyDataSlicePtr dataSlice,
        std::vector<TKeyBound> internalUpperBounds,
        std::vector<i64> sliceSizes = std::vector<i64>(),
        std::vector<i64> sliceRowCounts = std::vector<i64>())
    {
        auto chunk = dataSlice->GetSingleUnversionedChunk();
        if (sliceSizes.empty()) {
            sliceSizes.assign(internalUpperBounds.size() + 1, chunk->GetUncompressedDataSize() / (internalUpperBounds.size() + 1));
            // Fix the first size to fix the error because of integer division.
            sliceSizes[0] += chunk->GetUncompressedDataSize() - (internalUpperBounds.size() + 1) * sliceSizes[0];
        } else {
            YT_VERIFY(internalUpperBounds.size() + 1 == sliceSizes.size());
        }
        if (sliceRowCounts.empty()) {
            sliceRowCounts.assign(internalUpperBounds.size() + 1, chunk->GetRowCount() / (internalUpperBounds.size() + 1));
            sliceRowCounts[0] += chunk->GetRowCount() - (internalUpperBounds.size() + 1) * sliceRowCounts[0];
        } else {
            YT_VERIFY(internalUpperBounds.size() + 1 == sliceSizes.size());
        }

        YT_VERIFY(!InputTables_[chunk->GetTableIndex()].IsVersioned());

        auto lowerBound = dataSlice->LowerLimit().KeyBound;
        i64 currentRow = 0;
        std::vector<TInputChunkSlicePtr> slices;
        for (int index = 0; index <= std::ssize(internalUpperBounds); ++index) {
            auto upperBound = index < std::ssize(internalUpperBounds)
                ? internalUpperBounds[index]
                : dataSlice->UpperLimit().KeyBound;
            auto& chunkSlice = slices.emplace_back(New<TInputChunkSlice>(chunk));
            chunkSlice->TransformToNewKeyless();
            chunkSlice->LowerLimit().KeyBound = lowerBound;
            chunkSlice->UpperLimit().KeyBound = upperBound;

            if (!internalUpperBounds.empty()) {
                chunkSlice->LowerLimit().RowIndex = currentRow;
                currentRow += sliceRowCounts[index];
                chunkSlice->UpperLimit().RowIndex = currentRow;
                slices.back()->OverrideSize(sliceRowCounts[index], sliceSizes[index]);
            }
            lowerBound = upperBound.Invert();
        }
        return slices;
    }

    void CreateChunkPool(bool useGenericInputStreamDirectory = false)
    {
        Options_.SortedJobOptions.PrimaryComparator = PrimaryComparator_;
        Options_.SortedJobOptions.ForeignComparator = ForeignComparator_;

        ChunkPool_ = CreateNewSortedChunkPool(
            Options_,
            !MockBuilders_.empty() ? BuildMockChunkSliceFetcherFactory() : nullptr,
            useGenericInputStreamDirectory ? IntermediateInputStreamDirectory : TInputStreamDirectory(InputTables_));
        ChunkPool_->SubscribeChunkTeleported(
            BIND([this] (TInputChunkPtr teleportChunk, std::any /*tag*/) {
                TeleportChunks_.push_back(std::move(teleportChunk));
            }));
    }

    TLegacyDataSlicePtr CreateUnversionedInputDataSlice(TInputChunkPtr chunk)
    {
        using NYT::NChunkPools::CreateUnversionedInputDataSlice;

        auto dataSlice = CreateUnversionedInputDataSlice(CreateInputChunkSlice(chunk));
        dataSlice->SetInputStreamIndex(chunk->GetTableIndex());
        dataSlice->TransformToNewKeyless();

        const auto& chunkSlice = dataSlice->ChunkSlices[0];
        if (!chunkSlice->LowerLimit().KeyBound) {
            chunkSlice->LowerLimit().KeyBound = TKeyBound::MakeUniversal(/*isUpper*/ false);
        }
        if (!chunkSlice->UpperLimit().KeyBound) {
            chunkSlice->UpperLimit().KeyBound = TKeyBound::MakeUniversal(/*isUpper*/ true);
        }

        auto tableIndex = dataSlice->GetTableIndex();
        const auto& inputTable = InputTables_[tableIndex];
        if (!inputTable.IsVersioned()) {
            if (inputTable.IsForeign()) {
                CreatedUnversionedForeignDataSlices_.push_back(dataSlice);
            } else {
                CreatedUnversionedPrimaryDataSlices_.push_back(dataSlice);
            }
        }

        // NB: we assume that all chunks in our test suite have boundary keys of proper lengths.
        // In real life this is not true, but in not so distant future we are going to introduce
        // abstract data slice which will hopefully hide this fact from chunk pool.

        if (chunk->BoundaryKeys()->MinKey == MinKey()) {
            dataSlice->LowerLimit().KeyBound = TKeyBound::MakeUniversal(/*isUpper*/ false);
        } else {
            dataSlice->LowerLimit().KeyBound = TKeyBound::FromRow(
                chunk->BoundaryKeys()->MinKey,
                /*isInclusive*/ true,
                /*isUpper*/ false);
        }
        if (chunk->BoundaryKeys()->MaxKey == MaxKey()) {
            dataSlice->UpperLimit().KeyBound = TKeyBound::MakeUniversal(/*isUpper*/ true);
        } else {
            dataSlice->UpperLimit().KeyBound = TKeyBound::FromRow(
                chunk->BoundaryKeys()->MaxKey,
                /*isInclusive*/ true,
                /*isUpper*/ true);
        }
        dataSlice->IsTeleportable = true;

        CreatedUnversionedDataSlices_.emplace_back(dataSlice);

        return dataSlice;
    }

    IChunkPoolInput::TCookie AddMultiChunkStripe(std::vector<TInputChunkPtr> chunks)
    {
        std::vector<TLegacyDataSlicePtr> dataSlices;
        for (const auto& [index, chunk] : Enumerate(chunks)) {
            auto& dataSlice = dataSlices.emplace_back(CreateDataSlice(chunk));
            dataSlice->ChunkSlices[0]->SetSliceIndex(index);
        }
        auto stripe = New<TChunkStripe>();
        std::move(dataSlices.begin(), dataSlices.end(), std::back_inserter(stripe->DataSlices));
        return ChunkPool_->Add(stripe);
    }

    TChunkStripePtr CreateStripe(const std::vector<TLegacyDataSlicePtr>& dataSlices)
    {
        auto stripe = New<TChunkStripe>();
        for (const auto& dataSlice : dataSlices) {
            stripe->DataSlices.emplace_back(dataSlice);
            for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                ActiveChunks_.insert(chunkSlice->GetInputChunk()->GetChunkId());
            }
        }
        return stripe;
    }

    TLegacyDataSlicePtr CreateDataSlice(
        const TInputChunkPtr& chunk,
        TKeyBound lowerBound = TKeyBound::MakeUniversal(/*isUpper*/ false),
        TKeyBound upperBound = TKeyBound::MakeUniversal(/*isUpper*/ true))
    {
        auto dataSlice = CreateUnversionedInputDataSlice(chunk);
        dataSlice->SetInputStreamIndex(dataSlice->GetTableIndex());
        const auto& chunkSlice = dataSlice->ChunkSlices[0];

        const auto& comparator = InputTables_[chunk->GetTableIndex()].IsPrimary()
            ? PrimaryComparator_
            : ForeignComparator_;

        dataSlice->IsTeleportable = lowerBound.IsUniversal() && upperBound.IsUniversal();

        if (chunk->BoundaryKeys()->MinKey != MinKey()) {
            dataSlice->LowerLimit().KeyBound = chunkSlice->LowerLimit().KeyBound = comparator.StrongerKeyBound(
                lowerBound,
                TKeyBound::FromRow(chunk->BoundaryKeys()->MinKey, /*isInclusive*/ true, /*isUpper*/ false));
        } else {
            dataSlice->LowerLimit().KeyBound = lowerBound;
        }
        if (chunk->BoundaryKeys()->MaxKey != MaxKey()) {
            dataSlice->UpperLimit().KeyBound = chunkSlice->UpperLimit().KeyBound = comparator.StrongerKeyBound(
                upperBound,
                TKeyBound::FromRow(chunk->BoundaryKeys()->MaxKey, /*isInclusive*/ true, /*isUpper*/ true));
        } else {
            dataSlice->UpperLimit().KeyBound = upperBound;
        }

        return dataSlice;
    }

    TLegacyDataSlicePtr CreateInputDataSlice(const TLegacyDataSlicePtr& dataSlice)
    {
        auto copyDataSlice = NYT::NChunkPools::CreateInputDataSlice(dataSlice);
        copyDataSlice->SetInputStreamIndex(dataSlice->GetInputStreamIndex());
        return copyDataSlice;
    }

    TInputCookie AddDataSlice(TLegacyDataSlicePtr dataSlice)
    {
        auto stripe = CreateStripe({dataSlice});
        auto cookie = ChunkPool_->Add(std::move(stripe));
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            InputCookieToChunkIds_[cookie].push_back(chunkSlice->GetInputChunk()->GetChunkId());
        }
        return cookie;
    }

    TInputCookie AddDataSlice(
        const TInputChunkPtr& chunk,
        TKeyBound lowerBound = TKeyBound::MakeUniversal(/*isUpper*/ false),
        TKeyBound upperBound = TKeyBound::MakeUniversal(/*isUpper*/ true),
        i64 dataWeight = -1,
        int sliceIndex = -1)
    {
        auto dataSlice = CreateDataSlice(chunk, lowerBound, upperBound);
        dataSlice->SetInputStreamIndex(dataSlice->GetTableIndex());
        dataSlice->Type = InputTables_[chunk->GetTableIndex()].IsVersioned()
            ? EDataSourceType::VersionedTable
            : EDataSourceType::UnversionedTable;
        if (dataWeight != -1) {
            dataSlice->ChunkSlices[0]->OverrideSize(dataSlice->GetRowCount(), dataWeight);
        }
        if (sliceIndex != -1) {
            dataSlice->ChunkSlices[0]->SetSliceIndex(sliceIndex);
        }
        return AddDataSlice(std::move(dataSlice));
    }

    void SuspendDataSlice(IChunkPoolInput::TCookie cookie)
    {
        for (auto chunkId : InputCookieToChunkIds_[cookie]) {
            YT_VERIFY(chunkId);
            YT_VERIFY(ActiveChunks_.erase(chunkId));
        }
        ChunkPool_->Suspend(cookie);
    }

    void ResumeDataSlice(IChunkPoolInput::TCookie cookie)
    {
        for (auto chunkId: InputCookieToChunkIds_[cookie]) {
            YT_VERIFY(chunkId);
            ActiveChunks_.insert(chunkId);
        }
        ChunkPool_->Resume(cookie);
    }

    void ResetDataSlice(IChunkPoolInput::TCookie cookie, const TLegacyDataSlicePtr& dataSlice)
    {
        YT_VERIFY(!InputCookieToChunkIds_[cookie].empty());
        // TODO(max42): what is this?
        dataSlice->SetInputStreamIndex(dataSlice->GetTableIndex());
        ChunkPool_->Reset(cookie, New<TChunkStripe>(dataSlice), IdentityChunkMapping);
        InputCookieToChunkIds_[cookie].clear();
        for (const auto& chunkSlice : dataSlice->ChunkSlices) {
            InputCookieToChunkIds_[cookie].push_back(chunkSlice->GetInputChunk()->GetChunkId());
        }
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
        Save(saveContext, UnderlyingPools_);
        saveContext.Finish();
        auto blob = output.Flush();
        ChunkPool_.Reset();

        TMemoryInput input(blob.Begin(), blob.Size());
        TLoadContext loadContext(&input, RowBuffer_, GetCurrentSnapshotVersion());
        Load(loadContext, ChunkPool_);
        Load(loadContext, MultiChunkPool_);
        Load(loadContext, UnderlyingPools_);
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

                const auto& comparator = InputTables_[tableIndex].IsPrimary()
                    ? Options_.SortedJobOptions.PrimaryComparator
                    : Options_.SortedJobOptions.ForeignComparator;

                for (const auto& dataSlice : stripe->DataSlices) {
                    for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                        const auto& inputChunk = chunkSlice->GetInputChunk();
                        EXPECT_EQ(tableIndex, inputChunk->GetTableIndex());
                        auto& chunkSliceCopy = chunkSlicesByInputChunk[inputChunk].emplace_back(
                            CreateInputChunkSlice(*chunkSlice));
                        chunkSliceCopy->LowerLimit().MergeLower(dataSlice->LowerLimit(), comparator);
                        chunkSliceCopy->UpperLimit().MergeUpper(dataSlice->UpperLimit(), comparator);
                    }
                }
            }
        }

        // TODO(max42): this check looks quite sound; maybe extract it into a chunk pool
        // internal validation routine and run always as a sanity check?
        // First check.
        for (const auto& dataSlice : CreatedUnversionedPrimaryDataSlices_) {
            if (teleportChunksSet.contains(dataSlice->GetSingleUnversionedChunk())) {
                continue;
            }
            auto dataSliceLowerBound = dataSlice->LowerLimit().KeyBound;
            auto dataSliceUpperBound = dataSlice->UpperLimit().KeyBound;
            i64 dataSliceLowerRowIndex = dataSlice->LowerLimit().RowIndex.value_or(0);
            i64 dataSliceUpperRowIndex = dataSlice->UpperLimit().RowIndex.value_or(dataSlice->GetRowCount());

            const auto& comparator = InputTables_[dataSlice->GetTableIndex()].IsPrimary()
                ? Options_.SortedJobOptions.PrimaryComparator
                : Options_.SortedJobOptions.ForeignComparator;

            TKeyBound lastLowerBound = TKeyBound::MakeUniversal(/*isUpper*/ false);
            TKeyBound lastUpperBound = dataSliceLowerBound.Invert();
            i64 lastLowerRowIndex = -1;
            i64 lastUpperRowIndex = dataSliceLowerRowIndex;
            auto it = chunkSlicesByInputChunk.find(dataSlice->GetSingleUnversionedChunk());
            ASSERT_TRUE(chunkSlicesByInputChunk.end() != it);
            auto& chunkSlices = it->second;
            for (const auto& chunkSlice : chunkSlices) {
                auto lowerBound = chunkSlice->LowerLimit().KeyBound;
                auto upperBound = chunkSlice->UpperLimit().KeyBound;
                i64 lowerRowIndex = chunkSlice->LowerLimit().RowIndex.value_or(dataSliceLowerRowIndex);
                i64 upperRowIndex = chunkSlice->UpperLimit().RowIndex.value_or(dataSliceUpperRowIndex);

                bool keyBoundsCoincide = lastUpperBound == lowerBound.Invert();
                bool rowIndicesCoincide = lastUpperRowIndex == lowerRowIndex;
                EXPECT_TRUE(keyBoundsCoincide || rowIndicesCoincide);
                EXPECT_GE(comparator.CompareKeyBounds(lastUpperBound, lowerBound), 0);
                EXPECT_GE(lastUpperRowIndex, lowerRowIndex);
                if (!keyBoundsCoincide) {
                    EXPECT_EQ(lastLowerBound, lowerBound);
                    EXPECT_EQ(lastUpperBound, upperBound);
                }
                if (!rowIndicesCoincide) {
                    EXPECT_EQ(lastLowerRowIndex, lowerRowIndex);
                    EXPECT_EQ(lastUpperRowIndex, upperRowIndex);
                }
                lastLowerBound = lowerBound;
                lastUpperBound = upperBound;
                lastLowerRowIndex = lowerRowIndex;
                lastUpperRowIndex = upperRowIndex;
            }

            EXPECT_EQ(lastUpperBound, dataSliceUpperBound);
            EXPECT_EQ(lastUpperRowIndex, dataSliceUpperRowIndex);
        }

        // Second check. Verify some (weak) sort order for versioned data slices.
        auto unversionedDataSliceComparator = [this] (const TLegacyDataSlicePtr& lhs, const TLegacyDataSlicePtr& rhs) {
            auto lhsChunk = lhs->GetSingleUnversionedChunk();
            auto rhsChunk = rhs->GetSingleUnversionedChunk();
            if (lhsChunk != rhsChunk) {
                return lhsChunk->GetTableRowIndex() < rhsChunk->GetTableRowIndex();
            } else {
                return PrimaryComparator_.CompareKeyBounds(lhs->LowerLimit().KeyBound, rhs->LowerLimit().KeyBound) <= 0;
            }
        };
        auto versionedDataSliceComparator = [this] (const TLegacyDataSlicePtr& lhs, const TLegacyDataSlicePtr& rhs) {
            return PrimaryComparator_.CompareKeyBounds(lhs->LowerLimit().KeyBound, rhs->LowerLimit().KeyBound) < 0;
        };

        for (const auto& stripeList : stripeLists) {
            for (const auto& stripe : stripeList->Stripes) {
                ASSERT_TRUE(!stripe->DataSlices.empty());
                int tableIndex = stripe->DataSlices.front()->GetTableIndex();
                if (!InputTables_[tableIndex].IsForeign()) {
                    for (int index = 0; index + 1 < std::ssize(stripe->DataSlices); ++index) {
                        const auto& lhs = stripe->DataSlices[index];
                        const auto& rhs = stripe->DataSlices[index + 1];
                        if (InputTables_[tableIndex].IsVersioned()) {
                            EXPECT_TRUE(versionedDataSliceComparator(lhs, rhs));
                        } else {
                            EXPECT_TRUE(unversionedDataSliceComparator(lhs, rhs));
                        }
                    }
                }
            }
        }
    }

    void CheckSortedOutput(const std::vector<TChunkStripeListPtr>& stripeLists)
    {
        struct TChunkBoundLimit
        {
            TChunkId ChunkId;
            TInputSliceLimit Limit;
        };

        auto checkPrecedence = [&] (const TChunkStripeListPtr& lhs, const TChunkStripeListPtr& rhs) {
            // TODO(max42): it is possible to rewrite this check in linear time, but the
            // implementation is quite nasty even for a quadratic version, so let's stop on it.
            for (const auto& lhsStripe : lhs->Stripes) {
                if (lhsStripe->Foreign) {
                    continue;
                }
                for (const auto& lhsDataSlice : lhsStripe->DataSlices) {
                    for (const auto& rhsStripe : rhs->Stripes) {
                        if (rhsStripe->Foreign) {
                            continue;
                        }
                        for (const auto& rhsDataSlice : rhsStripe->DataSlices) {
                            bool separatedByKeys = false;
                            if (Options_.SortedJobOptions.EnableKeyGuarantee) {
                                separatedByKeys = PrimaryComparator_.IsRangeEmpty(
                                    rhsDataSlice->LowerLimit().KeyBound,
                                    lhsDataSlice->UpperLimit().KeyBound);
                            } else {
                                separatedByKeys = PrimaryComparator_.IsInteriorEmpty(
                                    rhsDataSlice->LowerLimit().KeyBound,
                                    lhsDataSlice->UpperLimit().KeyBound);
                            }
                            if (separatedByKeys) {
                                continue;
                            }
                            bool separatedByRows = false;
                            if (lhsDataSlice->Type == EDataSourceType::UnversionedTable &&
                                rhsDataSlice->Type == EDataSourceType::UnversionedTable)
                            {
                                const auto& lhsChunk = lhsDataSlice->GetSingleUnversionedChunk();
                                const auto& rhsChunk = rhsDataSlice->GetSingleUnversionedChunk();
                                separatedByRows =
                                    lhsChunk == rhsChunk &&
                                    lhsDataSlice->UpperLimit().RowIndex &&
                                    rhsDataSlice->LowerLimit().RowIndex &&
                                    *lhsDataSlice->UpperLimit().RowIndex <= *rhsDataSlice->LowerLimit().RowIndex;
                            }
                            ASSERT_TRUE(separatedByRows || separatedByKeys)
                                << Format("LhsUpperLimit: %v", lhsDataSlice->UpperLimit()) << std::endl
                                << Format("RhsUpperLimit: %v", rhsDataSlice->LowerLimit()) << std::endl;
                            if (!separatedByRows && !separatedByKeys) {
                                // YT_ABORT();
                            }
                        }
                    }
                }
            }
        };

        for (size_t index = 0; index + 1 < stripeLists.size(); ++index) {
            checkPrecedence(stripeLists[index], stripeLists[index + 1]);
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
        YT_VERIFY(Options_.SortedJobOptions.EnableKeyGuarantee);

        const auto& comparator = Options_.SortedJobOptions.PrimaryComparator;

        TKeyBound lastUpperBound = TKeyBound::MakeEmpty(/*isUpper*/ true);
        for (const auto& stripeList : stripeLists) {
            TKeyBound lowerBound = TKeyBound::MakeEmpty(/*isUpper*/ false);
            TKeyBound upperBound = TKeyBound::MakeEmpty(/*isUpper*/ true);
            for (const auto& stripe : stripeList->Stripes) {
                if (InputTables_[stripe->GetTableIndex()].IsForeign()) {
                    continue;
                }
                for (const auto& dataSlice : stripe->DataSlices) {
                    lowerBound = comparator.WeakerKeyBound(lowerBound, dataSlice->LowerLimit().KeyBound);
                    upperBound = comparator.WeakerKeyBound(upperBound, dataSlice->UpperLimit().KeyBound);
                }
            }
            EXPECT_LE(comparator.CompareKeyBounds(lastUpperBound, lowerBound), 0);
            lastUpperBound = upperBound;
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
        CheckNoEmptyStripeLists(stripeLists);
        CheckDataIntegrity(stripeLists);
        CheckSortedOutput(stripeLists);
        TryCheckJobConstraintsSatisfaction(stripeLists);
        CheckTeleportChunks();
        CheckStripeListsContainOnlyActiveChunks();
        CheckForeignStripesAreMarkedAsForeign();
        if (Options_.SortedJobOptions.EnableKeyGuarantee) {
            CheckKeyGuarantee(stripeLists);
        }
    }

    void CheckNoEmptyStripeLists(const std::vector<TChunkStripeListPtr>& stripeLists)
    {
        for (const auto& stripeList : stripeLists) {
            bool hasPrimarySlices = false;
            for (const auto& stripe : stripeList->Stripes) {
                if (stripe->Foreign) {
                    continue;
                }
                hasPrimarySlices |= !stripe->DataSlices.empty();
            }
            ASSERT_TRUE(hasPrimarySlices);
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

    void InitPrimaryComparator(int prefixLength)
    {
        PrimaryComparator_ = TComparator(std::vector<ESortOrder>(prefixLength, ESortOrder::Ascending));
    }

    void InitForeignComparator(int prefixLength)
    {
        ForeignComparator_ = TComparator(std::vector<ESortOrder>(prefixLength, ESortOrder::Ascending));
    }

    IPersistentChunkPoolPtr ChunkPool_;
    IMultiChunkPoolPtr MultiChunkPool_;

    std::vector<IPersistentChunkPoolPtr> UnderlyingPools_;

    //! Vector containing all unversioned data slices that have ever been created in order of their creation.
    std::vector<TLegacyDataSlicePtr> CreatedUnversionedDataSlices_;
    //! All unversioned primary data slices that have ever been created.
    std::vector<TLegacyDataSlicePtr> CreatedUnversionedPrimaryDataSlices_;
    //! All unversioned foreign data slices that have ever been created.
    std::vector<TLegacyDataSlicePtr> CreatedUnversionedForeignDataSlices_;
    //! Set containing all chunks that are added to the pool without being suspended.
    THashSet<TChunkId> ActiveChunks_;

    THashMap<IChunkPoolInput::TCookie, std::vector<TChunkId>> InputCookieToChunkIds_;

    TRowBufferPtr RowBuffer_ = New<TRowBuffer>();

    std::vector<TInputStreamDescriptor> InputTables_;

    THashSet<IChunkPoolOutput::TCookie> OutputCookies_;

    std::vector<int> UnversionedTableRowCounts_;

    TSortedChunkPoolOptions Options_;

    i64 DataSizePerJob_;
    i64 PrimaryDataWeightPerJob_ = std::numeric_limits<i64>::max() / 4;

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

    TComparator PrimaryComparator_;
    TComparator ForeignComparator_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedChunkPoolNewKeysTest, SortedMergeTeleports1)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false, false} /*isForeign*/,
        {true, true, true, true} /*isTeleportable*/,
        {false, false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({0}), BuildRow({1}), 0);
    auto chunkB = CreateChunk(BuildRow({1}), BuildRow({2}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({1}), 2);
    auto chunkD = CreateChunk(BuildRow({1}), BuildRow({2}), 3);
    auto dataSliceD = CreateDataSlice(chunkD, BuildBound(">=", {1}), BuildBound("<=", {1}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceD);

    CreateChunkPool();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);
    AddDataSlice(chunkC);
    AddDataSlice(dataSliceD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, UnorderedElementsAreArray({chunkA, chunkB, chunkC}));
    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, SortedMergeTeleports2)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false, false} /*isForeign*/,
        {false, true, true, true} /*isTeleportable*/,
        {false, false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({0}), BuildRow({1}), 0);
    auto dataSliceA = CreateDataSlice(chunkA);
    auto chunkB = CreateChunk(BuildRow({1}), BuildRow({2}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({1}), 2);
    auto chunkD = CreateChunk(BuildRow({1}), BuildRow({2}), 3, 1_KB);
    auto dataSliceD = CreateDataSlice(chunkD, BuildBound(">=", {1}), BuildBound("<=", {1}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceD);

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    AddDataSlice(chunkB);
    AddDataSlice(chunkC);
    AddDataSlice(dataSliceD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, UnorderedElementsAreArray({chunkB, chunkC}));

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, SortedMergeTeleports3)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({0}), BuildRow({1}), 0);
    auto chunkB = CreateChunk(BuildRow({1}), BuildRow({2}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({1}), 2);

    CreateChunkPool();
    PersistAndRestore();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);
    AddDataSlice(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, UnorderedElementsAreArray({chunkA, chunkB, chunkC}));
    EXPECT_EQ(0u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, SortedMergeTeleports4)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(2);
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({0, 10}), BuildRow({1, 11}), 0);
    auto chunkB = CreateChunk(BuildRow({1, 12}), BuildRow({2, 10}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 10}), BuildRow({1, 13}), 2);
    auto dataSliceA = CreateDataSlice(chunkA);
    auto dataSliceB = CreateDataSlice(chunkB);
    auto dataSliceC = CreateDataSlice(chunkC);

    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceC);

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    AddDataSlice(dataSliceB);
    AddDataSlice(dataSliceC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

// NB(max42): completely getting into this test may take several hours of your life.
// Double-think before reading it :)
TEST_F(TSortedChunkPoolNewKeysTest, SortedMergeAllKindsOfTeleports)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(3);
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
    auto dataSliceA1 = CreateDataSlice(chunkA1);
    auto chunkB1 = CreateChunk(BuildRow({1, 1, 3}), BuildRow({1, 1, 5}), 1);
    auto dataSliceB1 = CreateDataSlice(chunkB1);

    // Yes (they share only one boundary key).
    // [==]___
    // ___[==]
    auto chunkA2 = CreateChunk(BuildRow({2, 1, 0}), BuildRow({2, 1, 2}), 0);
    auto dataSliceA2 = CreateDataSlice(chunkA2);
    auto chunkB2 = CreateChunk(BuildRow({2, 1, 2}), BuildRow({2, 1, 4}), 1);
    auto dataSliceB2 = CreateDataSlice(chunkB2);

    // No (they partially intersect).
    // [===]__
    // __[===]
    auto chunkA3 = CreateChunk(BuildRow({3, 1, 0}), BuildRow({3, 1, 2}), 0);
    auto dataSliceA3 = CreateDataSlice(chunkA3);
    auto chunkB3 = CreateChunk(BuildRow({3, 1, 1}), BuildRow({3, 1, 4}), 1);
    auto dataSliceB3 = CreateDataSlice(chunkB3);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA3);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB3);

    // No (one contained in another).
    // [====]__
    // _[==]___
    auto chunkA4 = CreateChunk(BuildRow({4, 1, 0}), BuildRow({4, 1, 3}), 0);
    auto dataSliceA4 = CreateDataSlice(chunkA4);
    auto chunkB4 = CreateChunk(BuildRow({4, 1, 1}), BuildRow({4, 1, 2}), 1);
    auto dataSliceB4 = CreateDataSlice(chunkB4);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA4);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB4);

    // No (single_key one contained in another).
    // [====]__
    // __[]____
    auto chunkA5 = CreateChunk(BuildRow({5, 1, 0}), BuildRow({5, 1, 3}), 0);
    auto dataSliceA5 = CreateDataSlice(chunkA5);
    auto chunkB5 = CreateChunk(BuildRow({5, 1, 1}), BuildRow({5, 1, 1}), 1);
    auto dataSliceB5 = CreateDataSlice(chunkB5);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA5);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB5);

    // No (they coincide).
    // [===]__
    // [===]__
    auto chunkA6 = CreateChunk(BuildRow({6, 1, 0}), BuildRow({6, 1, 3}), 0);
    auto dataSliceA6 = CreateDataSlice(chunkA6);
    auto chunkB6 = CreateChunk(BuildRow({6, 1, 0}), BuildRow({6, 1, 3}), 1);
    auto dataSliceB6 = CreateDataSlice(chunkB6);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA6);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB6);

    // No (one covers another).
    // [===]__
    // [====]_
    auto chunkA7 = CreateChunk(BuildRow({7, 1, 0}), BuildRow({7, 1, 3}), 0);
    auto dataSliceA7 = CreateDataSlice(chunkA7);
    auto chunkB7 = CreateChunk(BuildRow({7, 1, 0}), BuildRow({7, 1, 4}), 1);
    auto dataSliceB7 = CreateDataSlice(chunkB7);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA7);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB7);

    // No (one covers another).
    // _[===]__
    // [====]__
    auto chunkA8 = CreateChunk(BuildRow({8, 1, 0}), BuildRow({8, 1, 4}), 0);
    auto dataSliceA8 = CreateDataSlice(chunkA8);
    auto chunkB8 = CreateChunk(BuildRow({8, 1, 1}), BuildRow({8, 1, 4}), 1);
    auto dataSliceB8 = CreateDataSlice(chunkB8);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA8);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB8);

    // Yes (single-key is located exactly at the max boundary key of another).
    // [===]__
    // ___[]__
    auto chunkA9 = CreateChunk(BuildRow({9, 1, 0}), BuildRow({9, 1, 4}), 0);
    auto dataSliceA9 = CreateDataSlice(chunkA9);
    auto chunkB9 = CreateChunk(BuildRow({9, 1, 4}), BuildRow({9, 1, 4}), 1);
    auto dataSliceB9 = CreateDataSlice(chunkB9);

    // Yes (single-key is located exactly at the min boundary key of another).
    // [===]__
    // []_____
    auto chunkA10 = CreateChunk(BuildRow({10, 1, 0}), BuildRow({10, 1, 4}), 0);
    auto dataSliceA10 = CreateDataSlice(chunkA10);
    auto chunkB10 = CreateChunk(BuildRow({10, 1, 0}), BuildRow({10, 1, 0}), 1);
    auto dataSliceB10 = CreateDataSlice(chunkB10);

    // Yes (single-key chunks coincide).
    // _[]___
    // _[]___
    auto chunkA11 = CreateChunk(BuildRow({11, 1, 4}), BuildRow({11, 1, 4}), 0);
    auto dataSliceA11 = CreateDataSlice(chunkA11);
    auto chunkB11 = CreateChunk(BuildRow({11, 1, 4}), BuildRow({11, 1, 4}), 1);
    auto dataSliceB11 = CreateDataSlice(chunkB11);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with read limits, keys of length exactly PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // Yes/No (non-trivial lower limit).
    // NB: chunkB12 may not be teleported because it has non-trivial read limits.
    // _[==]_____
    // ___===[==]
    auto chunkA12 = CreateChunk(BuildRow({12, 1, 0}), BuildRow({12, 1, 4}), 0);
    auto dataSliceA12 = CreateDataSlice(chunkA12);
    auto chunkB12 = CreateChunk(BuildRow({12, 1, 2}), BuildRow({12, 1, 8}), 1);
    auto dataSliceB12 = CreateDataSlice(chunkB12, BuildBound(">=", {12, 1, 5}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB12);

    // Yes/No (non-trivial lower limit coinciding with max key).
    // _[==]_____
    // ___=[====]
    auto chunkA13 = CreateChunk(BuildRow({13, 1, 0}), BuildRow({13, 1, 4}), 0);
    auto dataSliceA13 = CreateDataSlice(chunkA13);
    auto chunkB13 = CreateChunk(BuildRow({13, 1, 2}), BuildRow({13, 1, 8}), 1);
    auto dataSliceB13 = CreateDataSlice(chunkB13, BuildBound(">=", {13, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB13);

    // No/No (they partially intersect with each other).
    // _[===]____
    // ___=[===]_
    auto chunkA14 = CreateChunk(BuildRow({14, 1, 0}), BuildRow({14, 1, 4}), 0);
    auto dataSliceA14 = CreateDataSlice(chunkA14);
    auto chunkB14 = CreateChunk(BuildRow({14, 1, 2}), BuildRow({14, 1, 8}), 1);
    auto dataSliceB14 = CreateDataSlice(chunkB14, BuildBound(">=", {14, 1, 3}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA14);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB14);

    // Yes/No (second one is de-facto single-key coinciding with the max-key of the first one).
    // _[===]____
    // ___=[]____
    auto chunkA15 = CreateChunk(BuildRow({15, 1, 0}), BuildRow({15, 1, 4}), 0);
    auto dataSliceA15 = CreateDataSlice(chunkA15);
    auto chunkB15 = CreateChunk(BuildRow({15, 1, 2}), BuildRow({15, 1, 4}), 1);
    auto dataSliceB15 = CreateDataSlice(chunkB15, BuildBound(">=", {15, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB15);

    // Yes/No (non-trivial upper limit).
    // ______[===]_
    // _[==)===____
    auto chunkA16 = CreateChunk(BuildRow({16, 1, 4}), BuildRow({16, 1, 8}), 0);
    auto dataSliceA16 = CreateDataSlice(chunkA16);
    auto chunkB16 = CreateChunk(BuildRow({16, 1, 0}), BuildRow({16, 1, 6}), 1);
    auto dataSliceB16 = CreateDataSlice(chunkB16, BuildBound(">=", {}), BuildBound("<", {16, 1, 3}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB16);

    // Yes/No (non-trivial upper limit).
    // ____[===]_
    // _[==)===__
    auto chunkA17 = CreateChunk(BuildRow({17, 1, 4}), BuildRow({17, 1, 8}), 0);
    auto dataSliceA17 = CreateDataSlice(chunkA17);
    auto chunkB17 = CreateChunk(BuildRow({17, 1, 0}), BuildRow({17, 1, 6}), 1);
    auto dataSliceB17 = CreateDataSlice(chunkB17, BuildBound(">=", {}), BuildBound("<", {17, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB17);

    // No/No (non-trivial upper limit).
    // ____[===]_
    // _[====)=__
    auto chunkA18 = CreateChunk(BuildRow({18, 1, 4}), BuildRow({18, 1, 8}), 0);
    auto dataSliceA18 = CreateDataSlice(chunkA18);
    auto chunkB18 = CreateChunk(BuildRow({18, 1, 0}), BuildRow({18, 1, 6}), 1);
    auto dataSliceB18 = CreateDataSlice(chunkB18, BuildBound(">=", {}), BuildBound("<", {18, 1, 5}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA18);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB18);

    // Yes/No (first one is single-key touching the second one with non-trivial lower limit).
    // __[]_______
    // ===[==)____
    auto chunkA19 = CreateChunk(BuildRow({19, 1, 4}), BuildRow({19, 1, 4}), 0);
    auto dataSliceA19 = CreateDataSlice(chunkA19);
    auto chunkB19 = CreateChunk(BuildRow({19, 1, 0}), BuildRow({19, 1, 6}), 1);
    auto dataSliceB19 = CreateDataSlice(chunkB19, BuildBound(">=", {19, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB19);

    // Yes/No (first one is single-key touching the second one with non-trivial upper limit).
    // _____[]___
    // ___[==)===_
    auto chunkA20 = CreateChunk(BuildRow({20, 1, 4}), BuildRow({20, 1, 4}), 0);
    auto dataSliceA20 = CreateDataSlice(chunkA20);
    auto chunkB20 = CreateChunk(BuildRow({20, 1, 0}), BuildRow({20, 1, 6}), 1);
    auto dataSliceB20 = CreateDataSlice(chunkB20, BuildBound(">=", {}), BuildBound("<", {20, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB20);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with and without read limits, read limits shorter than PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // No/No (after shortening one chunks will be intersecting).
    //                0              ||              1              ||              2               <- 1-st component is shown here
    //   ...  |  0,0  |  0,1  |  ... || ...  |  1,0  |  1,1  |  ... || ...  |  2,0  |  2,1  |  ...  <- 1-st and 2-nd component are shown here
    // _____________________==========[======================]______________________________________
    // _______________[==============================]______________________________________________

    auto chunkA29 = CreateChunk(BuildRow({29, 0, 1}), BuildRow({29, 1, 1}), 0);
    auto dataSliceA29 = CreateDataSlice(chunkA29, BuildBound(">=", {29, 1}));
    auto chunkB29 = CreateChunk(BuildRow({29, 0, 1}), BuildRow({29, 1, 0}), 1);
    auto dataSliceB29 = CreateDataSlice(chunkB29);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA29);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB29);

    // No/Yes (after shortening one chunks will be intersecting).
    //                0              ||              1              ||              2               <- 1-st component is shown here
    //   ...  |  0,0  |  0,1  |  ... || ...  |  1,0  |  1,1  |  ... || ...  |  2,0  |  2,1  |  ...  <- 1-st and 2-nd component are shown here
    // ______________________________________[========================)==============________________
    // _____________________________________________________________________[=======]________________

    auto chunkA30 = CreateChunk(BuildRow({30, 1, 0}), BuildRow({30, 2, 1}), 0, 1_KB);
    auto dataSliceA30 = CreateDataSlice(chunkA30, BuildBound(">=", {}), BuildBound("<", {30, 2}));
    auto chunkB30 = CreateChunk(BuildRow({30, 2, 0}), BuildRow({30, 2, 0}), 1);
    auto dataSliceB30 = CreateDataSlice(chunkB30);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA30);

    CreateChunkPool();

    for (const auto& dataSlice : CreatedUnversionedDataSlices_) {
        AddDataSlice(dataSlice);
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
        chunkB30,
    }));

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, SortedMergeSimple)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({3}), 2);
    auto dataSliceA = CreateDataSlice(chunkA);
    auto dataSliceB = CreateDataSlice(chunkB);
    auto dataSliceC = CreateDataSlice(chunkC);
    auto chunkBSlices = SliceUnversionedDataSlice(dataSliceB, {BuildBound("<=", {3}), BuildBound("<=", {6})}, {1_KB / 4, 1_KB / 2, 1_KB / 4});
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA);
    CurrentMock().RegisterSliceableUnversionedDataSlice(dataSliceB, chunkBSlices);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceC);

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    AddDataSlice(dataSliceB);
    AddDataSlice(dataSliceC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, SortedMergeWithPersistBeforeFinish)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({3}), 2);

    CreateChunkPool();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);
    AddDataSlice(chunkC);

    PersistAndRestore();

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());
    EXPECT_EQ(3u, stripeLists.front()->Stripes.size());
}

TEST_F(TSortedChunkPoolNewKeysTest, SortedMergeSimpleWithGenericInputStreamDirectory)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({3}), 2);
    auto dataSliceA = CreateDataSlice(chunkA);
    auto dataSliceB = CreateDataSlice(chunkB);
    auto dataSliceC = CreateDataSlice(chunkC);
    auto chunkBSlices = SliceUnversionedDataSlice(dataSliceB, {BuildBound("<=", {3}), BuildBound("<=", {6})}, {1_KB / 4, 1_KB / 2, 1_KB / 4});
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA);
    CurrentMock().RegisterSliceableUnversionedDataSlice(dataSliceB, chunkBSlices);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceC);

    CreateChunkPool(true /*useGenericInputStreamDirectory*/);

    AddDataSlice(dataSliceA);
    AddDataSlice(dataSliceB);
    AddDataSlice(dataSliceC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, SlicingManiacs1)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    MaxDataSlicesPerJob_ = 3;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({5}), 0);
    auto dataSliceA = CreateDataSlice(chunkA);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA);
    std::vector<TInputChunkPtr> maniacDataSlicesB;
    for (int i = 0; i < 100; i++) {
        auto maniacChunkB = CreateChunk(BuildRow({3}), BuildRow({3}), 1);
        auto dataSliceB = CreateDataSlice(maniacChunkB);
        maniacDataSlicesB.emplace_back(maniacChunkB);
        CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB);
    }

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    for (const auto& dataSliceB : maniacDataSlicesB) {
        AddDataSlice(dataSliceB);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    // In an ideal world we would've split all this stuff into (100 + 2) / 3 == 34 jobs.
    // Since our implementation is not perfect, we ensure that there is at least 34 jobs
    // and at most 100 / 2 + 2
    EXPECT_LE((100u + 2u) / 3u, stripeLists.size());
    EXPECT_LE(stripeLists.size(), 100u / 2u + 2u);

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, SlicingManiacs2)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
        );
    InitPrimaryComparator(1);
    MaxDataSlicesPerJob_ = 3;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({5}), 0);
    auto dataSliceA = CreateDataSlice(chunkA);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA);
    std::vector<TInputChunkPtr> maniacDataSlicesB;
    for (int i = 0; i < 100; i++) {
        auto maniacChunkB = CreateChunk(BuildRow({3}), BuildRow({3}), 1);
        auto dataSliceB = CreateDataSlice(maniacChunkB);
        maniacDataSlicesB.emplace_back(maniacChunkB);
        CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB);
    }
    auto dataSliceBTail = CreateDataSlice(CreateChunk(BuildRow({3}), BuildRow({4}), 1));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceBTail);

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    for (const auto& dataSliceB : maniacDataSlicesB) {
        AddDataSlice(dataSliceB);
    }
    AddDataSlice(dataSliceBTail);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    // In an ideal world we would've split all this stuff into (100 + 2) / 3 == 34 jobs.
    // Since our implementation is not perfect, we ensure that there is at least 34 jobs
    // and at most 100 / 2 + 2
    EXPECT_LE((100u + 2u) / 3u, stripeLists.size());
    EXPECT_LE(stripeLists.size(), 100u / 2u + 2u);

    CheckEverything(stripeLists);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSortedChunkPoolNewKeysTest, SortedReduceSimple)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    Options_.MinTeleportChunkSize = 0;
    MaxDataSlicesPerJob_ = 1;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({0}), BuildRow({2}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({5}), 1);
    auto dataSliceA = CreateDataSlice(chunkA);
    auto dataSliceB = CreateDataSlice(chunkB);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB);

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    AddDataSlice(dataSliceB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    // At least one stripe list should be responsible for the shared key {2}.
    bool anyCovers2 = false;
    for (const auto& stripeList : stripeLists) {
        if (stripeList->Stripes.size() == 2u) {
            anyCovers2 = true;
        }
    }
    EXPECT_TRUE(anyCovers2);

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, SortedReduceManiacs)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({0}), BuildRow({2}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({2}), 1);
    auto dataSliceA = CreateDataSlice(chunkA);
    auto dataSliceB = CreateDataSlice(chunkB);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB);

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    AddDataSlice(dataSliceB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, SortedReduceAllKindsOfTeleports)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false, false, true} /*isForeign*/,
        {true, true, false} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(3);
    InitForeignComparator(3);
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
    auto dataSliceA1 = CreateDataSlice(chunkA1);
    auto chunkB1 = CreateChunk(BuildRow({1, 1, 3}), BuildRow({1, 1, 5}), 1);
    auto dataSliceB1 = CreateDataSlice(chunkB1);

    // No (they share only one boundary key).
    // [==]___
    // ___[==]
    auto chunkA2 = CreateChunk(BuildRow({2, 1, 0}), BuildRow({2, 1, 2}), 0);
    auto dataSliceA2 = CreateDataSlice(chunkA2);
    auto chunkB2 = CreateChunk(BuildRow({2, 1, 2}), BuildRow({2, 1, 4}), 1);
    auto dataSliceB2 = CreateDataSlice(chunkB2);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA2);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB2);

    // No (they partially intersect).
    // [===]__
    // __[===]
    auto chunkA3 = CreateChunk(BuildRow({3, 1, 0}), BuildRow({3, 1, 2}), 0);
    auto dataSliceA3 = CreateDataSlice(chunkA3);
    auto chunkB3 = CreateChunk(BuildRow({3, 1, 1}), BuildRow({3, 1, 4}), 1);
    auto dataSliceB3 = CreateDataSlice(chunkB3);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA3);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB3);

    // No (one contained in another).
    // [====]__
    // _[==]___
    auto chunkA4 = CreateChunk(BuildRow({4, 1, 0}), BuildRow({4, 1, 3}), 0);
    auto dataSliceA4 = CreateDataSlice(chunkA4);
    auto chunkB4 = CreateChunk(BuildRow({4, 1, 1}), BuildRow({4, 1, 2}), 1);
    auto dataSliceB4 = CreateDataSlice(chunkB4);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA4);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB4);

    // No (single_key one contained in another).
    // [====]__
    // __[]____
    auto chunkA5 = CreateChunk(BuildRow({5, 1, 0}), BuildRow({5, 1, 3}), 0);
    auto dataSliceA5 = CreateDataSlice(chunkA5);
    auto chunkB5 = CreateChunk(BuildRow({5, 1, 1}), BuildRow({5, 1, 1}), 1);
    auto dataSliceB5 = CreateDataSlice(chunkB5);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA5);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB5);

    // No (they coincide).
    // [===]__
    // [===]__
    auto chunkA6 = CreateChunk(BuildRow({6, 1, 0}), BuildRow({6, 1, 3}), 0);
    auto dataSliceA6 = CreateDataSlice(chunkA6);
    auto chunkB6 = CreateChunk(BuildRow({6, 1, 0}), BuildRow({6, 1, 3}), 1);
    auto dataSliceB6 = CreateDataSlice(chunkB6);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA6);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB6);

    // No (one covers another).
    // [===]__
    // [====]_
    auto chunkA7 = CreateChunk(BuildRow({7, 1, 0}), BuildRow({7, 1, 3}), 0);
    auto dataSliceA7 = CreateDataSlice(chunkA7);
    auto chunkB7 = CreateChunk(BuildRow({7, 1, 0}), BuildRow({7, 1, 4}), 1);
    auto dataSliceB7 = CreateDataSlice(chunkB7);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA7);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB7);

    // No (one covers another).
    // _[===]__
    // [====]__
    auto chunkA8 = CreateChunk(BuildRow({8, 1, 0}), BuildRow({8, 1, 4}), 0);
    auto dataSliceA8 = CreateDataSlice(chunkA8);
    auto chunkB8 = CreateChunk(BuildRow({8, 1, 1}), BuildRow({8, 1, 4}), 1);
    auto dataSliceB8 = CreateDataSlice(chunkB8);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA8);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB8);

    // No (single-key is located exactly at the max boundary key of another).
    // [===]__
    // ___[]__
    auto chunkA9 = CreateChunk(BuildRow({9, 1, 0}), BuildRow({9, 1, 4}), 0);
    auto dataSliceA9 = CreateDataSlice(chunkA9);
    auto chunkB9 = CreateChunk(BuildRow({9, 1, 4}), BuildRow({9, 1, 4}), 1);
    auto dataSliceB9 = CreateDataSlice(chunkB9);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA9);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB9);

    // No (single-key is located exactly at the min boundary key of another).
    // [===]__
    // []_____
    auto chunkA10 = CreateChunk(BuildRow({10, 1, 0}), BuildRow({10, 1, 4}), 0);
    auto dataSliceA10 = CreateDataSlice(chunkA10);
    auto chunkB10 = CreateChunk(BuildRow({10, 1, 0}), BuildRow({10, 1, 0}), 1);
    auto dataSliceB10 = CreateDataSlice(chunkB10);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA10);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB10);

    // No (single-key chunks coincide).
    // _[]___
    // _[]___
    auto chunkA11 = CreateChunk(BuildRow({11, 1, 4}), BuildRow({11, 1, 4}), 0);
    auto dataSliceA11 = CreateDataSlice(chunkA11);
    auto chunkB11 = CreateChunk(BuildRow({11, 1, 4}), BuildRow({11, 1, 4}), 1);
    auto dataSliceB11 = CreateDataSlice(chunkB11);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA11);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB11);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with read limits, keys of length exactly PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // Yes/No (non-trivial lower limit).
    // NB: chunkB12 may not be teleported because it has non-trivial read limits.
    // _[==]_____
    // ___===[==]
    auto chunkA12 = CreateChunk(BuildRow({12, 1, 0}), BuildRow({12, 1, 4}), 0);
    auto dataSliceA12 = CreateDataSlice(chunkA12);
    auto chunkB12 = CreateChunk(BuildRow({12, 1, 2}), BuildRow({12, 1, 8}), 1);
    auto dataSliceB12 = CreateDataSlice(chunkB12, BuildBound(">=", {12, 1, 5}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB12);

    // No/No (non-trivial lower limit coinciding with max key).
    // _[==]_____
    // ___=[====]
    auto chunkA13 = CreateChunk(BuildRow({13, 1, 0}), BuildRow({13, 1, 4}), 0);
    auto dataSliceA13 = CreateDataSlice(chunkA13);
    auto chunkB13 = CreateChunk(BuildRow({13, 1, 2}), BuildRow({13, 1, 8}), 1);
    auto dataSliceB13 = CreateDataSlice(chunkB13, BuildBound(">=", {13, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA13);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB13);

    // No/No (they partially intersect with each other).
    // _[===]____
    // ___=[===]_
    auto chunkA14 = CreateChunk(BuildRow({14, 1, 0}), BuildRow({14, 1, 4}), 0);
    auto dataSliceA14 = CreateDataSlice(chunkA14);
    auto chunkB14 = CreateChunk(BuildRow({14, 1, 2}), BuildRow({14, 1, 8}), 1);
    auto dataSliceB14 = CreateDataSlice(chunkB14, BuildBound(">=", {14, 1, 3}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA14);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB14);

    // No/No (second one is de-facto single-key coinciding with the max-key of the first one).
    // _[===]____
    // ___=[]____
    auto chunkA15 = CreateChunk(BuildRow({15, 1, 0}), BuildRow({15, 1, 4}), 0);
    auto dataSliceA15 = CreateDataSlice(chunkA15);
    auto chunkB15 = CreateChunk(BuildRow({15, 1, 2}), BuildRow({15, 1, 4}), 1);
    auto dataSliceB15 = CreateDataSlice(chunkB15, BuildBound(">=", {15, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA15);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB15);

    // Yes/No (non-trivial upper limit).
    // ______[===]_
    // _[==)===____
    auto chunkA16 = CreateChunk(BuildRow({16, 1, 4}), BuildRow({16, 1, 8}), 0);
    auto dataSliceA16 = CreateDataSlice(chunkA16);
    auto chunkB16 = CreateChunk(BuildRow({16, 1, 0}), BuildRow({16, 1, 6}), 1);
    auto dataSliceB16 = CreateDataSlice(chunkB16, BuildBound(">=", {}), BuildBound("<", {16, 1, 3}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB16);

    // Yes/No (non-trivial upper limit).
    // ____[===]_
    // _[==)===__
    auto chunkA17 = CreateChunk(BuildRow({17, 1, 4}), BuildRow({17, 1, 8}), 0);
    auto dataSliceA17 = CreateDataSlice(chunkA17);
    auto chunkB17 = CreateChunk(BuildRow({17, 1, 0}), BuildRow({17, 1, 6}), 1);
    auto dataSliceB17 = CreateDataSlice(chunkB17, BuildBound(">=", {}), BuildBound("<", {17, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB17);

    // No/No (non-trivial upper limit).
    // ____[===]_
    // _[====)=__
    auto chunkA18 = CreateChunk(BuildRow({18, 1, 4}), BuildRow({18, 1, 8}), 0);
    auto dataSliceA18 = CreateDataSlice(chunkA18);
    auto chunkB18 = CreateChunk(BuildRow({18, 1, 0}), BuildRow({18, 1, 6}), 1);
    auto dataSliceB18 = CreateDataSlice(chunkB18, BuildBound(">=", {}), BuildBound("<", {18, 1, 5}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA18);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB18);

    // No/No (first one is single-key touching the second one with non-trivial lower limit).
    // __[]_______
    // ===[==)____
    auto chunkA19 = CreateChunk(BuildRow({19, 1, 4}), BuildRow({19, 1, 4}), 0);
    auto dataSliceA19 = CreateDataSlice(chunkA19);
    auto chunkB19 = CreateChunk(BuildRow({19, 1, 0}), BuildRow({19, 1, 6}), 1);
    auto dataSliceB19 = CreateDataSlice(chunkB19, BuildBound(">=", {19, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA19);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB19);

    // Yes/No (first one is single-key touching the second one with non-trivial upper limit).
    // _____[]___
    // ___[==)===_
    auto chunkA20 = CreateChunk(BuildRow({20, 1, 4}), BuildRow({20, 1, 4}), 0);
    auto dataSliceA20 = CreateDataSlice(chunkA20);
    auto chunkB20 = CreateChunk(BuildRow({20, 1, 0}), BuildRow({20, 1, 6}), 1);
    auto dataSliceB20 = CreateDataSlice(chunkB20, BuildBound(">=", {}), BuildBound("<", {20, 1, 4}));
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB20);

    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    // Cases with and without read limits, read limits shorter than PrimaryPrefixLength.
    // ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    // No/No (after shortening one chunks will be intersecting).
    //                0              ||              1              ||              2               <- 1-st component is shown here
    //   ...  |  0,0  |  0,1  |  ... || ...  |  1,0  |  1,1  |  ... || ...  |  2,0  |  2,1  |  ...  <- 1-st and 2-nd component are shown here
    // _____________________==========[======================]______________________________________
    // _______________[==============================]______________________________________________

    auto chunkA29 = CreateChunk(BuildRow({29, 0, 1}), BuildRow({29, 1, 1}), 0);
    auto dataSliceA29 = CreateDataSlice(chunkA29, BuildBound(">=", {29, 1}));
    auto chunkB29 = CreateChunk(BuildRow({29, 0, 1}), BuildRow({29, 1, 0}), 1);
    auto dataSliceB29 = CreateDataSlice(chunkB29);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA29);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB29);

    // No/No (after shortening one chunks will be intersecting).
    //                0              ||              1              ||              2               <- 1-st component is shown here
    //   ...  |  0,0  |  0,1  |  ... || ...  |  1,0  |  1,1  |  ... || ...  |  2,0  |  2,1  |  ...  <- 1-st and 2-nd component are shown here
    // ______________________________________[========================)==============________________
    // _____________________________________________________________________[=======]________________

    auto chunkA30 = CreateChunk(BuildRow({30, 1, 0}), BuildRow({30, 2, 1}), 0, 1_KB);
    auto dataSliceA30 = CreateDataSlice(chunkA30, BuildBound(">=", {}), BuildBound("<", {30, 2}));
    auto chunkB30 = CreateChunk(BuildRow({30, 2, 0}), BuildRow({30, 2, 0}), 1);
    auto dataSliceB30 = CreateDataSlice(chunkB30);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA30);

    // Yes (primary and foreign chunks are non-intersecting).
    // [==]_____
    // _____[==]
    auto chunkA31 = CreateChunk(BuildRow({31, 1, 0}), BuildRow({31, 1, 2}), 0);
    auto dataSliceA31 = CreateDataSlice(chunkA31);
    auto chunkC31 = CreateChunk(BuildRow({31, 1, 3}), BuildRow({31, 1, 5}), 2);
    auto dataSliceC31 = CreateDataSlice(chunkC31);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceC31);

    // No (primary and foreign chunk share only one boundary key).
    // [==]___
    // ___[==]
    auto chunkA32 = CreateChunk(BuildRow({32, 1, 0}), BuildRow({32, 1, 2}), 0);
    auto dataSliceA32 = CreateDataSlice(chunkA32);
    auto chunkC32 = CreateChunk(BuildRow({32, 1, 2}), BuildRow({32, 1, 4}), 2);
    auto dataSliceC32 = CreateDataSlice(chunkC32);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA32);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceC32);

    // No (single-key primary and foreign chunks coincide).
    // _[]___
    // _[]___
    auto chunkA33 = CreateChunk(BuildRow({33, 1, 4}), BuildRow({33, 1, 4}), 0);
    auto dataSliceA33 = CreateDataSlice(chunkA33);
    auto chunkC33 = CreateChunk(BuildRow({33, 1, 4}), BuildRow({33, 1, 4}), 2);
    auto dataSliceC33 = CreateDataSlice(chunkC33);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA33);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceC33);

    CreateChunkPool();

    for (const auto& dataSlice : CreatedUnversionedDataSlices_) {
        AddDataSlice(dataSlice);
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
        chunkB30,
        chunkA31,
    }));

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, SortedReduceWithJoin)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {true, true, false, false} /*isForeign*/,
        {false, false, false, false} /*isTeleportable*/,
        {false, false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(3);
    InitForeignComparator(2);
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1, 21}), BuildRow({4, 24}), 0);
    auto chunkB = CreateChunk(BuildRow({2, 62}), BuildRow({4, 64}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 101, 11}), BuildRow({4, 402, 18}), 2);
    auto chunkD = CreateChunk(BuildRow({1, 102, 42}), BuildRow({4, 402, 48}), 3);
    auto dataSliceA = CreateDataSlice(chunkA);
    auto dataSliceB = CreateDataSlice(chunkB);
    auto dataSliceC = CreateDataSlice(chunkC);
    auto dataSliceD = CreateDataSlice(chunkD);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceC);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceD);

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    AddDataSlice(dataSliceB);
    AddDataSlice(dataSliceC);
    AddDataSlice(dataSliceD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, JoinReduce)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {true, true, false, false} /*isForeign*/,
        {false, false, false, false} /*isTeleportable*/,
        {false, false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(3);
    InitForeignComparator(2);
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1, 21}), BuildRow({4, 24}), 0);
    auto chunkB = CreateChunk(BuildRow({2, 62}), BuildRow({4, 64}), 1);
    auto chunkC = CreateChunk(BuildRow({1, 101, 11}), BuildRow({4, 402, 18}), 2);
    auto chunkD = CreateChunk(BuildRow({1, 102, 42}), BuildRow({4, 402, 48}), 3);
    auto dataSliceA = CreateDataSlice(chunkA);
    auto dataSliceB = CreateDataSlice(chunkB);
    auto dataSliceC = CreateDataSlice(chunkC);
    auto dataSliceD = CreateDataSlice(chunkD);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceC);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceD);

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    AddDataSlice(dataSliceB);
    AddDataSlice(dataSliceC);
    AddDataSlice(dataSliceD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, ManiacIsSliced)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 100_MB;
    InputSliceDataWeight_ = 16_MB;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({1}), 0, 10_GB);
    chunkA->SetTotalRowCount(1'000'000);
    auto dataSliceA = CreateDataSlice(chunkA);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA);

    CreateChunkPool();

    AddDataSlice(dataSliceA);

    ChunkPool_->Finish();
    EXPECT_GE(ChunkPool_->GetJobCounter()->GetPending(), 90);
    EXPECT_LE(ChunkPool_->GetJobCounter()->GetPending(), 110);

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, MaxTotalSliceCountExceeded)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {false, false, false} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    Options_.SortedJobOptions.MaxTotalSliceCount = 6;
    DataSizePerJob_ = 1000;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({1}), BuildRow({3}), 1);
    auto chunkC1 = CreateChunk(BuildRow({1}), BuildRow({1}), 2);
    auto chunkC2 = CreateChunk(BuildRow({2}), BuildRow({2}), 2);
    auto chunkC3 = CreateChunk(BuildRow({3}), BuildRow({3}), 2);

    CreateChunkPool();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);
    AddDataSlice(chunkC1);
    AddDataSlice(chunkC2);
    AddDataSlice(chunkC3);

    EXPECT_THROW(ChunkPool_->Finish(), std::exception);
}

TEST_F(TSortedChunkPoolNewKeysTest, MaxTotalSliceCountRetries)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {false, false, false} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
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

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);
    AddDataSlice(chunkC1);
    AddDataSlice(chunkC2);
    AddDataSlice(chunkC3);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, TestJobInterruption)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false, true} /*isForeign*/,
        {false, false, false, false} /*isTeleportable*/,
        {false, false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitForeignComparator(1);
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({20}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({42}), 1);
    auto chunkC = CreateChunk(BuildRow({10}), BuildRow({12}), 2);
    auto chunkD = CreateChunk(BuildRow({1}), BuildRow({42}), 3);
    auto dataSliceA = CreateDataSlice(chunkA);
    auto dataSliceB = CreateDataSlice(chunkB);
    auto dataSliceC = CreateDataSlice(chunkC);
    auto dataSliceD = CreateDataSlice(chunkD);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceC);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceD);

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    AddDataSlice(dataSliceB);
    AddDataSlice(dataSliceC);
    AddDataSlice(dataSliceD);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    ASSERT_EQ(stripeLists.size(), 1u);
    ASSERT_EQ(ExtractedCookies_.size(), 1u);

    const auto& stripeList = stripeLists[0];
    std::vector<TLegacyDataSlicePtr> unreadDataSlices = {
        CreateInputDataSlice(GetStripeByTableIndex(stripeList, 0)->DataSlices.front()),
        CreateInputDataSlice(GetStripeByTableIndex(stripeList, 1)->DataSlices.front()),
    };
    unreadDataSlices[0]->LowerLimit().KeyBound = BuildBound(">=", {13});
    unreadDataSlices[1]->LowerLimit().KeyBound = BuildBound(">=", {14});
    TCompletedJobSummary jobSummary;
    jobSummary.InterruptReason = EInterruptReason::Preemption;
    jobSummary.UnreadInputDataSlices = unreadDataSlices;
    ChunkPool_->Completed(ExtractedCookies_.front(), jobSummary);

    ExtractOutputCookiesWhilePossible();
    ASSERT_EQ(ExtractedCookies_.size(), 2u);
    auto newStripeList = ChunkPool_->GetStripeList(ExtractedCookies_.back());
    ASSERT_EQ(newStripeList->Stripes.size(), 3u);
    ASSERT_EQ(GetStripeByTableIndex(newStripeList, 0)->DataSlices.size(), 1u);
    ASSERT_EQ(GetStripeByTableIndex(newStripeList, 0)->DataSlices.front()->LowerLimit().KeyBound, BuildBound(">=", {13}));
    ASSERT_EQ(GetStripeByTableIndex(newStripeList, 1)->DataSlices.size(), 1u);
    ASSERT_EQ(GetStripeByTableIndex(newStripeList, 1)->DataSlices.front()->LowerLimit().KeyBound, BuildBound(">=", {14}));
    ASSERT_EQ(GetStripeByTableIndex(newStripeList, 3)->DataSlices.size(), 1u);
}

TEST_F(TSortedChunkPoolNewKeysTest, RowSlicingCriteria1)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 10_KB;
    InputSliceDataWeight_ = 1;
    InitJobConstraints();

    // Each slice must be sliced into approximately 100 parts.
    auto chunkA1 = CreateChunk(BuildRow({2}), BuildRow({4}), 0, 1_MB);
    auto chunkB1 = CreateChunk(BuildRow({4}), BuildRow({6}), 1, 1_MB);
    auto chunkA2 = CreateChunk(BuildRow({6}), BuildRow({6}), 0, 1_MB);
    auto chunkA3 = CreateChunk(BuildRow({6}), BuildRow({6}), 0, 1_MB);
    auto chunkB2 = CreateChunk(BuildRow({6}), BuildRow({6}), 1, 1_MB);
    auto chunkA4 = CreateChunk(BuildRow({7}), BuildRow({7}), 0, 1_MB);
    auto chunkB3 = CreateChunk(BuildRow({7}), BuildRow({9}), 1, 1_MB);
    auto chunkB4 = CreateChunk(BuildRow({10}), BuildRow({10}), 1, 1_MB);

    CreateChunkPool();

    AddDataSlice(chunkA1);
    AddDataSlice(chunkB1);
    AddDataSlice(chunkA2);
    AddDataSlice(chunkA3);
    AddDataSlice(chunkB2);
    AddDataSlice(chunkA4);
    AddDataSlice(chunkB3);
    AddDataSlice(chunkB4);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_LE(780u, stripeLists.size());
    EXPECT_LE(stripeLists.size(), 820u);

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, RowSlicingCriteria2)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 10_KB;
    InputSliceDataWeight_ = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({2}), BuildRow({4}), 0, 1_KB);
    // This slice must be sliced into approximately 100 parts.
    auto chunkB = CreateChunk(BuildRow({3}), BuildRow({3}), 1, 1_MB);

    CreateChunkPool();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_LE(90u, stripeLists.size());
    EXPECT_LE(stripeLists.size(), 110u);

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, RowSlicingCriteria2Dynamic)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, true}  /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 10_KB;
    InputSliceDataWeight_ = 1;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({2}), BuildRow({4}), 0, 1_KB);
    // This slice must not be further sliced because it corresponds to a dynamic table.
    auto chunkB = CreateChunk(BuildRow({3}), BuildRow({3}), 1, 1_MB);

    CreateChunkPool();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_LE(stripeLists.size(), 3u);

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, RowSlicingCriteria3)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {false, false, false} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 10_KB;
    InputSliceDataWeight_ = 1;
    InitJobConstraints();

    // No slice must be sliced by rows.
    auto chunkA1 = CreateChunk(BuildRow({2}), BuildRow({4}), 0, 1_KB);
    auto chunkB1 = CreateChunk(BuildRow({1}), BuildRow({3}), 1, 1_MB);
    auto chunkA2 = CreateChunk(BuildRow({5}), BuildRow({7}), 0, 1_KB);
    auto chunkB2 = CreateChunk(BuildRow({6}), BuildRow({8}), 1, 1_MB);
    auto chunkA3 = CreateChunk(BuildRow({10}), BuildRow({12}), 0, 1_KB);
    auto chunkB3 = CreateChunk(BuildRow({11}), BuildRow({13}), 1, 1_MB);
    // Technically we could slice this slice by rows, but current implementation does not do that...
    auto chunkC = CreateChunk(BuildRow({11}), BuildRow({11}), 1, 1_MB);

    CreateChunkPool();

    AddDataSlice(chunkA1);
    AddDataSlice(chunkB1);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_LE(stripeLists.size(), 30u);

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, RowSlicingWithForeigns)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, true} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitForeignComparator(1);
    DataSizePerJob_ = 10_KB;
    InputSliceDataWeight_ = 1;
    InitJobConstraints();

    // Primary slice must be sliced into approximately 100 parts.
    auto chunkA = CreateChunk(BuildRow({2}), BuildRow({4}), 0, 1_MB);
    auto chunkB = CreateChunk(BuildRow({3}), BuildRow({5}), 1);

    CreateChunkPool();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_LE(90u, stripeLists.size());
    EXPECT_LE(stripeLists.size(), 110u);

    CheckEverything(stripeLists);

    for (const auto& stripeList : stripeLists) {
        EXPECT_EQ(2u, stripeList->Stripes.size());
    }
}

TEST_F(TSortedChunkPoolNewKeysTest, TestJobSplitSimple)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = Inf64;
    InitJobConstraints();
    PrepareNewMock();

    const int chunkCount = 100;
    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
        auto dataSlice = CreateDataSlice(chunk);
        CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSlice);
    }

    CreateChunkPool();

    for (const auto& dataSlice : CreatedUnversionedDataSlices_) {
        AddDataSlice(dataSlice);
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

TEST_F(TSortedChunkPoolNewKeysTest, TestJobSplitWithForeign)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, true} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitForeignComparator(1);
    DataSizePerJob_ = Inf64;
    InitJobConstraints();
    PrepareNewMock();

    std::vector<TLegacyDataSlicePtr> allDataSlices;
    const int chunkCount = 100;
    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
        auto dataSlice = CreateDataSlice(chunk);
        CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSlice);
        allDataSlices.emplace_back(std::move(dataSlice));
    }

    const int foreignChunkCount = 5;

    for (int index = 0; index < foreignChunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({index * 40}), BuildRow({index * 40 + 39}), 1);
        auto dataSlice = CreateDataSlice(chunk);
        CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSlice);
        allDataSlices.emplace_back(std::move(dataSlice));
    }

    CreateChunkPool();

    for (const auto& dataSlice : allDataSlices) {
        AddDataSlice(std::move(dataSlice));
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

TEST_F(TSortedChunkPoolNewKeysTest, SuchForeignMuchData)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, true} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitForeignComparator(1);
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();

    std::vector<TLegacyDataSlicePtr> dataSlices;

    for (int index = 0; index < 10; ++index) {
        auto chunk = CreateChunk(BuildRow({100 * index}), BuildRow({100 * (index + 1)}), 0);
        dataSlices.emplace_back(CreateDataSlice(chunk));
    }

    for (int index = 0; index < 1000; ++index) {
        auto chunk = CreateChunk(BuildRow({index}), BuildRow({index + 1}), 1);
        dataSlices.emplace_back(CreateDataSlice(chunk));
    }

    CreateChunkPool();

    for (const auto& dataSlice : dataSlices) {
        AddDataSlice(dataSlice);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();
    EXPECT_GE(stripeLists.size(), 90u);
    EXPECT_LE(stripeLists.size(), 110u);

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, TestJobSplitStripeSuspension)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, true} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitForeignComparator(1);
    DataSizePerJob_ = Inf64;
    InitJobConstraints();
    PrepareNewMock();

    std::vector<TLegacyDataSlicePtr> allDataSlices;
    const int chunkCount = 100;
    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
        auto dataSlice = CreateDataSlice(chunk);
        CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSlice);
        allDataSlices.emplace_back(std::move(dataSlice));
    }

    const int foreignChunkCount = 5;

    for (int index = 0; index < foreignChunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({index * 40}), BuildRow({index * 40 + 39}), 1);
        auto dataSlice = CreateDataSlice(chunk);
        CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSlice);
        allDataSlices.emplace_back(std::move(dataSlice));
    }

    CreateChunkPool();

    for (const auto& dataSlice : allDataSlices) {
        AddDataSlice(std::move(dataSlice));
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
    SuspendDataSlice(0);
    ASSERT_EQ(ChunkPool_->GetJobCounter()->GetPending(), pendingJobCount - 1);
    for (int cookie = chunkCount; cookie < chunkCount + foreignChunkCount; ++cookie) {
        SuspendDataSlice(cookie);
    }
    ASSERT_EQ(0, ChunkPool_->GetJobCounter()->GetPending());
}

// TODO(max42): this test is no longer viable since we require
// slices to be added to new sorted pool in correct order.
TEST_F(TSortedChunkPoolNewKeysTest, DISABLED_TestCorrectOrderInsideStripe)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = Inf64;
    InitJobConstraints();
    PrepareNewMock();

    auto chunk = CreateChunk(BuildRow({10}), BuildRow({20}), 0);
    auto dataSlice = CreateDataSlice(chunk);
    std::vector<TInputChunkSlicePtr> slices;
    for (int index = 0; index < 100; ++index) {
        auto& slice = slices.emplace_back(CreateInputChunkSlice(chunk));
        slice->TransformToNewKeyless();
        slice->LowerLimit().RowIndex = 10 * index;
        slice->UpperLimit().RowIndex = 10 * (index + 1);
        int key1 = (10 * (100 - index) + 20 * index + 5) / 100;
        int key2 = (10 * (100 - (index + 1)) + 20 * (index + 1) + 5) / 100;
        slice->LowerLimit().KeyBound = BuildBound(">=", {key1});
        slice->UpperLimit().KeyBound = BuildBound("<=", {key2});
    }
    shuffle(slices.begin(), slices.end(), Gen_);

    CurrentMock().RegisterSliceableUnversionedDataSlice(dataSlice, slices);

    CreateChunkPool();

    AddDataSlice(dataSlice);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    ASSERT_EQ(ExtractedCookies_.size(), 1u);
    auto stripeList = ChunkPool_->GetStripeList(ExtractedCookies_.back());
    ASSERT_EQ(stripeList->Stripes.size(), 1u);
    const auto& stripe = stripeList->Stripes.front();
    ASSERT_EQ(stripe->DataSlices.size(), 100u);
    for (int index = 0; index + 1 < std::ssize(stripe->DataSlices); ++index) {
        ASSERT_EQ(*stripe->DataSlices[index]->UpperLimit().RowIndex, *stripe->DataSlices[index + 1]->LowerLimit().RowIndex);
    }
}

TEST_F(TSortedChunkPoolNewKeysTest, TestTrickyCase)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false},
        {false},
        {false}
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({100}), BuildRow({100}), 0, 12_KB);
    auto chunkB = CreateChunk(BuildRow({100}), BuildRow({200}), 0, 3_KB);
    auto dataSliceA = CreateDataSlice(chunkA);
    auto dataSliceB = CreateDataSlice(chunkB);
    auto chunkASlices = SliceUnversionedDataSlice(dataSliceA, {BuildBound("<=", {100})}, {9_KB, 3_KB}, {500, 500});
    chunkASlices[1]->LowerLimit().KeyBound = BuildBound(">=", {100});
    CurrentMock().RegisterSliceableUnversionedDataSlice(dataSliceA, chunkASlices);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB);

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    AddDataSlice(dataSliceB);

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

TEST_F(TSortedChunkPoolNewKeysTest, TestTrickyCase2)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false},
        {false},
        {false}
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({100}), BuildRow({100}), 0, 12_KB);
    auto dataSliceA = CreateDataSlice(chunkA);
    auto chunkB = CreateChunk(BuildRow({100}), BuildRow({100}), 0, 1_KB / 10);
    auto dataSliceB = CreateDataSlice(chunkB);
    auto chunkC = CreateChunk(BuildRow({100}), BuildRow({200}), 0, 3_KB);
    auto dataSliceC = CreateDataSlice(chunkC);
    auto chunkASlices = SliceUnversionedDataSlice(dataSliceA, {BuildBound("<=", {100})}, {9_KB, 3_KB}, {500, 500});
    chunkASlices[1]->LowerLimit().KeyBound = BuildBound(">=", {100});
    CurrentMock().RegisterSliceableUnversionedDataSlice(dataSliceA, chunkASlices);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceC);

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    AddDataSlice(dataSliceB);
    AddDataSlice(dataSliceC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();
    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, TestTrickyCase3)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {true, false, false},
        {false, false, false},
        {false, false, false}
    );
    InitPrimaryComparator(2);
    InitForeignComparator(1);
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({2}), BuildRow({2}), 0, 1_KB / 10);
    auto chunkB = CreateChunk(BuildRow({1, 0}), BuildRow({5, 0}), 1, 100_KB);
    auto chunkC = CreateChunk(BuildRow({2, 1}), BuildRow({2, 2}), 2, 3_KB);

    CreateChunkPool();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);
    AddDataSlice(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, TestTrickyCase4)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false},
        {false, false, false},
        {false, false, false}
    );
    InitPrimaryComparator(3);
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA1 = CreateChunk(BuildRow({133, 1, 1}), BuildRow({133, 3, 3}), 0, 8_KB);
    auto dataSliceA1 = CreateDataSlice(chunkA1);
    auto chunkA2 = CreateChunk(BuildRow({133, 3, 3}), BuildRow({133, 3, 3}), 0, 8_KB);
    auto dataSliceA2 = CreateDataSlice(chunkA2);
    auto chunkA3 = CreateChunk(BuildRow({133, 5, 5}), BuildRow({133, 9, 9}), 0, 8_KB);
    auto dataSliceA3 = CreateDataSlice(chunkA3);
    auto chunkB = CreateChunk(
        BuildRow({0, 0, 0}),
        BuildRow({200, 200, 200}),
        1);
    auto dataSliceB = CreateDataSlice(chunkB, BuildBound(">=", {133}), BuildBound("<=", {133}));

    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA1);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA2);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA3);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB);

    CreateChunkPool();

    AddDataSlice(dataSliceA1);
    AddDataSlice(dataSliceA2);
    AddDataSlice(dataSliceA3);
    AddDataSlice(dataSliceB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();

    CheckEverything(stripeLists);

    EXPECT_TRUE(TeleportChunks_.empty());
}

TEST_F(TSortedChunkPoolNewKeysTest, TestNoChunkSliceFetcher)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({3}), 2);

    CreateChunkPool();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);
    AddDataSlice(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, TestStripeListStatisticsAreSet)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC = CreateChunk(BuildRow({1}), BuildRow({3}), 2);

    CreateChunkPool();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);
    AddDataSlice(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());

    EXPECT_GT(stripeLists[0]->TotalChunkCount, 0);
    EXPECT_GT(stripeLists[0]->TotalRowCount, 0);
    EXPECT_GT(stripeLists[0]->TotalDataWeight, 0);
}

TEST_F(TSortedChunkPoolNewKeysTest, TestSeveralSlicesInInputStripe)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
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

TEST_F(TSortedChunkPoolNewKeysTest, TestPivotKeys1)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitJobConstraints();

    auto chunkA1 = CreateChunk(BuildRow({2}), BuildRow({2}), 0);
    auto chunkA2 = CreateChunk(BuildRow({3}), BuildRow({14}), 0);
    auto chunkB1 = CreateChunk(BuildRow({0}), BuildRow({1}), 1);
    auto chunkB2 = CreateChunk(BuildRow({8}), BuildRow({20}), 1);

    Options_.SortedJobOptions.PivotKeys = std::vector<TLegacyKey>{BuildRow({2}), BuildRow({5}), BuildRow({8})};

    CreateChunkPool();

    AddDataSlice(chunkA1);
    AddDataSlice(chunkA2);
    AddDataSlice(chunkB1);
    AddDataSlice(chunkB2);

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

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, TestPivotKeys2)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({2}), BuildRow({5}), 0);
    Options_.SortedJobOptions.PivotKeys = std::vector<TLegacyKey>{BuildRow({2}), BuildRow({3}), BuildRow({4}), BuildRow({5})};

    CreateChunkPool();

    AddDataSlice(chunkA);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(4u, stripeLists.size());
    EXPECT_EQ(1u, stripeLists[0]->Stripes.size());
    EXPECT_EQ(1u, stripeLists[0]->Stripes[0]->DataSlices.size());
    EXPECT_EQ(BuildBound(">=", {2}), stripeLists[0]->Stripes[0]->DataSlices[0]->LowerLimit().KeyBound);
    EXPECT_EQ(1u, stripeLists[1]->Stripes.size());
    EXPECT_EQ(1u, stripeLists[2]->Stripes.size());
    EXPECT_EQ(1u, stripeLists[3]->Stripes.size());

    CheckEverything(stripeLists);
}


TEST_F(TSortedChunkPoolNewKeysTest, SuspendFinishResumeTest)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({1}), BuildRow({1}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({2}), 0);
    auto chunkC = CreateChunk(BuildRow({3}), BuildRow({3}), 0);

    CreateChunkPool();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);
    AddDataSlice(chunkC);

    SuspendDataSlice(0);
    SuspendDataSlice(2);

    ChunkPool_->Finish();

    ResumeDataSlice(0);
    ResumeDataSlice(2);

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_EQ(1u, stripeLists.size());
    EXPECT_EQ(1u, stripeLists[0]->Stripes.size());
    EXPECT_EQ(3u, stripeLists[0]->Stripes[0]->DataSlices.size());
}

TEST_F(TSortedChunkPoolNewKeysTest, SliceByPrimaryDataSize)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {true, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    DataSizePerJob_ = 10_KB;
    PrimaryDataWeightPerJob_ = 1_KB;
    InitPrimaryComparator(1);
    InitForeignComparator(1);
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
        AddDataSlice(chunk);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_LE(5u, stripeLists.size());
    EXPECT_GE(20u, stripeLists.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, ExtractByDataSize)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 1;
    auto chunkA1 = CreateChunk(BuildRow({0}), BuildRow({5}), 0, 10_KB);
    auto chunkA2 = CreateChunk(BuildRow({6}), BuildRow({10}), 0, 5_KB);
    auto chunkA3 = CreateChunk(BuildRow({11}), BuildRow({15}), 0, 15_KB);

    InitJobConstraints();

    CreateChunkPool();

    AddDataSlice(chunkA1);
    AddDataSlice(chunkA2);
    AddDataSlice(chunkA3);

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

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, MaximumDataWeightPerJobViolation)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    MaxDataWeightPerJob_ = 10_KB;
    InitTables(
        {false, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 5_KB;
    auto chunkA1 = CreateChunk(BuildRow({0}), BuildRow({5}), 0, 7_KB);
    auto chunkB1 = CreateChunk(BuildRow({3}), BuildRow({8}), 0, 7_KB);

    InitJobConstraints();

    CreateChunkPool();

    AddDataSlice(chunkA1);
    AddDataSlice(chunkB1);

    EXPECT_THROW(ChunkPool_->Finish(), std::exception);
}

TEST_F(TSortedChunkPoolNewKeysTest, SingletonTeleportSingleton)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {false, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 100_KB;
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA1 = CreateChunk(BuildRow({1}), BuildRow({1}), 0, 8_KB);
    auto dataSliceA1 = CreateDataSlice(chunkA1);
    auto chunkB2 = CreateChunk(BuildRow({1}), BuildRow({9}), 1, 8_KB);
    auto chunkA3 = CreateChunk(BuildRow({9}), BuildRow({9}), 0, 8_KB);
    auto dataSliceA3 = CreateDataSlice(chunkA3);

    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA1);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceA3);

    CreateChunkPool();

    AddDataSlice(dataSliceA1);
    AddDataSlice(chunkB2);
    AddDataSlice(dataSliceA3);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();

    CheckEverything(stripeLists);

    EXPECT_THAT(TeleportChunks_, UnorderedElementsAreArray({chunkB2}));
    EXPECT_EQ(2u, stripeLists.size());
}

TEST_F(TSortedChunkPoolNewKeysTest, CartesianProductViaJoinReduce)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, true} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitForeignComparator(1);
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
        AddDataSlice(chunk);
    }

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();

    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());
    EXPECT_GE(stripeLists.size(), 90u);
    EXPECT_LE(stripeLists.size(), 110u);

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, ResetBeforeFinish)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false, false} /*isForeign*/,
        {true, true, true} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkA = CreateChunk(BuildRow({3}), BuildRow({3}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), BuildRow({15}), 1);
    auto chunkC1 = CreateChunk(BuildRow({0}), BuildRow({2}), 2);
    auto chunkC2 = CreateChunk(BuildRow({2}), BuildRow({3}), 2);
    auto chunkC1Replayed = CreateChunk(BuildRow({0}), BuildRow({1}), 2);
    auto chunkC2Replayed = CreateChunk(BuildRow({1}), BuildRow({3}), 2);

    CreateChunkPool();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);
    auto stripeC = CreateStripe({CreateUnversionedInputDataSlice(chunkC1), CreateUnversionedInputDataSlice(chunkC2)});
    auto cookie = ChunkPool_->Add(stripeC);
    auto stripeCReplayed = CreateStripe({CreateUnversionedInputDataSlice(chunkC1Replayed), CreateUnversionedInputDataSlice(chunkC2Replayed)});
    ChunkPool_->Reset(cookie, stripeCReplayed, IdentityChunkMapping);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_EQ(TeleportChunks_, std::vector<TInputChunkPtr>{chunkC1Replayed});
    EXPECT_EQ(1u, stripeLists.size());
}

TEST_F(TSortedChunkPoolNewKeysTest, TeleportChunkAndShortReadLimits)
{
    // YT-8836.
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {true, true} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(2);
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();

    auto chunkALeft = CreateChunk(BuildRow({1, 0}), BuildRow({10, 0}), 0);
    auto chunkARight = CreateChunk(BuildRow({1, 0}), BuildRow({10, 0}), 0);
    auto chunkB = CreateChunk(BuildRow({4, 2}), BuildRow({4, 2}), 1);

    CreateChunkPool();

    AddDataSlice(chunkALeft, BuildBound(">=", {}), BuildBound("<", {4}));
    AddDataSlice(chunkARight, BuildBound(">=", {5}));
    AddDataSlice(chunkB);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_EQ(1u, TeleportChunks_.size());
    ASSERT_EQ(2u, stripeLists.size());
    EXPECT_EQ(1u, stripeLists[0]->Stripes.size());
    EXPECT_EQ(1u, stripeLists[1]->Stripes.size());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, TwoTablesWithoutKeyGuarantee)
{
    // This test is similar to test_reduce_skewed_key_distribution_two_tables.
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 1_KB;
    InitJobConstraints();

    auto chunkA1 = CreateChunk(BuildRow({1}), BuildRow({2}), 0, 2_KB);
    auto chunkA2 = CreateChunk(BuildRow({1}), BuildRow({2}), 0, 2_KB);
    auto chunkB1 = CreateChunk(BuildRow({1}), BuildRow({2}), 1, 2_KB);
    auto chunkB2 = CreateChunk(BuildRow({1}), BuildRow({2}), 1, 2_KB);

    CreateChunkPool();

    AddDataSlice(chunkA1, BuildBound(">=", {}), BuildBound("<=", {1}), 1.9 * 1_KB, 0);
    AddDataSlice(chunkA2, BuildBound(">", {1}), BuildBound("<=", {}), 0.1 * 1_KB, 1);
    AddDataSlice(chunkB1, BuildBound(">=", {}), BuildBound("<=", {1}), 1.9 * 1_KB, 0);
    AddDataSlice(chunkB2, BuildBound(">", {1}), BuildBound("<=", {}), 0.1 * 1_KB, 1);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();
    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, Sampling)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 1;
    SamplingRate_ = 0.5;
    SamplingDataWeightPerJob_ = DataSizePerJob_;
    InitJobConstraints();

    CreateChunkPool();

    TLegacyDataSlicePtr dataSlice42;
    for (int index = 0; index < 100; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
        auto dataSlice = CreateDataSlice(chunk);
        const auto& cookie = AddDataSlice(dataSlice);
        if (cookie == 42) {
            dataSlice42 = dataSlice;
        }
    }

    ChunkPool_->Finish();

    Cerr << "Pending job count: " << ChunkPool_->GetJobCounter()->GetPending() << Endl;
    EXPECT_LE(40, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_GE(60, ChunkPool_->GetJobCounter()->GetPending());

    ResetDataSlice(42, dataSlice42);

    Cerr << "Pending job count: " << ChunkPool_->GetJobCounter()->GetPending() << Endl;
    EXPECT_LE(40, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_GE(60, ChunkPool_->GetJobCounter()->GetPending());
}

TEST_F(TSortedChunkPoolNewKeysTest, SamplingWithEnlarging)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 10_KB;
    SamplingRate_ = 0.5;
    SamplingDataWeightPerJob_ = 1;
    InitJobConstraints();

    CreateChunkPool();

    TLegacyDataSlicePtr dataSlice42;
    for (int index = 0; index < 100; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
        auto dataSlice = CreateDataSlice(chunk);
        auto cookie = AddDataSlice(dataSlice);
        if (cookie == 42) {
            dataSlice42 = dataSlice;
        }
    }

    ChunkPool_->Finish();

    Cerr << "Pending job count: " << ChunkPool_->GetJobCounter()->GetPending() << Endl;
    EXPECT_LE(3, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_GE(7, ChunkPool_->GetJobCounter()->GetPending());

    ResetDataSlice(42, dataSlice42);

    Cerr << "Pending job count: " << ChunkPool_->GetJobCounter()->GetPending() << Endl;
    EXPECT_LE(3, ChunkPool_->GetJobCounter()->GetPending());
    EXPECT_GE(7, ChunkPool_->GetJobCounter()->GetPending());
}

TEST_F(TSortedChunkPoolNewKeysTest, EnlargingWithTeleportation)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {true, false} /*isTeleportable*/,
        {false, false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    Options_.MinTeleportChunkSize = 0;
    DataSizePerJob_ = 10_KB;
    SamplingRate_ = 1.0;
    SamplingDataWeightPerJob_ = 10_KB;
    InitJobConstraints();

    CreateChunkPool();

    AddDataSlice(CreateChunk(BuildRow({5}), BuildRow({5}), 0));
    AddDataSlice(CreateChunk(BuildRow({0}), BuildRow({1}), 1));
    AddDataSlice(CreateChunk(BuildRow({8}), BuildRow({9}), 1));

    ChunkPool_->Finish();

    EXPECT_EQ(1u, TeleportChunks_.size());
    EXPECT_EQ(2, ChunkPool_->GetJobCounter()->GetPending());
}

// YT-9791
TEST_F(TSortedChunkPoolNewKeysTest, TrickySliceSortOrder)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();

    PrepareNewMock();
    CreateChunkPool();

    auto chunk = CreateChunk(BuildRow({0xA}), BuildRow({0xD}), 0);
    auto chunkSlice1 = CreateInputChunkSlice(chunk);
    chunkSlice1->TransformToNewKeyless();
    chunkSlice1->LowerLimit().RowIndex = 0;
    chunkSlice1->UpperLimit().RowIndex = 20;
    chunkSlice1->LowerLimit().KeyBound = BuildBound(">=", {0xA});
    chunkSlice1->UpperLimit().KeyBound = BuildBound("<", {0xB});
    auto chunkSlice2 = CreateInputChunkSlice(chunk);
    chunkSlice2->TransformToNewKeyless();
    chunkSlice2->LowerLimit().RowIndex = 0;
    chunkSlice2->UpperLimit().RowIndex = 20;
    chunkSlice2->LowerLimit().KeyBound = BuildBound(">=", {0xB});
    chunkSlice2->UpperLimit().KeyBound = BuildBound("<", {0xC});

    auto dataSlice = CreateDataSlice(chunk);

    CurrentMock().RegisterSliceableUnversionedDataSlice(dataSlice, {chunkSlice1, chunkSlice2});

    AddDataSlice(dataSlice);

    ChunkPool_->Finish();

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    auto outputCookie = ChunkPool_->Extract(NNodeTrackerClient::TNodeId(0));
    auto stripeList = ChunkPool_->GetStripeList(outputCookie);
    EXPECT_EQ(1u, stripeList->Stripes.size());
    EXPECT_EQ(2u, stripeList->Stripes[0]->DataSlices.size());
    EXPECT_EQ(BuildBound("<", {0xB}), stripeList->Stripes[0]->DataSlices[0]->UpperLimit().KeyBound);

    std::vector<TLegacyDataSlicePtr> unreadDataSlices = {
        CreateInputDataSlice(stripeList->Stripes[0]->DataSlices[0]),
        CreateInputDataSlice(stripeList->Stripes[0]->DataSlices[1]),
    };
    unreadDataSlices[0]->LowerLimit().RowIndex = 15;
    TCompletedJobSummary jobSummary;
    jobSummary.InterruptReason = EInterruptReason::Preemption;
    jobSummary.UnreadInputDataSlices = unreadDataSlices;
    ChunkPool_->Completed(outputCookie, jobSummary);

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    outputCookie = ChunkPool_->Extract(NNodeTrackerClient::TNodeId(0));
    stripeList = ChunkPool_->GetStripeList(outputCookie);
    EXPECT_EQ(1u, stripeList->Stripes.size());
    EXPECT_EQ(2u, stripeList->Stripes[0]->DataSlices.size());
    EXPECT_EQ(BuildBound("<", {0xB}), stripeList->Stripes[0]->DataSlices[0]->UpperLimit().KeyBound);
}

// YTADMINREQ-19334
TEST_F(TSortedChunkPoolNewKeysTest, TrickySliceSortOrder2)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 10_KB;
    InitJobConstraints();

    PrepareNewMock();
    CreateChunkPool();

    auto chunk = CreateChunk(BuildRow({0xA}), BuildRow({0xD}), 0);
    auto chunkSlice1 = CreateInputChunkSlice(chunk);
    chunkSlice1->TransformToNewKeyless();
    chunkSlice1->LowerLimit().RowIndex = 0;
    chunkSlice1->UpperLimit().RowIndex = 10;
    chunkSlice1->LowerLimit().KeyBound = BuildBound(">=", {0xA});
    chunkSlice1->UpperLimit().KeyBound = BuildBound("<", {0xD});
    auto chunkSlice2 = CreateInputChunkSlice(chunk);
    chunkSlice2->TransformToNewKeyless();
    chunkSlice2->LowerLimit().RowIndex = 10;
    chunkSlice2->UpperLimit().RowIndex = 20;
    chunkSlice2->LowerLimit().KeyBound = BuildBound(">=", {0xA});
    chunkSlice2->UpperLimit().KeyBound = BuildBound("<", {0xD});

    auto dataSlice = CreateDataSlice(chunk);

    CurrentMock().RegisterSliceableUnversionedDataSlice(dataSlice, {chunkSlice1, chunkSlice2});

    AddDataSlice(dataSlice);

    ChunkPool_->Finish();

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    auto outputCookie = ChunkPool_->Extract(NNodeTrackerClient::TNodeId(0));
    auto stripeList = ChunkPool_->GetStripeList(outputCookie);
    EXPECT_EQ(1u, stripeList->Stripes.size());
    EXPECT_EQ(2u, stripeList->Stripes[0]->DataSlices.size());
    EXPECT_EQ(10, stripeList->Stripes[0]->DataSlices[0]->UpperLimit().RowIndex);

    std::vector<TLegacyDataSlicePtr> unreadDataSlices = {
        CreateInputDataSlice(stripeList->Stripes[0]->DataSlices[0]),
        CreateInputDataSlice(stripeList->Stripes[0]->DataSlices[1]),
    };
    unreadDataSlices[0]->LowerLimit().RowIndex = 5;
    unreadDataSlices[0]->LowerLimit().KeyBound = BuildBound(">=", {0xB});
    TCompletedJobSummary jobSummary;
    jobSummary.InterruptReason = EInterruptReason::Preemption;
    jobSummary.UnreadInputDataSlices = unreadDataSlices;
    ChunkPool_->Completed(outputCookie, jobSummary);

    EXPECT_EQ(1, ChunkPool_->GetJobCounter()->GetPending());
    outputCookie = ChunkPool_->Extract(NNodeTrackerClient::TNodeId(0));
    stripeList = ChunkPool_->GetStripeList(outputCookie);
    EXPECT_EQ(1u, stripeList->Stripes.size());
    EXPECT_EQ(2u, stripeList->Stripes[0]->DataSlices.size());
    EXPECT_EQ(BuildBound(">=", {0xB}), stripeList->Stripes[0]->DataSlices[0]->LowerLimit().KeyBound);
    EXPECT_EQ(5, stripeList->Stripes[0]->DataSlices[0]->LowerLimit().RowIndex);
}

TEST_F(TSortedChunkPoolNewKeysTest, JoinReduceForeignChunkSlicing)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {true, false, false} /*isForeign*/,
        {false, false, false} /*isTeleportable*/,
        {false, false, false} /*isVersioned*/
    );
    InitPrimaryComparator(3);
    InitForeignComparator(2);
    Options_.MinTeleportChunkSize = 0;
    InitJobConstraints();
    PrepareNewMock();

    auto chunkA = CreateChunk(BuildRow({1, 21}), BuildRow({4, 24}), 0);
    auto dataSliceA = CreateDataSlice(chunkA);
    auto chunkASlices = SliceUnversionedDataSlice(dataSliceA, {BuildBound("<=", {3, 22})}, {1_KB / 2, 1_KB / 2});
    CurrentMock().RegisterSliceableUnversionedDataSlice(dataSliceA, chunkASlices);
    auto chunkB = CreateChunk(BuildRow({1, 101, 11}), BuildRow({4, 402, 18}), 1);
    auto dataSliceB = CreateDataSlice(chunkB);
    auto chunkC = CreateChunk(BuildRow({1, 102, 42}), BuildRow({4, 402, 48}), 2);
    auto dataSliceC = CreateDataSlice(chunkC);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceB);
    CurrentMock().RegisterTriviallySliceableUnversionedDataSlice(dataSliceC);

    CreateChunkPool();

    AddDataSlice(dataSliceA);
    AddDataSlice(dataSliceB);
    AddDataSlice(dataSliceC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_THAT(TeleportChunks_, IsEmpty());

    CheckEverything(stripeLists);
    EXPECT_EQ(2u, stripeLists[0]->Stripes[0]->DataSlices.size());
}

TEST_F(TSortedChunkPoolNewKeysTest, DynamicStores)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = false;
    InitTables(
        {false, false} /*isForeign*/,
        {false, false} /*isTeleportable*/,
        {true, true} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    InitJobConstraints();

    auto chunkA = CreateChunk(MinKey(), BuildRow({1}), 0);
    auto chunkB = CreateChunk(BuildRow({2}), MaxKey(), 0);
    auto chunkC = CreateChunk(MinKey(), MaxKey(), 1);

    CreateChunkPool();

    AddDataSlice(chunkA);
    AddDataSlice(chunkB);
    AddDataSlice(chunkC);

    ChunkPool_->Finish();

    ExtractOutputCookiesWhilePossible();
    auto stripeLists = GetAllStripeLists();

    EXPECT_TRUE(TeleportChunks_.empty());

    CheckEverything(stripeLists);
}

TEST_F(TSortedChunkPoolNewKeysTest, RowSlicingCorrectnessCustom)
{
    for (int iter = 0; iter < 100; ++iter) {
        SetUp();
        Gen_.seed(4243 + iter);
        Options_.SortedJobOptions.EnableKeyGuarantee = false;

        if (iter % 10 == 0) {
            Cerr << iter << Endl;
        }

        InitTables(
            {false, false} /*isForeign*/,
            {false, false} /*isTeleportable*/,
            {false, false} /*isVersioned*/
        );
        InitPrimaryComparator(1);

        const int contiguousChunkCount = 123;
        const int contiguousChunkWidth = 100;
        const int jobCount = 100;

        DataSizePerJob_ = contiguousChunkCount * 100_KB / jobCount;
        InitJobConstraints();

        std::vector<TLegacyDataSlicePtr> dataSlices;

        for (int index = 0; index < contiguousChunkCount; ++index) {
            auto chunk = CreateChunk(BuildRow({contiguousChunkWidth * index}), BuildRow({contiguousChunkWidth * (index + 1)}), 0, /*size*/ 100_KB);
            dataSlices.emplace_back(CreateDataSlice(chunk));
        }

        const int randomChunkCount = 10;

        auto distribution = std::uniform_int_distribution<int>(0, contiguousChunkWidth * contiguousChunkCount);

        std::vector<int> endpoints;
        for (int index = 0; index < 2 * randomChunkCount; ++index) {
            endpoints.push_back(distribution(Gen_));
        }

        std::sort(endpoints.begin(), endpoints.end());

        for (int index = 0; index < randomChunkCount; ++index) {
            // These chunks are so small that they do not affect job count.
            auto chunk = CreateChunk(BuildRow({endpoints[2 * index]}), BuildRow({endpoints[2 * index + 1]}), 1, /*size*/ 1, /*rowCount*/ 1);
            dataSlices.emplace_back(CreateDataSlice(chunk));
        }

        CreateChunkPool();

        for (const auto& dataSlice : dataSlices) {
            AddDataSlice(dataSlice);
        }

        ChunkPool_->Finish();

        ExtractOutputCookiesWhilePossible();

        auto stripeLists = GetAllStripeLists();
        EXPECT_GE(stripeLists.size(), 0.9 * jobCount);
        EXPECT_LE(stripeLists.size(), 1.1 * jobCount);

        CheckEverything(stripeLists);
    }
}

TEST_F(TSortedChunkPoolNewKeysTest, RowSlicingCorrectnessStrong)
{
    const int defaultIterCount = 10000;

    int iterCount = defaultIterCount;

    Cerr << "Default iteration count = " << defaultIterCount << ", in order to override it use YT_ITER_COUNT env var" << Endl;
    if (auto iterCountStr = GetEnv("YT_ITER_COUNT"); !iterCountStr.empty()) {
        iterCount = FromString<int>(iterCountStr);
    }

    Cerr << "Running " << iterCount << " iterations" << Endl;

    for (int iter = 0; iter < iterCount; ++iter) {
        SetUp();
        Gen_.seed(iter);
        Options_.SortedJobOptions.EnableKeyGuarantee = false;

        if (iter % 1000 == 0) {
            Cerr << iter << Endl;
        }

        const int maxTableCount = 3;
        const int maxChunkCount = 10;
        const int maxMaxKey = 100;
        const double coincidenceProbability = 0.5;
        const int minDataWeight = 1_KB;
        const int maxDataWeight = 100_KB;
        const int maxJobCount = 5;

        int tableCount = std::uniform_int_distribution<int>(1, maxTableCount)(Gen_);

        InitTables(
            std::vector<bool>(tableCount, false) /*isForeign*/,
            std::vector<bool>(tableCount, false) /*isTeleportable*/,
            std::vector<bool>(tableCount, false) /*isVersioned*/
        );
        InitPrimaryComparator(1);


        int maxKey = std::uniform_int_distribution<int>(1, maxMaxKey)(Gen_);

        std::vector<TLegacyDataSlicePtr> dataSlices;

        int totalSize = 0;

        for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
            std::vector<int> endpoints;
            int chunkCount = std::uniform_int_distribution<int>(1, maxChunkCount)(Gen_);
            for (int endpointIndex = 0; endpointIndex < 2 * chunkCount; ++endpointIndex) {
                if (!endpoints.empty() && std::bernoulli_distribution(coincidenceProbability)(Gen_)) {
                    endpoints.push_back(endpoints[std::uniform_int_distribution<int>(0, endpoints.size() - 1)(Gen_)]);
                } else {
                    endpoints.push_back(std::uniform_int_distribution<int>(0, maxKey)(Gen_));
                }
            }
            std::sort(endpoints.begin(), endpoints.end());
            for (int chunkIndex = 0; chunkIndex < chunkCount; ++chunkIndex) {
                int size = std::uniform_int_distribution<int>(minDataWeight, maxDataWeight)(Gen_);
                auto chunk = CreateChunk(BuildRow({endpoints[2 * chunkIndex]}), BuildRow({endpoints[2 * chunkIndex + 1]}), tableIndex, size);
                dataSlices.emplace_back(CreateDataSlice(chunk));
                totalSize += size;
            }
        }

        int jobCount = std::uniform_int_distribution<int>(1, maxJobCount)(Gen_);

        DataSizePerJob_ = totalSize / jobCount;
        InitJobConstraints();

        CreateChunkPool();

        for (const auto& dataSlice : dataSlices) {
            AddDataSlice(dataSlice);
        }

        ChunkPool_->Finish();

        ExtractOutputCookiesWhilePossible();
        auto stripeLists = GetAllStripeLists();

        CheckEverything(stripeLists);

        if (HasFailure()) {
            Cerr << "Failed on iteration " << iter << Endl;
            break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkPoolNewKeysTestRandomized
    : public WithParamInterface<std::tuple<int, bool>>
    , public TSortedChunkPoolNewKeysTest
{
public:
    TSortedChunkPoolNewKeysTestRandomized() = default;

    void SetUp() final
    {
        TSortedChunkPoolNewKeysTest::SetUp();
        Gen_.seed(get<0>(GetParam()));
    }
};


static constexpr int NumberOfRepeats = 100;

TEST_P(TSortedChunkPoolNewKeysTestRandomized, JobDataWeightDistribution)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = get<0>(GetParam()) % 2 == 0;
    Gen_.seed(get<0>(GetParam()));

    const int TableCount = 10;
    InitTables(
        std::vector<bool>(TableCount, false) /*isForeign*/,
        std::vector<bool>(TableCount, false) /*isTeleportable*/,
        std::vector<bool>(TableCount, false) /*isVersioned*/
    );
    InitPrimaryComparator(1);
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
        AddDataSlice(std::move(chunk));
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

    CheckEverything(stripeLists);
}

TEST_P(TSortedChunkPoolNewKeysTestRandomized, VariousOperationsWithPoolTest)
{
    Options_.SortedJobOptions.EnableKeyGuarantee = true;
    InitTables(
        {false} /*isForeign*/,
        {false} /*isTeleportable*/,
        {false} /*isVersioned*/
    );
    InitPrimaryComparator(1);
    DataSizePerJob_ = 1;
    InitJobConstraints();

    constexpr int maxChunkCount = 50;
    constexpr int maxUnderlyingPoolCount = 5;
    constexpr int maxJobLosts = 25;
    constexpr int maxInvalidationCount = 5;

    int chunkCount = std::uniform_int_distribution<>(0, maxChunkCount)(Gen_);

    for (int index = 0; index < chunkCount; ++index) {
        auto chunk = CreateChunk(BuildRow({2 * index}), BuildRow({2 * index + 1}), 0);
        CreateDataSlice(chunk);
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
            Options_.SortedJobOptions.PrimaryComparator = PrimaryComparator_;
            Options_.SortedJobOptions.ForeignComparator = ForeignComparator_;
            UnderlyingPools_.push_back(CreateNewSortedChunkPool(
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
    THashMap<TChunkId, TLegacyDataSlicePtr> chunkIdToDataSlice;

    std::vector<std::vector<TChunkStripePtr>> stripesByPoolIndex;
    if (useMultiPool) {
        stripesByPoolIndex.resize(underlyingPoolCount);
    } else {
        stripesByPoolIndex.resize(1);
    }

    THashMap<TChunkStripePtr, TChunkId> stripeToChunkId;

    for (const auto& dataSlice : CreatedUnversionedPrimaryDataSlices_) {
        auto chunkId = dataSlice->GetSingleUnversionedChunk()->GetChunkId();
        chunkIdToDataSlice[chunkId] = dataSlice;
        auto stripe = CreateStripe({dataSlice});
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
        InputCookieToChunkIds_[cookie] = {chunkId};
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
                auto chunkId = InputCookieToChunkIds_[cookie].front();
                YT_VERIFY(chunkId);
                Cdebug << Format(" that corresponds to a chunk %v", chunkId) << Endl;
                ASSERT_TRUE(resumedCookies.erase(cookie));
                ASSERT_TRUE(suspendedCookies.insert(cookie).second);
                ASSERT_TRUE(resumedChunks.erase(chunkId));
                ASSERT_TRUE(suspendedChunks.insert(chunkId).second);
                SuspendDataSlice(cookie);
            }
        } else if (eventType <= 39) {
            if (auto randomElement = chooseRandomElement(suspendedCookies)) {
                auto cookie = *randomElement;
                Cdebug << Format("Resuming cookie %v", cookie);
                auto chunkId = InputCookieToChunkIds_[cookie].front();
                YT_VERIFY(chunkId);
                Cdebug << Format(" that corresponds to a chunk %v", chunkId) << Endl;
                ASSERT_TRUE(suspendedCookies.erase(cookie));
                ASSERT_TRUE(resumedCookies.insert(cookie).second);
                ASSERT_TRUE(suspendedChunks.erase(chunkId));
                ASSERT_TRUE(resumedChunks.insert(chunkId).second);
                ResumeDataSlice(cookie);
            }
        } else if (eventType <= 49 && invalidationCount < maxInvalidationCount && std::ssize(completedChunks) > chunkCount / 2) {
            if (auto randomElement = chooseRandomElement(suspendedCookies)) {
                auto cookie = *randomElement;
                Cdebug << Format("Resetting cookie %v", cookie);
                auto chunkId = InputCookieToChunkIds_[cookie].front();
                YT_VERIFY(chunkId);
                Cdebug << Format(" that corresponds to a chunk %v", chunkId) << Endl;
                // TODO(max42): reset to something different.
                const auto& dataSlice = chunkIdToDataSlice.at(chunkId);
                ResetDataSlice(cookie, dataSlice);
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
    TSortedChunkPoolNewKeysTestRandomized,
    ::testing::Combine(::testing::Range(0, NumberOfRepeats), ::testing::Bool()));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkPools

#include "legacy_sorted_chunk_pool.h"

#include "legacy_job_manager.h"
#include "helpers.h"
#include "input_chunk_mapping.h"
#include "legacy_sorted_job_builder.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/library/random/bernoulli_sampler.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/core/logging/logger_owner.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NLogging;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TLegacySortedChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputWithLegacyJobManagerBase
    , public ISortedChunkPool
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
    , public TJobSplittingBase
    , public virtual TLoggerOwner
{
public:
    //! Used only for persistence.
    TLegacySortedChunkPool() = default;

    TLegacySortedChunkPool(
        const TSortedChunkPoolOptions& options,
        IChunkSliceFetcherFactoryPtr chunkSliceFetcherFactory,
        TInputStreamDirectory inputStreamDirectory)
        : TChunkPoolOutputWithLegacyJobManagerBase(options.Logger)
        , SortedJobOptions_(options.SortedJobOptions)
        , ChunkSliceFetcherFactory_(std::move(chunkSliceFetcherFactory))
        , EnableKeyGuarantee_(options.SortedJobOptions.EnableKeyGuarantee)
        , InputStreamDirectory_(std::move(inputStreamDirectory))
        , PrimaryPrefixLength_(options.SortedJobOptions.PrimaryPrefixLength)
        , ForeignPrefixLength_(options.SortedJobOptions.ForeignPrefixLength)
        , ShouldSlicePrimaryTableByKeys_(options.SortedJobOptions.ShouldSlicePrimaryTableByKeys)
        , SliceForeignChunks_(options.SliceForeignChunks)
        , MinTeleportChunkSize_(options.MinTeleportChunkSize)
        , JobSizeConstraints_(options.JobSizeConstraints)
        , TeleportChunkSampler_(JobSizeConstraints_->GetSamplingRate())
        , SupportLocality_(options.SupportLocality)
        , ReturnNewDataSlices_(options.ReturnNewDataSlices)
        , RowBuffer_(options.RowBuffer)
    {
        Logger = options.Logger;
        ValidateLogger(Logger);

        if (options.SortedJobOptions.PrimaryComparator.HasDescendingSortOrder() ||
            options.SortedJobOptions.ForeignComparator.HasDescendingSortOrder())
        {
            THROW_ERROR_EXCEPTION("Legacy sorted chunk pool does not support descending sort order");
        }

        YT_VERIFY(RowBuffer_);

        YT_LOG_DEBUG("Legacy sorted chunk pool created (EnableKeyGuarantee: %v, PrimaryPrefixLength: %v, "
            "ForeignPrefixLength: %v, DataWeightPerJob: %v, "
            "PrimaryDataWeightPerJob: %v, MaxDataSlicesPerJob: %v, InputSliceDataWeight: %v)",
            SortedJobOptions_.EnableKeyGuarantee,
            SortedJobOptions_.PrimaryPrefixLength,
            SortedJobOptions_.ForeignPrefixLength,
            JobSizeConstraints_->GetDataWeightPerJob(),
            JobSizeConstraints_->GetPrimaryDataWeightPerJob(),
            JobSizeConstraints_->GetMaxDataSlicesPerJob(),
            JobSizeConstraints_->GetInputSliceDataWeight());
    }

    IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        if (stripe->DataSlices.empty()) {
            return IChunkPoolInput::NullCookie;
        }

        for (const auto& dataSlice : stripe->DataSlices) {
            YT_VERIFY(dataSlice->IsLegacy);
        }

        auto cookie = static_cast<int>(Stripes_.size());
        Stripes_.emplace_back(stripe);

        return cookie;
    }

    void Finish() override
    {
        if (IsFinished()) {
            return;
        }

        TChunkPoolInputBase::Finish();

        // NB: this method accounts all the stripes that were suspended before
        // the chunk pool was finished. It should be called only once.
        SetupSuspendedStripes();

        DoFinish();
    }

    void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        auto& suspendableStripe = Stripes_[cookie];
        suspendableStripe.Suspend();
        if (Finished) {
            JobManager_->Suspend(cookie);
        }
    }

    void Resume(IChunkPoolInput::TCookie cookie) override
    {
        Stripes_[cookie].Resume();
        if (Finished) {
            JobManager_->Resume(cookie);
        }
    }

    void Reset(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe, TInputChunkMappingPtr mapping) override
    {
        for (int index = 0; index < std::ssize(Stripes_); ++index) {
            auto newStripe = (index == cookie) ? stripe : mapping->GetMappedStripe(Stripes_[index].GetStripe());
            Stripes_[index].Reset(newStripe);
        }
        if (Finished) {
            InvalidateCurrentJobs();
            DoFinish();
        }

        CheckCompleted();
    }

    bool IsCompleted() const override
    {
        return IsCompleted_;
    }

    void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        TJobSplittingBase::Completed(cookie, jobSummary);

        if (jobSummary.InterruptReason != EInterruptReason::None) {
            YT_LOG_DEBUG("Splitting job (OutputCookie: %v, InterruptReason: %v, SplitJobCount: %v)",
                cookie,
                jobSummary.InterruptReason,
                jobSummary.SplitJobCount);
            auto foreignSlices = JobManager_->ReleaseForeignSlices(cookie);

            auto childCookies = SplitJob(jobSummary.UnreadInputDataSlices, std::move(foreignSlices), jobSummary.SplitJobCount);
            RegisterChildCookies(cookie, std::move(childCookies));
        }
        JobManager_->Completed(cookie, jobSummary.InterruptReason);
        CheckCompleted();
    }

    void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        TChunkPoolOutputWithJobManagerBase::Lost(cookie);

        CheckCompleted();
    }

    void Persist(const TPersistenceContext& context) final
    {
        TChunkPoolInputBase::Persist(context);
        TChunkPoolOutputWithJobManagerBase::Persist(context);
        TJobSplittingBase::Persist(context);
        // TLoggerOwner is persisted by TJobSplittingBase.

        using NYT::Persist;
        Persist(context, SortedJobOptions_);
        Persist(context, ChunkSliceFetcherFactory_);
        Persist(context, EnableKeyGuarantee_);
        Persist(context, InputStreamDirectory_);
        Persist(context, PrimaryPrefixLength_);
        Persist(context, ForeignPrefixLength_);
        Persist(context, ShouldSlicePrimaryTableByKeys_);
        Persist(context, SliceForeignChunks_);
        Persist(context, MinTeleportChunkSize_);
        Persist(context, Stripes_);
        Persist(context, JobSizeConstraints_);
        Persist(context, TeleportChunkSampler_);
        Persist(context, SupportLocality_);
        Persist(context, TeleportChunks_);
        Persist(context, IsCompleted_);
        Persist(context, ReturnNewDataSlices_);

        if (context.IsLoad()) {
            // TODO(max42): Why is it here?
            RowBuffer_ = New<TRowBuffer>();
            ValidateLogger(Logger);
        }
    }

    std::pair<TKeyBound, TKeyBound> GetBounds(IChunkPoolOutput::TCookie /*cookie*/) const override
    {
        // We drop support for this method in legacy pool as it is used only in CHYT which already uses new pool.
        YT_UNIMPLEMENTED();
    }

    TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        // NB: recall that sorted pool deals with legacy data slices. If we simply call TransformToNew
        // on each data slice each time we extract a stripe list, it can lead to a controller-lifetime
        // long memory leak as modified unversioned rows will be stored in row buffer permanently.
        // Considering the fact that jobs may be failed/aborted and restarte arbitrarily number of times,
        // this may be a serious problem. That's why we cache transformed stripe lists in the pool.

        if (!ReturnNewDataSlices_) {
            return JobManager_->GetStripeList(cookie);
        }

        if (std::ssize(CachedNewStripeLists_) > cookie && CachedNewStripeLists_[cookie]) {
            return CachedNewStripeLists_[cookie];
        }

        if (std::ssize(CachedNewStripeLists_) <= cookie) {
            CachedNewStripeLists_.resize(cookie + 1);
        }

        auto newStripeList = New<TChunkStripeList>();
        auto legacyStripeList = JobManager_->GetStripeList(cookie);
        newStripeList->PartitionTag = legacyStripeList->PartitionTag;
        newStripeList->IsApproximate = legacyStripeList->IsApproximate;
        for (const auto& legacyStripe : legacyStripeList->Stripes) {
            auto newStripe = New<TChunkStripe>();
            newStripe->Foreign = legacyStripe->Foreign;
            for (const auto& legacyDataSlice : legacyStripe->DataSlices) {
                auto newDataSlice = CreateInputDataSlice(legacyDataSlice);
                auto prefixLength = InputStreamDirectory_.GetDescriptor(legacyDataSlice->GetInputStreamIndex()).IsPrimary()
                    ? PrimaryPrefixLength_
                    : ForeignPrefixLength_;
                newDataSlice->TransformToNew(RowBuffer_, prefixLength);
                newStripe->DataSlices.emplace_back(std::move(newDataSlice));
            }
            AddStripeToList(newStripe, newStripeList);
        }
        return CachedNewStripeLists_[cookie] = newStripeList;
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TLegacySortedChunkPool, 0x91bca805);

    //! All options necessary for sorted job builder.
    TSortedJobOptions SortedJobOptions_;

    //! A factory that is used to spawn chunk slice fetcher.
    IChunkSliceFetcherFactoryPtr ChunkSliceFetcherFactory_;

    //! Guarantee that each key goes to the single job.
    bool EnableKeyGuarantee_;

    //! Information about input sources (e.g. input tables for sorted reduce operation).
    TInputStreamDirectory InputStreamDirectory_;

    //! Length of the key according to which primary tables should be sorted during
    //! sorted reduce / sorted merge.
    int PrimaryPrefixLength_;

    //! Length of the key that defines a range in foreign tables that should be joined
    //! to the job.
    int ForeignPrefixLength_;

    //! Whether primary tables chunks should be sliced by keys.
    bool ShouldSlicePrimaryTableByKeys_;

    //! Whether foreign chunks should be sliced.
    bool SliceForeignChunks_ = false;

    //! An option to control chunk teleportation logic. Only large complete
    //! chunks of at least that size will be teleported.
    i64 MinTeleportChunkSize_;

    //! All stripes that were added to this pool.
    std::vector<TSuspendableStripe> Stripes_;

    IJobSizeConstraintsPtr JobSizeConstraints_;
    TBernoulliSampler TeleportChunkSampler_;

    bool SupportLocality_ = false;

    bool ReturnNewDataSlices_ = true;

    TRowBufferPtr RowBuffer_;

    std::vector<TInputChunkPtr> TeleportChunks_;

    bool IsCompleted_ = false;

    std::vector<TChunkStripeListPtr> CachedNewStripeLists_;

    //! This method processes all input stripes that do not correspond to teleported chunks
    //! and either slices them using ChunkSliceFetcher (for unversioned stripes) or leaves them as is
    //! (for versioned stripes).
    void FetchNonTeleportDataSlices(const ILegacySortedJobBuilderPtr& builder)
    {
        auto chunkSliceFetcher = ChunkSliceFetcherFactory_ ? ChunkSliceFetcherFactory_->CreateChunkSliceFetcher() : nullptr;
        auto primarySliceSize = JobSizeConstraints_->GetInputSliceDataWeight();
        auto foreignSliceSize = JobSizeConstraints_->GetForeignSliceDataWeight();

        YT_LOG_DEBUG("Fetching non-teleport data slices (HasChunkSliceFetcher: %v, PrimarySliceSize: %v, ForeignSliceSize: %v, SliceForeignChunks: %v)",
            static_cast<bool>(chunkSliceFetcher),
            primarySliceSize,
            foreignSliceSize,
            SliceForeignChunks_);

        // If chunkSliceFetcher == nullptr, we form chunk slices manually by putting them
        // into this vector.
        std::vector<TInputChunkSlicePtr> unversionedChunkSlices;

        THashMap<TInputChunkPtr, IChunkPoolInput::TCookie> unversionedInputChunkToInputCookie;
        THashMap<TInputChunkPtr, TLegacyDataSlicePtr> unversionedInputChunkToOwningDataSlice;

        auto processDataSlice = [&] (const TLegacyDataSlicePtr& dataSlice, int inputCookie) {
            if (InputStreamDirectory_.GetDescriptor(dataSlice->GetInputStreamIndex()).IsPrimary()) {
                builder->AddPrimaryDataSlice(dataSlice, inputCookie);
            } else {
                builder->AddForeignDataSlice(dataSlice, inputCookie);
            }
        };

        for (int inputCookie = 0; inputCookie < std::ssize(Stripes_); ++inputCookie) {
            const auto& suspendableStripe = Stripes_[inputCookie];
            const auto& stripe = suspendableStripe.GetStripe();

            if (suspendableStripe.GetTeleport()) {
                continue;
            }

            for (const auto& dataSlice : stripe->DataSlices) {
                // Unversioned data slices should be additionally sliced using chunkSliceFetcher,
                // while versioned slices are taken as is.
                if (dataSlice->Type == EDataSourceType::UnversionedTable) {
                    auto inputChunk = dataSlice->GetSingleUnversionedChunk();
                    auto isPrimary = InputStreamDirectory_.GetDescriptor(dataSlice->GetInputStreamIndex()).IsPrimary();
                    auto sliceSize = isPrimary
                        ? primarySliceSize
                        : foreignSliceSize;
                    auto keyColumnCount = isPrimary
                        ? PrimaryPrefixLength_
                        : ForeignPrefixLength_;
                    auto sliceByKeys = isPrimary
                        ? ShouldSlicePrimaryTableByKeys_
                        : false;

                    if (chunkSliceFetcher && (isPrimary || SliceForeignChunks_)) {
                        YT_LOG_TRACE("Slicing chunk (ChunkId: %v, DataWeight: %v, IsPrimary: %v, SliceSize: %v, KeyColumnCount: %v, SliceByKeys: %v)",
                            inputChunk->GetChunkId(),
                            inputChunk->GetDataWeight(),
                            isPrimary,
                            sliceSize,
                            keyColumnCount,
                            sliceByKeys);
                        TComparator comparator(std::vector<ESortOrder>(keyColumnCount, ESortOrder::Ascending));
                        chunkSliceFetcher->AddDataSliceForSlicing(dataSlice, comparator, sliceSize, sliceByKeys);
                    } else if (!isPrimary) {
                        // Take foreign slice as-is.
                        processDataSlice(dataSlice, inputCookie);
                    } else {
                        auto chunkSlice = CreateInputChunkSlice(inputChunk);
                        InferLimitsFromBoundaryKeys(chunkSlice, RowBuffer_, PrimaryPrefixLength_);
                        unversionedChunkSlices.emplace_back(std::move(chunkSlice));
                    }

                    unversionedInputChunkToOwningDataSlice[inputChunk] = dataSlice;
                    unversionedInputChunkToInputCookie[inputChunk] = inputCookie;
                } else {
                    processDataSlice(dataSlice, inputCookie);
                }
            }
        }

        if (chunkSliceFetcher) {
            WaitFor(chunkSliceFetcher->Fetch())
                .ThrowOnError();
            unversionedChunkSlices = chunkSliceFetcher->GetChunkSlices();
        }

        for (const auto& chunkSlice : unversionedChunkSlices) {
            const auto& originalDataSlice = unversionedInputChunkToOwningDataSlice[chunkSlice->GetInputChunk()];
            int inputCookie = GetOrCrash(unversionedInputChunkToInputCookie, chunkSlice->GetInputChunk());

            // We additionally slice maniac slices by evenly by row indices.
            auto chunk = chunkSlice->GetInputChunk();
            if (!EnableKeyGuarantee_ &&
                chunk->IsCompleteChunk() &&
                CompareRows(chunk->BoundaryKeys()->MinKey, chunk->BoundaryKeys()->MaxKey, PrimaryPrefixLength_) == 0 &&
                chunkSlice->GetDataWeight() > JobSizeConstraints_->GetInputSliceDataWeight() &&
                InputStreamDirectory_.GetDescriptor(originalDataSlice->GetInputStreamIndex()).IsPrimary())
            {
                auto smallerSlices = chunkSlice->SliceEvenly(
                    JobSizeConstraints_->GetInputSliceDataWeight(),
                    JobSizeConstraints_->GetInputSliceRowCount());
                for (const auto& smallerSlice : smallerSlices) {
                    auto dataSlice = CreateUnversionedInputDataSlice(smallerSlice);
                    dataSlice->CopyPayloadFrom(*originalDataSlice);
                    processDataSlice(dataSlice, inputCookie);
                }
            } else {
                auto dataSlice = CreateUnversionedInputDataSlice(chunkSlice);
                dataSlice->CopyPayloadFrom(*originalDataSlice);
                processDataSlice(dataSlice, inputCookie);
            }
        }
    }

    //! In this function all data slices that correspond to teleportable unversioned input chunks
    //! are added to `TeleportChunks_`.
    void FindTeleportChunks()
    {
        // Consider each chunk as a segment [minKey, maxKey]. Chunk may be teleported if:
        // 1) it is unversioned;
        // 2) it is complete (i. e. does not contain non-trivial read limits);
        // 3a) in case of SortedMerge: no other key (belonging to the different input chunk) lies in the interval (minKey, maxKey);
        // 3b) in case of SortedReduce: no other key lies in the s [minKey', maxKey'] (NB: if some other chunk shares endpoint
        //     with our chunk, our chunk can not be teleported since all instances of each key must be either teleported or
        //     be processed in the same job).
        //
        // We use the following logic to determine how different kinds of intervals are located on a line:
        //
        // * with respect to the s [a; b] data slice [l; r) is located:
        // *** to the left iff r <= a;
        // *** to the right iff l > b;
        // * with respect to the interval (a; b) interval [l; r) is located:
        // *** to the left iff r <= succ(a);
        // *** to the right iff l >= b.
        //
        // Using it, we find how many upper/lower keys are located to the left/right of candidate s/interval using the binary search
        // over the vectors of all lower and upper limits (including or excluding the chunk endpoints if `includeChunkBoundaryKeys`)
        // and check that in total there are exactly |ss| - 1 segments, this would exactly mean that all data slices (except the one
        // that corresponds to the chosen chunk) are either to the left or to the right of the chosen chunk.
        //
        // Unfortunately, there is a tricky corner case that should be treated differently: when chunk pool type is SortedMerge
        // (i. e. `includeChunkBoundaryKeys` is true) and the interval (a, b) is empty. For example, consider the following data slices:
        // (1) ["a"; "n",max), (2) ["n"; "n",max), (3) ["n"; "z",max) and suppose that we test the chunk (*) ["n"; "n"] to be teleportable.
        // In fact it is teleportable: (*) can be located between slices (1) and (2) or between slices (2) and (3). But if we try
        // to use the approach described above and calculate number of slices to the left of ("n"; "n") and slices to the right of ("n"; "n"),
        // we will account slice (2) twice: it is located to the left of ("n"; "n") because "n",max <= succ("n") and it is also located
        // to the right of ("n"; "n") because "n" >= "n". In the other words, when chunk pool type is SortedMerge teleporting of single-key chunks
        // is a hard work :(
        //
        // To overcome this difficulty, we additionally subtract the number of single-key slices that define the same key as our chunk.
        auto yielder = CreatePeriodicYielder();

        if (!SortedJobOptions_.PivotKeys.empty()) {
            return;
        }

        std::vector<TLegacyKey> lowerLimits, upperLimits;
        THashMap<TLegacyKey, int> singleKeySliceNumber;
        std::vector<std::pair<TInputChunkPtr, IChunkPoolInput::TCookie>> teleportCandidates;

        for (int inputCookie = 0; inputCookie < std::ssize(Stripes_); ++inputCookie) {
            const auto& stripe = Stripes_[inputCookie].GetStripe();
            auto primary = InputStreamDirectory_.GetDescriptor(stripe->GetInputStreamIndex()).IsPrimary();
            for (const auto& dataSlice : stripe->DataSlices) {
                yielder.TryYield();

                if (InputStreamDirectory_.GetDescriptor(stripe->GetInputStreamIndex()).IsTeleportable() &&
                    dataSlice->GetSingleUnversionedChunk()->IsLargeCompleteChunk(MinTeleportChunkSize_) &&
                    primary)
                {
                    teleportCandidates.emplace_back(dataSlice->GetSingleUnversionedChunk(), inputCookie);
                }

                lowerLimits.emplace_back(GetKeyPrefix(dataSlice->LegacyLowerLimit().Key, PrimaryPrefixLength_, RowBuffer_));
                if (static_cast<int>(dataSlice->LegacyUpperLimit().Key.GetCount()) > PrimaryPrefixLength_) {
                    upperLimits.emplace_back(GetKeySuccessor(GetKeyPrefix(dataSlice->LegacyUpperLimit().Key, PrimaryPrefixLength_, RowBuffer_), RowBuffer_));
                } else {
                    upperLimits.emplace_back(dataSlice->LegacyUpperLimit().Key);
                }

                if (CompareRows(dataSlice->LegacyLowerLimit().Key, dataSlice->LegacyUpperLimit().Key, PrimaryPrefixLength_) == 0) {
                    ++singleKeySliceNumber[lowerLimits.back()];
                }
            }
        }

        if (teleportCandidates.empty()) {
            return;
        }

        std::sort(lowerLimits.begin(), lowerLimits.end());
        std::sort(upperLimits.begin(), upperLimits.end());
        yielder.TryYield();

        int dataSlicesCount = lowerLimits.size();

        // Some chunks will be dropped due to sampling.
        int droppedTeleportChunkCount = 0;

        for (const auto& [teleportCandidate, cookie] : teleportCandidates) {
            yielder.TryYield();

            // NB: minKey and maxKey are inclusive, in contrast to the lower/upper limits.
            auto minKey = GetKeyPrefix(teleportCandidate->BoundaryKeys()->MinKey, PrimaryPrefixLength_, RowBuffer_);
            auto maxKey = GetKeyPrefix(teleportCandidate->BoundaryKeys()->MaxKey, PrimaryPrefixLength_, RowBuffer_);

            int slicesToTheLeft = (EnableKeyGuarantee_
                ? std::upper_bound(upperLimits.begin(), upperLimits.end(), minKey)
                : std::upper_bound(upperLimits.begin(), upperLimits.end(), GetKeySuccessor(minKey, RowBuffer_))) - upperLimits.begin();
            int slicesToTheRight = lowerLimits.end() - (EnableKeyGuarantee_
                ? std::upper_bound(lowerLimits.begin(), lowerLimits.end(), maxKey)
                : std::lower_bound(lowerLimits.begin(), lowerLimits.end(), maxKey));
            int extraCoincidingSingleKeySlices = 0;
            if (minKey == maxKey && !EnableKeyGuarantee_) {
                auto& sliceNumber = GetOrCrash(singleKeySliceNumber, minKey);
                // +1 because we accounted data slice for the current chunk twice (in slicesToTheLeft and slicesToTheRight),
                // but we actually want to account it zero time since we condier only data slices different from current.
                extraCoincidingSingleKeySlices = sliceNumber + 1;
            }
            int nonIntersectingSlices = slicesToTheLeft + slicesToTheRight - extraCoincidingSingleKeySlices;
            YT_VERIFY(nonIntersectingSlices <= dataSlicesCount - 1);
            if (nonIntersectingSlices == dataSlicesCount - 1) {
                Stripes_[cookie].SetTeleport(true);
                if (TeleportChunkSampler_.Sample()) {
                    TeleportChunks_.emplace_back(teleportCandidate);
                } else {
                    // Teleport chunk to /dev/null. He did not make it.
                    ++droppedTeleportChunkCount;
                }
            }
        }

        // The last step is to sort the resulting teleport chunks in order to be able to use
        // them while we build the jobs (they provide us with mandatory places where we have to
        // break the jobs).
        std::sort(
            TeleportChunks_.begin(),
            TeleportChunks_.end(),
            [this] (const TInputChunkPtr& lhs, const TInputChunkPtr& rhs) {
                auto lhsMinKey = GetKeyPrefix(lhs->BoundaryKeys()->MinKey, PrimaryPrefixLength_, RowBuffer_);
                auto lhsMaxKey = GetKeyPrefix(lhs->BoundaryKeys()->MaxKey, PrimaryPrefixLength_, RowBuffer_);
                auto rhsMinKey = GetKeyPrefix(rhs->BoundaryKeys()->MinKey, PrimaryPrefixLength_, RowBuffer_);
                auto rhsMaxKey = GetKeyPrefix(rhs->BoundaryKeys()->MaxKey, PrimaryPrefixLength_, RowBuffer_);

                int cmpMin = CompareRows(lhsMinKey, rhsMinKey);
                if (cmpMin != 0) {
                    return cmpMin < 0;
                }
                int cmpMax = CompareRows(lhsMaxKey, rhsMaxKey);
                if (cmpMax != 0) {
                    return cmpMax < 0;
                }
                // This is possible only when both chunks contain the same only key or we comparing chunk with itself.
                YT_VERIFY(&lhs == &rhs || lhsMinKey == lhsMaxKey);
                return false;
            });

        i64 totalTeleportChunkSize = 0;
        for (const auto& teleportChunk : TeleportChunks_) {
            ChunkTeleported_.Fire(teleportChunk, /*tag=*/std::any{});
            totalTeleportChunkSize += teleportChunk->GetUncompressedDataSize();
        }

        YT_LOG_DEBUG("Chunks teleported (ChunkCount: %v, DroppedChunkCount: %v, TotalSize: %v)",
            TeleportChunks_.size(),
            droppedTeleportChunkCount,
            totalTeleportChunkSize);
    }

    void SetupSuspendedStripes()
    {
        for (int inputCookie = 0; inputCookie < std::ssize(Stripes_); ++inputCookie) {
            const auto& stripe = Stripes_[inputCookie];
            if (stripe.IsSuspended()) {
                JobManager_->Suspend(inputCookie);
            }
        }
    }

    bool CanScheduleJob() const
    {
        return Finished && GetJobCounter()->GetPending() != 0;
    }

    TPeriodicYielder CreatePeriodicYielder()
    {
        if (SortedJobOptions_.EnablePeriodicYielder) {
            return TPeriodicYielder(PrepareYieldPeriod);
        } else {
            return TPeriodicYielder();
        }
    }

    TOutputOrderPtr GetOutputOrder() const override
    {
        return nullptr;
    }

    void DoFinish()
    {
        // NB(max42): this method may be run several times (in particular, when
        // the resumed input is not consistent with the original input).

        FindTeleportChunks();

        bool succeeded = false;
        ILegacySortedJobBuilderPtr builder;
        std::vector<std::unique_ptr<TLegacyJobStub>> jobStubs;
        std::vector<TError> errors;
        for (int retryIndex = 0; retryIndex < JobSizeConstraints_->GetMaxBuildRetryCount(); ++retryIndex) {
            try {
                builder = CreateLegacySortedJobBuilder(
                    SortedJobOptions_,
                    JobSizeConstraints_,
                    RowBuffer_,
                    TeleportChunks_,
                    retryIndex,
                    Logger);

                FetchNonTeleportDataSlices(builder);
                jobStubs = builder->Build();
                succeeded = true;
                break;
            } catch (TErrorException& ex) {
                if (ex.Error().FindMatching(EErrorCode::DataSliceLimitExceeded)) {
                    YT_LOG_DEBUG(ex,
                        "Retriable error during job building (RetryIndex: %v, MaxBuildRetryCount: %v)",
                        retryIndex,
                        JobSizeConstraints_->GetMaxBuildRetryCount());
                    errors.emplace_back(std::move(ex.Error()));
                    continue;
                }
                throw;
            }
        }
        if (!succeeded) {
            YT_LOG_DEBUG("Retry limit exceeded (MaxBuildRetryCount: %v)", JobSizeConstraints_->GetMaxBuildRetryCount());
            THROW_ERROR_EXCEPTION("Retry limit exceeded while building jobs")
                << errors;
        }
        JobManager_->AddJobs(std::move(jobStubs));

        if (JobSizeConstraints_->GetSamplingRate()) {
            JobManager_->Enlarge(
                JobSizeConstraints_->GetDataWeightPerJob(),
                JobSizeConstraints_->GetPrimaryDataWeightPerJob());
        }

        auto oldDataSliceCount = GetDataSliceCounter()->GetTotal();
        GetDataSliceCounter()->AddUncategorized(builder->GetTotalDataSliceCount() - oldDataSliceCount);

        CheckCompleted();
    }

    std::vector<IChunkPoolOutput::TCookie> SplitJob(
        std::vector<TLegacyDataSlicePtr> unreadInputDataSlices,
        std::vector<TLegacyDataSlicePtr> foreignInputDataSlices,
        int splitJobCount)
    {
        i64 dataWeight = 0;
        for (auto& dataSlice : unreadInputDataSlices) {
            dataWeight += dataSlice->GetDataWeight();
        }

        for (const auto& dataSlice : foreignInputDataSlices) {
            dataWeight += dataSlice->GetDataWeight();
        }
        i64 dataWeightPerJob = splitJobCount == 1
            ? std::numeric_limits<i64>::max() / 4
            : DivCeil(dataWeight, static_cast<i64>(splitJobCount));

        // We create new job size constraints by incorporating the new desired data size per job
        // into the old job size constraints.
        auto jobSizeConstraints = CreateExplicitJobSizeConstraints(
            false /*canAdjustDataSizePerJob*/,
            false /*isExplicitJobCount*/,
            splitJobCount /*jobCount*/,
            dataWeightPerJob,
            std::numeric_limits<i64>::max() / 4,
            JobSizeConstraints_->GetMaxDataSlicesPerJob(),
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob(),
            JobSizeConstraints_->GetInputSliceDataWeight(),
            JobSizeConstraints_->GetInputSliceRowCount(),
            JobSizeConstraints_->GetBatchRowCount(),
            JobSizeConstraints_->GetForeignSliceDataWeight(),
            std::nullopt /*samplingRate*/);

        // Teleport chunks do not affect the job split process since each original
        // job is already located between the teleport chunks.
        std::vector<TInputChunkPtr> teleportChunks;
        auto splitSortedJobOptions = SortedJobOptions_;
        // We do not want to yield during job splitting because it may potentially lead
        // to snapshot creation that will catch pool in inconsistent state.
        splitSortedJobOptions.EnablePeriodicYielder = false;
        auto builder = CreateLegacySortedJobBuilder(
            splitSortedJobOptions,
            std::move(jobSizeConstraints),
            RowBuffer_,
            teleportChunks,
            0 /*retryIndex*/,
            Logger);

        for (const auto& dataSlice : unreadInputDataSlices) {
            int inputCookie = *dataSlice->Tag;
            YT_VERIFY(InputStreamDirectory_.GetDescriptor(dataSlice->GetInputStreamIndex()).IsPrimary());
            builder->AddPrimaryDataSlice(dataSlice, inputCookie);
        }
        for (const auto& dataSlice : foreignInputDataSlices) {
            int inputCookie = *dataSlice->Tag;
            YT_VERIFY(InputStreamDirectory_.GetDescriptor(dataSlice->GetInputStreamIndex()).IsForeign());
            builder->AddForeignDataSlice(dataSlice, inputCookie);
        }

        auto jobs = builder->Build();
        auto childCookies = JobManager_->AddJobs(std::move(jobs));

        return childCookies;
    }

    void InvalidateCurrentJobs()
    {
        TeleportChunks_.clear();
        for (auto& stripe : Stripes_) {
            stripe.SetTeleport(false);
        }
        JobManager_->InvalidateAllJobs();
    }

    void CheckCompleted()
    {
        bool completed =
            Finished &&
            JobManager_->JobCounter()->GetPending() == 0 &&
            JobManager_->JobCounter()->GetRunning() == 0 &&
            JobManager_->JobCounter()->GetSuspended() == 0;

        if (!IsCompleted_ && completed) {
            Completed_.Fire();
        } else if (IsCompleted_ && !completed) {
            Uncompleted_.Fire();
        }

        IsCompleted_ = completed;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TLegacySortedChunkPool);

////////////////////////////////////////////////////////////////////////////////

ISortedChunkPoolPtr CreateLegacySortedChunkPool(
    const TSortedChunkPoolOptions& options,
    IChunkSliceFetcherFactoryPtr chunkSliceFetcherFactory,
    TInputStreamDirectory inputStreamDirectory)
{
    return New<TLegacySortedChunkPool>(options, std::move(chunkSliceFetcherFactory), std::move(inputStreamDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

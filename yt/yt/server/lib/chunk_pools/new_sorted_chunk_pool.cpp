#include "sorted_chunk_pool.h"

#include "job_manager.h"
#include "helpers.h"
#include "input_chunk_mapping.h"
#include "new_sorted_job_builder.h"

#include <yt/server/lib/controller_agent/job_size_constraints.h>
#include <yt/server/lib/controller_agent/structs.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/table_client/chunk_slice_fetcher.h>

#include <yt/library/random/bernoulli_sampler.h>

#include <yt/client/table_client/row_buffer.h>

#include <yt/core/concurrency/periodic_yielder.h>

#include <yt/core/misc/numeric_helpers.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NLogging;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TNewSortedChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputWithJobManagerBase
    , public ISortedChunkPool
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    //! Used only for persistence.
    TNewSortedChunkPool() = default;

    TNewSortedChunkPool(
        const TSortedChunkPoolOptions& options,
        IChunkSliceFetcherFactoryPtr chunkSliceFetcherFactory,
        TInputStreamDirectory inputStreamDirectory)
        : SortedJobOptions_(options.SortedJobOptions)
        , PrimaryComparator_(std::vector<ESortOrder>(options.SortedJobOptions.PrimaryPrefixLength, ESortOrder::Ascending))
        , ForeignComparator_(std::vector<ESortOrder>(options.SortedJobOptions.ForeignPrefixLength, ESortOrder::Ascending))
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
        , OperationId_(options.OperationId)
        , Task_(options.Task)
        , RowBuffer_(options.RowBuffer)
    {
        ForeignDataSlicesByStreamIndex_.resize(InputStreamDirectory_.GetDescriptorCount());
        Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
        Logger.AddTag("OperationId: %v", OperationId_);
        Logger.AddTag("Task: %v", Task_);
        JobManager_->SetLogger(Logger);

        if (!RowBuffer_) {
            RowBuffer_ = New<TRowBuffer>();
        }

        YT_LOG_DEBUG("Sorted chunk pool created (EnableKeyGuarantee: %v, PrimaryPrefixLength: %v, "
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

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        if (stripe->DataSlices.empty()) {
            return IChunkPoolInput::NullCookie;
        }

        bool isForeign = InputStreamDirectory_.GetDescriptor(stripe->GetInputStreamIndex()).IsForeign();

        for (auto& dataSlice : stripe->DataSlices) {
            dataSlice->TransformToNew(RowBuffer_, PrimaryComparator_.GetLength());

            int prefixLength = isForeign ? ForeignPrefixLength_ : PrimaryPrefixLength_;

            dataSlice->LowerLimit().KeyBound = ShortenKeyBound(dataSlice->LowerLimit().KeyBound, prefixLength, RowBuffer_);
            dataSlice->UpperLimit().KeyBound = ShortenKeyBound(dataSlice->UpperLimit().KeyBound, prefixLength, RowBuffer_);
        }

        auto cookie = static_cast<int>(Stripes_.size());
        Stripes_.emplace_back(stripe);

        if (isForeign && !HasForeignData_) {
            YT_LOG_DEBUG("Foreign data is present");
            HasForeignData_ = true;
        }

        return cookie;
    }

    virtual void Finish() override
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

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        auto& suspendableStripe = Stripes_[cookie];
        suspendableStripe.Suspend();
        if (Finished) {
            JobManager_->Suspend(cookie);
        }
    }

    virtual void Resume(IChunkPoolInput::TCookie cookie) override
    {
        Stripes_[cookie].Resume();
        if (Finished) {
            JobManager_->Resume(cookie);
        }
    }

    virtual void Reset(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe, TInputChunkMappingPtr mapping) override
    {
        for (int index = 0; index < Stripes_.size(); ++index) {
            auto newStripe = (index == cookie) ? stripe : mapping->GetMappedStripe(Stripes_[index].GetStripe());
            Stripes_[index].Reset(newStripe);
        }

        for (auto& stripe : Stripes_) {
            bool isForeign = InputStreamDirectory_.GetDescriptor(stripe.GetStripe()->GetInputStreamIndex()).IsForeign();
            for (auto& dataSlice : stripe.GetStripe()->DataSlices) {
                if (dataSlice->IsLegacy) {
                    dataSlice->TransformToNew(RowBuffer_, isForeign ? ForeignPrefixLength_ : PrimaryPrefixLength_);
                }
            }
        }

        if (Finished) {
            InvalidateCurrentJobs();
            DoFinish();
        }

        CheckCompleted();
    }

    virtual bool IsCompleted() const override
    {
        return IsCompleted_;
    }

    virtual void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        if (jobSummary.InterruptReason != EInterruptReason::None) {
            YT_LOG_DEBUG("Splitting job (OutputCookie: %v, InterruptReason: %v, SplitJobCount: %v)",
                cookie,
                jobSummary.InterruptReason,
                jobSummary.SplitJobCount);
            auto foreignSlices = JobManager_->ReleaseForeignSlices(cookie);
            SplitJob(jobSummary.UnreadInputDataSlices, std::move(foreignSlices), jobSummary.SplitJobCount);
        }
        JobManager_->Completed(cookie, jobSummary.InterruptReason);
        CheckCompleted();
    }

    virtual void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        TChunkPoolOutputWithJobManagerBase::Lost(cookie);

        CheckCompleted();
    }

    virtual void Persist(const TPersistenceContext& context) final override
    {
        TChunkPoolInputBase::Persist(context);
        TChunkPoolOutputWithJobManagerBase::Persist(context);

        using NYT::Persist;
        Persist(context, SortedJobOptions_);
        Persist(context, PrimaryComparator_);
        Persist(context, ForeignComparator_);
        Persist(context, ChunkSliceFetcherFactory_);
        Persist(context, EnableKeyGuarantee_);
        Persist(context, InputStreamDirectory_);
        Persist(context, PrimaryPrefixLength_);
        Persist(context, ForeignPrefixLength_);
        Persist(context, ShouldSlicePrimaryTableByKeys_);
        Persist(context, SliceForeignChunks_);
        Persist(context, MinTeleportChunkSize_);
        Persist(context, Stripes_);
        Persist(context, ForeignDataSlicesByStreamIndex_);
        Persist(context, JobSizeConstraints_);
        Persist(context, TeleportChunkSampler_);
        Persist(context, SupportLocality_);
        Persist(context, OperationId_);
        Persist(context, Task_);
        Persist(context, ChunkPoolId_);
        Persist(context, HasForeignData_);
        Persist(context, TeleportChunks_);
        Persist(context, IsCompleted_);
        if (context.IsLoad()) {
            Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
            Logger.AddTag("OperationId: %v", OperationId_);
            Logger.AddTag("Task: %v", Task_);
            JobManager_->SetLogger(Logger);
            RowBuffer_ = New<TRowBuffer>();
        }
    }

    virtual std::pair<TUnversionedRow, TUnversionedRow> GetLimits(IChunkPoolOutput::TCookie cookie) const override
    {
        return JobManager_->GetLimits(cookie);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TNewSortedChunkPool, 0x8ff1db04);

    //! All options necessary for sorted job builder.
    TSortedJobOptions SortedJobOptions_;

    //! Comparator corresponding to the primary merge or reduce key.
    TComparator PrimaryComparator_;

    //! Comparator corresponding to the foreign reduce key.
    TComparator ForeignComparator_;

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

    //! Stores all foreign data slices grouped by input stream index.
    std::vector<std::vector<TInputDataSlicePtr>> ForeignDataSlicesByStreamIndex_;

    IJobSizeConstraintsPtr JobSizeConstraints_;
    TBernoulliSampler TeleportChunkSampler_;

    bool SupportLocality_ = false;

    TLogger Logger = ChunkPoolLogger;

    TOperationId OperationId_;
    TString Task_;

    TGuid ChunkPoolId_ = TGuid::Create();

    TRowBufferPtr RowBuffer_;

    bool HasForeignData_ = false;

    std::vector<TInputChunkPtr> TeleportChunks_;

    bool IsCompleted_ = false;

    //! This method processes all input stripes that do not correspond to teleported chunks
    //! and either slices them using ChunkSliceFetcher (for unversioned stripes) or leaves them as is
    //! (for versioned stripes).
    void FetchNonTeleportDataSlices(const ISortedJobBuilderPtr& builder)
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
        THashMap<TInputChunkPtr, TInputDataSlicePtr> unversionedInputChunkToOwningDataSlice;

        //! Either add data slice to builder or put it into `ForeignDataSlicesByStreamIndex_' depending on whether
        //! it is primary or foreign.
        auto processDataSlice = [&] (const TInputDataSlicePtr& dataSlice, int inputCookie) {
            YT_VERIFY(!dataSlice->IsLegacy);
            if (InputStreamDirectory_.GetDescriptor(dataSlice->InputStreamIndex).IsPrimary()) {
                builder->AddPrimaryDataSlice(dataSlice, inputCookie);
            } else {
                dataSlice->Tag = inputCookie;
                ForeignDataSlicesByStreamIndex_[dataSlice->InputStreamIndex].emplace_back(dataSlice);
            }
        };

        for (int inputCookie = 0; inputCookie < Stripes_.size(); ++inputCookie) {
            const auto& suspendableStripe = Stripes_[inputCookie];
            const auto& stripe = suspendableStripe.GetStripe();

            if (suspendableStripe.GetTeleport()) {
                continue;
            }

            for (const auto& dataSlice : stripe->DataSlices) {
                // Unversioned data slices should be additionally sliced using chunkSliceFetcher,
                // while versioned slices are taken as is.
                if (dataSlice->Type == EDataSourceType::UnversionedTable) {
                    auto inputChunk = dataSlice->GetSingleUnversionedChunkOrThrow();
                    auto isPrimary = InputStreamDirectory_.GetDescriptor(dataSlice->InputStreamIndex).IsPrimary();
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
                        if (SortedJobOptions_.LogDetails) {
                            YT_LOG_DEBUG("Slicing chunk (ChunkId: %v, DataWeight: %v, IsPrimary: %v, SliceSize: %v, KeyColumnCount: %v, SliceByKeys: %v)",
                                inputChunk->ChunkId(),
                                inputChunk->GetDataWeight(),
                                isPrimary,
                                sliceSize,
                                keyColumnCount,
                                sliceByKeys);
                        }
                        chunkSliceFetcher->AddChunkForSlicing(inputChunk, sliceSize, keyColumnCount, sliceByKeys);
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
            int inputCookie = unversionedInputChunkToInputCookie.at(chunkSlice->GetInputChunk());

            // We additionally slice primary maniac slices by evenly by row indices.
            auto chunk = chunkSlice->GetInputChunk();
            if (!EnableKeyGuarantee_ &&
                chunk->IsCompleteChunk() &&
                CompareRows(chunk->BoundaryKeys()->MinKey, chunk->BoundaryKeys()->MaxKey, PrimaryPrefixLength_) == 0 &&
                chunkSlice->GetDataWeight() > JobSizeConstraints_->GetInputSliceDataWeight() &&
                InputStreamDirectory_.GetDescriptor(originalDataSlice->InputStreamIndex).IsPrimary())
            {
                auto smallerSlices = chunkSlice->SliceEvenly(
                    JobSizeConstraints_->GetInputSliceDataWeight(),
                    JobSizeConstraints_->GetInputSliceRowCount());
                for (const auto& smallerSlice : smallerSlices) {
                    auto dataSlice = CreateUnversionedInputDataSlice(smallerSlice);
                    dataSlice->CopyPayloadFrom(*originalDataSlice);
                    dataSlice->TransformToNew(RowBuffer_, PrimaryPrefixLength_);
                    processDataSlice(dataSlice, inputCookie);
                }
            } else {
                auto dataSlice = CreateUnversionedInputDataSlice(chunkSlice);
                dataSlice->CopyPayloadFrom(*originalDataSlice);
                dataSlice->TransformToNew(RowBuffer_, PrimaryPrefixLength_);
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
        // 2) it is complete (i.e. does not contain non-trivial read limits);
        // 3a) if enable_key_guarantee = %false: no other key (belonging to the different input chunk) lies in the interval (minKey, maxKey);
        // 3b) if enable_key_guarantee = %true: no other key lies in the segment [minKey, maxKey] (NB: if some other chunk shares endpoint
        //     with our chunk, our chunk can not be teleported since all instances of each key must be either teleported or
        //     be processed in the same job).
        //
        // We find how many upper/lower bounds are located (strictly or not, depending on EnableKeyGuarantee) to the left/right
        // of candidate chunk using the binary search over the vectors of all lower and upper limits and check that in total there are
        // exactly n - 1 segments, this would exactly mean that all data slices (except the one
        // that corresponds to the chosen chunk) are either to the left or to the right of the chosen chunk.
        //
        // Unfortunately, there is a tricky corner case that should be treated differently: when chunk pool type is SortedMerge
        // (i.e. !enableKeyGuarantee) and chunk is singleton. For example, consider the following data slices:
        // (1) ["a"; "n"), (2) ["n"; "n"], (3) ["n"; "z") and suppose that we test the chunk (*) ["n"; "n"] to be teleportable.
        // In fact it is teleportable: (*) can be located between slices (1) and (2) as well as between slices (2) and (3).
        // But if we try to use the approach described above and, we will account slice (2) twice: it is located both to the left
        // and to the right from (*) ["n"; "n"].
        //
        // To overcome this difficulty, we additionally subtract the number of singleton slices that define the same key as our singleton chunk.
        auto yielder = CreatePeriodicYielder();

        if (!SortedJobOptions_.PivotKeys.empty()) {
            return;
        }

        std::vector<TKeyBound> lowerLimits;
        std::vector<TKeyBound> upperLimits;
        THashMap<TKey, int> keyToSingletonSliceCount;

        struct TTeleportCandidate
        {
            TInputChunkPtr InputChunk;
            TKeyBound LowerBound;
            TKeyBound UpperBound;
            IChunkPoolInput::TCookie InputCookie;
        };

        std::vector<TTeleportCandidate> teleportCandidates;

        for (int inputCookie = 0; inputCookie < Stripes_.size(); ++inputCookie) {
            const auto& stripe = Stripes_[inputCookie].GetStripe();
            auto primary = InputStreamDirectory_.GetDescriptor(stripe->GetInputStreamIndex()).IsPrimary();
            for (const auto& dataSlice : stripe->DataSlices) {
                yielder.TryYield();

                auto lowerBound = dataSlice->LowerLimit().KeyBound;
                auto upperBound = dataSlice->UpperLimit().KeyBound;

                if (InputStreamDirectory_.GetDescriptor(stripe->GetInputStreamIndex()).IsTeleportable() &&
                    dataSlice->GetSingleUnversionedChunkOrThrow()->IsLargeCompleteChunk(MinTeleportChunkSize_) &&
                    primary)
                {
                    teleportCandidates.emplace_back(TTeleportCandidate{
                        .InputChunk = dataSlice->GetSingleUnversionedChunkOrThrow(),
                        .LowerBound = lowerBound,
                        .UpperBound = upperBound,
                        .InputCookie = inputCookie
                    });
                }

                lowerLimits.emplace_back(lowerBound);
                upperLimits.emplace_back(upperBound);

                if (auto key = PrimaryComparator_.TryAsSingletonKey(lowerBound, upperBound)) {
                    ++keyToSingletonSliceCount[*key];
                }
            }
        }

        if (teleportCandidates.empty()) {
            return;
        }

        auto keyBoundComparator = [&] (const auto& lhs, const auto& rhs) {
            return PrimaryComparator_.CompareKeyBounds(lhs, rhs) < 0;
        };

        std::sort(lowerLimits.begin(), lowerLimits.end(), keyBoundComparator);
        std::sort(upperLimits.begin(), upperLimits.end(), keyBoundComparator);
        yielder.TryYield();

        int dataSlicesCount = lowerLimits.size();

        // Some chunks will be dropped due to sampling.
        int droppedTeleportChunkCount = 0;

        for (const auto& teleportCandidate : teleportCandidates) {
            yielder.TryYield();

            auto lowerBound = teleportCandidate.LowerBound;
            auto upperBound = teleportCandidate.UpperBound;

            i64 slicesToTheLeft = (EnableKeyGuarantee_
                ? std::upper_bound(upperLimits.begin(), upperLimits.end(), lowerBound, keyBoundComparator)
                : std::upper_bound(upperLimits.begin(), upperLimits.end(), lowerBound.ToggleInclusiveness(), keyBoundComparator)) - upperLimits.begin();
            i64 slicesToTheRight = lowerLimits.end() - (EnableKeyGuarantee_
                ? std::lower_bound(lowerLimits.begin(), lowerLimits.end(), upperBound, keyBoundComparator)
                : std::lower_bound(lowerLimits.begin(), lowerLimits.end(), upperBound.ToggleInclusiveness(), keyBoundComparator));
            int extraCoincidingSingletonSlices = 0;
            if (auto key = PrimaryComparator_.TryAsSingletonKey(lowerBound, upperBound); key && !EnableKeyGuarantee_) {
                auto singletonSliceCount = GetOrCrash(keyToSingletonSliceCount, *key);
                // +1 because we accounted data slice for the current chunk twice (in slicesToTheLeft and slicesToTheRight),
                // but we actually want to account it zero times since we consider only data slices different from current.
                extraCoincidingSingletonSlices = singletonSliceCount + 1;
            }
            i64 disjointSlices = slicesToTheLeft + slicesToTheRight - extraCoincidingSingletonSlices;
            YT_VERIFY(disjointSlices <= dataSlicesCount - 1);
            YT_VERIFY(disjointSlices >= 0);
            if (disjointSlices == dataSlicesCount - 1) {
                Stripes_[teleportCandidate.InputCookie].SetTeleport(true);
                if (TeleportChunkSampler_.Sample()) {
                    TeleportChunks_.emplace_back(teleportCandidate.InputChunk);
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
            [] (const TInputChunkPtr& lhs, const TInputChunkPtr& rhs) {
                // TODO(max42): should we introduce key bounds instead of boundary keys for input chunk?
                int cmpMin = CompareRows(lhs->BoundaryKeys()->MinKey, rhs->BoundaryKeys()->MinKey);
                if (cmpMin != 0) {
                    return cmpMin < 0;
                }
                int cmpMax = CompareRows(lhs->BoundaryKeys()->MaxKey, rhs->BoundaryKeys()->MaxKey);
                if (cmpMax != 0) {
                    return cmpMax < 0;
                }
                // This is possible only when both chunks contain the same only key or we comparing chunk with itself.
                YT_VERIFY(&lhs == &rhs || lhs->BoundaryKeys()->MinKey == lhs->BoundaryKeys()->MaxKey);
                return false;
            });

        i64 totalTeleportChunkSize = 0;
        for (const auto& teleportChunk : TeleportChunks_) {
            ChunkTeleported_.Fire(teleportChunk, /* tag = */ std::any{});
            totalTeleportChunkSize += teleportChunk->GetUncompressedDataSize();
        }

        YT_LOG_DEBUG("Chunks teleported (ChunkCount: %v, DroppedChunkCount: %v, TotalSize: %v)",
            TeleportChunks_.size(),
            droppedTeleportChunkCount,
            totalTeleportChunkSize);
    }

    void PrepareForeignDataSlices(const ISortedJobBuilderPtr& builder)
    {
        auto yielder = CreatePeriodicYielder();

        std::vector<std::pair<TInputDataSlicePtr, IChunkPoolInput::TCookie>> foreignDataSlices;

        for (int streamIndex = 0; streamIndex < ForeignDataSlicesByStreamIndex_.size(); ++streamIndex) {
            if (!InputStreamDirectory_.GetDescriptor(streamIndex).IsForeign()) {
                continue;
            }

            yielder.TryYield();

            auto& dataSlices = ForeignDataSlicesByStreamIndex_[streamIndex];

            // In most cases the foreign table stripes follow in sorted order, but still let's ensure that.
            auto cmpStripesByKey = [&] (const TInputDataSlicePtr& lhs, const TInputDataSlicePtr& rhs) {
                auto lowerComparisonResult = ForeignComparator_.CompareKeyBounds(
                    lhs->LowerLimit().KeyBound,
                    rhs->LowerLimit().KeyBound);
                auto upperComparisonResult = ForeignComparator_.CompareKeyBounds(
                    lhs->UpperLimit().KeyBound,
                    rhs->UpperLimit().KeyBound);
                // NB: ranges do not overlap because each stream is sorted.
                auto comparisonResult = lowerComparisonResult + upperComparisonResult;
                return comparisonResult < 0;
            };
            if (!std::is_sorted(dataSlices.begin(), dataSlices.end(), cmpStripesByKey)) {
                std::stable_sort(dataSlices.begin(), dataSlices.end(), cmpStripesByKey);
            }

            for (const auto& dataSlice : dataSlices) {
                builder->AddForeignDataSlice(dataSlice, /* inputCookie */ *dataSlice->Tag);
            }
        }
    }

    void SetupSuspendedStripes()
    {
        for (int inputCookie = 0; inputCookie < Stripes_.size(); ++inputCookie) {
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

    TOutputOrderPtr GetOutputOrder() const
    {
        return nullptr;
    }

    void DoFinish()
    {
        // NB(max42): this method may be run several times (in particular, when
        // the resumed input is not consistent with the original input).

        FindTeleportChunks();

        bool succeeded = false;
        ISortedJobBuilderPtr builder;
        std::vector<std::unique_ptr<TJobStub>> jobStubs;
        std::vector<TError> errors;
        for (int retryIndex = 0; retryIndex < JobSizeConstraints_->GetMaxBuildRetryCount(); ++retryIndex) {
            try {
                builder = CreateNewSortedJobBuilder(
                    SortedJobOptions_,
                    JobSizeConstraints_,
                    RowBuffer_,
                    TeleportChunks_,
                    false /* inSplit */,
                    retryIndex,
                    Logger);

                FetchNonTeleportDataSlices(builder);
                if (HasForeignData_) {
                    PrepareForeignDataSlices(builder);
                }
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

    void SplitJob(
        std::vector<TInputDataSlicePtr> unreadInputDataSlices,
        std::vector<TInputDataSlicePtr> foreignInputDataSlices,
        int splitJobCount)
    {
        i64 dataWeight = 0;
        for (auto& dataSlice : unreadInputDataSlices) {
            // NB(psushin): this is important, since we prune trivial limits from slices when serializing to proto.
            // Here we restore them back.
            InferLimitsFromBoundaryKeys(dataSlice, RowBuffer_);
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
            false /* canAdjustDataSizePerJob */,
            false /* isExplicitJobCount */,
            splitJobCount /* jobCount */,
            dataWeightPerJob,
            std::numeric_limits<i64>::max() / 4,
            JobSizeConstraints_->GetMaxDataSlicesPerJob(),
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob(),
            JobSizeConstraints_->GetInputSliceDataWeight(),
            JobSizeConstraints_->GetInputSliceRowCount(),
            JobSizeConstraints_->GetForeignSliceDataWeight(),
            std::nullopt /* samplingRate */);

        // Teleport chunks do not affect the job split process since each original
        // job is already located between the teleport chunks.
        std::vector<TInputChunkPtr> teleportChunks;
        auto splitSortedJobOptions = SortedJobOptions_;
        // We do not want to yield during job splitting because it may potentially lead
        // to snapshot creation that will catch pool in inconsistent state.
        splitSortedJobOptions.EnablePeriodicYielder = false;
        auto builder = CreateNewSortedJobBuilder(
            splitSortedJobOptions,
            std::move(jobSizeConstraints),
            RowBuffer_,
            teleportChunks,
            true /* inSplit */,
            0 /* retryIndex */,
            Logger);

        for (const auto& dataSlice : unreadInputDataSlices) {
            int inputCookie = *dataSlice->Tag;
            YT_VERIFY(InputStreamDirectory_.GetDescriptor(dataSlice->InputStreamIndex).IsPrimary());
            dataSlice->TransformToNew(RowBuffer_, PrimaryPrefixLength_);
            builder->AddPrimaryDataSlice(dataSlice, inputCookie);
        }
        for (const auto& dataSlice : foreignInputDataSlices) {
            int inputCookie = *dataSlice->Tag;
            YT_VERIFY(InputStreamDirectory_.GetDescriptor(dataSlice->InputStreamIndex).IsForeign());
            dataSlice->TransformToNew(RowBuffer_, ForeignPrefixLength_);
            builder->AddForeignDataSlice(dataSlice, inputCookie);
        }

        auto jobs = builder->Build();
        JobManager_->AddJobs(std::move(jobs));
    }

    void InvalidateCurrentJobs()
    {
        TeleportChunks_.clear();
        for (auto& stripe : Stripes_) {
            stripe.SetTeleport(false);
        }
        JobManager_->InvalidateAllJobs();
    }

    void CheckCompleted() {
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

DEFINE_DYNAMIC_PHOENIX_TYPE(TNewSortedChunkPool);

////////////////////////////////////////////////////////////////////////////////

ISortedChunkPoolPtr CreateNewSortedChunkPool(
    const TSortedChunkPoolOptions& options,
    IChunkSliceFetcherFactoryPtr chunkSliceFetcherFactory,
    TInputStreamDirectory inputStreamDirectory)
{
    return New<TNewSortedChunkPool>(options, std::move(chunkSliceFetcherFactory), std::move(inputStreamDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

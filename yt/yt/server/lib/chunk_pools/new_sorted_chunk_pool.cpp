#include "new_sorted_chunk_pool.h"

#include "new_job_manager.h"
#include "helpers.h"
#include "input_chunk_mapping.h"
#include "new_sorted_job_builder.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/library/random/bernoulli_sampler.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/core/logging/logger_owner.h>
#include <yt/yt/core/logging/serializable_logger.h>

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

class TNewSortedChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputWithNewJobManagerBase
    , public ISortedChunkPool
    , public virtual TLoggerOwner
    , public TJobSplittingBase
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    //! Used only for persistence.
    TNewSortedChunkPool() = default;

    TNewSortedChunkPool(
        const TSortedChunkPoolOptions& options,
        IChunkSliceFetcherFactoryPtr chunkSliceFetcherFactory,
        TInputStreamDirectory inputStreamDirectory)
        : TChunkPoolOutputWithNewJobManagerBase(options.Logger)
        , SortedJobOptions_(options.SortedJobOptions)
        , PrimaryComparator_(options.SortedJobOptions.PrimaryComparator)
        , ForeignComparator_(options.SortedJobOptions.ForeignComparator)
        , ChunkSliceFetcherFactory_(std::move(chunkSliceFetcherFactory))
        , EnableKeyGuarantee_(options.SortedJobOptions.EnableKeyGuarantee)
        , InputStreamDirectory_(std::move(inputStreamDirectory))
        , PrimaryPrefixLength_(PrimaryComparator_.GetLength())
        , ForeignPrefixLength_(ForeignComparator_.GetLength())
        , ShouldSlicePrimaryTableByKeys_(options.SortedJobOptions.ShouldSlicePrimaryTableByKeys)
        , SliceForeignChunks_(options.SliceForeignChunks)
        , MinTeleportChunkSize_(options.MinTeleportChunkSize)
        , JobSizeConstraints_(options.JobSizeConstraints)
        , TeleportChunkSampler_(JobSizeConstraints_->GetSamplingRate())
        , SupportLocality_(options.SupportLocality)
        , RowBuffer_(options.RowBuffer)
    {
        Logger = options.Logger;
        StructuredLogger = options.StructuredLogger;
        ValidateLogger(Logger);

        YT_VERIFY(RowBuffer_);

        YT_LOG_DEBUG("New sorted chunk pool created (EnableKeyGuarantee: %v, PrimaryPrefixLength: %v, "
            "ForeignPrefixLength: %v, DataWeightPerJob: %v, "
            "PrimaryDataWeightPerJob: %v, MaxDataSlicesPerJob: %v, InputSliceDataWeight: %v)",
            SortedJobOptions_.EnableKeyGuarantee,
            PrimaryPrefixLength_,
            ForeignPrefixLength_,
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

        const auto& inputStreamDescriptor = InputStreamDirectory_.GetDescriptor(stripe->GetInputStreamIndex());
        bool isForeign = inputStreamDescriptor.IsForeign();
        int prefixLength = isForeign ? ForeignPrefixLength_ : PrimaryPrefixLength_;

        for (auto& dataSlice : stripe->DataSlices) {
            YT_VERIFY(!dataSlice->IsLegacy);

            // For simplicity, we require that any data slice in sorted chunk pool has
            // non-trivial lower and upper key bounds. In particular this means that
            // all limit inference from chunk boundary keys should be done in task.
            YT_VERIFY(dataSlice->LowerLimit().KeyBound);
            YT_VERIFY(dataSlice->UpperLimit().KeyBound);
            YT_VERIFY(static_cast<int>(dataSlice->LowerLimit().KeyBound.Prefix.GetCount()) <= prefixLength);
            YT_VERIFY(static_cast<int>(dataSlice->UpperLimit().KeyBound.Prefix.GetCount()) <= prefixLength);

            if (!inputStreamDescriptor.IsVersioned()) {
                YT_VERIFY(!dataSlice->LowerLimit().KeyBound.IsUniversal());
                YT_VERIFY(!dataSlice->UpperLimit().KeyBound.IsUniversal());
            }

            YT_LOG_TRACE("Data slice added (DataSlice: %v)", GetDataSliceDebugString(dataSlice));

            dataSlice->LowerLimit().KeyBound = ShortenKeyBound(dataSlice->LowerLimit().KeyBound, prefixLength, RowBuffer_);
            dataSlice->UpperLimit().KeyBound = ShortenKeyBound(dataSlice->UpperLimit().KeyBound, prefixLength, RowBuffer_);
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

        for (auto& stripe : Stripes_) {
            for (auto& dataSlice : stripe.GetStripe()->DataSlices) {
                YT_VERIFY(!dataSlice->IsLegacy);
            }
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
        Persist(context, JobSizeConstraints_);
        Persist(context, TeleportChunkSampler_);
        Persist(context, SupportLocality_);
        Persist(context, TeleportChunks_);
        Persist(context, IsCompleted_);
        Persist(context, StructuredLogger);

        if (context.IsLoad()) {
            ValidateLogger(Logger);
            RowBuffer_ = New<TRowBuffer>();
        }
    }

    std::pair<TKeyBound, TKeyBound> GetBounds(IChunkPoolOutput::TCookie cookie) const override
    {
        return JobManager_->GetBounds(cookie);
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

    IJobSizeConstraintsPtr JobSizeConstraints_;
    TBernoulliSampler TeleportChunkSampler_;

    bool SupportLocality_ = false;

    TRowBufferPtr RowBuffer_;

    std::vector<TInputChunkPtr> TeleportChunks_;

    bool IsCompleted_ = false;

    TSerializableLogger StructuredLogger;

    //! This method processes all input stripes that do not correspond to teleported chunks
    //! and either slices them using ChunkSliceFetcher (for unversioned stripes) or leaves them as is
    //! (for versioned stripes).
    void FetchNonTeleportDataSlices(const INewSortedJobBuilderPtr& builder)
    {
        auto chunkSliceFetcher = ChunkSliceFetcherFactory_ ? ChunkSliceFetcherFactory_->CreateChunkSliceFetcher() : nullptr;
        auto primarySliceSize = JobSizeConstraints_->GetInputSliceDataWeight();
        auto foreignSliceSize = JobSizeConstraints_->GetForeignSliceDataWeight();

        // TODO(max42): job size constraints is incredibly bloated :( get rid of such workarounds.
        primarySliceSize = std::min(primarySliceSize, JobSizeConstraints_->GetPrimaryDataWeightPerJob());

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

        //! Either add data slice to builder or put it into `ForeignDataSlicesByStreamIndex_' depending on whether
        //! it is primary or foreign.
        auto processDataSlice = [&] (const TLegacyDataSlicePtr& dataSlice, int inputCookie) {
            YT_VERIFY(!dataSlice->IsLegacy);
            dataSlice->Tag = inputCookie;
            builder->AddDataSlice(dataSlice);
        };

        // TODO(max42): logic here is schizophrenic :( introduce IdentityChunkSliceFetcher and
        // stop converting chunk slices to data slices and forth.

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
                    auto comparator = isPrimary
                        ? PrimaryComparator_
                        : ForeignComparator_;
                    auto sliceSize = isPrimary
                        ? primarySliceSize
                        : foreignSliceSize;
                    if (chunkSliceFetcher && (isPrimary || SliceForeignChunks_)) {
                        YT_LOG_TRACE("Slicing chunk (ChunkId: %v, DataWeight: %v, IsPrimary: %v, Comparator: %v, SliceSize: %v)",
                            inputChunk->GetChunkId(),
                            inputChunk->GetDataWeight(),
                            isPrimary,
                            comparator,
                            sliceSize);

                        chunkSliceFetcher->AddDataSliceForSlicing(dataSlice, comparator, sliceSize, /*sliceByKeys*/ true);
                    } else if (!isPrimary) {
                        // Take foreign slice as-is.
                        processDataSlice(dataSlice, inputCookie);
                    } else {
                        YT_VERIFY(dataSlice->ChunkSlices.size() == 1);
                        auto chunkSlice = dataSlice->ChunkSlices[0];
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
            auto dataSlice = CreateUnversionedInputDataSlice(chunkSlice);
            dataSlice->CopyPayloadFrom(*originalDataSlice);

            auto isPrimary = InputStreamDirectory_.GetDescriptor(dataSlice->GetInputStreamIndex()).IsPrimary();
            auto comparator = isPrimary
                ? PrimaryComparator_
                : ForeignComparator_;

            comparator.ReplaceIfStrongerKeyBound(dataSlice->LowerLimit().KeyBound, originalDataSlice->LowerLimit().KeyBound);
            comparator.ReplaceIfStrongerKeyBound(dataSlice->UpperLimit().KeyBound, originalDataSlice->UpperLimit().KeyBound);

            YT_VERIFY(!dataSlice->IsLegacy);
            processDataSlice(dataSlice, inputCookie);
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
            TLegacyDataSlicePtr DataSlice;
            TKeyBound LowerBound;
            TKeyBound UpperBound;
            IChunkPoolInput::TCookie InputCookie;
        };

        std::vector<TTeleportCandidate> teleportCandidates;

        for (int inputCookie = 0; inputCookie < std::ssize(Stripes_); ++inputCookie) {
            const auto& stripe = Stripes_[inputCookie].GetStripe();
            auto inputStreamIndex = stripe->GetInputStreamIndex();
            auto isPrimary = InputStreamDirectory_.GetDescriptor(inputStreamIndex).IsPrimary();
            auto isTeleportable = InputStreamDirectory_.GetDescriptor(inputStreamIndex).IsTeleportable();
            for (const auto& dataSlice : stripe->DataSlices) {
                yielder.TryYield();

                auto lowerBound = dataSlice->LowerLimit().KeyBound;
                auto upperBound = dataSlice->UpperLimit().KeyBound;

                if (dataSlice->IsTeleportable && isPrimary && isTeleportable) {
                    teleportCandidates.emplace_back(TTeleportCandidate{
                        .DataSlice = dataSlice,
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

        std::vector<TLegacyDataSlicePtr> teleportDataSlices;

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
                    teleportDataSlices.emplace_back(teleportCandidate.DataSlice);
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
            teleportDataSlices.begin(),
            teleportDataSlices.end(),
            [&] (const TLegacyDataSlicePtr& lhs, const TLegacyDataSlicePtr& rhs) {
                const auto& lhsLowerBound = lhs->LowerLimit().KeyBound;
                const auto& lhsUpperBound = lhs->UpperLimit().KeyBound;
                const auto& rhsLowerBound = rhs->LowerLimit().KeyBound;
                const auto& rhsUpperBound = rhs->UpperLimit().KeyBound;

                int cmpLower = PrimaryComparator_.CompareKeyBounds(lhsLowerBound, rhsLowerBound);
                if (cmpLower != 0) {
                    return cmpLower < 0;
                }
                int cmpUpper = PrimaryComparator_.CompareKeyBounds(lhsUpperBound, rhsUpperBound);
                if (cmpUpper != 0) {
                    return cmpUpper < 0;
                }
                // This is possible only when both chunks contain the same only key or we comparing chunk with itself.
                YT_VERIFY(&lhs == &rhs || PrimaryComparator_.TryAsSingletonKey(lhsLowerBound, lhsUpperBound));
                return false;
            });

        TeleportChunks_.reserve(teleportDataSlices.size());

        for (const auto& dataSlice : teleportDataSlices) {
            TeleportChunks_.emplace_back(dataSlice->GetSingleUnversionedChunk());
        }

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
        INewSortedJobBuilderPtr builder;
        std::vector<TNewJobStub> jobStubs;
        std::vector<TError> errors;
        for (int retryIndex = 0; retryIndex < JobSizeConstraints_->GetMaxBuildRetryCount(); ++retryIndex) {
            try {
                builder = CreateNewSortedJobBuilder(
                    SortedJobOptions_,
                    JobSizeConstraints_,
                    RowBuffer_,
                    TeleportChunks_,
                    retryIndex,
                    InputStreamDirectory_,
                    Logger,
                    StructuredLogger);

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

        // TODO(max42): why does job manager accept a vector of unique pointers to job stubs
        // instead of simply vector of job stubs as a prvalue?
        std::vector<std::unique_ptr<TNewJobStub>> jobStubPtrs;
        jobStubPtrs.reserve(jobStubs.size());
        for (auto& jobStub : jobStubs) {
            jobStubPtrs.emplace_back(std::make_unique<TNewJobStub>(std::move(jobStub)));
        }

        JobManager_->AddJobs(std::move(jobStubPtrs));

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
        auto validateDataSlices = [&] (const std::vector<TLegacyDataSlicePtr>& dataSlices, int prefixLength) {
            for (const auto& dataSlice : dataSlices) {
                YT_VERIFY(!dataSlice->IsLegacy);
                YT_VERIFY(dataSlice->LowerLimit().KeyBound);
                YT_VERIFY(dataSlice->UpperLimit().KeyBound);
                YT_VERIFY(static_cast<int>(dataSlice->LowerLimit().KeyBound.Prefix.GetCount()) <= prefixLength);
                YT_VERIFY(static_cast<int>(dataSlice->UpperLimit().KeyBound.Prefix.GetCount()) <= prefixLength);
            }
        };

        validateDataSlices(unreadInputDataSlices, PrimaryPrefixLength_);
        validateDataSlices(foreignInputDataSlices, ForeignPrefixLength_);

        // Note that reader returns lower limit which may be more accurate in terms of lower key bound.
        // This may lead to a situation when first data slice from certain input stream has
        // lower key bound >=K1 while second has lower bound >=K2 s.t. K2 < K1.
        // This leads to issues in sorted job builder which expects data slices from same stream
        // to go "from left to right".
        std::vector<TLegacyDataSlicePtr> inputStreamIndexToLastDataSlice;
        for (const auto& dataSlice : unreadInputDataSlices) {
            auto inputStreamIndex = dataSlice->GetInputStreamIndex();
            if (inputStreamIndex >= std::ssize(inputStreamIndexToLastDataSlice)) {
                inputStreamIndexToLastDataSlice.resize(inputStreamIndex + 1);
            }
            auto& lastDataSlice = inputStreamIndexToLastDataSlice[inputStreamIndex];
            if (lastDataSlice &&
                PrimaryComparator_.CompareKeyBounds(dataSlice->LowerLimit().KeyBound, lastDataSlice->LowerLimit().KeyBound) < 0)
            {
                dataSlice->LowerLimit().KeyBound = lastDataSlice->LowerLimit().KeyBound;
            }
            lastDataSlice = dataSlice;
        }

        i64 dataWeight = 0;
        for (auto& dataSlice : unreadInputDataSlices) {
            YT_VERIFY(!dataSlice->IsLegacy);
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
        auto builder = CreateNewSortedJobBuilder(
            splitSortedJobOptions,
            std::move(jobSizeConstraints),
            RowBuffer_,
            teleportChunks,
            0 /*retryIndex*/,
            InputStreamDirectory_,
            Logger,
            StructuredLogger);

        for (const auto& dataSlice : unreadInputDataSlices) {
            YT_VERIFY(InputStreamDirectory_.GetDescriptor(dataSlice->GetInputStreamIndex()).IsPrimary());
            YT_VERIFY(!dataSlice->IsLegacy);
            builder->AddDataSlice(dataSlice);
        }
        for (const auto& dataSlice : foreignInputDataSlices) {
            YT_VERIFY(InputStreamDirectory_.GetDescriptor(dataSlice->GetInputStreamIndex()).IsForeign());
            YT_VERIFY(!dataSlice->IsLegacy);
            builder->AddDataSlice(dataSlice);
        }

        auto jobs = builder->Build();
        std::vector<std::unique_ptr<TNewJobStub>> jobStubs;
        jobStubs.reserve(jobs.size());
        for (auto& job : jobs) {
            jobStubs.emplace_back(std::make_unique<TNewJobStub>(std::move(job)));
        }
        auto childCookies = JobManager_->AddJobs(std::move(jobStubs));

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

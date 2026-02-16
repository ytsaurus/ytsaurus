#include "new_sorted_chunk_pool.h"

#include "helpers.h"
#include "input_chunk_mapping.h"
#include "job_size_adjuster.h"
#include "new_job_manager.h"
#include "new_sorted_job_builder.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/library/random/bernoulli_sampler.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/logging/logger_owner.h>
#include <yt/yt/core/logging/serializable_logger.h>

#include <yt/yt/core/phoenix/type_decl.h>
#include <yt/yt/core/phoenix/type_def.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NTableClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNewSortedChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputWithNewJobManagerBase
    , public ISortedChunkPool
    , public virtual TLoggerOwner
    , public TJobSplittingBase
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
        , ChunkSliceFetcherFactory_(std::move(chunkSliceFetcherFactory))
        , InputStreamDirectory_(std::move(inputStreamDirectory))
        , SliceForeignChunks_(options.SliceForeignChunks)
        , MinManiacDataWeight_(options.MinManiacDataWeight)
        , JobSizeConstraints_(options.JobSizeConstraints)
        , TeleportChunkSampler_(JobSizeConstraints_->GetSamplingRate())
        , RowBuffer_(options.RowBuffer)
        , ChunkPoolStatistics_(options.ChunkPoolStatistics)
    {
        Logger = options.Logger;
        StructuredLogger = options.StructuredLogger;
        ValidateLogger(Logger);

        YT_VERIFY(RowBuffer_);

        if (options.JobSizeAdjusterConfig && JobSizeConstraints_->CanAdjustDataWeightPerJob()) {
            JobSizeAdjuster_ = CreateDiscreteJobSizeAdjuster(
                JobSizeConstraints_->GetDataWeightPerJob(),
                options.JobSizeAdjusterConfig);
        }

        YT_LOG_INFO("New sorted chunk pool created (EnableKeyGuarantee: %v, PrimaryPrefixLength: %v, "
            "ForeignPrefixLength: %v, DataWeightPerJob: %v, "
            "PrimaryDataWeightPerJob: %v, MaxDataSlicesPerJob: %v, InputSliceDataWeight: %v, "
            "MinManiacDataWeight: %v, HasJobSizeAdjuster: %v)",
            SortedJobOptions_.EnableKeyGuarantee,
            SortedJobOptions_.PrimaryComparator.GetLength(),
            SortedJobOptions_.ForeignComparator.GetLength(),
            JobSizeConstraints_->GetDataWeightPerJob(),
            JobSizeConstraints_->GetPrimaryDataWeightPerJob(),
            JobSizeConstraints_->GetMaxDataSlicesPerJob(),
            JobSizeConstraints_->GetInputSliceDataWeight(),
            MinManiacDataWeight_,
            static_cast<bool>(JobSizeAdjuster_));
    }

    IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        if (stripe->DataSlices().empty()) {
            return IChunkPoolInput::NullCookie;
        }

        const auto& inputStreamDescriptor = InputStreamDirectory_.GetDescriptor(stripe->GetInputStreamIndex());
        bool isForeign = inputStreamDescriptor.IsForeign();
        int prefixLength = isForeign ? SortedJobOptions_.ForeignComparator.GetLength() : SortedJobOptions_.PrimaryComparator.GetLength();

        for (auto& dataSlice : stripe->DataSlices()) {
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

        auto cookie = std::ssize(Stripes_);
        Stripes_.emplace_back(stripe);

        return cookie;
    }

    void Finish() override
    {
        if (IsFinished()) {
            return;
        }

        TChunkPoolInputBase::Finish();

        // NB: This method accounts all the stripes that were suspended before
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
            for (auto& dataSlice : stripe.GetStripe()->DataSlices()) {
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

        if (jobSummary.InterruptionReason != EInterruptionReason::None) {
            YT_LOG_DEBUG(
                "Splitting job (JobId: %v, OutputCookie: %v, InterruptionReason: %v, SplitJobCount: %v)",
                jobSummary.Id,
                cookie,
                jobSummary.InterruptionReason,
                jobSummary.SplitJobCount);

            std::vector<TLegacyDataSlicePtr> foreignSlices;
            for (const auto& stripe : GetStripeList(cookie)->Stripes()) {
                if (!stripe->IsForeign()) {
                    continue;
                }
                for (const auto& dataSlice : stripe->DataSlices()) {
                    dataSlice->LowerLimit().KeyBound = ShortenKeyBound(
                        dataSlice->LowerLimit().KeyBound,
                        SortedJobOptions_.ForeignComparator.GetLength(),
                        RowBuffer_);

                    dataSlice->UpperLimit().KeyBound = ShortenKeyBound(
                        dataSlice->UpperLimit().KeyBound,
                        SortedJobOptions_.ForeignComparator.GetLength(),
                        RowBuffer_);

                    foreignSlices.push_back(dataSlice);
                }
            }

            auto childCookies = SplitJob(
                jobSummary.UnreadInputDataSlices,
                foreignSlices,
                jobSummary.SplitJobCount,
                cookie);

            ValidateChildJobSizes(cookie, childCookies, [&] (TOutputCookie cookie) {
                return GetStripeList(cookie);
            });

            RegisterChildCookies(jobSummary.Id, cookie, std::move(childCookies));
        }
        JobManager_->Completed(cookie, jobSummary.InterruptionReason);

        if (JobSizeAdjuster_) {
            // No point in enlarging jobs if we cannot at least double them.
            auto action = EJobAdjustmentAction::None;
            if (JobManager_->JobCounter()->GetPending() > JobManager_->JobCounter()->GetRunning()) {
                action = JobSizeAdjuster_->UpdateStatistics(jobSummary);
            }

            if (action == EJobAdjustmentAction::RebuildJobs) {
                YT_LOG_INFO("Job completion triggered enlargement (JobId: %v, JobCookie: %v)", jobSummary.Id, cookie);
                auto primaryDataWeightRatio = static_cast<double>(JobSizeConstraints_->GetPrimaryDataWeightPerJob())
                    / JobSizeConstraints_->GetDataWeightPerJob();
                JobManager_->Enlarge(
                    JobSizeAdjuster_->GetDataWeightPerJob(),
                    JobSizeAdjuster_->GetDataWeightPerJob() * primaryDataWeightRatio,
                    JobSizeConstraints_);
            }
        }

        CheckCompleted();
    }

    void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        TChunkPoolOutputWithJobManagerBase::Lost(cookie);

        CheckCompleted();
    }

    std::pair<TKeyBound, TKeyBound> GetBounds(IChunkPoolOutput::TCookie cookie) const override
    {
        return JobManager_->GetBounds(cookie);
    }

private:
    //! All options necessary for sorted job builder.
    TSortedJobOptions SortedJobOptions_;

    //! A factory that is used to spawn chunk slice fetcher.
    IChunkSliceFetcherFactoryPtr ChunkSliceFetcherFactory_;

    //! Information about input sources (e.g. input tables for sorted reduce operation).
    TInputStreamDirectory InputStreamDirectory_;

    //! Whether foreign chunks should be sliced.
    bool SliceForeignChunks_ = false;

    std::optional<i64> MinManiacDataWeight_;

    //! All stripes that were added to this pool.
    std::vector<TSuspendableStripe> Stripes_;

    IJobSizeConstraintsPtr JobSizeConstraints_;
    TBernoulliSampler TeleportChunkSampler_;

    TRowBufferPtr RowBuffer_;

    std::vector<TInputChunkPtr> TeleportChunks_;

    bool IsCompleted_ = false;

    TSerializableLogger StructuredLogger;

    std::unique_ptr<IDiscreteJobSizeAdjuster> JobSizeAdjuster_;

    TSortedChunkPoolStatisticsPtr ChunkPoolStatistics_;

    //! This method processes all input stripes that do not correspond to teleported chunks
    //! and either slices them using ChunkSliceFetcher (for unversioned stripes) or leaves them as is
    //! (for versioned stripes).
    void FetchNonTeleportDataSlices(const INewSortedJobBuilderPtr& builder)
    {
        auto chunkSliceFetcher = ChunkSliceFetcherFactory_ ? ChunkSliceFetcherFactory_->CreateChunkSliceFetcher() : nullptr;

        auto processDataSlice = [&] (const TLegacyDataSlicePtr& dataSlice, int inputCookie) {
            YT_VERIFY(!dataSlice->IsLegacy);
            dataSlice->Tag = inputCookie;
            builder->AddDataSlice(dataSlice);
        };

        if (!chunkSliceFetcher) {
            YT_LOG_DEBUG("Data slices will not be fetched, as chunk slice fetcher is not present");
            for (const auto& [inputCookie, suspendableStripe] : SEnumerate(Stripes_)) {
                if (suspendableStripe.GetTeleport()) {
                    continue;
                }
                for (const auto& dataSlice : suspendableStripe.GetStripe()->DataSlices()) {
                    processDataSlice(dataSlice, inputCookie);
                }
            }
            return;
        }

        // TODO(max42): job size constraints is incredibly bloated :( get rid of such workarounds.
        i64 primarySliceSize = std::min(JobSizeConstraints_->GetInputSliceDataWeight(), JobSizeConstraints_->GetPrimaryDataWeightPerJob());
        i64 foreignSliceSize = JobSizeConstraints_->GetForeignSliceDataWeight();

        YT_LOG_DEBUG(
            "Fetching non-teleport data slices (HasChunkSliceFetcher: %v, PrimarySliceSize: %v, ForeignSliceSize: %v, SliceForeignChunks: %v)",
            static_cast<bool>(chunkSliceFetcher),
            primarySliceSize,
            foreignSliceSize,
            SliceForeignChunks_);

        THashMap<TInputChunkPtr, std::pair<TLegacyDataSlicePtr, IChunkPoolInput::TCookie>> inputChunkToOwningDataSlice;

        // TODO(max42): logic here is schizophrenic :( introduce IdentityChunkSliceFetcher and
        // stop converting chunk slices to data slices and forth.
        for (const auto& [inputCookie, suspendableStripe] : SEnumerate(Stripes_)) {
            if (suspendableStripe.GetTeleport()) {
                continue;
            }
            for (const auto& dataSlice : suspendableStripe.GetStripe()->DataSlices()) {
                // Unversioned data slices should be additionally sliced using chunkSliceFetcher,
                // while versioned slices are taken as is.
                bool isPrimary = InputStreamDirectory_.GetDescriptor(dataSlice->GetInputStreamIndex()).IsPrimary();
                bool shouldSliceUnversioned = isPrimary || SliceForeignChunks_;
                if (dataSlice->Type != EDataSourceType::UnversionedTable || !shouldSliceUnversioned) {
                    processDataSlice(dataSlice, inputCookie);
                    continue;
                }

                const auto& comparator = isPrimary
                    ? SortedJobOptions_.PrimaryComparator
                    : SortedJobOptions_.ForeignComparator;
                i64 sliceSize = isPrimary
                    ? primarySliceSize
                    : foreignSliceSize;
                auto inputChunk = dataSlice->GetSingleUnversionedChunk();
                YT_LOG_TRACE(
                    "Slicing chunk (ChunkId: %v, DataWeight: %v, IsPrimary: %v, Comparator: %v, SliceSize: %v)",
                    inputChunk->GetChunkId(),
                    inputChunk->GetDataWeight(),
                    isPrimary,
                    comparator,
                    sliceSize);

                chunkSliceFetcher->AddDataSliceForSlicing(dataSlice, comparator, sliceSize, /*sliceByKeys*/ true, MinManiacDataWeight_);

                YT_VERIFY(!inputChunkToOwningDataSlice.contains(inputChunk));
                inputChunkToOwningDataSlice[inputChunk] = std::pair(dataSlice, inputCookie);
            }
        }

        WaitFor(chunkSliceFetcher->Fetch())
            .ThrowOnError();

        for (auto& chunkSlice : chunkSliceFetcher->GetChunkSlices()) {
            YT_VERIFY(!chunkSlice->IsLegacy);
            auto* originalDataSliceAndInputCookie = inputChunkToOwningDataSlice.FindPtr(chunkSlice->GetInputChunk());
            YT_VERIFY(originalDataSliceAndInputCookie);
            const auto& [originalDataSlice, inputCookie] = *originalDataSliceAndInputCookie;
            auto dataSlice = CreateUnversionedInputDataSlice(std::move(chunkSlice));
            dataSlice->CopyPayloadFrom(*originalDataSlice);

            const auto& comparator = InputStreamDirectory_.GetDescriptor(dataSlice->GetInputStreamIndex()).IsPrimary()
                ? SortedJobOptions_.PrimaryComparator
                : SortedJobOptions_.ForeignComparator;

            comparator.ReplaceIfStrongerKeyBound(dataSlice->LowerLimit().KeyBound, originalDataSlice->LowerLimit().KeyBound);
            comparator.ReplaceIfStrongerKeyBound(dataSlice->UpperLimit().KeyBound, originalDataSlice->UpperLimit().KeyBound);

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
        // 3b) if enable_key_guarantee = %true: no other key lies in the segment [minKey, maxKey] (NB: If some other chunk shares endpoint
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
        auto yielder = CreatePeriodicYielder(
            SortedJobOptions_.EnablePeriodicYielder
                ? std::optional(PrepareYieldPeriod)
                : std::nullopt);

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
            for (const auto& dataSlice : stripe->DataSlices()) {
                yielder.TryYield();

                auto lowerBound = dataSlice->LowerLimit().KeyBound;
                auto upperBound = dataSlice->UpperLimit().KeyBound;

                if (dataSlice->IsTeleportable && isPrimary && isTeleportable) {
                    teleportCandidates.emplace_back(TTeleportCandidate{
                        .DataSlice = dataSlice,
                        .LowerBound = lowerBound,
                        .UpperBound = upperBound,
                        .InputCookie = inputCookie,
                    });
                }

                lowerLimits.emplace_back(lowerBound);
                upperLimits.emplace_back(upperBound);

                if (auto key = SortedJobOptions_.PrimaryComparator.TryAsSingletonKey(lowerBound, upperBound)) {
                    ++keyToSingletonSliceCount[*key];
                }
            }
        }

        if (teleportCandidates.empty()) {
            return;
        }

        auto keyBoundComparator = [&] (const auto& lhs, const auto& rhs) {
            return SortedJobOptions_.PrimaryComparator.CompareKeyBounds(lhs, rhs) < 0;
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

            i64 slicesToTheLeft = (SortedJobOptions_.EnableKeyGuarantee
                ? std::upper_bound(upperLimits.begin(), upperLimits.end(), lowerBound, keyBoundComparator)
                : std::upper_bound(upperLimits.begin(), upperLimits.end(), lowerBound.ToggleInclusiveness(), keyBoundComparator)) - upperLimits.begin();
            i64 slicesToTheRight = lowerLimits.end() - (SortedJobOptions_.EnableKeyGuarantee
                ? std::lower_bound(lowerLimits.begin(), lowerLimits.end(), upperBound, keyBoundComparator)
                : std::lower_bound(lowerLimits.begin(), lowerLimits.end(), upperBound.ToggleInclusiveness(), keyBoundComparator));
            int extraCoincidingSingletonSlices = 0;
            if (auto key = SortedJobOptions_.PrimaryComparator.TryAsSingletonKey(lowerBound, upperBound); key && !SortedJobOptions_.EnableKeyGuarantee) {
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

                int cmpLower = SortedJobOptions_.PrimaryComparator.CompareKeyBounds(lhsLowerBound, rhsLowerBound);
                if (cmpLower != 0) {
                    return cmpLower < 0;
                }
                int cmpUpper = SortedJobOptions_.PrimaryComparator.CompareKeyBounds(lhsUpperBound, rhsUpperBound);
                if (cmpUpper != 0) {
                    return cmpUpper < 0;
                }
                // This is possible only when both chunks contain the same only key or we comparing chunk with itself.
                YT_VERIFY(&lhs == &rhs || SortedJobOptions_.PrimaryComparator.TryAsSingletonKey(lhsLowerBound, lhsUpperBound));
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
                    ChunkPoolStatistics_,
                    Logger,
                    StructuredLogger);

                FetchNonTeleportDataSlices(builder);
                jobStubs = builder->Build();
                succeeded = true;
                break;
            } catch (TErrorException& ex) {
                if (ex.Error().FindMatching(NChunkPools::EErrorCode::DataSliceLimitExceeded)) {
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

        auto cookies = JobManager_->AddJobs(std::move(jobStubPtrs));

        YT_LOG_TRACE(
            "Jobs are built (CookieCount: %v, Statistics: %v)",
            cookies,
            ChunkPoolStatistics_);

        if (JobSizeConstraints_->GetSamplingRate()) {
            JobManager_->Enlarge(
                JobSizeConstraints_->GetDataWeightPerJob(),
                JobSizeConstraints_->GetPrimaryDataWeightPerJob(),
                JobSizeConstraints_);
        }

        auto oldDataSliceCount = GetDataSliceCounter()->GetTotal();
        GetDataSliceCounter()->AddUncategorized(builder->GetTotalDataSliceCount() - oldDataSliceCount);

        CheckCompleted();
    }

    std::vector<IChunkPoolOutput::TCookie> SplitJob(
        std::vector<TLegacyDataSlicePtr> unreadInputDataSlices,
        const std::vector<TLegacyDataSlicePtr>& foreignInputDataSlices,
        int splitJobCount,
        TOutputCookie cookie)
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

        validateDataSlices(unreadInputDataSlices, SortedJobOptions_.PrimaryComparator.GetLength());
        validateDataSlices(foreignInputDataSlices, SortedJobOptions_.ForeignComparator.GetLength());

        // Note that reader returns lower limit which may be more accurate in terms of lower key bound.
        // This may lead to a situation when first data slice from certain input stream has
        // lower key bound >=K1 while second has lower bound >=K2 s.t. K2 < K1.
        // This leads to issues in sorted job builder which expects data slices from same stream
        // to go "from left to right".
        std::vector<TLegacyDataSlicePtr> inputStreamIndexToLastDataSlice;
        for (const auto& dataSlice : unreadInputDataSlices) {
            auto inputStreamIndex = dataSlice->GetInputStreamIndex();
            EnsureVectorIndex(inputStreamIndexToLastDataSlice, inputStreamIndex);
            auto& lastDataSlice = inputStreamIndexToLastDataSlice[inputStreamIndex];
            if (lastDataSlice &&
                SortedJobOptions_.PrimaryComparator.CompareKeyBounds(dataSlice->LowerLimit().KeyBound, lastDataSlice->LowerLimit().KeyBound) < 0)
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

        // NB(coteeq): We do not set isExplicitJob count because sorted pool
        // does not support this flag and it would be *very* hard to support
        // (and it probably is not worth it).
        // So we just increase all constraints to effective infinity to trick
        // the pool to always make a single job.
        auto adjustSizeIfSingleJob = [singleJob = splitJobCount == 1] (i64 size) {
            return singleJob ? std::numeric_limits<i64>::max() / 4 : size;
        };

        i64 dataWeightPerJob = adjustSizeIfSingleJob(DivCeil(dataWeight, static_cast<i64>(splitJobCount)));

        // We create new job size constraints by incorporating the new desired data size per job
        // into the old job size constraints.
        auto jobSizeConstraints = CreateExplicitJobSizeConstraints(
            /*canAdjustDataSizePerJob*/ false,
            /*isExplicitJobCount*/ false,
            /*jobCount*/ splitJobCount,
            dataWeightPerJob,
            /*primaryDataWeightPerJob*/ std::numeric_limits<i64>::max() / 4,
            adjustSizeIfSingleJob(JobSizeConstraints_->GetCompressedDataSizePerJob()),
            adjustSizeIfSingleJob(JobSizeConstraints_->GetPrimaryCompressedDataSizePerJob()),
            adjustSizeIfSingleJob(JobSizeConstraints_->GetMaxDataSlicesPerJob()),
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob(),
            JobSizeConstraints_->GetMaxCompressedDataSizePerJob(),
            JobSizeConstraints_->GetMaxPrimaryCompressedDataSizePerJob(),
            JobSizeConstraints_->GetInputSliceDataWeight(),
            JobSizeConstraints_->GetInputSliceRowCount(),
            JobSizeConstraints_->GetBatchRowCount(),
            JobSizeConstraints_->GetForeignSliceDataWeight(),
            /*samplingRate*/ std::nullopt);

        auto splitSortedJobOptions = SortedJobOptions_;
        // We do not want to yield during job splitting because it may potentially lead
        // to snapshot creation that will catch pool in inconsistent state.
        splitSortedJobOptions.EnablePeriodicYielder = false;
        auto builder = CreateNewSortedJobBuilder(
            splitSortedJobOptions,
            std::move(jobSizeConstraints),
            RowBuffer_,
            /*teleportChunks*/ {}, // Each job is already located between the teleport chunks.
            0 /*retryIndex*/,
            InputStreamDirectory_,
            ChunkPoolStatistics_,
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
        std::vector<bool> isBarrierJob; // TODO(apollo1321): Should be removed in YT-26946.
        jobStubs.reserve(jobs.size());
        isBarrierJob.reserve(jobs.size());
        for (auto& job : jobs) {
            jobStubs.push_back(std::make_unique<TNewJobStub>(std::move(job)));
            isBarrierJob.push_back(job.GetIsBarrier());
        }
        JobManager_->SeekOrder(cookie);
        auto childCookies = JobManager_->AddJobs(std::move(jobStubs));
        YT_VERIFY(std::ssize(childCookies) == std::ssize(isBarrierJob));

        int writeIndex = 0;
        for (int i : std::views::iota(0, std::ssize(childCookies))) {
            if (!isBarrierJob[i]) {
                childCookies[writeIndex] = childCookies[i];
                ++writeIndex;
            }
        }
        childCookies.resize(writeIndex);

        YT_LOG_TRACE(
            "Jobs are built (CookieCount: %v, ChunkPoolStatistics: %v)",
            std::ssize(childCookies),
            ChunkPoolStatistics_);

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

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TNewSortedChunkPool, 0x8ff1db04);
};

void TNewSortedChunkPool::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkPoolInputBase>();
    registrar.template BaseType<TChunkPoolOutputWithJobManagerBase>();
    registrar.template BaseType<TJobSplittingBase>();
    // TLoggerOwner is persisted by TJobSplittingBase.

    PHOENIX_REGISTER_FIELD(1, SortedJobOptions_);
    PHOENIX_REGISTER_DELETED_FIELD(2, TComparator, PrimaryComparator_, ESnapshotVersion::DropRedundantFieldsInSortedChunkPool);
    PHOENIX_REGISTER_DELETED_FIELD(3, TComparator, ForeignComparator_, ESnapshotVersion::DropRedundantFieldsInSortedChunkPool);
    PHOENIX_REGISTER_FIELD(4, ChunkSliceFetcherFactory_);
    PHOENIX_REGISTER_DELETED_FIELD(5, bool, EnableKeyGuarantee_, ESnapshotVersion::DropRedundantFieldsInSortedChunkPool);
    PHOENIX_REGISTER_FIELD(6, InputStreamDirectory_);

    PHOENIX_REGISTER_DELETED_FIELD(7, int, PrimaryPrefixLength_, ESnapshotVersion::DropRedundantFieldsInSortedChunkPool);
    PHOENIX_REGISTER_DELETED_FIELD(8, int, ForeignPrefixLength_, ESnapshotVersion::DropRedundantFieldsInSortedChunkPool);
    PHOENIX_REGISTER_DELETED_FIELD(9, bool, ShouldSlicePrimaryTableByKeys_, ESnapshotVersion::DropShouldSlicePrimaryTableByKeys);
    PHOENIX_REGISTER_FIELD(10, SliceForeignChunks_);
    PHOENIX_REGISTER_DELETED_FIELD(11, i64, MinTeleportChunkSize_, ESnapshotVersion::DropRedundantFieldsInSortedChunkPool);
    PHOENIX_REGISTER_FIELD(12, Stripes_);
    PHOENIX_REGISTER_FIELD(13, JobSizeConstraints_);
    PHOENIX_REGISTER_FIELD(14, TeleportChunkSampler_);
    PHOENIX_REGISTER_DELETED_FIELD(15, bool, SupportLocality_, ESnapshotVersion::DropSupportLocality);
    PHOENIX_REGISTER_FIELD(16, TeleportChunks_);
    PHOENIX_REGISTER_FIELD(17, IsCompleted_);
    PHOENIX_REGISTER_FIELD(18, StructuredLogger);
    PHOENIX_REGISTER_FIELD(19, MinManiacDataWeight_,
        .SinceVersion(ESnapshotVersion::IsolateManiacsInSlicing));

    PHOENIX_REGISTER_FIELD(20, JobSizeAdjuster_,
        .SinceVersion(ESnapshotVersion::OrderedAndSortedJobSizeAdjuster));

    PHOENIX_REGISTER_FIELD(21, ChunkPoolStatistics_,
        .SinceVersion(ESnapshotVersion::ChunkPoolStatistics));

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        ValidateLogger(this_->Logger);
        this_->RowBuffer_ = New<TRowBuffer>();
    });
}

PHOENIX_DEFINE_TYPE(TNewSortedChunkPool);

////////////////////////////////////////////////////////////////////////////////

} // namespace

ISortedChunkPoolPtr CreateNewSortedChunkPool(
    const TSortedChunkPoolOptions& options,
    IChunkSliceFetcherFactoryPtr chunkSliceFetcherFactory,
    TInputStreamDirectory inputStreamDirectory)
{
    return New<TNewSortedChunkPool>(options, std::move(chunkSliceFetcherFactory), std::move(inputStreamDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

#include "new_sorted_job_builder.h"

#include "helpers.h"
#include "input_stream.h"
#include "new_job_manager.h"
#include "job_size_tracker.h"
#include "sorted_staging_area.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/key_bound_compressor.h>

#include <yt/yt/library/random/bernoulli_sampler.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/finally.h>

#include <cmath>

namespace NYT::NChunkPools {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPrimaryEndpointType,
    (Barrier)
    (Primary)
);

DEFINE_ENUM(ERowSliceabilityDecision,
    (SliceByRows)
    // All decisions below lead to slicing by keys.
    (KeyGuaranteeDisabled)
    (NoJobSizeTracker)
    (VersionedSlicesPresent)
    (NonSingletonSliceCountIsAtLeastTwo)
    (NextPrimaryLowerBoundTooClose)
    (OverlapsWithStagedDataSlice)
    (TooMuchForeignData)
);

class TNewSortedJobBuilder
    : public INewSortedJobBuilder
{
public:
    TNewSortedJobBuilder(
        const TSortedJobOptions& options,
        IJobSizeConstraintsPtr jobSizeConstraints,
        const TRowBufferPtr& rowBuffer,
        const std::vector<TInputChunkPtr>& teleportChunks,
        int retryIndex,
        const TInputStreamDirectory& inputStreamDirectory,
        const TLogger& logger,
        const TLogger& structuredLogger)
        : Options_(options)
        , PrimaryComparator_(options.PrimaryComparator)
        , ForeignComparator_(options.ForeignComparator)
        , JobSizeConstraints_(std::move(jobSizeConstraints))
        , JobSampler_(JobSizeConstraints_->GetSamplingRate())
        , RowBuffer_(rowBuffer)
        , RetryIndex_(retryIndex)
        , InputStreamDirectory_(inputStreamDirectory)
        , Logger(logger)
        , StructuredLogger(structuredLogger)
    {
        double retryFactor = std::pow(JobSizeConstraints_->GetDataWeightPerJobRetryFactor(), RetryIndex_);

        // Pivot keys provide guarantee that we won't introduce more jobs than
        // defined by them, so we do not try to flush by ourself if they are present.
        if (Options_.PivotKeys.empty()) {
            LimitVector_.Values[EResourceKind::DataWeight] = static_cast<i64>(std::min<double>(
                std::numeric_limits<i64>::max() / 2,
                GetDataWeightPerJob() * retryFactor));
            LimitVector_.Values[EResourceKind::PrimaryDataWeight] = static_cast<i64>(std::min<double>(
                std::numeric_limits<i64>::max() / 2,
                GetPrimaryDataWeightPerJob() * retryFactor));

            if (Options_.ConsiderOnlyPrimarySize) {
                LimitVector_.Values[EResourceKind::DataWeight] = std::numeric_limits<i64>::max() / 2;
            }

            LimitVector_.Values[EResourceKind::DataSliceCount] = JobSizeConstraints_->GetMaxDataSlicesPerJob();

            JobSizeTracker_ = CreateJobSizeTracker(LimitVector_, Logger);
        }

        if (Options_.EnablePeriodicYielder) {
            PeriodicYielder_ = TPeriodicYielder(PrepareYieldPeriod);
        }

        // We divide key space into segments between teleport chunks. Then we build jobs independently
        // on each segment.
        for (const auto& chunk : teleportChunks) {
            auto maxKey = RowBuffer_->CaptureRow(MakeRange(chunk->BoundaryKeys()->MaxKey.Begin(), PrimaryComparator_.GetLength()));
            TeleportChunkUpperBounds_.emplace_back(TKeyBound::FromRow() <= maxKey);
        }

        SegmentPrimaryEndpoints_.resize(teleportChunks.size() + 1);
    }

    void AddDataSlice(const TLegacyDataSlicePtr& dataSlice) override
    {
        YT_VERIFY(!dataSlice->IsLegacy);
        YT_VERIFY(dataSlice->LowerLimit().KeyBound);
        YT_VERIFY(dataSlice->UpperLimit().KeyBound);

        // Making a copy here is crucial since provided data slice object may be modified in-place.
        InputDataSlices_.push_back(CreateInputDataSlice(dataSlice));

        auto inputStreamIndex = dataSlice->GetInputStreamIndex();
        auto isPrimary = InputStreamDirectory_.GetDescriptor(inputStreamIndex).IsPrimary();

        const auto& comparator = isPrimary ? PrimaryComparator_ : ForeignComparator_;

        if (comparator.IsRangeEmpty(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound)) {
            // This can happen if ranges were specified.
            // Chunk slice fetcher can produce empty slices.
            return;
        }

        if (isPrimary) {
            TPrimaryEndpoint endpoint = {
                EPrimaryEndpointType::Primary,
                dataSlice,
                dataSlice->LowerLimit().KeyBound,
            };

            // Count the number of teleport chunks that are to the right of our data slice.
            // Singleton data slice is considered to be to the right of coinciding singleton teleport chunk.
            // See also: comment for #SegmentPrimaryEndpoints_.
            auto segmentIndex = std::upper_bound(
                TeleportChunkUpperBounds_.begin(),
                TeleportChunkUpperBounds_.end(),
                dataSlice->LowerLimit().KeyBound,
                [&] (const TKeyBound& lowerBound, const TKeyBound& upperBound) {
                    return !comparator.IsInteriorEmpty(lowerBound, upperBound);
                }) - TeleportChunkUpperBounds_.begin();
            SegmentPrimaryEndpoints_[segmentIndex].push_back(endpoint);

            YT_LOG_TRACE(
                "Adding primary data slice to builder (DataSlice: %v, SegmentIndex: %v)",
                GetDataSliceDebugString(dataSlice),
                segmentIndex);
        } else {
            ForeignSlices_.push_back(dataSlice);

            YT_LOG_TRACE(
                "Adding foreign data slice to builder (DataSlice: %v)",
                GetDataSliceDebugString(dataSlice));
        }

        // Verify that in each input stream data slice lower key bounds and upper key bounds are monotonic.
        // This does not seem to affect any logic in sorted job builder but it would rather simplify
        // reading logs...

        if (std::ssize(InputStreamIndexToLastDataSlice_) <= inputStreamIndex) {
            InputStreamIndexToLastDataSlice_.resize(inputStreamIndex + 1);
        }

        auto& lastDataSlice = InputStreamIndexToLastDataSlice_[inputStreamIndex];

        if (lastDataSlice &&
            (comparator.CompareKeyBounds(lastDataSlice->LowerLimit().KeyBound, dataSlice->LowerLimit().KeyBound) > 0 ||
            comparator.CompareKeyBounds(lastDataSlice->UpperLimit().KeyBound, dataSlice->UpperLimit().KeyBound) > 0))
        {
            YT_LOG_FATAL(
                "Input data slices non-monotonic (InputStreamIndex: %v, Lhs: %v, Rhs: %v)",
                inputStreamIndex,
                GetDataSliceDebugString(lastDataSlice),
                GetDataSliceDebugString(dataSlice));
        }
        lastDataSlice = dataSlice;
    }

    std::vector<TNewJobStub> Build() override
    {
        AddPivotKeysEndpoints();

        SortForeignSlices();
        for (size_t index = 0; index < SegmentPrimaryEndpoints_.size(); ++index) {
            auto& endpoints = SegmentPrimaryEndpoints_[index];
            if (endpoints.empty()) {
                continue;
            }
            YT_LOG_TRACE("Processing segment (SegmentIndex: %v, EndpointCount: %v)", index, endpoints.size());
            SortPrimaryEndpoints(endpoints);
            LogDetails(endpoints);
            BuildJobs(endpoints);
        }

        InputStreamIndexToLastDataSlice_.assign(InputStreamDirectory_.GetDescriptorCount(), nullptr);

        for (auto& job : Jobs_) {
            if (job.GetIsBarrier()) {
                continue;
            }
            job.Finalize();
        }

        for (auto& job : Jobs_) {
            if (job.GetIsBarrier()) {
                continue;
            }
            ValidateJob(&job);
        }

        LogStructured();

        return std::move(Jobs_);
    }

    void ValidateJob(const TNewJobStub* job)
    {
        // These are user-facing checks.
        if (job->GetDataWeight() > JobSizeConstraints_->GetMaxDataWeightPerJob()) {
            YT_LOG_DEBUG("Maximum allowed data weight per sorted job exceeds the limit (DataWeight: %v, MaxDataWeightPerJob: %v, "
                "PrimaryLowerBound: %v, PrimaryUpperBound: %v, JobDebugString: %v)",
                job->GetDataWeight(),
                JobSizeConstraints_->GetMaxDataWeightPerJob(),
                job->GetPrimaryLowerBound(),
                job->GetPrimaryUpperBound(),
                job->GetDebugString());

            THROW_ERROR_EXCEPTION(
                EErrorCode::MaxDataWeightPerJobExceeded, "Maximum allowed data weight per sorted job exceeds the limit: %v > %v",
                job->GetDataWeight(),
                JobSizeConstraints_->GetMaxDataWeightPerJob())
                << TErrorAttribute("lower_bound", job->GetPrimaryLowerBound())
                << TErrorAttribute("upper_bound", job->GetPrimaryUpperBound());
        }

        if (job->GetPrimaryDataWeight() > JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob()) {
            YT_LOG_DEBUG("Maximum allowed primary data weight per sorted job exceeds the limit (PrimaryDataWeight: %v, MaxPrimaryDataWeightPerJob: %v, "
                "PrimaryLowerBound: %v, PrimaryUpperBound: %v, JobDebugString: %v)",
                job->GetPrimaryDataWeight(),
                JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob(),
                job->GetPrimaryLowerBound(),
                job->GetPrimaryUpperBound(),
                job->GetDebugString());

            THROW_ERROR_EXCEPTION(
                EErrorCode::MaxPrimaryDataWeightPerJobExceeded, "Maximum allowed primary data weight per sorted job exceeds the limit: %v > %v",
                job->GetPrimaryDataWeight(),
                JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob())
                << TErrorAttribute("lower_bound", job->GetPrimaryLowerBound())
                << TErrorAttribute("upper_bound", job->GetPrimaryUpperBound());
        }

        // These are internal assertions.
        if (Options_.ValidateOrder) {
            // New sorted pool implementation guarantees that each stripe contains data slices in the
            // order they follow in the input, but achieving that is utterly hard, so we put some
            // effort in verification of this fact.

            auto validatePair = [&] (
                int inputStreamIndex,
                const TComparator& comparator,
                const TLegacyDataSlicePtr& lhs,
                const TLegacyDataSlicePtr& rhs)
            {
                YT_VERIFY(lhs->Tag);
                YT_VERIFY(rhs->Tag);
                auto lhsLocation = std::pair(*lhs->Tag, lhs->GetSliceIndex());
                auto rhsLocation = std::pair(*rhs->Tag, rhs->GetSliceIndex());
                if (lhsLocation < rhsLocation) {
                    return;
                }
                if (lhsLocation == rhsLocation) {
                    // These are two slices of the same unversioned chunk or versioned data slice.
                    //
                    // Note that our current implementation meets the following formal property.
                    // Consider some "initial" data slice (one from chunk slice fetcher or initial
                    // dynamic table slice). We claim that all resulting subslices are obtained from
                    // it by repeatedly splitting some subslice into two parts by either key bound or
                    // row limit.
                    //
                    // Also note that reader in job proxy may adjust lower bound of data slice by
                    // making it tighter. This becomes important when we obtain unread data slices
                    // during job interruption.
                    //
                    // Thus we may validate that for any two adjacent data slices they are separated
                    // either by key bounds, or by row indices; this property is enough to conclude
                    // that resulting slices follow in correct order.
                    bool separatedByKeyBounds = lhs->UpperLimit().KeyBound && rhs->LowerLimit().KeyBound &&
                        comparator.CompareKeyBounds(lhs->UpperLimit().KeyBound, rhs->LowerLimit().KeyBound) <= 0;
                    bool separatedByRowIndices = lhs->UpperLimit().RowIndex && rhs->LowerLimit().RowIndex &&
                        *lhs->UpperLimit().RowIndex <= *rhs->LowerLimit().RowIndex;
                    if (separatedByKeyBounds || separatedByRowIndices) {
                        return;
                    }
                }
                LogStructured();
                // Actually a safe core dump.
                YT_LOG_FATAL(
                    "Error validating slice order guarantee (InputStreamIndex: %v, Lhs: %v, Rhs: %v)",
                    inputStreamIndex,
                    GetDataSliceDebugString(lhs),
                    GetDataSliceDebugString(rhs));
            };

            // Validate slice order between slices in each stripe.
            for (const auto& stripe : job->GetStripeList()->Stripes) {
                auto inputStreamIndex = stripe->GetInputStreamIndex();
                for (int index = 0; index + 1 < std::ssize(stripe->DataSlices); ++index) {
                    validatePair(
                        inputStreamIndex,
                        InputStreamDirectory_.GetDescriptor(inputStreamIndex).IsPrimary()
                            ? PrimaryComparator_
                            : ForeignComparator_,
                        stripe->DataSlices[index],
                        stripe->DataSlices[index + 1]);
                }
            }

            // Validate slice order between primary stripes.
            for (const auto& stripe : job->GetStripeList()->Stripes) {
                auto inputStreamIndex = stripe->GetInputStreamIndex();
                if (InputStreamDirectory_.GetDescriptor(inputStreamIndex).IsForeign()) {
                    continue;
                }
                if (stripe->DataSlices.empty()) {
                    continue;
                }
                if (static_cast<int>(InputStreamIndexToLastDataSlice_.size()) <= inputStreamIndex) {
                    InputStreamIndexToLastDataSlice_.resize(inputStreamIndex + 1, nullptr);
                }
                auto& lastDataSlice = InputStreamIndexToLastDataSlice_[inputStreamIndex];
                const auto& firstDataSlice = stripe->DataSlices.front();
                const auto& newLastDataSlice = stripe->DataSlices.back();
                if (lastDataSlice) {
                    validatePair(
                        inputStreamIndex,
                        PrimaryComparator_,
                        lastDataSlice,
                        firstDataSlice);
                }
                lastDataSlice = newLastDataSlice;
            }
        }
    }

    i64 GetTotalDataSliceCount() const override
    {
        return TotalDataSliceCount_;
    }

private:
    TSortedJobOptions Options_;

    std::vector<TKeyBound> TeleportChunkUpperBounds_;

    TComparator PrimaryComparator_;
    TComparator ForeignComparator_;

    IJobSizeConstraintsPtr JobSizeConstraints_;
    IJobSizeTrackerPtr JobSizeTracker_;
    ISortedStagingAreaPtr StagingArea_;
    TBernoulliSampler JobSampler_;

    TResourceVector LimitVector_;

    TRowBufferPtr RowBuffer_;

    struct TPrimaryEndpoint
    {
        EPrimaryEndpointType Type;
        TLegacyDataSlicePtr DataSlice;
        TKeyBound KeyBound;
    };

    //! Consider teleport chunks. They divide key space into segments. Each segment may be processed
    //! independently. Some segments may remain empty.
    //! There are two tricky details in this statement (obvious at the first glance)
    //! - If key guarantee is disabled, singleton data slice position is not uniquely defined
    //!   if there is a coinciding teleport chunk. Any choice would be fine.
    //! - But if data slice is foreign, it may not overlap with teleport chunks even by a single
    //!   key; this would break the guarantee of non-teleportation for keys that have some foreign data.
    //!
    //! E.g.:                     3 is effectively
    //!  0       1          2        missing        4     5
    //! ---[]---------[--]-------[--------][]----------[]---
    std::vector<std::vector<TPrimaryEndpoint>> SegmentPrimaryEndpoints_;

    //! Foreign slices are tracked separately from primary endpoints.
    std::vector<TLegacyDataSlicePtr> ForeignSlices_;
    //! Points to the leftmost unstaged foreign slice (ordered by lower key bound).
    size_t FirstUnstagedForeignIndex_ = 0;

    //! Vector keeping the pool-side state of all jobs that depend on the data from this pool.
    //! These items are merely stubs of a future jobs that are filled during the BuildJobsBy{Key/TableIndices}()
    //! call, and when current job is finished it is passed to the `JobManager_` that becomes responsible
    //! for its future.
    std::vector<TNewJobStub> Jobs_;

    int JobIndex_ = 0;
    i64 TotalDataWeight_ = 0;

    i64 TotalDataSliceCount_ = 0;

    int RetryIndex_;

    const TInputStreamDirectory& InputStreamDirectory_;

    //! Contains last data slice for each input stream in order to validate important requirement
    //! for sorted pool: lower bounds and upper bounds must be monotonic among each input stream.
    //! Used in two contexts: in order to validate added input data slices and in order to validate
    //! resulting data slices produced by builder.
    std::vector<TLegacyDataSlicePtr> InputStreamIndexToLastDataSlice_;

    //! Used for structured logging.
    std::vector<TLegacyDataSlicePtr> InputDataSlices_;

    TPeriodicYielder PeriodicYielder_;
    const TLogger& Logger;
    const TLogger& StructuredLogger;

    void AddPivotKeysEndpoints()
    {
        if (Options_.PivotKeys.empty()) {
            return;
        }

        // When pivot keys are set, teleportation is disabled.
        YT_VERIFY(SegmentPrimaryEndpoints_.size() == 1);

        for (const auto& pivotKey : Options_.PivotKeys) {
            // Pivot keys act as key bounds of type >=.
            TPrimaryEndpoint endpoint = {
                EPrimaryEndpointType::Barrier,
                nullptr,
                TKeyBound::FromRow(pivotKey, /*isInclusive*/ true, /*isUpper*/ false),
            };
            SegmentPrimaryEndpoints_[0].emplace_back(endpoint);
        }
    }

    //! Sort foreign slices in ascending order of lower key bound using foreign comparator.
    void SortForeignSlices()
    {
        YT_LOG_DEBUG("Sorting foreign slices (Count: %v)", ForeignSlices_.size());
        std::stable_sort(
            ForeignSlices_.begin(),
            ForeignSlices_.end(),
            [&] (const TLegacyDataSlicePtr& lhs, const TLegacyDataSlicePtr& rhs) {
                return ForeignComparator_.CompareKeyBounds(lhs->LowerLimit().KeyBound, rhs->LowerLimit().KeyBound) < 0;
            });
    }

    //! This method attaches all necessary foreign slices according to current staging area
    //! primary upper bound. It works correctly under assumption that StagingArea_->GetPrimaryUpperBound()
    //! monotonically increases.
    //! This method is idempotent and cheap to call (on average).
    void AttachForeignSlices(TKeyBound primaryUpperBound)
    {
        YT_LOG_TRACE(
            "Attaching foreign slices (PrimaryUpperBound: %v, FirstUnstagedForeignIndex: %v)",
            primaryUpperBound,
            FirstUnstagedForeignIndex_);

        auto shouldBeStaged = [&] (size_t index) {
            return
                index < ForeignSlices_.size() &&
                !PrimaryComparator_.IsRangeEmpty(ForeignSlices_[index]->LowerLimit().KeyBound, primaryUpperBound);
        };

        if (!shouldBeStaged(FirstUnstagedForeignIndex_)) {
            return;
        }

        while (shouldBeStaged(FirstUnstagedForeignIndex_)) {
            PeriodicYielder_.TryYield();
            Stage(ForeignSlices_[FirstUnstagedForeignIndex_], ESliceType::Foreign);
            FirstUnstagedForeignIndex_++;
        }

        YT_LOG_TRACE(
            "Foreign slices attached (FirstUnstagedForeignIndex: %v)",
            FirstUnstagedForeignIndex_);
    }

    void SortPrimaryEndpoints(std::vector<TPrimaryEndpoint>& endpoints)
    {
        YT_LOG_DEBUG("Sorting primary endpoints (Count: %v)", endpoints.size());
        // We sort endpoints by their location. In each group of endpoints at the same point
        // we sort them by type: barriers first, then foreign endpoints, then primary ones.
        std::sort(
            endpoints.begin(),
            endpoints.end(),
            [&] (const TPrimaryEndpoint& lhs, const TPrimaryEndpoint& rhs) {
                auto result = PrimaryComparator_.CompareKeyBounds(lhs.KeyBound, rhs.KeyBound);
                if (result != 0) {
                    return result < 0;
                }
                if (lhs.Type != rhs.Type) {
                    return lhs.Type < rhs.Type;
                }
                if (lhs.Type == EPrimaryEndpointType::Barrier) {
                    return false;
                }
                if (lhs.DataSlice->GetInputStreamIndex() != rhs.DataSlice->GetInputStreamIndex()) {
                    return lhs.DataSlice->GetInputStreamIndex() < rhs.DataSlice->GetInputStreamIndex();
                }
                YT_VERIFY(lhs.DataSlice->Tag);
                YT_VERIFY(rhs.DataSlice->Tag);
                if (*lhs.DataSlice->Tag != *rhs.DataSlice->Tag) {
                    return *lhs.DataSlice->Tag < *rhs.DataSlice->Tag;
                }
                return lhs.DataSlice->GetSliceIndex() < rhs.DataSlice->GetSliceIndex();
            });
    }

    void LogDetails(const std::vector<TPrimaryEndpoint>& endpoints)
    {
        if (!Logger.IsLevelEnabled(ELogLevel::Trace)) {
            return;
        }
        for (int index = 0; index < std::ssize(endpoints); ++index) {
            const auto& endpoint = endpoints[index];
            YT_LOG_TRACE("Endpoint (Index: %v, KeyBound: %v, Type: %v, DataSlice: %v)",
                index,
                endpoint.KeyBound,
                endpoint.Type,
                endpoint.DataSlice.Get());
        }
    }

    void LogStructured() const
    {
        if (!Logger.IsLevelEnabled(ELogLevel::Trace)) {
            return;
        }

        {
            TKeyBoundCompressor compressor(PrimaryComparator_);

            for (const auto& job : Jobs_) {
                if (job.GetIsBarrier()) {
                    continue;
                }
                compressor.Add(job.GetPrimaryLowerBound());
                compressor.Add(job.GetPrimaryUpperBound());
                for (const auto& stripe : job.GetStripeList()->Stripes) {
                    for (const auto& dataSlice : stripe->DataSlices) {
                        compressor.Add(dataSlice->LowerLimit().KeyBound);
                        compressor.Add(dataSlice->UpperLimit().KeyBound);
                    }
                }
            }

            for (const auto& dataSlice : InputDataSlices_) {
                compressor.Add(dataSlice->LowerLimit().KeyBound);
                compressor.Add(dataSlice->UpperLimit().KeyBound);
            }

            compressor.InitializeMapping();
            compressor.Dump(
                StructuredLogger
                    .WithStructuredTag("batch_kind", TStringBuf("compressor")));
        }

        {
            TStructuredLogBatcher batcher(
                StructuredLogger
                .WithStructuredTag("batch_kind", TStringBuf("input_data_slices")));
            for (const auto& dataSlice : InputDataSlices_) {
                batcher.AddItemFluently()
                    .Value(dataSlice);
            }
        }

        {
            TStructuredLogBatcher batcher(
                StructuredLogger
                    .WithStructuredTag("batch_kind", TStringBuf("job_data_slices")));
            for (const auto& job : Jobs_) {
                if (job.GetIsBarrier()) {
                    continue;
                }
                std::vector<TLegacyDataSlicePtr> dataSlices;
                for (const auto& stripe : job.GetStripeList()->Stripes) {
                    for (const auto& dataSlice : stripe->DataSlices) {
                        dataSlices.push_back(dataSlice);
                    }
                }
                batcher.AddItemFluently()
                    .Value(dataSlices);
            }
        }

        {
            TStructuredLogBatcher batcher(
                StructuredLogger
                    .WithStructuredTag("batch_kind", TStringBuf("jobs")));
            for (const auto& job : Jobs_) {
                if (job.GetIsBarrier()) {
                    continue;
                }
                batcher.AddItemFluently()
                    .Value(job);
            }
        }

    }

    i64 GetDataWeightPerJob() const
    {
        return
            JobSizeConstraints_->GetSamplingRate()
            ? JobSizeConstraints_->GetSamplingDataWeightPerJob()
            : JobSizeConstraints_->GetDataWeightPerJob();
    }

    i64 GetPrimaryDataWeightPerJob() const
    {
        return
            JobSizeConstraints_->GetSamplingRate()
            ? JobSizeConstraints_->GetSamplingPrimaryDataWeightPerJob()
            : JobSizeConstraints_->GetPrimaryDataWeightPerJob();
    }

    void AddJob(TNewJobStub& job)
    {
        if (JobSampler_.Sample()) {
            YT_LOG_DEBUG("Sorted job created (JobIndex: %v, BuiltJobCount: %v, PrimaryDataSize: %v, PrimaryRowCount: %v, "
                "PrimarySliceCount: %v, ForeignDataSize: %v, ForeignRowCount: %v, "
                "ForeignSliceCount: %v, PrimaryLowerBound: %v, PrimaryUpperBound: %v)",
                JobIndex_,
                Jobs_.size(),
                job.GetPrimaryDataWeight(),
                job.GetPrimaryRowCount(),
                job.GetPrimarySliceCount(),
                job.GetForeignDataWeight(),
                job.GetForeignRowCount(),
                job.GetForeignSliceCount(),
                job.GetPrimaryLowerBound(),
                job.GetPrimaryUpperBound());

            TotalDataWeight_ += job.GetDataWeight();

            YT_LOG_TRACE("Sorted job details (JobIndex: %v, BuiltJobCount: %v, Details: %v)",
                JobIndex_,
                Jobs_.size(),
                job.GetDebugString());

            Jobs_.emplace_back(std::move(job));
        } else {
            YT_LOG_DEBUG("Sorted job skipped (JobIndex: %v, BuiltJobCount: %v, PrimaryDataSize: %v, "
                "ForeignDataSize: %v, PrimaryLowerBound: %v, PrimaryUpperBound: %v)",
                JobIndex_,
                static_cast<int>(Jobs_.size()),
                job.GetPrimaryDataWeight(),
                job.GetForeignDataWeight(),
                job.GetPrimaryLowerBound(),
                job.GetPrimaryUpperBound());
        }
        ++JobIndex_;
    }

    //! Decide if range of slices defined by their left endpoints must be added as a whole, or if it
    //! May be added one by one with row slicing.
    ERowSliceabilityDecision DecideRowSliceability(TRange<TPrimaryEndpoint> endpoints, TKeyBound nextPrimaryLowerBound)
    {
        YT_VERIFY(!endpoints.empty());

        // With key guarantee there is no row slicing at all.
        if (Options_.EnableKeyGuarantee) {
            return ERowSliceabilityDecision::KeyGuaranteeDisabled;
        }

        // No job size tracker means present pivot keys, also no row slicing.
        if (!JobSizeTracker_) {
            return ERowSliceabilityDecision::NoJobSizeTracker;
        }

        bool versionedSlicesPresent = false;
        for (const auto& endpoint : endpoints) {
            if (endpoint.DataSlice->Type == EDataSourceType::VersionedTable) {
                versionedSlicesPresent = true;
            }
        }

        if (versionedSlicesPresent) {
            return ERowSliceabilityDecision::VersionedSlicesPresent;
        }

        auto stagedUpperBound = StagingArea_->GetPrimaryUpperBound();
        auto currentLowerBound = endpoints[0].KeyBound;

        TKeyBound maxUpperBound;

        // Count the number of non-singleton data slices in our range.
        int nonSingletonCount = 0;
        for (const auto& endpoint : endpoints) {
            const auto& dataSlice = endpoint.DataSlice;
            auto lowerBound = dataSlice->LowerLimit().KeyBound;
            auto upperBound = dataSlice->UpperLimit().KeyBound;
            YT_VERIFY(!PrimaryComparator_.IsRangeEmpty(lowerBound, upperBound));
            if (!PrimaryComparator_.TryAsSingletonKey(lowerBound, upperBound)) {
                ++nonSingletonCount;
            }
            if (!maxUpperBound || PrimaryComparator_.CompareKeyBounds(maxUpperBound, upperBound) < 0) {
                maxUpperBound = upperBound;
            }
        }

        YT_VERIFY(maxUpperBound);

        // If there are two or more non-singleton data slices, they block each other from row slicing.
        // TODO(max42): actually we may still slice singleton data slices in this case, but I already
        // spent too much time on this logic... maybe next time.
        if (nonSingletonCount >= 2) {
            return ERowSliceabilityDecision::NonSingletonSliceCountIsAtLeastTwo;
        }

        // If next primary data slice is too close to us, we also cannot use row slicing.
        if (nextPrimaryLowerBound &&
            !PrimaryComparator_.IsInteriorEmpty(nextPrimaryLowerBound, maxUpperBound))
        {
            return ERowSliceabilityDecision::NextPrimaryLowerBoundTooClose;
        }

        // Finally, if some of the already staged data slices overlaps with us, we also discard row slicing.
        if (stagedUpperBound &&
            !PrimaryComparator_.IsInteriorEmpty(currentLowerBound, stagedUpperBound) &&
            !PrimaryComparator_.IsInteriorEmpty(currentLowerBound, maxUpperBound))
        {
            return ERowSliceabilityDecision::OverlapsWithStagedDataSlice;
        }

        // Now goes the tricky moment. If the range defined by the only wide enough primary data slice
        // overlaps with too many foreign data slices (namely, enough to induce an overflow), we do not
        // attempt to slice by rows, preferring slicing by keys instead.
        // Refer to SuchForeignMuchData unittest for an example, or to "TFilterRedirectsReducer" operation
        // by Jupiter.
        auto foreignVector = StagingArea_->GetForeignResourceVector();
        for (const auto& dataSlice : ForeignSlices_) {
            if (PrimaryComparator_.IsRangeEmpty(dataSlice->LowerLimit().KeyBound, maxUpperBound)) {
                break;
            }
            foreignVector += TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ false);
        }
        if (LimitVector_.GetDataWeight() < foreignVector.GetDataWeight()) {
            return ERowSliceabilityDecision::TooMuchForeignData;
        }

        return ERowSliceabilityDecision::SliceByRows;
    }

    // Flush job size tracker (if it is present) and staging area.
    void Flush(std::optional<std::any> overflowToken = std::nullopt)
    {
        YT_LOG_TRACE("Flushing job");
        if (JobSizeTracker_) {
            JobSizeTracker_->Flush(std::move(overflowToken));
        }
        StagingArea_->Flush();
    }

    //! Put data slice to staging area.
    void Stage(TLegacyDataSlicePtr dataSlice, ESliceType sliceType)
    {
        if (JobSizeTracker_) {
            auto vector = TResourceVector::FromDataSlice(
                dataSlice,
                /*isPrimary*/ sliceType == ESliceType::Buffer || sliceType == ESliceType::Solid);
            JobSizeTracker_->AccountSlice(vector);
        }
        StagingArea_->Put(dataSlice, sliceType);

        if (sliceType == ESliceType::Solid) {
            AttachForeignSlices(dataSlice->UpperLimit().KeyBound);
        } else if (sliceType == ESliceType::Buffer) {
            AttachForeignSlices(dataSlice->LowerLimit().KeyBound.Invert());
        }
    }

    //! Partition data slices into singletons and long data slices keeping their original order among each input stream.
    void PartitionSingletonAndLongDataSlices(TKeyBound lowerBound, std::deque<TLegacyDataSlicePtr>& dataSlices)
    {
        std::stable_sort(dataSlices.begin(), dataSlices.end(), [&] (const TLegacyDataSlicePtr& lhs, const TLegacyDataSlicePtr& rhs) {
            bool lhsToEnd = !PrimaryComparator_.IsInteriorEmpty(lowerBound, lhs->UpperLimit().KeyBound);
            bool rhsToEnd = !PrimaryComparator_.IsInteriorEmpty(lowerBound, rhs->UpperLimit().KeyBound);
            return lhsToEnd < rhsToEnd;
        });
    }

    //! Stage several data slices using row slicing for better job size constraints meeting.
    void StageRangeWithRowSlicing(TRange<TPrimaryEndpoint> endpoints)
    {
        YT_LOG_TRACE("Processing endpoint range with row slicing (EndpointCount: %v)", endpoints.size());

        auto lowerBound = endpoints[0].KeyBound;

        // TODO(max42): describe this situation, refer to RowSlicingCorrectnessCustom unittest.
        if (!lowerBound.Invert().IsInclusive && lowerBound.Prefix.GetCount() == static_cast<ui32>(PrimaryComparator_.GetLength())) {
            StagingArea_->PromoteUpperBound(endpoints[0].KeyBound.Invert().ToggleInclusiveness());
        }

        YT_VERIFY(JobSizeTracker_);

        std::deque<TLegacyDataSlicePtr> dataSlices;

        for (const auto& endpoint : endpoints) {
            dataSlices.push_back(endpoint.DataSlice);
        }

        PartitionSingletonAndLongDataSlices(lowerBound, dataSlices);

        // We consider data slices one by one, adding them into the staging area.
        // Data slices are being added into staging area in solid manner meaning that
        // under no circumstances staging area would try to cut solid slice by key.

        while (!dataSlices.empty()) {
            PeriodicYielder_.TryYield();

            auto dataSlice = std::move(dataSlices.front());
            dataSlices.pop_front();

            auto vector = TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ true);

            auto overflowToken = JobSizeTracker_->CheckOverflow(vector);

            // First, check if we may put this data slice without producing overflow.
            if (!overflowToken) {
                Stage(dataSlice, ESliceType::Solid);
                continue;
            }

            // We know that this data slice produces overflow. This means no matter what happens next,
            // we must flush on this iteration. Note that we cannot use Finally() since Flush may throw
            // exceptions. Thus, in order to circumvent the sacred ban of goto in our code, we enclose
            // the remaining logic into IIFE.
            [&] {
                auto fraction = JobSizeTracker_->SuggestRowSplitFraction(vector);
                YT_LOG_TRACE("Row split factor suggested (Factor: %v)", fraction);

                // Due to rounding issues, we still decided to take data slice as a whole. This is ok.
                if (fraction == 1.0) {
                    YT_LOG_TRACE("Fraction for the remaining data slice is high enough to take it as a whole (Fraction: %v)", fraction);
                    Stage(std::move(dataSlice), ESliceType::Solid);
                    return;
                }

                // Divide slice in desired proportion using row indices.
                auto lowerRowIndex = dataSlice->LowerLimit().RowIndex.value_or(0);
                auto upperRowIndex = dataSlice->UpperLimit().RowIndex.value_or(dataSlice->GetSingleUnversionedChunk()->GetRowCount());
                YT_VERIFY(lowerRowIndex < upperRowIndex);
                auto rowCount = static_cast<i64>(std::ceil((upperRowIndex - lowerRowIndex) * fraction));
                rowCount = ClampVal<i64>(rowCount, 0, upperRowIndex - lowerRowIndex);

                YT_LOG_TRACE(
                    "Splitting data slice by rows (Fraction: %v, LowerRowIndex: %v, UpperRowIndex: %v, RowCount: %v, MiddleRowIndex: %v)",
                    fraction,
                    lowerRowIndex,
                    upperRowIndex,
                    rowCount,
                    lowerRowIndex + rowCount);

                auto [leftDataSlice, rightDataSlice] = dataSlice->SplitByRowIndex(rowCount);

                if (rowCount == upperRowIndex - lowerRowIndex) {
                    // In some borderline cases this may still happen... just put the original data slice.
                    Stage(std::move(dataSlice), ESliceType::Solid);
                    return;
                } else if (rowCount == 0) {
                    dataSlices.emplace_front(std::move(dataSlice));
                    return;
                } else {
                    Stage(std::move(leftDataSlice), ESliceType::Solid);
                    dataSlices.emplace_front(std::move(rightDataSlice));
                }
            }();

            Flush(overflowToken);
        }
    }

    //! Stage several data slices without row slicing "atomically".
    void StageRangeWithoutRowSlicing(
        TRange<TPrimaryEndpoint> endpoints,
        TKeyBound nextPrimaryLowerBound,
        ERowSliceabilityDecision decision)
    {
        YT_LOG_TRACE(
            "Processing endpoint range without row slicing (EndpointCount: %v, Decision: %v)",
            endpoints.size(),
            decision);

        // Note that data slices may still be sliced by key in staging area.

        auto lowerBound = endpoints[0].KeyBound;

        std::deque<TLegacyDataSlicePtr> dataSlices;

        for (const auto& endpoint : endpoints) {
            dataSlices.push_back(endpoint.DataSlice);
        }

        PartitionSingletonAndLongDataSlices(lowerBound, dataSlices);

        bool inLong = Options_.EnableKeyGuarantee;
        bool haveSolids = false;

        for (const auto& dataSlice : dataSlices) {
            if (!PrimaryComparator_.IsInteriorEmpty(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound)) {
                inLong = true;
                if (haveSolids) {
                    StagingArea_->Flush();
                    haveSolids = false;
                }
            }
            if (!inLong) {
                auto vector = TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ true);
                auto overflowToken = JobSizeTracker_->CheckOverflow(vector);
                if (overflowToken) {
                    Flush(overflowToken);
                    haveSolids = false;
                }
                Stage(dataSlice, ESliceType::Solid);
                haveSolids = true;
            } else {
                Stage(dataSlice, ESliceType::Buffer);
            }
        }

        if (!nextPrimaryLowerBound) {
            nextPrimaryLowerBound = TKeyBound::MakeEmpty(/*isUpper*/ false);
        }

        auto tryFlush = [&] {
            if (JobSizeTracker_) {
                auto overflowToken = JobSizeTracker_->CheckOverflow();
                if (overflowToken) {
                    Flush(overflowToken);
                }
            }
        };

        AttachForeignSlices(endpoints[0].KeyBound.Invert());
        tryFlush();

        for (size_t foreignDataSliceIndex = FirstUnstagedForeignIndex_; foreignDataSliceIndex < ForeignSlices_.size(); ++foreignDataSliceIndex) {
            const auto& dataSlice = ForeignSlices_[foreignDataSliceIndex];
            if (PrimaryComparator_.CompareKeyBounds(dataSlice->LowerLimit().KeyBound, nextPrimaryLowerBound) >= 0) {
                break;
            }

            auto upperBound = dataSlice->LowerLimit().KeyBound.Invert();
            StagingArea_->PromoteUpperBound(upperBound);
            AttachForeignSlices(upperBound);
            tryFlush();
        }
    }

    void BuildJobs(const std::vector<TPrimaryEndpoint>& endpoints)
    {
        if (auto samplingRate = JobSizeConstraints_->GetSamplingRate()) {
            YT_LOG_DEBUG(
                "Building jobs with sampling "
                "(SamplingRate: %v, SamplingDataWeightPerJob: %v, SamplingPrimaryDataWeightPerJob: %v)",
                *samplingRate,
                JobSizeConstraints_->GetSamplingDataWeightPerJob(),
                JobSizeConstraints_->GetSamplingPrimaryDataWeightPerJob());
        }

        StagingArea_ = CreateSortedStagingArea(
            Options_.EnableKeyGuarantee,
            PrimaryComparator_,
            ForeignComparator_,
            RowBuffer_,
            /*initialTotalDataSliceCount*/ TotalDataSliceCount_,
            Options_.MaxTotalSliceCount,
            InputStreamDirectory_,
            Logger);

        // Iterate over groups of coinciding endpoints.
        // Recall that coinciding endpoints are ordered by their type as follows:
        // Barrier < Foreign < Primary.
        for (int startIndex = 0, endIndex = 0; startIndex < std::ssize(endpoints); startIndex = endIndex) {
            PeriodicYielder_.TryYield();

            YT_LOG_TRACE("Moving to next endpoint (Endpoint: %v)", endpoints[startIndex].KeyBound);
            StagingArea_->PromoteUpperBound(endpoints[startIndex].KeyBound.Invert());

            int primaryIndex = startIndex;

            // Extract contiguous group of barrier & foreign endpoints.
            while (
                primaryIndex != std::ssize(endpoints) &&
                PrimaryComparator_.CompareKeyBounds(endpoints[startIndex].KeyBound, endpoints[primaryIndex].KeyBound) == 0 &&
                endpoints[primaryIndex].Type != EPrimaryEndpointType::Primary)
            {
                ++primaryIndex;
            }

            // No need to add more than one barrier at the same point.
            bool barriersPresent = (primaryIndex != startIndex);
            if (barriersPresent) {
                YT_LOG_TRACE("Putting barrier");
                Flush();
                StagingArea_->PutBarrier();
            }

            endIndex = primaryIndex;

            while (
                endIndex != std::ssize(endpoints) &&
                PrimaryComparator_.CompareKeyBounds(endpoints[startIndex].KeyBound, endpoints[endIndex].KeyBound) == 0)
            {
                ++endIndex;
            }

            auto primaryEndpoints = MakeRange(endpoints).Slice(primaryIndex, endIndex);

            if (primaryEndpoints.empty()) {
                continue;
            }

            int nextPrimaryIndex = endIndex;
            while (nextPrimaryIndex != std::ssize(endpoints) && endpoints[nextPrimaryIndex].Type != EPrimaryEndpointType::Primary) {
                ++nextPrimaryIndex;
            }
            TKeyBound nextPrimaryLowerBound = (nextPrimaryIndex == std::ssize(endpoints))
                ? TKeyBound()
                : endpoints[nextPrimaryIndex].KeyBound;

            auto decision = DecideRowSliceability(primaryEndpoints, nextPrimaryLowerBound);
            if (decision == ERowSliceabilityDecision::SliceByRows) {
                StageRangeWithRowSlicing(primaryEndpoints);
            } else {
                StageRangeWithoutRowSlicing(primaryEndpoints, nextPrimaryLowerBound, decision);
            }
        }

        StagingArea_->PutBarrier();

        AttachForeignSlices(TKeyBound::MakeUniversal(/*isUpper*/ true));

        StagingArea_->Finish();

        for (auto& preparedJob : StagingArea_->PreparedJobs()) {
            PeriodicYielder_.TryYield();

            if (preparedJob.GetIsBarrier()) {
                Jobs_.emplace_back(std::move(preparedJob));
            } else {
                AddJob(preparedJob);
            }
        }

        JobSizeConstraints_->UpdateInputDataWeight(TotalDataWeight_);

        YT_LOG_DEBUG("Jobs created (Count: %v)", Jobs_.size());

        TotalDataSliceCount_ = StagingArea_->GetTotalDataSliceCount();
    }
};

DEFINE_REFCOUNTED_TYPE(TNewSortedJobBuilder)

////////////////////////////////////////////////////////////////////////////////

INewSortedJobBuilderPtr CreateNewSortedJobBuilder(
    const TSortedJobOptions& options,
    IJobSizeConstraintsPtr jobSizeConstraints,
    const TRowBufferPtr& rowBuffer,
    const std::vector<TInputChunkPtr>& teleportChunks,
    int retryIndex,
    const TInputStreamDirectory& inputStreamDirectory,
    const TLogger& logger,
    const TLogger& structuredLogger)
{
    return New<TNewSortedJobBuilder>(
        options,
        std::move(jobSizeConstraints),
        rowBuffer,
        teleportChunks,
        retryIndex,
        inputStreamDirectory,
        logger,
        structuredLogger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

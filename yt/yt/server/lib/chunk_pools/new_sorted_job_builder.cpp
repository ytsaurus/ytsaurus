#include "new_sorted_job_builder.h"

#include "input_stream.h"
#include "job_size_tracker.h"
#include "new_job_manager.h"
#include "sorted_staging_area.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/client/table_client/key_bound_compressor.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/library/random/bernoulli_sampler.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/logging/fluent_log.h>

#include <cmath>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NLogging;
using namespace NTableClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPrimaryEndpointType,
    (Barrier)
    (Primary)
);

DEFINE_ENUM(ERowSliceabilityDecision,
    (SliceByRows)
    // All decisions below lead to slicing by keys.
    (KeyGuaranteeEnabled)
    (NoJobSizeTracker)
    (VersionedSlicesPresent)
    (NonSingletonSliceCountIsAtLeastTwo)
    (NextEndpointLowerBoundTooClose)
    (OverlapsWithStagedDataSlice)
    (TooMuchForeignData)
);

struct TPrimaryEndpoint
{
    EPrimaryEndpointType Type;
    TLegacyDataSlicePtr DataSlice;
    TKeyBound KeyBound;
};

////////////////////////////////////////////////////////////////////////////////

std::optional<TResourceVector> BuildLimitVector(
    const TSortedJobOptions& options,
    const IJobSizeConstraintsPtr& jobSizeConstraints,
    int retryIndex)
{
    if (!options.PivotKeys.empty()) {
        // Pivot keys provide guarantee that we won't introduce more jobs than
        // defined by them, so we do not try to flush by ourself if they are present.
        return std::nullopt;
    }

    double retryFactor = std::pow(jobSizeConstraints->GetDataWeightPerJobRetryFactor(), retryIndex);

    i64 dataWeightPerJob = jobSizeConstraints->GetSamplingRate()
        ? jobSizeConstraints->GetSamplingDataWeightPerJob()
        : jobSizeConstraints->GetDataWeightPerJob();

    i64 primaryDataWeightPerJob = jobSizeConstraints->GetSamplingRate()
            ? jobSizeConstraints->GetSamplingPrimaryDataWeightPerJob()
            : jobSizeConstraints->GetPrimaryDataWeightPerJob();

    // NB(apollo1321): Initial blocked sampling by compressed data size per job is not currently supported.
    i64 compressedDataSizePerJob = jobSizeConstraints->GetSamplingRate()
            ? std::numeric_limits<i64>::max()
            : jobSizeConstraints->GetCompressedDataSizePerJob();

    i64 primaryCompressedDataSizePerJob = jobSizeConstraints->GetSamplingRate()
            ? std::numeric_limits<i64>::max()
            : jobSizeConstraints->GetPrimaryCompressedDataSizePerJob();

    TResourceVector limitVector;

    limitVector.Values[EResourceKind::DataWeight] = static_cast<i64>(std::min<double>(
        std::numeric_limits<i64>::max() / 2,
        dataWeightPerJob * retryFactor));

    limitVector.Values[EResourceKind::PrimaryDataWeight] = static_cast<i64>(std::min<double>(
        std::numeric_limits<i64>::max() / 2,
        primaryDataWeightPerJob * retryFactor));

    limitVector.Values[EResourceKind::CompressedDataSize] = static_cast<i64>(std::min<double>(
        std::numeric_limits<i64>::max() / 2,
        compressedDataSizePerJob * retryFactor));

    limitVector.Values[EResourceKind::PrimaryCompressedDataSize] = static_cast<i64>(std::min<double>(
        std::numeric_limits<i64>::max() / 2,
        primaryCompressedDataSizePerJob * retryFactor));

    if (options.ConsiderOnlyPrimarySize) {
        limitVector.Values[EResourceKind::DataWeight] = std::numeric_limits<i64>::max() / 2;
        limitVector.Values[EResourceKind::CompressedDataSize] = std::numeric_limits<i64>::max() / 2;
    }

    limitVector.Values[EResourceKind::DataSliceCount] = jobSizeConstraints->GetMaxDataSlicesPerJob();

    return limitVector;
}

std::vector<TKeyBound> BuildTeleportChunkUpperBounds(
    const TSortedJobOptions& options,
    const std::vector<TInputChunkPtr>& teleportChunks,
    const TRowBufferPtr& rowBuffer)
{
    // We divide key space into segments between teleport chunks. Then we build jobs independently
    // on each segment.
    std::vector<TKeyBound> teleportChunkUpperBounds;
    teleportChunkUpperBounds.reserve(std::ssize(teleportChunks));

    for (const auto& chunk : teleportChunks) {
        auto maxKey = rowBuffer->CaptureRow(TRange(chunk->BoundaryKeys()->MaxKey.Begin(), options.PrimaryComparator.GetLength()));
        teleportChunkUpperBounds.emplace_back(TKeyBound::FromRow() <= maxKey);
    }

    return teleportChunkUpperBounds;
}

////////////////////////////////////////////////////////////////////////////////

class TNewSortedJobBuilder
    : public INewSortedJobBuilder
{
public:
    TNewSortedJobBuilder(
        const TSortedJobOptions& options,
        IJobSizeConstraintsPtr jobSizeConstraints,
        TRowBufferPtr rowBuffer,
        const std::vector<TInputChunkPtr>& teleportChunks,
        int retryIndex,
        const TInputStreamDirectory& inputStreamDirectory,
        TSortedChunkPoolStatisticsPtr chunkPoolStatistics,
        TLogger logger,
        TLogger structuredLogger)
        : Options_(options)
        , Logger(std::move(logger))
        , StructuredLogger(std::move(structuredLogger))
        , JobSizeConstraints_(std::move(jobSizeConstraints))
        , JobSampler_(JobSizeConstraints_->GetSamplingRate())
        , LimitVector_(BuildLimitVector(Options_, JobSizeConstraints_, retryIndex))
        , JobSizeTracker_(Options_.PivotKeys.empty()
            ? CreateJobSizeTracker(*LimitVector_, Options_.JobSizeTrackerOptions, Logger)
            : nullptr)
        , RowBuffer_(std::move(rowBuffer))
        , InputStreamDirectory_(inputStreamDirectory)
        , TeleportChunkUpperBounds_(BuildTeleportChunkUpperBounds(Options_, teleportChunks, RowBuffer_))
        , ChunkPoolStatistics_(std::move(chunkPoolStatistics))
    {
        SegmentPrimaryEndpoints_.resize(teleportChunks.size() + 1);

        if (auto samplingRate = JobSizeConstraints_->GetSamplingRate()) {
            YT_LOG_DEBUG(
                "Building jobs with sampling "
                "(SamplingRate: %v, SamplingDataWeightPerJob: %v, SamplingPrimaryDataWeightPerJob: %v)",
                *samplingRate,
                JobSizeConstraints_->GetSamplingDataWeightPerJob(),
                JobSizeConstraints_->GetSamplingPrimaryDataWeightPerJob());
        }
    }

    void AddDataSlice(const TLegacyDataSlicePtr& originalDataSlice) override
    {
        // Making a copy here is crucial since the copy may be modified in-place,
        // while the caller assumes that the original will not be modified.
        auto dataSlice = CreateInputDataSlice(originalDataSlice);

        YT_VERIFY(!dataSlice->IsLegacy);
        YT_VERIFY(dataSlice->LowerLimit().KeyBound);
        YT_VERIFY(dataSlice->UpperLimit().KeyBound);

        // Log the original data slices that should not be modified.
        InputDataSlices_.push_back(originalDataSlice);

        int inputStreamIndex = dataSlice->GetInputStreamIndex();
        bool isPrimary = InputStreamDirectory_.GetDescriptor(inputStreamIndex).IsPrimary();

        const auto& comparator = isPrimary ? Options_.PrimaryComparator : Options_.ForeignComparator;

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
            // TODO(apollo1321): Refactor to use separate TSegmentJobBuilder instances for each segment
            // since segments are processed independently. This would eliminate the need to share
            // StagingArea_ between segments, remove the need to pass periodicYielder between segments,
            // and improve overall code modularity and maintainability.
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
        auto validateConstraint = [&] (i64 size, i64 maxSize, EErrorCode errorCode, TStringBuf message) {
            if (size <= maxSize) {
                return;
            }

            YT_LOG_DEBUG(
                "Maximum allowed size per sorted job exceeds the limit (ErrorCode: %v, Size: %v, MaxSize: %v, "
                "PrimaryLowerBound: %v, PrimaryUpperBound: %v, JobDebugString: %v)",
                errorCode,
                size,
                maxSize,
                job->GetPrimaryLowerBound(),
                job->GetPrimaryUpperBound(),
                job->GetDebugString());

            THROW_ERROR_EXCEPTION(
                errorCode,
                "%v: %v > %v",
                message,
                size,
                maxSize)
                << TErrorAttribute("lower_bound", job->GetPrimaryLowerBound())
                << TErrorAttribute("upper_bound", job->GetPrimaryUpperBound());
        };

        validateConstraint(
            job->GetDataWeight(),
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            EErrorCode::MaxDataWeightPerJobExceeded,
            "Maximum allowed data weight per sorted job exceeds the limit");

        validateConstraint(
            job->GetPrimaryDataWeight(),
            JobSizeConstraints_->GetMaxPrimaryDataWeightPerJob(),
            EErrorCode::MaxPrimaryDataWeightPerJobExceeded,
            "Maximum allowed primary data weight per sorted job exceeds the limit");

        validateConstraint(
            job->GetCompressedDataSize(),
            JobSizeConstraints_->GetMaxCompressedDataSizePerJob(),
            EErrorCode::MaxCompressedDataSizePerJobExceeded,
            "Maximum allowed compressed data size per sorted job exceeds the limit");

        validateConstraint(
            job->GetPrimaryCompressedDataSize(),
            JobSizeConstraints_->GetMaxPrimaryCompressedDataSizePerJob(),
            EErrorCode::MaxCompressedDataSizePerJobExceeded,
            "Maximum allowed primary compressed data size per sorted job exceeds the limit");

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
            for (const auto& stripe : job->GetStripeList()->Stripes()) {
                auto inputStreamIndex = stripe->GetInputStreamIndex();
                for (int index = 0; index + 1 < std::ssize(stripe->DataSlices()); ++index) {
                    validatePair(
                        inputStreamIndex,
                        InputStreamDirectory_.GetDescriptor(inputStreamIndex).IsPrimary()
                            ? Options_.PrimaryComparator
                            : Options_.ForeignComparator,
                        stripe->DataSlices()[index],
                        stripe->DataSlices()[index + 1]);
                }
            }

            // Validate slice order between primary stripes.
            for (const auto& stripe : job->GetStripeList()->Stripes()) {
                auto inputStreamIndex = stripe->GetInputStreamIndex();
                if (InputStreamDirectory_.GetDescriptor(inputStreamIndex).IsForeign()) {
                    continue;
                }
                if (stripe->DataSlices().empty()) {
                    continue;
                }
                if (std::ssize(InputStreamIndexToLastDataSlice_) <= inputStreamIndex) {
                    InputStreamIndexToLastDataSlice_.resize(inputStreamIndex + 1, nullptr);
                }
                auto& lastDataSlice = InputStreamIndexToLastDataSlice_[inputStreamIndex];
                const auto& firstDataSlice = stripe->DataSlices().front();
                const auto& newLastDataSlice = stripe->DataSlices().back();
                if (lastDataSlice) {
                    validatePair(
                        inputStreamIndex,
                        Options_.PrimaryComparator,
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
    const TSortedJobOptions& Options_;

    const TLogger Logger;
    const TLogger StructuredLogger;

    const IJobSizeConstraintsPtr JobSizeConstraints_;
    TBernoulliSampler JobSampler_;

    const std::optional<TResourceVector> LimitVector_;
    const std::unique_ptr<IJobSizeTracker> JobSizeTracker_;

    const TRowBufferPtr RowBuffer_;

    const TInputStreamDirectory& InputStreamDirectory_;

    const std::vector<TKeyBound> TeleportChunkUpperBounds_;

    const TSortedChunkPoolStatisticsPtr ChunkPoolStatistics_;

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

    //! Contains last data slice for each input stream in order to validate important requirement
    //! for sorted pool: lower bounds and upper bounds must be monotonic among each input stream.
    //! Used in two contexts: in order to validate added input data slices and in order to validate
    //! resulting data slices produced by builder.
    std::vector<TLegacyDataSlicePtr> InputStreamIndexToLastDataSlice_;

    //! Used for structured logging.
    std::vector<TLegacyDataSlicePtr> InputDataSlices_;

    std::unique_ptr<ISortedStagingArea> StagingArea_;

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
                return Options_.ForeignComparator.CompareKeyBounds(lhs->LowerLimit().KeyBound, rhs->LowerLimit().KeyBound) < 0;
            });
    }

    //! This method attaches all necessary foreign slices according to current staging area
    //! primary upper bound. It works correctly under assumption that StagingArea_->GetPrimaryUpperBound()
    //! monotonically increases.
    //! This method is idempotent and cheap to call (on average).
    void AttachForeignSlices(TKeyBound primaryUpperBound, const TPeriodicYielderGuard& periodicYielder)
    {
        if (!Options_.EnableKeyGuarantee && !primaryUpperBound.IsInclusive) {
            primaryUpperBound = primaryUpperBound.ToggleInclusiveness();
        }

        YT_LOG_TRACE(
            "Attaching foreign slices (PrimaryUpperBound: %v, FirstUnstagedForeignIndex: %v)",
            primaryUpperBound,
            FirstUnstagedForeignIndex_);

        auto shouldBeStaged = [&] (size_t index) {
            return
                index < ForeignSlices_.size() &&
                !Options_.PrimaryComparator.IsRangeEmpty(ForeignSlices_[index]->LowerLimit().KeyBound, primaryUpperBound);
        };

        if (!shouldBeStaged(FirstUnstagedForeignIndex_)) {
            return;
        }

        while (shouldBeStaged(FirstUnstagedForeignIndex_)) {
            periodicYielder.TryYield();
            Stage(ForeignSlices_[FirstUnstagedForeignIndex_], ESliceType::Foreign, periodicYielder);
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
        // we sort them by type: barriers first, then primary endpoints.
        // NB(coteeq): stable_sort is needed not to mess up row-sliced slices.
        // See TSortedChunkPoolNewKeysTest::InterruptRowSlicedAfterAdjustment.
        std::stable_sort(
            endpoints.begin(),
            endpoints.end(),
            [&] (const TPrimaryEndpoint& lhs, const TPrimaryEndpoint& rhs) {
                auto result = Options_.PrimaryComparator.CompareKeyBounds(lhs.KeyBound, rhs.KeyBound);
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
                endpoint.DataSlice ? GetDataSliceDebugString(endpoint.DataSlice) : "<null>");
        }
    }

    void LogStructured() const
    {
        if (!Logger.IsLevelEnabled(ELogLevel::Trace)) {
            return;
        }

        {
            TKeyBoundCompressor compressor(Options_.PrimaryComparator);

            for (const auto& job : Jobs_) {
                if (job.GetIsBarrier()) {
                    continue;
                }
                compressor.Add(job.GetPrimaryLowerBound());
                compressor.Add(job.GetPrimaryUpperBound());
                for (const auto& stripe : job.GetStripeList()->Stripes()) {
                    for (const auto& dataSlice : stripe->DataSlices()) {
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
                for (const auto& stripe : job.GetStripeList()->Stripes()) {
                    for (const auto& dataSlice : stripe->DataSlices()) {
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
                std::ssize(Jobs_),
                job.GetPrimaryDataWeight(),
                job.GetForeignDataWeight(),
                job.GetPrimaryLowerBound(),
                job.GetPrimaryUpperBound());
        }
        ++JobIndex_;
    }

    //! Decide if range of slices defined by their left endpoints must be added as a whole, or if it
    //! may be added one by one with row slicing.
    ERowSliceabilityDecision DecideRowSliceability(TRange<TPrimaryEndpoint> endpoints, TKeyBound nextEndpointLowerBound)
    {
        YT_VERIFY(!endpoints.empty());

        // With key guarantee there is no row slicing at all.
        if (Options_.EnableKeyGuarantee) {
            return ERowSliceabilityDecision::KeyGuaranteeEnabled;
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
            YT_VERIFY(!Options_.PrimaryComparator.IsRangeEmpty(lowerBound, upperBound));
            if (!Options_.PrimaryComparator.TryAsSingletonKey(lowerBound, upperBound)) {
                ++nonSingletonCount;
            }
            if (!maxUpperBound || Options_.PrimaryComparator.CompareKeyBounds(maxUpperBound, upperBound) < 0) {
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
        if (nextEndpointLowerBound &&
            !Options_.PrimaryComparator.IsInteriorEmpty(nextEndpointLowerBound, maxUpperBound))
        {
            return ERowSliceabilityDecision::NextEndpointLowerBoundTooClose;
        }

        // Finally, if some of the already staged data slices overlaps with us, we also discard row slicing.
        if (stagedUpperBound &&
            !Options_.PrimaryComparator.IsInteriorEmpty(currentLowerBound, stagedUpperBound) &&
            !Options_.PrimaryComparator.IsInteriorEmpty(currentLowerBound, maxUpperBound))
        {
            return ERowSliceabilityDecision::OverlapsWithStagedDataSlice;
        }

        // Now goes the tricky moment. If the range defined by the only wide enough primary data slice
        // overlaps with too many foreign data slices (namely, enough to induce an overflow), we do not
        // attempt to slice by rows, preferring slicing by keys instead.
        // Refer to SuchForeignMuchData unittest for an example, or to "TFilterRedirectsReducer" operation
        // by Jupiter.
        auto foreignVector = StagingArea_->GetForeignResourceVector();
        for (const auto& dataSlice : ForeignSlices_ | std::views::drop(FirstUnstagedForeignIndex_)) {
            if (ChunkPoolStatistics_) {
                ChunkPoolStatistics_->ForeignSlicesCheckCountInDecideRowSliceability++;
            }

            if (Options_.PrimaryComparator.IsRangeEmpty(dataSlice->LowerLimit().KeyBound, maxUpperBound)) {
                break;
            }
            foreignVector += TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ false);
        }
        YT_VERIFY(LimitVector_.has_value());
        if (LimitVector_->GetDataWeight() < foreignVector.GetDataWeight()) {
            return ERowSliceabilityDecision::TooMuchForeignData;
        }

        return ERowSliceabilityDecision::SliceByRows;
    }

    void ValidateDataSliceLimit(ISortedStagingArea::TCurrentJobsStatistics currentJobsStatistics)
    {
        if (TotalDataSliceCount_ + currentJobsStatistics.DataSliceCount > Options_.MaxTotalSliceCount) {
            THROW_ERROR_EXCEPTION(NChunkPools::EErrorCode::DataSliceLimitExceeded, "Total number of data slices in sorted pool is too large")
                << TErrorAttribute("total_data_slice_count", TotalDataSliceCount_ + currentJobsStatistics.DataSliceCount)
                << TErrorAttribute("max_total_data_slice_count", Options_.MaxTotalSliceCount)
                << TErrorAttribute("current_job_count", currentJobsStatistics.JobCount);
        }
    }

    // Flush job size tracker (if it is present) and staging area.
    void Flush(const std::optional<std::any>& overflowToken = std::nullopt)
    {
        YT_LOG_TRACE("Flushing job");
        if (JobSizeTracker_) {
            JobSizeTracker_->Flush(overflowToken);
        }
        auto statistics = StagingArea_->Flush();
        ValidateDataSliceLimit(statistics);
    }

    void PromoteUpperBound(TKeyBound newUpperBound, const TPeriodicYielderGuard& periodicYielder)
    {
        StagingArea_->PromoteUpperBound(newUpperBound);
        AttachForeignSlices(std::move(newUpperBound), periodicYielder);
    }

    //! Put data slice to staging area.
    void Stage(TLegacyDataSlicePtr dataSlice, ESliceType sliceType, const TPeriodicYielderGuard& periodicYielder)
    {
        if (JobSizeTracker_) {
            auto vector = TResourceVector::FromDataSlice(
                dataSlice,
                /*isPrimary*/ sliceType == ESliceType::Buffer || sliceType == ESliceType::Solid);
            JobSizeTracker_->AccountSlice(vector);
        }
        StagingArea_->Put(dataSlice, sliceType);

        if (sliceType == ESliceType::Solid) {
            AttachForeignSlices(dataSlice->UpperLimit().KeyBound, periodicYielder);
        } else if (sliceType == ESliceType::Buffer) {
            AttachForeignSlices(dataSlice->LowerLimit().KeyBound.Invert(), periodicYielder);
        }
    }

    //! Partition data slices into singletons and long data slices keeping their original order among each input stream.
    void PartitionSingletonAndLongDataSlices(TKeyBound lowerBound, std::deque<TLegacyDataSlicePtr>& dataSlices)
    {
        std::stable_sort(dataSlices.begin(), dataSlices.end(), [&] (const TLegacyDataSlicePtr& lhs, const TLegacyDataSlicePtr& rhs) {
            bool lhsToEnd = !Options_.PrimaryComparator.IsInteriorEmpty(lowerBound, lhs->UpperLimit().KeyBound);
            bool rhsToEnd = !Options_.PrimaryComparator.IsInteriorEmpty(lowerBound, rhs->UpperLimit().KeyBound);
            return lhsToEnd < rhsToEnd;
        });
    }

    //! Stage several data slices using row slicing for better job size constraints meeting.
    void StageRangeWithRowSlicing(TRange<TPrimaryEndpoint> endpoints, const TPeriodicYielderGuard& periodicYielder)
    {
        YT_LOG_TRACE("Processing endpoint range with row slicing (EndpointCount: %v)", endpoints.size());

        auto lowerBound = endpoints[0].KeyBound;

        // TODO(apollo1321): This code seems useless. It should be removed later, see YT-26022.
        // Moreover, this code complicates TSortedStagingArea and allows upper bound "<= K" accept
        // solids with lower bound "=> K".
        // TODO(coteeq): Do Max's todo.
        // TODO(max42): describe this situation, refer to RowSlicingCorrectnessCustom unittest.
        if (!lowerBound.Invert().IsInclusive && lowerBound.Prefix.GetCount() == static_cast<ui32>(Options_.PrimaryComparator.GetLength())) {
            YT_LOG_TRACE("Weird max42 case (LowerBound: %v)", lowerBound);
            PromoteUpperBound(endpoints[0].KeyBound.Invert().ToggleInclusiveness(), periodicYielder);
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
            periodicYielder.TryYield();

            auto dataSlice = std::move(dataSlices.front());
            dataSlices.pop_front();

            auto vector = TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ true);

            auto overflowToken = JobSizeTracker_->CheckOverflow(vector);

            // First, check if we may put this data slice without producing overflow.
            if (!overflowToken) {
                Stage(dataSlice, ESliceType::Solid, periodicYielder);
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
                    Stage(std::move(dataSlice), ESliceType::Solid, periodicYielder);
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
                    Stage(std::move(dataSlice), ESliceType::Solid, periodicYielder);
                } else if (rowCount == 0) {
                    dataSlices.emplace_front(std::move(dataSlice));
                } else {
                    Stage(std::move(leftDataSlice), ESliceType::Solid, periodicYielder);
                    dataSlices.emplace_front(std::move(rightDataSlice));
                }
            }();

            // XXX(apollo1321): Is it ok that we use possibly outdated token here? Some foreign slices could be added.
            Flush(overflowToken);
        }
    }

    //! Stage several data slices without row slicing "atomically".
    void StageRangeWithoutRowSlicing(
        TRange<TPrimaryEndpoint> endpoints,
        TKeyBound nextEndpointLowerBound,
        ERowSliceabilityDecision decision,
        const TPeriodicYielderGuard& periodicYielder)
    {
        YT_LOG_TRACE(
            "Processing endpoint range without row slicing (EndpointCount: %v, Decision: %v)",
            endpoints.size(),
            decision);

        YT_VERIFY(nextEndpointLowerBound);

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
            if (!Options_.PrimaryComparator.IsInteriorEmpty(dataSlice->LowerLimit().KeyBound, dataSlice->UpperLimit().KeyBound)) {
                inLong = true;
                if (haveSolids) {
                    Flush();
                    haveSolids = false;
                }
            }
            if (!inLong) {
                auto vector = TResourceVector::FromDataSlice(dataSlice, /*isPrimary*/ true);
                // XXX(apollo1321): Is it really safe to dereference JobSizeTracker_ here?
                auto overflowToken = JobSizeTracker_->CheckOverflow(vector);
                if (overflowToken) {
                    Flush(overflowToken);
                    haveSolids = false;
                }
                Stage(dataSlice, ESliceType::Solid, periodicYielder);
                haveSolids = true;
            } else {
                Stage(dataSlice, ESliceType::Buffer, periodicYielder);
            }
        }

        auto tryFlush = [&] {
            if (JobSizeTracker_) {
                if (auto overflowToken = JobSizeTracker_->CheckOverflow()) {
                    Flush(overflowToken);
                }
            }
        };

        AttachForeignSlices(endpoints[0].KeyBound.Invert(), periodicYielder);
        tryFlush();

        for (size_t foreignDataSliceIndex = FirstUnstagedForeignIndex_; foreignDataSliceIndex < ForeignSlices_.size(); ++foreignDataSliceIndex) {
            const auto& dataSlice = ForeignSlices_[foreignDataSliceIndex];
            if (Options_.PrimaryComparator.CompareKeyBounds(dataSlice->LowerLimit().KeyBound, nextEndpointLowerBound) >= 0) {
                break;
            }

            YT_LOG_TRACE(
                "Trying to flush since a new foreign slice can be attached (ForeignSliceIndex: %v)",
                foreignDataSliceIndex);
            auto upperBound = dataSlice->LowerLimit().KeyBound.Invert();
            PromoteUpperBound(upperBound, periodicYielder);
            tryFlush();
        }
    }

    void BuildJobs(const std::vector<TPrimaryEndpoint>& endpoints)
    {
        auto periodicYielder = CreatePeriodicYielder(
            Options_.EnablePeriodicYielder
                ? std::optional(PrepareYieldPeriod)
                : std::nullopt);

        FirstUnstagedForeignIndex_ = 0;

        StagingArea_ = CreateSortedStagingArea(
            Options_.EnableKeyGuarantee,
            Options_.PrimaryComparator,
            Options_.ForeignComparator,
            RowBuffer_,
            Logger);

        // Iterate over groups of coinciding endpoints.
        // Recall that coinciding endpoints are ordered by their type as follows:
        // Barrier < Primary.
        for (int startIndex = 0, endIndex = 0; startIndex < std::ssize(endpoints); startIndex = endIndex) {
            periodicYielder.TryYield();

            YT_LOG_TRACE("Moving to next endpoint (Endpoint: %v)", endpoints[startIndex].KeyBound);
            PromoteUpperBound(endpoints[startIndex].KeyBound.Invert(), periodicYielder);

            int primaryIndex = startIndex;

            // Extract contiguous group of barriers.
            while (
                primaryIndex != std::ssize(endpoints) &&
                Options_.PrimaryComparator.CompareKeyBounds(endpoints[startIndex].KeyBound, endpoints[primaryIndex].KeyBound) == 0 &&
                endpoints[primaryIndex].Type != EPrimaryEndpointType::Primary)
            {
                ++primaryIndex;
            }

            // No need to add more than one barrier at the same point.
            if (bool barriersPresent = (primaryIndex != startIndex); barriersPresent) {
                YT_LOG_TRACE("Putting barrier");
                Flush();
                StagingArea_->PutBarrier();
            }

            endIndex = primaryIndex;

            while (
                endIndex != std::ssize(endpoints) &&
                Options_.PrimaryComparator.CompareKeyBounds(endpoints[startIndex].KeyBound, endpoints[endIndex].KeyBound) == 0)
            {
                ++endIndex;
            }

            auto primaryEndpoints = TRange(endpoints).Slice(primaryIndex, endIndex);

            if (primaryEndpoints.empty()) {
                continue;
            }

            TKeyBound nextEndpointLowerBound = (endIndex == std::ssize(endpoints))
                ? TKeyBound::MakeEmpty(/*isUpper*/ false)
                : endpoints[endIndex].KeyBound;

            auto decision = DecideRowSliceability(primaryEndpoints, nextEndpointLowerBound);
            if (decision == ERowSliceabilityDecision::SliceByRows) {
                StageRangeWithRowSlicing(primaryEndpoints, periodicYielder);
            } else {
                StageRangeWithoutRowSlicing(primaryEndpoints, nextEndpointLowerBound, decision, periodicYielder);
            }
        }

        AttachForeignSlices(TKeyBound::MakeUniversal(/*isUpper*/ true), periodicYielder);

        auto [preparedJobs, statistics] = std::move(*StagingArea_).Finish();
        ValidateDataSliceLimit(statistics);

        preparedJobs.emplace_back().SetIsBarrier(true);

        i64 actualSegmentSliceCount = 0;
        i64 barrierCount = 0;
        for (auto& preparedJob : preparedJobs) {
            periodicYielder.TryYield();
            actualSegmentSliceCount += preparedJob.GetSliceCount();

            if (preparedJob.GetIsBarrier()) {
                Jobs_.emplace_back(std::move(preparedJob));
                ++barrierCount;
            } else {
                AddJob(preparedJob);
            }
        }

        YT_VERIFY(statistics.DataSliceCount == actualSegmentSliceCount);
        TotalDataSliceCount_ += statistics.DataSliceCount;

        JobSizeConstraints_->UpdateInputDataWeight(TotalDataWeight_);

        YT_LOG_DEBUG("Jobs created (JobCount: %v, BarrierCount: %v)", std::ssize(Jobs_) - barrierCount, barrierCount);
    }
};

DEFINE_REFCOUNTED_TYPE(TNewSortedJobBuilder)

} // namespace

////////////////////////////////////////////////////////////////////////////////

INewSortedJobBuilderPtr CreateNewSortedJobBuilder(
    const TSortedJobOptions& options,
    IJobSizeConstraintsPtr jobSizeConstraints,
    TRowBufferPtr rowBuffer,
    const std::vector<TInputChunkPtr>& teleportChunks,
    int retryIndex,
    const TInputStreamDirectory& inputStreamDirectory,
    TSortedChunkPoolStatisticsPtr chunkPoolStatistics,
    TLogger logger,
    TLogger structuredLogger)
{
    return New<TNewSortedJobBuilder>(
        options,
        std::move(jobSizeConstraints),
        std::move(rowBuffer),
        teleportChunks,
        retryIndex,
        inputStreamDirectory,
        std::move(chunkPoolStatistics),
        std::move(logger),
        std::move(structuredLogger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

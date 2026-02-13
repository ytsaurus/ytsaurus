#include "ordered_chunk_pool.h"

#include "config.h"
#include "helpers.h"
#include "job_size_adjuster.h"
#include "new_job_manager.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <yt/yt/library/random/bernoulli_sampler.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/logging/logger_owner.h>

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

////////////////////////////////////////////////////////////////////////////////

void TOrderedChunkPoolOptions::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, MaxTotalSliceCount);
    PHOENIX_REGISTER_FIELD(2, MinTeleportChunkSize);
    PHOENIX_REGISTER_FIELD(3, JobSizeConstraints);
    registrar.template VirtualField<4>("SupportLocality", [] (TThis* /*this_*/, auto& context) {
        Load<bool>(context);
    })
        .BeforeVersion(ESnapshotVersion::DropSupportLocality)();
    PHOENIX_REGISTER_FIELD(5, EnablePeriodicYielder);
    PHOENIX_REGISTER_FIELD(6, ShouldSliceByRowIndices);
    PHOENIX_REGISTER_FIELD(7, Logger);

    PHOENIX_REGISTER_FIELD(8, JobSizeAdjusterConfig,
        .SinceVersion(ESnapshotVersion::OrderedAndSortedJobSizeAdjuster));
}

PHOENIX_DEFINE_TYPE(TOrderedChunkPoolOptions);

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputWithNewJobManagerBase
    , public IOrderedChunkPool
    , public TJobSplittingBase
    , public virtual NLogging::TLoggerOwner
{
public:
    //! Used only for persistence.
    TOrderedChunkPool() = default;

    TOrderedChunkPool(
        const TOrderedChunkPoolOptions& options,
        TInputStreamDirectory inputStreamDirectory)
        : TChunkPoolOutputWithNewJobManagerBase(options.Logger)
        , InputStreamDirectory_(std::move(inputStreamDirectory))
        , MinTeleportChunkSize_(options.MinTeleportChunkSize)
        , JobSizeConstraints_(options.JobSizeConstraints)
        , Sampler_(JobSizeConstraints_->GetSamplingRate())
        , MaxTotalSliceCount_(options.MaxTotalSliceCount)
        , ShouldSliceByRowIndices_(options.ShouldSliceByRowIndices)
        , EnablePeriodicYielder_(options.EnablePeriodicYielder)
    {
        Logger = options.Logger;

        ValidateLogger(Logger);

        if (JobSizeConstraints_->IsExplicitJobCount() && JobSizeConstraints_->GetJobCount() == 1) {
            SingleJob_ = true;
        }

        if (options.JobSizeAdjusterConfig && JobSizeConstraints_->CanAdjustDataWeightPerJob()) {
            JobSizeAdjuster_ = CreateDiscreteJobSizeAdjuster(
                JobSizeConstraints_->GetDataWeightPerJob(),
                options.JobSizeAdjusterConfig);
        }

        if (JobSizeConstraints_->GetBatchRowCount()) {
            YT_VERIFY(!JobSizeConstraints_->GetSamplingRate());
        }

        YT_LOG_INFO(
            "Ordered chunk pool created (DataWeightPerJob: %v, SamplingDataWeightPerJob: %v, MaxDataWeightPerJob: %v, "
            "CompressedDataSizePerJob: %v, MaxCompressedDataSizePerJob: %v, "
            "MaxDataSlicesPerJob: %v, InputSliceDataWeight: %v, InputSliceRowCount: %v, "
            "BatchRowCount: %v, SamplingRate: %v, SingleJob: %v, HasJobSizeAdjuster: %v)",
            JobSizeConstraints_->GetDataWeightPerJob(),
            JobSizeConstraints_->GetSamplingRate() ? std::optional<i64>(JobSizeConstraints_->GetSamplingDataWeightPerJob()) : std::nullopt,
            JobSizeConstraints_->GetMaxDataWeightPerJob(),
            JobSizeConstraints_->GetCompressedDataSizePerJob(),
            JobSizeConstraints_->GetMaxCompressedDataSizePerJob(),
            JobSizeConstraints_->GetMaxDataSlicesPerJob(),
            JobSizeConstraints_->GetInputSliceDataWeight(),
            JobSizeConstraints_->GetInputSliceRowCount(),
            JobSizeConstraints_->GetBatchRowCount(),
            JobSizeConstraints_->GetSamplingRate(),
            SingleJob_,
            static_cast<bool>(JobSizeAdjuster_));
    }

    // IPersistentChunkPoolInput implementation.

    IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YT_VERIFY(!Finished);

        if (stripe->DataSlices().empty()) {
            return IChunkPoolInput::NullCookie;
        }

        for (const auto& dataSlice : stripe->DataSlices()) {
            YT_VERIFY(!dataSlice->IsLegacy);
        }

        auto cookie = std::ssize(Stripes_);
        Stripes_.emplace_back(stripe);

        CheckCompleted();

        return cookie;
    }

    void Finish() override
    {
        YT_VERIFY(!Finished);
        TChunkPoolInputBase::Finish();

        // NB: This method accounts all the stripes that were suspended before
        // the chunk pool was finished. It should be called only once.
        SetupSuspendedStripes();

        BuildJobsAndFindTeleportChunks();
        CheckCompleted();
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
            auto childCookies = SplitJob(jobSummary.UnreadInputDataSlices, jobSummary.SplitJobCount, cookie);
            ValidateChildJobSizes(cookie, childCookies, [this] (TOutputCookie cookie) {
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
                YT_LOG_INFO("Job completion triggered enlargement (JobCookie: %v)", cookie);
                JobManager_->Enlarge(
                    JobSizeAdjuster_->GetDataWeightPerJob(),
                    JobSizeAdjuster_->GetDataWeightPerJob(),
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

    std::vector<TChunkTreeId> ArrangeOutputChunkTrees(
        const std::vector<std::pair<TOutputCookie, TChunkTreeId>>& chunkTrees) const override
    {
        auto cookieToPosition = JobManager_->GetCookieToPosition();

        std::vector<TChunkTreeId> orderedTreeIds;
        orderedTreeIds.resize(cookieToPosition.size());

        for (const auto& [cookie, chunkTreeId] : chunkTrees) {
            YT_ASSERT(chunkTreeId);
            orderedTreeIds[cookieToPosition[cookie]] = chunkTreeId;
        }

        // NB: Chunk trees count may be smaller than valid cookies count
        // because of RichYPath's max_row_count.
        auto end = std::remove(
            orderedTreeIds.begin(),
            orderedTreeIds.end(),
            TChunkTreeId());

        return {orderedTreeIds.begin(), end};
    }

    std::vector<TOutputCookie> GetOutputCookiesInOrder() const override
    {
        std::vector<TOutputCookie> cookies;
        auto cookieToPosition = JobManager_->GetCookieToPosition();

        auto totalValidCookies = std::count_if(
            cookieToPosition.begin(),
            cookieToPosition.end(),
            [] (const auto& position) { return position != -1; });

        cookies.resize(totalValidCookies);
        for (auto [cookie, position] : Enumerate(cookieToPosition)) {
            if (position != -1) {
                cookies[position] = cookie;
            }
        }
        return cookies;
    }

private:
    //! Information about input sources (e.g. input tables for sorted reduce operation).
    TInputStreamDirectory InputStreamDirectory_;

    //! An option to control chunk teleportation logic. Only large complete
    //! chunks of at least that size will be teleported.
    i64 MinTeleportChunkSize_;

    //! All stripes that were added to this pool.
    std::vector<TSuspendableStripe> Stripes_;

    IJobSizeConstraintsPtr JobSizeConstraints_;
    //! Used both for job sampling and teleport chunk sampling.
    TBernoulliSampler Sampler_;

    std::unique_ptr<IDiscreteJobSizeAdjuster> JobSizeAdjuster_;

    i64 MaxTotalSliceCount_ = 0;

    bool ShouldSliceByRowIndices_ = false;

    bool EnablePeriodicYielder_;

    // If the value is not set, it indicates that the row count until job split
    // cannot be currently estimated. In this scenario, a new slice may be
    // added without requiring a split.
    std::optional<i64> RowCountUntilJobSplit_;
    std::unique_ptr<TNewJobStub> CurrentJob_;

    int JobIndex_ = 0;
    int BuiltJobCount_ = 0;

    bool SingleJob_ = false;

    bool IsCompleted_ = false;

    void SetupSuspendedStripes()
    {
        for (int inputCookie = 0; inputCookie < std::ssize(Stripes_); ++inputCookie) {
            const auto& stripe = Stripes_[inputCookie];
            if (stripe.IsSuspended()) {
                JobManager_->Suspend(inputCookie);
            }
        }
    }

    bool IsTeleportable(const TLegacyDataSlicePtr& dataSlice) const
    {
        if (dataSlice->Type != EDataSourceType::UnversionedTable) {
            return false;
        }

        if (SingleJob_ || JobSizeConstraints_->GetBatchRowCount()) {
            return false;
        }

        if (!InputStreamDirectory_.GetDescriptor(dataSlice->GetInputStreamIndex()).IsTeleportable()) {
            return false;
        }

        return dataSlice->GetSingleUnversionedChunk()->IsLargeCompleteChunk(MinTeleportChunkSize_);
    }

    void BuildJobsAndFindTeleportChunks()
    {
        if (auto samplingRate = JobSizeConstraints_->GetSamplingRate()) {
            YT_LOG_DEBUG(
                "Building jobs with sampling "
                "(SamplingRate: %v, SamplingDataWeightPerJob: %v, SamplingPrimaryDataWeightPerJob: %v)",
                *samplingRate,
                JobSizeConstraints_->GetSamplingDataWeightPerJob(),
                JobSizeConstraints_->GetSamplingPrimaryDataWeightPerJob());
        }

        // TODO(apollo1321): Is it really a good place to set up job size constraints for partitioning tables?
        // Probably it should be reworked, it seems like this code is only relevant for partition tables.
        if (JobSizeConstraints_->IsExplicitJobCount() && !SingleJob_ && !JobSizeConstraints_->GetSamplingRate()) {
            i64 totalDataWeight = 0;
            for (int inputCookie = 0; inputCookie < std::ssize(Stripes_); ++inputCookie) {
                const auto& stripe = Stripes_[inputCookie].GetStripe();
                for (const auto& dataSlice : stripe->DataSlices()) {
                    totalDataWeight += dataSlice->GetDataWeight();
                }
            }
            JobSizeConstraints_->UpdateInputDataWeight(totalDataWeight);
        }

        int droppedTeleportChunkCount = 0;
        int chunksTeleported = 0;

        auto yielder = CreatePeriodicYielder(
            EnablePeriodicYielder_
                ? std::optional(PrepareYieldPeriod)
                : std::nullopt);
        for (int inputCookie = 0; inputCookie < std::ssize(Stripes_); ++inputCookie) {
            const auto& stripe = Stripes_[inputCookie].GetStripe();
            for (const auto& dataSlice : stripe->DataSlices()) {
                yielder.TryYield();
                if (IsTeleportable(dataSlice)) {
                    // NB(apollo1321): RowCountUntilJobSplit_ may be non-empty after
                    // AddSplittablePrimaryDataSlice only if BatchRowCount is set, and
                    // BatchRowCount disables teleporting chunks.
                    YT_VERIFY(!RowCountUntilJobSplit_.has_value());
                    if (Sampler_.Sample()) {
                        EndJob();

                        // Add barrier.
                        CurrentJob()->SetIsBarrier(true);
                        auto cookie = JobManager_->AddJob(std::move(CurrentJob()));

                        auto inputChunk = dataSlice->GetSingleUnversionedChunk();
                        ChunkTeleported_.Fire(inputChunk, /*tag*/ std::any{cookie});
                        ++chunksTeleported;
                    } else {
                        // This teleport chunk goes to /dev/null.
                        ++droppedTeleportChunkCount;
                    }

                    continue;
                }

                YT_VERIFY(!dataSlice->IsLegacy);
                if (IsDataSliceSplittable(dataSlice)) {
                    AddSplittablePrimaryDataSlice(dataSlice, inputCookie);
                } else {
                    AddUnsplittablePrimaryDataSlice(dataSlice, inputCookie, GetDataWeightPerJobForBuildingJobs(), JobSizeConstraints_->GetCompressedDataSizePerJob());
                }
            }
        }
        EndJob();

        YT_LOG_INFO("Jobs created (Count: %v, TeleportChunkCount: %v, DroppedTeleportChunkCount: %v)",
            BuiltJobCount_,
            chunksTeleported,
            droppedTeleportChunkCount);

        if (JobSizeConstraints_->GetSamplingRate()) {
            JobManager_->Enlarge(
                JobSizeConstraints_->GetDataWeightPerJob(),
                JobSizeConstraints_->GetPrimaryDataWeightPerJob(),
                JobSizeConstraints_);
        }

        // TODO(apollo1321): Why do we need to update input data weight here?
        JobSizeConstraints_->UpdateInputDataWeight(JobManager_->DataWeightCounter()->GetTotal());
    }

    std::vector<IChunkPoolOutput::TCookie> SplitJob(
        std::vector<TLegacyDataSlicePtr> unreadInputDataSlices,
        int splitJobCount,
        IChunkPoolOutput::TCookie cookie)
    {
        i64 dataWeight = 0;
        i64 compressedDataSize = 0;
        for (const auto& dataSlice : unreadInputDataSlices) {
            dataWeight += dataSlice->GetDataWeight();
            compressedDataSize += dataSlice->GetCompressedDataSize();
        }
        i64 dataWeightPerJob;
        i64 compressedDataSizePerJob;
        if (splitJobCount == 1) {
            dataWeightPerJob = std::numeric_limits<i64>::max() / 4;
            compressedDataSizePerJob = std::numeric_limits<i64>::max() / 4;
        } else {
            dataWeightPerJob = DivCeil(dataWeight, static_cast<i64>(splitJobCount));
            compressedDataSizePerJob = DivCeil(compressedDataSize, static_cast<i64>(splitJobCount));
        }

        YT_LOG_DEBUG(
            "Job split parameters computed (OutputCookie: %v, SplitJobCount: %v, "
            "DataWeightPerJob: %v, CompressedDataSizePerJob: %v, "
            "UnreadDataWeight: %v, UnreadCompressedDataSize: %v)",
            cookie,
            splitJobCount,
            dataWeightPerJob,
            compressedDataSizePerJob,
            dataWeight,
            compressedDataSize);

        // NB: Teleport chunks do not affect the job split process since each original
        // job is already located between the teleport chunks.

        // Insert consequent jobs after the parent cookie.
        JobManager_->SeekOrder(cookie);

        std::vector<IChunkPoolOutput::TCookie> childCookies;
        for (const auto& dataSlice : unreadInputDataSlices) {
            int inputCookie = *dataSlice->Tag;
            auto outputCookie = AddUnsplittablePrimaryDataSlice(dataSlice, inputCookie, dataWeightPerJob, compressedDataSizePerJob);
            if (outputCookie != IChunkPoolOutput::NullCookie) {
                childCookies.push_back(outputCookie);
            }
        }

        if (auto outputCookie = EndJob(); outputCookie != IChunkPoolOutput::NullCookie) {
            childCookies.push_back(outputCookie);
        }

        return childCookies;
    }

    // NB(apollo1321): Might be smaller than actual DWPJ when sampling is enabled.
    // Small job stripes will be enlarged after processing all input data.
    i64 GetDataWeightPerJobForBuildingJobs() const
    {
        return
            JobSizeConstraints_->GetSamplingRate()
            ? JobSizeConstraints_->GetSamplingDataWeightPerJob()
            : JobSizeConstraints_->GetDataWeightPerJob();
    }

    i64 GetCompressedDataSizePerJobForBuildingJobs() const
    {
        // NB(apollo1321): Initial blocked sampling by compressed data size per job is not currently supported.
        // Sampling by compressed data size would be preferable, but this functionality has not been implemented yet.
        return
            JobSizeConstraints_->GetSamplingRate()
            ? std::numeric_limits<i64>::max()
            : JobSizeConstraints_->GetCompressedDataSizePerJob();
    }

    IChunkPoolOutput::TCookie AddUnsplittablePrimaryDataSlice(
        const TLegacyDataSlicePtr& dataSlice,
        IChunkPoolInput::TCookie cookie,
        i64 dataWeightPerJob,
        i64 compressedDataSizePerJob)
    {
        YT_VERIFY(!dataSlice->IsLegacy);

        RowCountUntilJobSplit_ = std::nullopt;

        auto dataSliceCopy = CreateInputDataSlice(dataSlice);
        dataSliceCopy->Tag = cookie;
        CurrentJob()->AddDataSlice(dataSliceCopy, cookie, /*isPrimary*/ true);

        bool jobIsLargeEnough =
            CurrentJob()->GetPreliminarySliceCount() >= JobSizeConstraints_->GetMaxDataSlicesPerJob() ||
            CurrentJob()->GetDataWeight() >= dataWeightPerJob ||
            CurrentJob()->GetCompressedDataSize() >= compressedDataSizePerJob;
        if (jobIsLargeEnough && !SingleJob_) {
            return EndJob();
        }

        return IChunkPoolOutput::NullCookie;
    }

    bool IsDataSliceSplittable(const TLegacyDataSlicePtr& dataSlice) const
    {
        if (dataSlice->Type != EDataSourceType::UnversionedTable) {
            return false;
        }

        // Unbounded dynamic store cannot be split.
        if (dataSlice->GetSingleUnversionedChunkSlice()->GetInputChunk()->IsOrderedDynamicStore() &&
            !dataSlice->GetSingleUnversionedChunkSlice()->UpperLimit().RowIndex)
        {
            return false;
        }

        return ShouldSliceByRowIndices_;
    }

    void AddSplittablePrimaryDataSlice(
        const TLegacyDataSlicePtr& dataSlice,
        IChunkPoolInput::TCookie cookie)
    {
        YT_VERIFY(IsDataSliceSplittable(dataSlice));

        auto chunkSlice = dataSlice->GetSingleUnversionedChunkSlice();

        while (chunkSlice && chunkSlice->GetRowCount() > 0) {
            UpdateRowCountUntilJobSplit(chunkSlice);

            TInputChunkSlicePtr chunkSliceToAdd;

            if (!RowCountUntilJobSplit_.has_value() || *RowCountUntilJobSplit_ >= chunkSlice->GetRowCount()) {
                std::swap(chunkSliceToAdd, chunkSlice);
            } else {
                YT_VERIFY(*RowCountUntilJobSplit_ >= 0);
                std::tie(chunkSliceToAdd, chunkSlice) = chunkSlice->SplitByRowIndex(*RowCountUntilJobSplit_);
            }

            if (RowCountUntilJobSplit_.has_value()) {
                *RowCountUntilJobSplit_ -= chunkSliceToAdd->GetRowCount();
            }

            i64 addedRowCount = chunkSliceToAdd->GetRowCount();

            if (chunkSliceToAdd->GetRowCount() > 0) {
                auto dataSliceToAdd = CreateUnversionedInputDataSlice(std::move(chunkSliceToAdd));
                dataSliceToAdd->CopyPayloadFrom(*dataSlice);
                dataSliceToAdd->Tag = cookie;

                CurrentJob()->AddDataSlice(dataSliceToAdd, cookie, /*isPrimary*/ true);
            }

            if (RowCountUntilJobSplit_.has_value() && *RowCountUntilJobSplit_ == 0) {
                RowCountUntilJobSplit_ = std::nullopt;
                YT_VERIFY(CurrentJob()->GetRowCount() > 0);
                EndJob();
            } else {
                YT_VERIFY(addedRowCount > 0);
            }
        }
    }

    void UpdateRowCountUntilJobSplit(const TInputChunkSlicePtr& chunkSlice)
    {
        if (SingleJob_) {
            // Do not split input in case of single job.
            return;
        }

        YT_VERIFY(chunkSlice->GetRowCount() > 0);
        YT_VERIFY(CurrentJob()->GetPreliminarySliceCount() < JobSizeConstraints_->GetMaxDataSlicesPerJob());

        i64 maxRowCountUntilJobSplit = std::numeric_limits<i64>::max();

        i64 maxCompressedDataSizeUntilJobSplit = std::max<i64>(0, JobSizeConstraints_->GetMaxCompressedDataSizePerJob() - CurrentJob()->GetCompressedDataSize());
        if (maxCompressedDataSizeUntilJobSplit <= chunkSlice->GetCompressedDataSize()) {
            maxRowCountUntilJobSplit = SignedSaturationConversion(
                (static_cast<double>(maxCompressedDataSizeUntilJobSplit) * chunkSlice->GetRowCount()) / chunkSlice->GetCompressedDataSize());
            maxRowCountUntilJobSplit = std::clamp<i64>(maxRowCountUntilJobSplit, 0, chunkSlice->GetRowCount());
        }

        // NB: Hitting this limit means we cannot take more than one slice.
        // In this case the final row count of this job might not be divisible by batch row count.
        if (CurrentJob()->GetPreliminarySliceCount() + 1 == JobSizeConstraints_->GetMaxDataSlicesPerJob()) {
            maxRowCountUntilJobSplit = std::min(chunkSlice->GetRowCount(), maxRowCountUntilJobSplit);
        }

        // We should always return at least one slice, even if we get Max...PerJob overflow.
        if (CurrentJob()->GetSliceCount() == 0) {
            // Don't make too small stripes even if we overflow Max..PerJob constraints.
            maxRowCountUntilJobSplit = std::max(
                maxRowCountUntilJobSplit,
                std::min(JobSizeConstraints_->GetInputSliceRowCount(), chunkSlice->GetRowCount()));
        }

        if (!RowCountUntilJobSplit_.has_value()) {
            i64 dataWeightUntilJobSplit = std::max<i64>(0, GetDataWeightPerJobForBuildingJobs() - CurrentJob()->GetDataWeight());
            i64 compressedDataSizeUntilJobSplit = std::max<i64>(0, GetCompressedDataSizePerJobForBuildingJobs() - CurrentJob()->GetCompressedDataSize());

            if (dataWeightUntilJobSplit <= chunkSlice->GetDataWeight() || compressedDataSizeUntilJobSplit <= chunkSlice->GetCompressedDataSize()) {
                // Finally, we can estimate row count until job split.
                RowCountUntilJobSplit_ = SignedSaturationConversion(std::min(
                    std::ceil(static_cast<double>(dataWeightUntilJobSplit) * chunkSlice->GetRowCount() / chunkSlice->GetDataWeight()),
                    std::ceil(static_cast<double>(compressedDataSizeUntilJobSplit) * chunkSlice->GetRowCount() / chunkSlice->GetCompressedDataSize())));

                RowCountUntilJobSplit_ = std::clamp<i64>(*RowCountUntilJobSplit_, 1, chunkSlice->GetRowCount());
            }
        }

        if (maxRowCountUntilJobSplit > chunkSlice->GetRowCount()) {
            // We can't precisely extrapolate maxRowCount for future slices.
            // Leave it for future iterations.
            maxRowCountUntilJobSplit = std::numeric_limits<i64>::max();
        }

        if (maxRowCountUntilJobSplit > chunkSlice->GetRowCount() && !RowCountUntilJobSplit_.has_value()) {
            // Did not hit any constraints, continue adding new slices.
            return;
        }

        if (!RowCountUntilJobSplit_.has_value()) {
            RowCountUntilJobSplit_ = maxRowCountUntilJobSplit;
        } else if (maxRowCountUntilJobSplit <= chunkSlice->GetRowCount()) {
            RowCountUntilJobSplit_ = std::min(*RowCountUntilJobSplit_, maxRowCountUntilJobSplit);
        }

        i64 batchRowCount = JobSizeConstraints_->GetBatchRowCount().value_or(1);
        i64 batchRowCountRemainder = (CurrentJob()->GetRowCount() + *RowCountUntilJobSplit_) % batchRowCount;
        if (batchRowCountRemainder == 0) {
            return;
        }

        i64 maxEnlargeRowCount = std::max<i64>(0, maxRowCountUntilJobSplit - *RowCountUntilJobSplit_);
        i64 maxShrinkRowCount = CurrentJob()->GetRowCount() == 0 ? 0 : *RowCountUntilJobSplit_;
        if (batchRowCount - batchRowCountRemainder <= maxEnlargeRowCount) {
            *RowCountUntilJobSplit_ += batchRowCount - batchRowCountRemainder;
        } else if (batchRowCountRemainder <= maxShrinkRowCount) {
            *RowCountUntilJobSplit_ -= batchRowCountRemainder;
        } else {
            // Now we give up. Forget about batchRowCount :(
        }
    }

    IChunkPoolOutput::TCookie EndJob()
    {
        if (CurrentJob()->GetSliceCount() > 0) {
            auto jobIndex = JobIndex_++;
            if (Sampler_.Sample()) {
                YT_LOG_DEBUG("Ordered job created (JobIndex: %v, BuiltJobCount: %v, PrimaryDataWeight: %v, RowCount: %v, SliceCount: %v)",
                    jobIndex,
                    BuiltJobCount_,
                    CurrentJob()->GetPrimaryDataWeight(),
                    CurrentJob()->GetPrimaryRowCount(),
                    CurrentJob()->GetPrimarySliceCount());

                GetDataSliceCounter()->AddUncategorized(CurrentJob()->GetSliceCount());

                ++BuiltJobCount_;

                if (GetDataSliceCounter()->GetTotal() > MaxTotalSliceCount_) {
                    THROW_ERROR_EXCEPTION(NChunkPools::EErrorCode::DataSliceLimitExceeded, "Total number of data slices in ordered pool is too large")
                        << TErrorAttribute("actual_total_slice_count", GetDataSliceCounter()->GetTotal())
                        << TErrorAttribute("max_total_slice_count", MaxTotalSliceCount_)
                        << TErrorAttribute("current_job_count", jobIndex);
                }

                CurrentJob()->Finalize();

                auto cookie = JobManager_->AddJob(std::move(CurrentJob()));

                YT_ASSERT(!CurrentJob_);

                return cookie;
            } else {
                YT_LOG_DEBUG("Ordered job skipped (JobIndex: %v, BuiltJobCount: %v, DataWeight: %v, RowCount: %v, SliceCount: %v)",
                    jobIndex,
                    BuiltJobCount_,
                    CurrentJob()->GetPrimaryDataWeight(),
                    CurrentJob()->GetPrimaryRowCount(),
                    CurrentJob()->GetPrimarySliceCount());
                CurrentJob().reset();
            }
        }
        return IChunkPoolOutput::NullCookie;
    }

    std::unique_ptr<TNewJobStub>& CurrentJob()
    {
        if (!CurrentJob_) {
            CurrentJob_ = std::make_unique<TNewJobStub>();
        }
        return CurrentJob_;
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

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TOrderedChunkPool, 0xffe92abc);
};

void TOrderedChunkPool::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkPoolInputBase>();
    registrar.template BaseType<TChunkPoolOutputWithJobManagerBase>();
    registrar.template BaseType<TJobSplittingBase>();
    // TLoggerOwner is persisted by TJobSplittingBase.

    PHOENIX_REGISTER_FIELD(1, InputStreamDirectory_);
    PHOENIX_REGISTER_FIELD(2, MinTeleportChunkSize_);
    PHOENIX_REGISTER_FIELD(3, Stripes_);
    PHOENIX_REGISTER_FIELD(4, JobSizeConstraints_);
    PHOENIX_REGISTER_FIELD(5, Sampler_);
    registrar.template VirtualField<6>("SupportLocality_", [] (TThis* /*this_*/, auto& context) {
        Load<bool>(context);
    })
        .BeforeVersion(ESnapshotVersion::DropSupportLocality)();
    PHOENIX_REGISTER_FIELD(7, MaxTotalSliceCount_);
    PHOENIX_REGISTER_FIELD(8, ShouldSliceByRowIndices_);
    PHOENIX_REGISTER_FIELD(9, EnablePeriodicYielder_);
    PHOENIX_REGISTER_FIELD(11, JobIndex_);
    PHOENIX_REGISTER_FIELD(12, BuiltJobCount_);
    PHOENIX_REGISTER_FIELD(13, SingleJob_);
    PHOENIX_REGISTER_FIELD(14, IsCompleted_);
    PHOENIX_REGISTER_DELETED_FIELD(15, bool, UseNewSlicingImplementation_, ESnapshotVersion::RemoveOldOrderedChunkPoolSlicing);
    PHOENIX_REGISTER_FIELD(16, JobSizeAdjuster_,
        .SinceVersion(ESnapshotVersion::OrderedAndSortedJobSizeAdjuster));

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        ValidateLogger(this_->Logger);
    });
}

PHOENIX_DEFINE_TYPE(TOrderedChunkPool);

////////////////////////////////////////////////////////////////////////////////

IOrderedChunkPoolPtr CreateOrderedChunkPool(
    const TOrderedChunkPoolOptions& options,
    TInputStreamDirectory inputStreamDirectory)
{
    return New<TOrderedChunkPool>(options, std::move(inputStreamDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

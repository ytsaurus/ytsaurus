#include "ordered_chunk_pool.h"

#include "helpers.h"
#include "new_job_manager.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>
#include <yt/yt/ytlib/chunk_client/input_chunk.h>

#include <yt/yt/ytlib/chunk_pools/output_order.h>

#include <yt/yt/library/random/bernoulli_sampler.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/logging/logger_owner.h>

#include <yt/yt/core/phoenix/type_decl.h>
#include <yt/yt/core/phoenix/type_def.h>

#include <library/cpp/yt/memory/ref_tracked.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

namespace NYT::NChunkPools {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NLogging;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

void TOrderedChunkPoolOptions::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, MaxTotalSliceCount);
    PHOENIX_REGISTER_FIELD(2, MinTeleportChunkSize);
    PHOENIX_REGISTER_FIELD(3, JobSizeConstraints);
    PHOENIX_REGISTER_FIELD(4, SupportLocality);
    PHOENIX_REGISTER_FIELD(5, EnablePeriodicYielder);
    PHOENIX_REGISTER_FIELD(6, ShouldSliceByRowIndices);
    PHOENIX_REGISTER_FIELD(7, Logger);
}

PHOENIX_DEFINE_TYPE(TOrderedChunkPoolOptions);

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputWithNewJobManagerBase
    , public IPersistentChunkPool
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
        , SupportLocality_(options.SupportLocality)
        , MaxTotalSliceCount_(options.MaxTotalSliceCount)
        , ShouldSliceByRowIndices_(options.ShouldSliceByRowIndices)
        , EnablePeriodicYielder_(options.EnablePeriodicYielder)
        , OutputOrder_(options.KeepOutputOrder ? New<TOutputOrder>() : nullptr)
    {
        Logger = options.Logger;

        ValidateLogger(Logger);

        if (JobSizeConstraints_->IsExplicitJobCount() && JobSizeConstraints_->GetJobCount() == 1) {
            SingleJob_ = true;
        }

        YT_LOG_DEBUG("Ordered chunk pool created (DataWeightPerJob: %v, MaxDataSlicesPerJob: %v, "
            "InputSliceDataWeight: %v, InputSliceRowCount: %v, BatchRowCount: %v, SingleJob: %v)",
            GetDataWeightPerJob(),
            JobSizeConstraints_->GetMaxDataSlicesPerJob(),
            JobSizeConstraints_->GetInputSliceDataWeight(),
            JobSizeConstraints_->GetInputSliceRowCount(),
            JobSizeConstraints_->GetBatchRowCount(),
            SingleJob_);
    }

    // IPersistentChunkPoolInput implementation.

    IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YT_VERIFY(!Finished);

        if (stripe->DataSlices.empty()) {
            return IChunkPoolInput::NullCookie;
        }

        for (const auto& dataSlice : stripe->DataSlices) {
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
                "Splitting job (OutputCookie: %v, InterruptionReason: %v, SplitJobCount: %v)",
                cookie,
                jobSummary.InterruptionReason,
                jobSummary.SplitJobCount);
            auto childCookies = SplitJob(jobSummary.UnreadInputDataSlices, jobSummary.SplitJobCount, cookie);
            RegisterChildCookies(cookie, std::move(childCookies));
        }
        JobManager_->Completed(cookie, jobSummary.InterruptionReason);
        CheckCompleted();
    }

    void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        TChunkPoolOutputWithJobManagerBase::Lost(cookie);

        CheckCompleted();
    }

    TOutputOrderPtr GetOutputOrder() const override
    {
        return OutputOrder_;
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

    bool SupportLocality_ = false;

    i64 MaxTotalSliceCount_ = 0;

    bool ShouldSliceByRowIndices_ = false;

    bool EnablePeriodicYielder_;

    TOutputOrderPtr OutputOrder_ = nullptr;

    i64 RowCountUntilJobSplit_ = 0;
    i64 JobCarryOverDataWeight_ = 0;
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

    TPeriodicYielder CreatePeriodicYielder()
    {
        if (EnablePeriodicYielder_) {
            return TPeriodicYielder(PrepareYieldPeriod);
        } else {
            return TPeriodicYielder();
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
                *JobSizeConstraints_->GetSamplingRate(),
                JobSizeConstraints_->GetSamplingDataWeightPerJob(),
                JobSizeConstraints_->GetSamplingPrimaryDataWeightPerJob());
        }

        if (JobSizeConstraints_->IsExplicitJobCount() && !SingleJob_ && !JobSizeConstraints_->GetSamplingRate()) {
            i64 totalDataWeight = 0;
            for (int inputCookie = 0; inputCookie < std::ssize(Stripes_); ++inputCookie) {
                const auto& stripe = Stripes_[inputCookie].GetStripe();
                for (const auto& dataSlice : stripe->DataSlices) {
                    totalDataWeight += dataSlice->GetDataWeight();
                }
            }
            JobSizeConstraints_->UpdateInputDataWeight(totalDataWeight);
        }

        int droppedTeleportChunkCount = 0;
        int chunksTeleported = 0;

        auto yielder = CreatePeriodicYielder();
        for (int inputCookie = 0; inputCookie < std::ssize(Stripes_); ++inputCookie) {
            const auto& stripe = Stripes_[inputCookie].GetStripe();
            for (const auto& dataSlice : stripe->DataSlices) {
                yielder.TryYield();
                if (IsTeleportable(dataSlice)) {
                    if (Sampler_.Sample()) {
                        EndJob();

                        // Add barrier.
                        CurrentJob()->SetIsBarrier(true);
                        JobManager_->AddJob(std::move(CurrentJob()));

                        auto inputChunk = dataSlice->GetSingleUnversionedChunk();
                        ChunkTeleported_.Fire(inputChunk, /*tag*/ std::any{});
                        ++chunksTeleported;

                        if (OutputOrder_) {
                            OutputOrder_->Push(TOutputOrder::TEntry(inputChunk));
                        }
                    } else {
                        // This teleport chunk goes to /dev/null.
                        ++droppedTeleportChunkCount;
                    }

                    continue;
                }

                YT_VERIFY(!dataSlice->IsLegacy);
                AddPrimaryDataSlice(dataSlice, inputCookie, GetDataWeightPerJob());
            }
        }
        EndJob();

        YT_LOG_INFO("Jobs created (Count: %v, TeleportChunkCount: %v, DroppedTeleportChunkCount: %v)",
            BuiltJobCount_,
            chunksTeleported,
            droppedTeleportChunkCount);

        if (JobSizeConstraints_->GetSamplingRate()) {
            JobManager_->Enlarge(
                GetDataWeightPerJob(),
                JobSizeConstraints_->GetPrimaryDataWeightPerJob());
        }

        JobSizeConstraints_->UpdateInputDataWeight(JobManager_->DataWeightCounter()->GetTotal());
    }

    std::vector<IChunkPoolOutput::TCookie> SplitJob(
        std::vector<TLegacyDataSlicePtr> unreadInputDataSlices,
        int splitJobCount,
        IChunkPoolOutput::TCookie cookie)
    {
        i64 dataSize = 0;
        for (const auto& dataSlice : unreadInputDataSlices) {
            dataSize += dataSlice->GetDataWeight();
        }
        i64 dataSizePerJob;
        if (splitJobCount == 1) {
            dataSizePerJob = std::numeric_limits<i64>::max() / 4;
        } else {
            dataSizePerJob = DivCeil(dataSize, static_cast<i64>(splitJobCount));
        }

        // Teleport chunks do not affect the job split process since each original
        // job is already located between the teleport chunks.
        std::vector<TInputChunkPtr> teleportChunks;
        if (OutputOrder_) {
            OutputOrder_->SeekCookie(cookie);
        }
        std::vector<IChunkPoolOutput::TCookie> childCookies;
        for (const auto& dataSlice : unreadInputDataSlices) {
            int inputCookie = *dataSlice->Tag;
            auto outputCookie = AddUnsplittablePrimaryDataSlice(dataSlice, inputCookie, dataSizePerJob);
            if (outputCookie != IChunkPoolOutput::NullCookie) {
                childCookies.push_back(outputCookie);
            }
        }

        {
            auto outputCookie = EndJob();
            if (outputCookie != IChunkPoolOutput::NullCookie) {
                childCookies.push_back(outputCookie);
            }
        }

        return childCookies;
    }

    i64 GetDataWeightPerJob() const
    {
        return
            JobSizeConstraints_->GetSamplingRate()
            ? std::max(JobSizeConstraints_->GetDataWeightPerJob(), JobSizeConstraints_->GetSamplingDataWeightPerJob())
            : JobSizeConstraints_->GetDataWeightPerJob();
    }

    i64 GetCurrentJobDataWeight()
    {
        return JobCarryOverDataWeight_ + CurrentJob()->GetDataWeight();
    }

    IChunkPoolOutput::TCookie AddUnsplittablePrimaryDataSlice(
        const TLegacyDataSlicePtr& dataSlice,
        IChunkPoolInput::TCookie cookie,
        i64 dataSizePerJob)
    {
        YT_VERIFY(!dataSlice->IsLegacy);

        RowCountUntilJobSplit_ = 0;
        JobCarryOverDataWeight_ = 0;

        auto dataSliceCopy = CreateInputDataSlice(dataSlice);
        dataSliceCopy->Tag = cookie;
        CurrentJob()->AddDataSlice(dataSliceCopy, cookie, /*isPrimary*/ true);

        bool jobIsLargeEnough =
            CurrentJob()->GetPreliminarySliceCount() >= JobSizeConstraints_->GetMaxDataSlicesPerJob() ||
            GetCurrentJobDataWeight() >= dataSizePerJob;
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

    void AddPrimaryDataSlice(
        const TLegacyDataSlicePtr& dataSlice,
        IChunkPoolInput::TCookie cookie,
        i64 dataSizePerJob)
    {
        if (IsDataSliceSplittable(dataSlice)) {
            AddSplittablePrimaryDataSlice(dataSlice, cookie, dataSizePerJob);
        } else {
            AddUnsplittablePrimaryDataSlice(dataSlice, cookie, dataSizePerJob);
        }
    }

    // Adding slices via this method essentially circles between multiple stages.
    // First, we add slices while none of the limits (data weight / slice count) are violated.
    // The job slice limit is considered hard: whenever it is hit, we just end the current job.
    // Once we reach a slice adding which would violate the data weight limit, we break into multiple cases:
    //   - First, we compute the ideal split index by data weight.
    //   - Then, if batch row count is not set, we try to split the job by the index above.
    //   - Otherwise, we compute the next split that would make the number of rows in the job divisible by batch row count and store it for the next iteration.
    // Next iterations operate on a data weight "discount", decrementing the stored row count until next split.
    void AddSplittablePrimaryDataSlice(
        const TLegacyDataSlicePtr& dataSlice,
        IChunkPoolInput::TCookie cookie,
        i64 dataSizePerJob)
    {
        YT_VERIFY(IsDataSliceSplittable(dataSlice));

        auto chunkSlices = dataSlice->GetSingleUnversionedChunkSlice()
            ->SliceEvenly(JobSizeConstraints_->GetInputSliceDataWeight(), JobSizeConstraints_->GetInputSliceRowCount());

        auto batchRowCount = JobSizeConstraints_->GetBatchRowCount();

        TInputChunkSlicePtr pocket;
        int chunkSliceIndex = 0;
        while (chunkSliceIndex < std::ssize(chunkSlices) || pocket) {
            // The pocket holds the slice that should be handled before handling the next chunk slice index.
            // It is used to store the suffix of the current slice when we only add a prefix of it to the current job.
            auto chunkSlice = pocket ? pocket : chunkSlices[chunkSliceIndex];
            pocket = nullptr;

            // If the pocket is empty, we can handle the next slice in our input order.
            // Note the continuation statements below that will lead to the execution of this code.
            auto nextIteration = Finally([&] () {
                if (!pocket) {
                    ++chunkSliceIndex;
                }
            });

            if (!chunkSlice->GetRowCount()) {
                continue;
            }

            // NB: Hitting this limit means we cannot take more than one slice.
            // In this case the final row count of this job might not be divisible by batch row count.
            // NB: We need >= instead of == to handle the case of an explicit single job.
            if (CurrentJob()->GetPreliminarySliceCount() + 1 >= JobSizeConstraints_->GetMaxDataSlicesPerJob()) {
                RowCountUntilJobSplit_ = chunkSlice->GetRowCount();
                // This will lead to the carry-over value being zero after adding `chunkSliceToAdd->GetDataWeight()`.
                JobCarryOverDataWeight_ = -chunkSlice->GetDataWeight();
            }

            if (RowCountUntilJobSplit_ == 0 && GetCurrentJobDataWeight() + chunkSlice->GetDataWeight() >= dataSizePerJob) {
                // Taking this maximum is needed if jobs of batch row count rows are significantly larger than data size per job.
                // In these cases we simply try to end the jobs as soon as we can hit an acceptable split.
                auto dataWeightUntilSplit = std::max<i64>(dataSizePerJob - GetCurrentJobDataWeight(), 0);

                RowCountUntilJobSplit_ = DivCeil(dataWeightUntilSplit * chunkSlice->GetRowCount(), chunkSlice->GetDataWeight());
                YT_VERIFY(RowCountUntilJobSplit_ <= chunkSlice->GetRowCount());

                // We only carry-over from one previous job.
                // NB: We use splitting here in order to get the exact same data weight we would get later on in the process.
                JobCarryOverDataWeight_ = RowCountUntilJobSplit_ ? -chunkSlice->SplitByRowIndex(RowCountUntilJobSplit_).first->GetDataWeight() : 0;

                if (batchRowCount) {
                    YT_VERIFY(*batchRowCount > 0);
                    // Zero rows until split force us to look for the next acceptable split (even when batchRowCountRemainder = 0) since it usually means that a job just ended.
                    if (auto batchRowCountRemainder = (CurrentJob()->GetRowCount() + RowCountUntilJobSplit_) % *batchRowCount; !RowCountUntilJobSplit_ || batchRowCountRemainder) {
                        RowCountUntilJobSplit_ += *batchRowCount - batchRowCountRemainder;
                    }
                } else if (RowCountUntilJobSplit_ == 0) {
                    // We should not actually end up here, since both slice addition implementations finish jobs
                    // as soon as the limit is hit when batch row count is not set.
                    // However, if we do, let's reset the carry-over data weight, end the job and reprocess the current slice.
                    YT_LOG_WARNING(
                        "Creating ordered job prematurely (CurrentJobDataWeight: %v, JobCarryOverDataWeight: %v, ActiveSliceDataWeight: %v, ActiveSliceRowCount: %v)",
                        GetCurrentJobDataWeight(),
                        JobCarryOverDataWeight_,
                        chunkSlice->GetDataWeight(),
                        chunkSlice->GetRowCount());
                    JobCarryOverDataWeight_ = 0;
                    EndJob();
                    // Current job data weight should be zero now, so we can handle the whole slice again.
                    pocket = chunkSlice;
                    continue;
                }
            }

            auto chunkSliceToAdd = chunkSlice;

            bool endJob = RowCountUntilJobSplit_ > 0 && RowCountUntilJobSplit_ <= chunkSlice->GetRowCount();
            if (endJob && RowCountUntilJobSplit_ < chunkSlice->GetRowCount()) {
                std::tie(chunkSliceToAdd, pocket) = chunkSlice->SplitByRowIndex(RowCountUntilJobSplit_);
            }

            if (RowCountUntilJobSplit_ > 0) {
                RowCountUntilJobSplit_ -= chunkSliceToAdd->GetRowCount();
                JobCarryOverDataWeight_ += chunkSliceToAdd->GetDataWeight();
            }

            auto dataSliceToAdd = CreateUnversionedInputDataSlice(chunkSliceToAdd);
            dataSliceToAdd->CopyPayloadFrom(*dataSlice);
            dataSliceToAdd->Tag = cookie;

            CurrentJob()->AddDataSlice(dataSliceToAdd, cookie, /*isPrimary*/ true);
            if (endJob && !SingleJob_) {
                YT_VERIFY(RowCountUntilJobSplit_ == 0);
                EndJob();
            }
        }
    }

    IChunkPoolOutput::TCookie EndJob()
    {
        if (CurrentJob()->GetSliceCount() > 0) {
            if (Sampler_.Sample()) {
                YT_LOG_DEBUG("Ordered job created (JobIndex: %v, BuiltJobCount: %v, PrimaryDataWeight: %v, RowCount: %v, SliceCount: %v)",
                    JobIndex_,
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
                        << TErrorAttribute("current_job_count", JobIndex_);
                }

                CurrentJob()->Finalize();

                auto cookie = JobManager_->AddJob(std::move(CurrentJob()));
                if (OutputOrder_) {
                    OutputOrder_->Push(cookie);
                }

                YT_ASSERT(!CurrentJob_);

                return cookie;
            } else {
                YT_LOG_DEBUG("Ordered job skipped (JobIndex: %v, BuiltJobCount: %v, DataWeight: %v, RowCount: %v, SliceCount: %v)",
                    JobIndex_,
                    BuiltJobCount_,
                    CurrentJob()->GetPrimaryDataWeight(),
                    CurrentJob()->GetPrimaryRowCount(),
                    CurrentJob()->GetPrimarySliceCount());
                CurrentJob().reset();
            }
            ++JobIndex_;
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
    PHOENIX_REGISTER_FIELD(6, SupportLocality_);
    PHOENIX_REGISTER_FIELD(7, MaxTotalSliceCount_);
    PHOENIX_REGISTER_FIELD(8, ShouldSliceByRowIndices_);
    PHOENIX_REGISTER_FIELD(9, EnablePeriodicYielder_);
    PHOENIX_REGISTER_FIELD(10, OutputOrder_);
    PHOENIX_REGISTER_FIELD(11, JobIndex_);
    PHOENIX_REGISTER_FIELD(12, BuiltJobCount_);
    PHOENIX_REGISTER_FIELD(13, SingleJob_);
    PHOENIX_REGISTER_FIELD(14, IsCompleted_);

    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        ValidateLogger(this_->Logger);
    });
}

PHOENIX_DEFINE_TYPE(TOrderedChunkPool);

////////////////////////////////////////////////////////////////////////////////

IPersistentChunkPoolPtr CreateOrderedChunkPool(
    const TOrderedChunkPoolOptions& options,
    TInputStreamDirectory inputStreamDirectory)
{
    return New<TOrderedChunkPool>(options, std::move(inputStreamDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

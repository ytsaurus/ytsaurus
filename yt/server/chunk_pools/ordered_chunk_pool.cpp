#include "ordered_chunk_pool.h"

#include "helpers.h"
#include "job_manager.h"
#include "output_order.h"

#include <yt/server/controller_agent/helpers.h>

#include <yt/core/concurrency/periodic_yielder.h>

#include <yt/core/misc/numeric_helpers.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYT {
namespace NChunkPools {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NLogging;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

void TOrderedChunkPoolOptions::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, MaxTotalSliceCount);
    Persist(context, MinTeleportChunkSize);
    Persist(context, JobSizeConstraints);
    Persist(context, SupportLocality);
    Persist(context, OperationId);
    Persist(context, EnablePeriodicYielder);
    Persist(context, ExtractionOrder);
    Persist(context, ShouldSliceByRowIndices);
}

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkPool
    : public TChunkPoolInputBase
    // We delegate dealing with progress counters to the TJobManager class,
    // so we can't inherit from TChunkPoolOutputBase since it binds all the
    // interface methods to the progress counters stored as pool fields.
    , public IChunkPool
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
    , public TRefTracked<TOrderedChunkPool>
{
public:
    DEFINE_SIGNAL(void(const TError& error), PoolOutputInvalidated)

public:
    //! Used only for persistence.
    TOrderedChunkPool()
    { }

    TOrderedChunkPool(
        const TOrderedChunkPoolOptions& options,
        TInputStreamDirectory inputStreamDirectory)
        : JobManager_(New<TJobManager>(options.ExtractionOrder))
        , InputStreamDirectory_(std::move(inputStreamDirectory))
        , MinTeleportChunkSize_(options.MinTeleportChunkSize)
        , JobSizeConstraints_(options.JobSizeConstraints)
        , SupportLocality_(options.SupportLocality)
        , OperationId_(options.OperationId)
        , MaxTotalSliceCount_(options.MaxTotalSliceCount)
        , ShouldSliceByRowIndices_(options.ShouldSliceByRowIndices)
        , EnablePeriodicYielder_(options.EnablePeriodicYielder)
        , OutputOrder_(options.KeepOutputOrder ? New<TOutputOrder>() : nullptr)
    {
        Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
        Logger.AddTag("OperationId: %v", OperationId_);
        JobManager_->SetLogger(Logger);
    }

    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);

        if (stripe->DataSlices.empty()) {
            return IChunkPoolInput::NullCookie;
        }

        auto cookie = static_cast<int>(Stripes_.size());
        Stripes_.emplace_back(stripe);

        return cookie;
    }

    virtual void Finish() override
    {
        YCHECK(!Finished);
        TChunkPoolInputBase::Finish();

        // NB: this method accounts all the stripes that were suspended before
        // the chunk pool was finished. It should be called only once.
        SetupSuspendedStripes();

        InitInputChunkMapping();

        BuildJobsAndFindTeleportChunks();
    }

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        auto& suspendableStripe = Stripes_[cookie];
        suspendableStripe.Suspend();
        if (Finished) {
            JobManager_->Suspend(cookie);
        }
    }

    virtual void Resume(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe) override
    {
        auto& suspendableStripe = Stripes_[cookie];
        if (!Finished) {
            suspendableStripe.Resume(stripe);
        } else {
            JobManager_->Resume(cookie);
            THashMap<TInputChunkPtr, TInputChunkPtr> newChunkMapping;
            try {
                newChunkMapping = suspendableStripe.ResumeAndBuildChunkMapping(stripe);
            } catch (std::exception& ex) {
                suspendableStripe.Resume(stripe);
                auto error = TError("Chunk stripe resumption failed")
                    << ex
                    << TErrorAttribute("input_cookie", cookie);
                LOG_ERROR(error);
                YCHECK(false && "Caught an error during resumption");
            }
            for (const auto& pair : newChunkMapping) {
                InputChunkMapping_[pair.first] = pair.second;
            }
        }
    }

    // IChunkPoolOutput implementation.

    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
    {
        return JobManager_->GetApproximateStripeStatistics();
    }

    virtual bool IsCompleted() const override
    {
        return
            Finished &&
            GetPendingJobCount() == 0 &&
            JobManager_->JobCounter()->GetRunning() == 0 &&
            JobManager_->GetSuspendedJobCount() == 0;
    }

    virtual int GetTotalJobCount() const override
    {
        return JobManager_->JobCounter()->GetTotal();
    }

    virtual int GetPendingJobCount() const override
    {
        return CanScheduleJob() ? JobManager_->GetPendingJobCount() : 0;
    }

    virtual i64 GetLocality(TNodeId /* nodeId */) const override
    {
        if (SupportLocality_) {
            // TODO(max42): YT-6551
            Y_UNREACHABLE();
        }
        return 0;
    }

    virtual IChunkPoolOutput::TCookie Extract(TNodeId /* nodeId */) override
    {
        YCHECK(Finished);

        return JobManager_->ExtractCookie();
    }

    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        return ApplyChunkMappingToStripe(JobManager_->GetStripeList(cookie), InputChunkMapping_);
    }

    virtual int GetStripeListSliceCount(IChunkPoolOutput::TCookie cookie) const override
    {
        auto stripeList = JobManager_->GetStripeList(cookie);
        return stripeList->TotalChunkCount;
    }

    virtual void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        if (jobSummary.InterruptReason != EInterruptReason::None) {
            LOG_DEBUG("Splitting job (OutputCookie: %v, InterruptReason: %v, SplitJobCount: %v)",
                cookie,
                jobSummary.InterruptReason,
                jobSummary.SplitJobCount);
            JobManager_->Invalidate(cookie);
            SplitJob(std::move(jobSummary.UnreadInputDataSlices), jobSummary.SplitJobCount, cookie);
        }
        JobManager_->Completed(cookie, jobSummary.InterruptReason);
    }

    virtual void Failed(IChunkPoolOutput::TCookie cookie) override
    {
        JobManager_->Failed(cookie);
    }

    virtual void Aborted(IChunkPoolOutput::TCookie cookie, EAbortReason reason) override
    {
        JobManager_->Aborted(cookie, reason);
    }

    virtual void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        JobManager_->Lost(cookie);
    }

    virtual i64 GetTotalDataWeight() const override
    {
        return JobManager_->DataWeightCounter()->GetTotal();
    }

    virtual i64 GetRunningDataWeight() const override
    {
        return JobManager_->DataWeightCounter()->GetRunning();
    }

    virtual i64 GetCompletedDataWeight() const override
    {
        return JobManager_->DataWeightCounter()->GetCompletedTotal();
    }

    virtual i64 GetPendingDataWeight() const override
    {
        return JobManager_->DataWeightCounter()->GetPending();
    }

    virtual i64 GetTotalRowCount() const override
    {
        return JobManager_->RowCounter()->GetTotal();
    }

    const TProgressCounterPtr& GetJobCounter() const
    {
        return JobManager_->JobCounter();
    }

    const std::vector<TInputChunkPtr>& GetTeleportChunks() const
    {
        return TeleportChunks_;
    }

    virtual TOutputOrderPtr GetOutputOrder() const override
    {
        return OutputOrder_;
    }

    virtual i64 GetDataSliceCount() const override
    {
        return TotalSliceCount_;
    }

    virtual void Persist(const TPersistenceContext& context) final override
    {
        TChunkPoolInputBase::Persist(context);

        using NYT::Persist;

        Persist(context, JobManager_);
        Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, InputChunkMapping_);
        Persist(context, InputStreamDirectory_);
        Persist(context, MinTeleportChunkSize_);
        Persist(context, Stripes_);
        Persist(context, TeleportChunks_);
        Persist(context, JobSizeConstraints_);
        Persist(context, SupportLocality_);
        Persist(context, OperationId_);
        Persist(context, ChunkPoolId_);
        Persist(context, MaxTotalSliceCount_);
        Persist(context, ShouldSliceByRowIndices_);
        Persist(context, EnablePeriodicYielder_);
        Persist(context, OutputOrder_);
        Persist(context, JobIndex_);
        Persist(context, TotalSliceCount_);
        if (context.IsLoad()) {
            Logger.AddTag("ChunkPoolId: %v", ChunkPoolId_);
            Logger.AddTag("OperationId: %v", OperationId_);
            JobManager_->SetLogger(Logger);
        }
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TOrderedChunkPool, 0xffe92abc);

    //! A data structure responsible for keeping the prepared jobs, extracting them and dealing with suspend/resume
    //! events.
    TJobManagerPtr JobManager_;

    //! During the pool lifetime some input chunks may be suspended and replaced with
    //! another chunks on resumption. We keep track of all such substitutions in this
    //! map and apply it whenever the `GetStripeList` is called.
    THashMap<TInputChunkPtr, TInputChunkPtr> InputChunkMapping_;

    //! Information about input sources (e.g. input tables for sorted reduce operation).
    TInputStreamDirectory InputStreamDirectory_;

    //! An option to control chunk teleportation logic. Only large complete
    //! chunks of at least that size will be teleported.
    i64 MinTeleportChunkSize_;

    //! All stripes that were added to this pool.
    std::vector<TSuspendableStripe> Stripes_;

    //! Stores all input chunks to be teleported.
    std::vector<NChunkClient::TInputChunkPtr> TeleportChunks_;

    IJobSizeConstraintsPtr JobSizeConstraints_;

    bool SupportLocality_ = false;

    TLogger Logger = ChunkPoolLogger;

    TOperationId OperationId_;

    TGuid ChunkPoolId_ = TGuid::Create();

    i64 MaxTotalSliceCount_ = 0;

    bool ShouldSliceByRowIndices_ = false;

    bool EnablePeriodicYielder_;

    TOutputOrderPtr OutputOrder_ = nullptr;

    std::unique_ptr<TJobStub> CurrentJob_;

    int JobIndex_ = 0;

    i64 TotalSliceCount_ = 0;

    void InitInputChunkMapping()
    {
        for (const auto& suspendableStripe : Stripes_) {
            for (const auto& dataSlice : suspendableStripe.GetStripe()->DataSlices) {
                for (const auto& chunkSlice : dataSlice->ChunkSlices) {
                    InputChunkMapping_[chunkSlice->GetInputChunk()] = chunkSlice->GetInputChunk();
                }
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
        return Finished && JobManager_->GetPendingJobCount() != 0;
    }

    TPeriodicYielder CreatePeriodicYielder()
    {
        if (EnablePeriodicYielder_) {
            return TPeriodicYielder(PrepareYieldPeriod);
        } else {
            return TPeriodicYielder();
        }
    }

    void BuildJobsAndFindTeleportChunks()
    {
        auto yielder = CreatePeriodicYielder();
        for (int inputCookie = 0; inputCookie < Stripes_.size(); ++inputCookie) {
            const auto& stripe = Stripes_[inputCookie].GetStripe();
            for (const auto& dataSlice : stripe->DataSlices) {
                yielder.TryYield();
                if (dataSlice->Type == EDataSourceType::UnversionedTable) {
                    auto inputChunk = dataSlice->GetSingleUnversionedChunkOrThrow();
                    if (InputStreamDirectory_.GetDescriptor(stripe->GetInputStreamIndex()).IsTeleportable() &&
                        inputChunk->IsLargeCompleteChunk(MinTeleportChunkSize_))
                    {
                        EndJob();
                        TeleportChunks_.emplace_back(inputChunk);
                        if (OutputOrder_) {
                            OutputOrder_->Push(TOutputOrder::TEntry(inputChunk));
                        }
                        continue;
                    }
                }

                std::vector<TInputDataSlicePtr> slicedDataSlices;
                if (dataSlice->Type == EDataSourceType::UnversionedTable && ShouldSliceByRowIndices_) {
                    auto chunkSlices = CreateInputChunkSlice(dataSlice->GetSingleUnversionedChunkOrThrow())
                        ->SliceEvenly(JobSizeConstraints_->GetInputSliceDataWeight(), JobSizeConstraints_->GetInputSliceRowCount());
                    for (const auto& chunkSlice : chunkSlices) {
                        auto dataSlice = CreateUnversionedInputDataSlice(chunkSlice);
                        dataSlice->InputStreamIndex = dataSlice->InputStreamIndex;
                        AddPrimaryDataSlice(dataSlice, inputCookie, JobSizeConstraints_->GetDataWeightPerJob());
                    }
                } else {
                    AddPrimaryDataSlice(dataSlice, inputCookie, JobSizeConstraints_->GetDataWeightPerJob());
                }
            }
        }
        EndJob();
    }

    void SplitJob(
        std::vector<TInputDataSlicePtr> unreadInputDataSlices,
        int splitJobCount,
        IChunkPoolOutput::TCookie cookie)
    {
        i64 dataSize = 0;
        for (const auto& dataSlice : unreadInputDataSlices) {
            dataSize += dataSlice->GetDataWeight();
        }
        i64 dataSizePerJob;
        if (splitJobCount == 1) {
            dataSizePerJob = std::numeric_limits<i64>::max();
        } else {
            dataSizePerJob = DivCeil(dataSize, static_cast<i64>(splitJobCount));
        }

        // Teleport chunks do not affect the job split process since each original
        // job is already located between the teleport chunks.
        std::vector<TInputChunkPtr> teleportChunks;
        if (OutputOrder_) {
            OutputOrder_->SeekCookie(cookie);
        }
        for (const auto& dataSlice : unreadInputDataSlices) {
            int inputCookie = *dataSlice->Tag;
            AddPrimaryDataSlice(dataSlice, inputCookie, dataSizePerJob);
        }
        EndJob();
    }

    void AddPrimaryDataSlice(
        const TInputDataSlicePtr& dataSlice,
        IChunkPoolInput::TCookie cookie,
        i64 dataSizePerJob)
    {
        bool jobIsLargeEnough =
            CurrentJob()->GetPreliminarySliceCount() + 1 > JobSizeConstraints_->GetMaxDataSlicesPerJob() ||
                CurrentJob()->GetDataWeight() >= dataSizePerJob;
        if (jobIsLargeEnough) {
            EndJob();
        }
        auto dataSliceCopy = CreateInputDataSlice(dataSlice);
        dataSliceCopy->InputStreamIndex = 0;
        dataSliceCopy->Tag = cookie;
        CurrentJob()->AddDataSlice(dataSliceCopy, cookie, true /* isPrimary */);
    }

    void EndJob()
    {
        if (CurrentJob()->GetSliceCount() > 0) {
            LOG_DEBUG("Ordered job created (Index: %v, DataWeight: %v, RowCount: %v, SliceCount: %v)",
                JobIndex_,
                CurrentJob()->GetPrimaryDataWeight(),
                CurrentJob()->GetPrimaryRowCount(),
                CurrentJob()->GetPrimarySliceCount());

            TotalSliceCount_ += CurrentJob()->GetSliceCount();

            if (TotalSliceCount_ > MaxTotalSliceCount_) {
                THROW_ERROR_EXCEPTION("Total number of data slices in ordered pool is too large")
                    << TErrorAttribute("actual_total_slice_count", TotalSliceCount_)
                    << TErrorAttribute("max_total_slice_count", MaxTotalSliceCount_)
                    << TErrorAttribute("current_job_count", JobIndex_);
            }

            ++JobIndex_;

            CurrentJob()->Finalize(false /* sortByPosition */);

            auto cookie = JobManager_->AddJob(std::move(CurrentJob()));
            if (OutputOrder_) {
                OutputOrder_->Push(cookie);
            }

            Y_ASSERT(!CurrentJob_);
        }
    }

    std::unique_ptr<TJobStub>& CurrentJob()
    {
        if (!CurrentJob_) {
            CurrentJob_ = std::make_unique<TJobStub>();
        }
        return CurrentJob_;
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_DYNAMIC_PHOENIX_TYPE(TOrderedChunkPool);

std::unique_ptr<IChunkPool> CreateOrderedChunkPool(
    const TOrderedChunkPoolOptions& options,
    TInputStreamDirectory inputStreamDirectory)
{
    return std::make_unique<TOrderedChunkPool>(options, std::move(inputStreamDirectory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT

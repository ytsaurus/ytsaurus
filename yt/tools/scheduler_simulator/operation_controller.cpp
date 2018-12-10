#include "operation_controller.h"
#include "operation.h"

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/server/controller_agent/scheduling_context.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

using namespace NScheduler;
using namespace NControllerAgent;
using namespace NConcurrency;

using NJobTrackerClient::EJobType;

////////////////////////////////////////////////////////////////////////////////

class TJobBucket;

class IJobBucketHost
{
public:
    virtual ~IJobBucketHost() = default;

    virtual void OnBucketActivated(TJobBucket* activatedBucket) = 0;

    virtual void OnBucketCompleted(TJobBucket* deactivatedBucket) = 0;
};

// This class is not thread-safe. It is caller's responsibility to access it sequentially.
class TJobBucket
{
public:
    const EJobType JobType;

    TJobBucket(EJobType jobType, const std::vector<TJobDescription>& jobs, IJobBucketHost* host)
        : JobType(jobType)
        , PendingJobs_(jobs.begin(), jobs.end())
        , Host_(host)
        , BucketJobCount_(jobs.size())
    { }

    void OnJobCompleted()
    {
        YCHECK(InitializationFinished_);

        BucketCompletedJobCount_ += 1;
        if (BucketCompletedJobCount_ == BucketJobCount_) {
            for (auto dependentBucket : DependentBuckets_) {
                dependentBucket->OnResolvedDependency();
            }
            Host_->OnBucketCompleted(this);
        }
    }

    void OnNonscheduledJobAborted(const TJobDescription& abortedJobDescription)
    {
        YCHECK(InitializationFinished_);
        PendingJobs_.push_back(abortedJobDescription);
    }

    void FinishInitialization()
    {
        YCHECK(!InitializationFinished_);

        InitializationFinished_ = true;

        if (ActiveDependenciesCount_ == 0) {
            Host_->OnBucketActivated(this);
        }
    }

    int GetPendingJobCount()
    {
        YCHECK(InitializationFinished_);
        return PendingJobs_.size();
    }

    bool FindJobToSchedule(
        const TJobResourcesWithQuota& nodeLimits,
        TJobDescription* jobToScheduleOutput,
        EScheduleJobFailReason* failReasonOutput)
    {
        YCHECK(InitializationFinished_);

        if (PendingJobs_.empty()) {
            *failReasonOutput = EScheduleJobFailReason::NoPendingJobs;
            return false;
        }
        auto jobDescription = PendingJobs_.front();

        // TODO(ignat, antonkikh): support disk quota in scheduler simulator (YT-9009)
        if (!Dominates(nodeLimits.ToJobResources(), jobDescription.ResourceLimits)) {
            *failReasonOutput = EScheduleJobFailReason::NotEnoughResources;
            return false;
        }

        PendingJobs_.pop_front();

        *jobToScheduleOutput = jobDescription;
        return true;
    }

    // Note that this method is quite slow. Optimize it if you want to call it frequently.
    TJobResources GetNeededResources()
    {
        YCHECK(InitializationFinished_);

        TJobResources neededResources = ZeroJobResources();
        for (const auto& job : PendingJobs_) {
            neededResources += job.ResourceLimits;
        }

        return neededResources;
    }

    static void AddDependency(TJobBucket* from, TJobBucket* to)
    {
        from->AddDependentBucket(to);
        to->IncreaseActiveDependenciesCount();
    }

private:
    std::deque<TJobDescription> PendingJobs_;
    IJobBucketHost* Host_;

    int ActiveDependenciesCount_ = 0;
    int BucketCompletedJobCount_ = 0;
    const int BucketJobCount_;

    std::vector<TJobBucket*> DependentBuckets_;

    bool InitializationFinished_ = false;

    void AddDependentBucket(TJobBucket* other)
    {
        YCHECK(!InitializationFinished_);
        DependentBuckets_.push_back(other);
    }

    void IncreaseActiveDependenciesCount()
    {
        YCHECK(!InitializationFinished_);
        ActiveDependenciesCount_ += 1;
    }

    void OnResolvedDependency()
    {
        YCHECK(InitializationFinished_);
        ActiveDependenciesCount_ -= 1;
        if (ActiveDependenciesCount_ == 0) {
            Host_->OnBucketActivated(this);
        }
    }
};

class TSimulatorOperationController
    : public ISimulatorOperationController
    , public IJobBucketHost
{
public:
    TSimulatorOperationController(const TOperation* operation, const TOperationDescription* operationDescription);

    // Lock_ must be acquired.
    virtual void OnBucketActivated(TJobBucket* activatedBucket) override;

    // Lock_ must be acquired.
    virtual void OnBucketCompleted(TJobBucket* deactivatedBucket) override;

    //! Returns the number of jobs the controller still needs to start right away.
    virtual int GetPendingJobCount() const override;

    //! Returns the total resources that are additionally needed.
    virtual TJobResources GetNeededResources() const override;

    virtual void OnJobCompleted(std::unique_ptr<TCompletedJobSummary> jobSummary) override;

    virtual void OnNonscheduledJobAborted(const TJobId&, EAbortReason) override;

    virtual bool IsOperationCompleted() const override ;

    //! Called during heartbeat processing to request actions the node must perform.
    virtual TFuture<TScheduleJobResultPtr> ScheduleJob(
        const ISchedulingContextPtr& context,
        const TJobResourcesWithQuota& nodeLimits,
        const TString& /* treeId */) override;

    virtual void UpdateMinNeededJobResources() override;
    virtual TJobResourcesWithQuotaList GetMinNeededJobResources() const override;

    virtual TString GetLoggingProgress() const override;

private:
    using TJobBuckets = THashMap<EJobType, std::unique_ptr<TJobBucket>>;

    const TOperation* Operation_;
    const TOperationDescription* OperationDescription_;
    const TJobBuckets JobBuckets_;

    TLockProtectedMap<TJobId, TJobDescription> IdToDescription_;
    NLogging::TLogger Logger;

    TSpinLock Lock_;
    // Protected by Lock_.
    int PendingJobCount_ = 0;
    int CompletedJobCount_ = 0;
    int AbortedJobCount_ = 0;
    int RunningJobCount_ = 0;
    TJobResources NeededResources_ = ZeroJobResources();
    SmallVector<TJobBucket*, TEnumTraits<EJobType>::GetDomainSize()> ActiveBuckets_;
    ///////////////////////

    // Lock_ must be acquired.
    bool FindJobToSchedule(
        const TJobResourcesWithQuota& nodeLimits,
        TJobDescription* jobToScheduleOutput,
        EScheduleJobFailReason* failReasonOutput);

    TJobBuckets InitializeJobBuckets(const TOperationDescription* operationDescription);
};

DEFINE_REFCOUNTED_TYPE(TSimulatorOperationController)

ISimulatorOperationControllerPtr CreateSimulatorOperationController(
    const TOperation* operation,
    const TOperationDescription* operationDescription)
{
    return New<TSimulatorOperationController>(operation, operationDescription);
}

const static THashMap<EJobType, std::vector<EJobType>> dependencyTable = [] {
    using JT = EJobType;
    THashMap<EJobType, std::vector<EJobType>> result = {
        { JT::Partition, { JT::PartitionReduce, JT::FinalSort, JT::SortedReduce, JT::SortedMerge, JT::UnorderedMerge, JT::IntermediateSort, JT::ReduceCombiner } },
        { JT::PartitionMap, { JT::PartitionReduce, JT::FinalSort, JT::IntermediateSort, JT::SortedReduce, JT::ReduceCombiner, JT::UnorderedMerge } },
        { JT::IntermediateSort, { JT::SortedMerge, JT::SortedReduce } },
        { JT::FinalSort, { JT::SortedMerge, JT::SortedReduce } },
        { JT::Map, { JT::UnorderedMerge } },
        { JT::ReduceCombiner, { JT::SortedReduce } },
        { JT::SimpleSort, { JT::SortedMerge } },
        { JT::SortedMerge, { /*Nothing here.*/ } },
        { JT::UnorderedMerge, { /*Nothing here.*/ } },
        { JT::PartitionReduce, { /*Nothing here.*/ } },
        { JT::OrderedMap, { /*Nothing here.*/ } },
        { JT::OrderedMerge, { /*Nothing here.*/ } },
        { JT::JoinReduce, { /*Nothing here.*/ } },
        { JT::RemoteCopy, { /*Nothing here.*/ } },
        { JT::Vanilla, { /*Nothing here.*/ } },
        { JT::SortedReduce, { /*Nothing here.*/ } }};

    return result;
}();

////////////////////////////////////////////////////////////////////////////////

auto TSimulatorOperationController::InitializeJobBuckets(const TOperationDescription* operationDescription) -> TJobBuckets
{
    THashMap<EJobType, std::vector<TJobDescription>> jobsByType;
    for (const auto& jobDescription : operationDescription->JobDescriptions) {
        jobsByType[jobDescription.Type].push_back(jobDescription);
    }

    TJobBuckets jobBuckets;

    for (const auto& pair : jobsByType) {
        auto jobType = pair.first;
        const auto& jobs = pair.second;

        jobBuckets[jobType] = std::make_unique<TJobBucket>(
            jobType,
            jobs,
            /* host */ this);
    }

    for (auto& pair : jobBuckets) {
        auto jobType = pair.first;
        auto& bucket = pair.second;

        auto dependencyTableIt = dependencyTable.find(jobType);
        if (dependencyTableIt == dependencyTable.end()) {
            THROW_ERROR_EXCEPTION("Unknown job type: %v" , jobType);
        }

        for (auto dependentType : dependencyTableIt->second) {
            auto dependentBucketIt = jobBuckets.find(dependentType);
            if (dependentBucketIt != jobBuckets.end()) {
                auto& dependentBucket = dependentBucketIt->second;
                TJobBucket::AddDependency(bucket.get(), dependentBucket.get());
            }
        }
    }

    return jobBuckets;
}

TSimulatorOperationController::TSimulatorOperationController(
    const TOperation* operation,
    const TOperationDescription* operationDescription)
    : Operation_(operation)
    , OperationDescription_(operationDescription)
    , JobBuckets_(InitializeJobBuckets(operationDescription))
    , Logger("OperationController")
{
    for (auto& pair : JobBuckets_) {
        auto& bucket = pair.second;
        bucket->FinishInitialization();
    }
}

// Lock_ must be acquired.
void TSimulatorOperationController::OnBucketActivated(TJobBucket* activatedBucket)
{
    ActiveBuckets_.push_back(activatedBucket);
    PendingJobCount_ += activatedBucket->GetPendingJobCount();
    NeededResources_ += activatedBucket->GetNeededResources();
}

// Lock_ must be acquired.
void TSimulatorOperationController::OnBucketCompleted(TJobBucket* deactivatedBucket)
{
    for (int i = 0; i < ActiveBuckets_.size(); ++i) {
        if (ActiveBuckets_[i] == deactivatedBucket) {
            std::swap(ActiveBuckets_[i], ActiveBuckets_.back());
            ActiveBuckets_.pop_back();
            return;
        }
    }
    YCHECK(false);
}

int TSimulatorOperationController::GetPendingJobCount() const
{
    auto guard = Guard(Lock_);
    return PendingJobCount_;
}

TJobResources TSimulatorOperationController::GetNeededResources() const
{
    auto guard = Guard(Lock_);
    return NeededResources_;
}

void TSimulatorOperationController::OnJobCompleted(std::unique_ptr<TCompletedJobSummary> jobSummary)
{
    const auto& jobDescription = IdToDescription_.Get(jobSummary->Id);

    // TODO: try to avoid this boilerplate code (map::at throws very uninformative exceptions)
    auto jobBucketIt = JobBuckets_.find(jobDescription.Type);
    YCHECK(jobBucketIt != JobBuckets_.end());
    auto& jobBucket = jobBucketIt->second;

    {
        auto guard = Guard(Lock_);
        CompletedJobCount_ += 1;
        RunningJobCount_ -= 1;
        jobBucket->OnJobCompleted();
    }
}

void TSimulatorOperationController::OnNonscheduledJobAborted(const TJobId& jobId, EAbortReason)
{
    const auto& jobDescription = IdToDescription_.Get(jobId);

    auto jobBucketIt = JobBuckets_.find(jobDescription.Type);
    YCHECK(jobBucketIt != JobBuckets_.end());
    auto& jobBucket = jobBucketIt->second;

    {
        auto guard = Guard(Lock_);

        NeededResources_ += jobDescription.ResourceLimits;
        PendingJobCount_ += 1;
        RunningJobCount_ -= 1;
        AbortedJobCount_ += 1;

        jobBucket->OnNonscheduledJobAborted(jobDescription);
    }
}

bool TSimulatorOperationController::IsOperationCompleted() const
{
    auto guard = Guard(Lock_);
    return ActiveBuckets_.empty();
}

// Lock_ must be acquired.
bool TSimulatorOperationController::FindJobToSchedule(
    const TJobResourcesWithQuota& nodeLimits,
    TJobDescription* jobToScheduleOutput,
    EScheduleJobFailReason* failReasonOutput)
{
    EScheduleJobFailReason commonFailReason = EScheduleJobFailReason::NoPendingJobs;

    for (auto& activeBucket : ActiveBuckets_) {
        EScheduleJobFailReason lastFailReason;
        if (activeBucket->FindJobToSchedule(nodeLimits, jobToScheduleOutput, &lastFailReason)) {
            return true;
        }

        switch (lastFailReason) {
            case EScheduleJobFailReason::NotEnoughResources: {
                commonFailReason = lastFailReason;
                break;
            }
            case EScheduleJobFailReason::NoPendingJobs: {
                // Nothing to do.
                break;
            }
            default: {
                THROW_ERROR_EXCEPTION("Unexpected schedule job fail reason: %v", lastFailReason);
            }
        }
    }

    *failReasonOutput = commonFailReason;
    return false;
}

TFuture<TScheduleJobResultPtr> TSimulatorOperationController::ScheduleJob(
    const ISchedulingContextPtr& context,
    const TJobResourcesWithQuota& nodeLimits,
    const TString& /* treeId */)
{
    auto guard = Guard(Lock_);

    auto scheduleJobResult = New<TScheduleJobResult>();

    TJobDescription jobToSchedule;
    EScheduleJobFailReason failReason;
    if (!FindJobToSchedule(nodeLimits, &jobToSchedule, &failReason)) {
        scheduleJobResult->RecordFail(failReason);
        return MakeFuture(scheduleJobResult);
    }

    auto jobId = TJobId::Create();
    scheduleJobResult->StartDescriptor.emplace(
        jobId,
        jobToSchedule.Type,
        jobToSchedule.ResourceLimits,
        /* interruptible */ false);

    dynamic_cast<TSchedulingContext*>(context.Get())->SetDurationForStartedJob(jobId, jobToSchedule.Duration);
    IdToDescription_.Insert(jobId, jobToSchedule);

    NeededResources_ -= jobToSchedule.ResourceLimits;
    PendingJobCount_ -= 1;
    RunningJobCount_ += 1;

    return MakeFuture(scheduleJobResult);
}

void TSimulatorOperationController::UpdateMinNeededJobResources()
{
    // Nothing to do.
}

TJobResourcesWithQuotaList TSimulatorOperationController::GetMinNeededJobResources() const
{
    return {ZeroJobResourcesWithQuota()};
}

TString TSimulatorOperationController::GetLoggingProgress() const
{
    return Format(
        "Buckets = {T: %v, R: %v}, Jobs = {T: %v, R: %v, C: %v, P: %v, A: %v}",
        JobBuckets_.size(),
        ActiveBuckets_.size(),
        OperationDescription_->JobDescriptions.size(),
        RunningJobCount_,
        CompletedJobCount_,
        PendingJobCount_,
        AbortedJobCount_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerSimulator

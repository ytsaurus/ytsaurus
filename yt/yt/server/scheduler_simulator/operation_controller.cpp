#include "operation_controller.h"
#include "operation.h"

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT::NSchedulerSimulator {

////////////////////////////////////////////////////////////////////////////////

using namespace NScheduler;
using namespace NControllerAgent;
using namespace NConcurrency;
using namespace NYTree;

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
        YT_VERIFY(InitializationFinished_);

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
        YT_VERIFY(InitializationFinished_);
        PendingJobs_.push_back(abortedJobDescription);
    }

    void FinishInitialization()
    {
        YT_VERIFY(!InitializationFinished_);

        InitializationFinished_ = true;

        if (ActiveDependenciesCount_ == 0) {
            Host_->OnBucketActivated(this);
        }
    }

    int GetPendingJobCount()
    {
        YT_VERIFY(InitializationFinished_);
        return PendingJobs_.size();
    }

    bool FindJobToSchedule(
        const TJobResourcesWithQuota& nodeLimits,
        TJobDescription* jobToScheduleOutput,
        EScheduleJobFailReason* failReasonOutput)
    {
        YT_VERIFY(InitializationFinished_);

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

    // Note that this method is quite slow. Add caching if you want to call it frequently.
    TJobResources GetNeededResources()
    {
        YT_VERIFY(InitializationFinished_);

        TJobResources neededResources;
        for (const auto& job : PendingJobs_) {
            neededResources += job.ResourceLimits;
        }

        return neededResources;
    }

    // Note that this method is quite slow. Add caching if you want to call it frequently.
    TJobResources GetMinNeededResources()
    {
        YT_VERIFY(InitializationFinished_);

        TJobResources minNeededResources = TJobResources::Infinite();
        for (const auto& job : PendingJobs_) {
            minNeededResources = Min(minNeededResources, job.ResourceLimits);
        }

        return minNeededResources;
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
        YT_VERIFY(!InitializationFinished_);
        DependentBuckets_.push_back(other);
    }

    void IncreaseActiveDependenciesCount()
    {
        YT_VERIFY(!InitializationFinished_);
        ActiveDependenciesCount_ += 1;
    }

    void OnResolvedDependency()
    {
        YT_VERIFY(InitializationFinished_);
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
    TSimulatorOperationController(
        const TOperation* operation,
        const TOperationDescription* operationDescription,
        std::optional<TDuration> scheduleJobDelay);

    // Lock_ must be acquired.
    virtual void OnBucketActivated(TJobBucket* activatedBucket) override;

    // Lock_ must be acquired.
    virtual void OnBucketCompleted(TJobBucket* deactivatedBucket) override;

    //! Returns the number of jobs the controller still needs to start right away.
    virtual int GetPendingJobCount() const override;

    //! Returns the mode which says how to preempt jobs of this operation.
    virtual EPreemptionMode GetPreemptionMode() const override;

    //! Returns the total resources that are additionally needed.
    virtual TJobResources GetNeededResources() const override;

    virtual void OnJobCompleted(std::unique_ptr<TCompletedJobSummary> jobSummary) override;

    virtual void OnNonscheduledJobAborted(TJobId, EAbortReason) override;

    virtual bool IsOperationCompleted() const override ;

    //! Called during heartbeat processing to request actions the node must perform.
    virtual TFuture<TControllerScheduleJobResultPtr> ScheduleJob(
        const ISchedulingContextPtr& context,
        const TJobResourcesWithQuota& nodeLimits,
        const TString& /* treeId */) override;

    virtual void UpdateMinNeededJobResources() override;
    virtual TJobResourcesWithQuotaList GetMinNeededJobResources() const override;

    virtual TString GetLoggingProgress() const override;

private:
    using TJobBuckets = THashMap<EJobType, std::unique_ptr<TJobBucket>>;

    const TOperationDescription* OperationDescription_;
    const TJobBuckets JobBuckets_;

    std::optional<TDuration> ScheduleJobDelay_;

    TLockProtectedMap<TJobId, TJobDescription> IdToDescription_;
    NLogging::TLogger Logger;

    TSpinLock Lock_;
    // Protected by Lock_.
    int PendingJobCount_ = 0;
    int CompletedJobCount_ = 0;
    int AbortedJobCount_ = 0;
    int RunningJobCount_ = 0;
    TJobResources NeededResources_;
    SmallVector<TJobBucket*, TEnumTraits<EJobType>::DomainSize> ActiveBuckets_;
    TJobResourcesWithQuotaList CachedMinNeededJobResources;
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
    const TOperationDescription* operationDescription,
    std::optional<TDuration> scheduleJobDelay)
{
    return New<TSimulatorOperationController>(operation, operationDescription, scheduleJobDelay);
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
    const TOperation* /*operation*/,
    const TOperationDescription* operationDescription,
    std::optional<TDuration> scheduleJobDelay)
    : OperationDescription_(operationDescription)
    , JobBuckets_(InitializeJobBuckets(operationDescription))
    , ScheduleJobDelay_(scheduleJobDelay)
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
    YT_VERIFY(false);
}

int TSimulatorOperationController::GetPendingJobCount() const
{
    auto guard = Guard(Lock_);
    return PendingJobCount_;
}

EPreemptionMode TSimulatorOperationController::GetPreemptionMode() const
{
    return ConvertTo<TOperationSpecBasePtr>(OperationDescription_->Spec)->PreemptionMode;
}

TJobResources TSimulatorOperationController::GetNeededResources() const
{
    auto guard = Guard(Lock_);
    return NeededResources_;
}

void TSimulatorOperationController::OnJobCompleted(std::unique_ptr<TCompletedJobSummary> jobSummary)
{
    const auto& jobDescription = IdToDescription_.Get(jobSummary->Id);

    auto& jobBucket = GetOrCrash(JobBuckets_, jobDescription.Type);

    {
        auto guard = Guard(Lock_);
        CompletedJobCount_ += 1;
        RunningJobCount_ -= 1;
        jobBucket->OnJobCompleted();
    }
}

void TSimulatorOperationController::OnNonscheduledJobAborted(TJobId jobId, EAbortReason)
{
    const auto& jobDescription = IdToDescription_.Get(jobId);

    auto& jobBucket = GetOrCrash(JobBuckets_, jobDescription.Type);

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

TFuture<TControllerScheduleJobResultPtr> TSimulatorOperationController::ScheduleJob(
    const ISchedulingContextPtr& context,
    const TJobResourcesWithQuota& nodeLimits,
    const TString& /* treeId */)
{
    if (ScheduleJobDelay_) {
        TDelayedExecutor::WaitForDuration(*ScheduleJobDelay_);
    }

    auto guard = Guard(Lock_);

    auto scheduleJobResult = New<TControllerScheduleJobResult>();

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
    auto guard = Guard(Lock_);
    TJobResourcesWithQuotaList result;

    for (const auto& [jobType, bucket] : JobBuckets_) {
        if (bucket->GetPendingJobCount() == 0) {
            continue;
        }
        auto resources = bucket->GetMinNeededResources();

        result.push_back(resources);
        YT_LOG_DEBUG("Aggregated minimal needed resources for jobs (JobType: %v, MinNeededResources: %v)",
            jobType,
            FormatResources(resources));
    }

    CachedMinNeededJobResources = std::move(result);
}

TJobResourcesWithQuotaList TSimulatorOperationController::GetMinNeededJobResources() const
{
    auto guard = Guard(Lock_);
    return CachedMinNeededJobResources;
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

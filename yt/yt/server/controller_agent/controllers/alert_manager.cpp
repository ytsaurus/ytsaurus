#include "alert_manager.h"
#include "job_helpers.h"

#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NControllerAgent::NControllers {

using namespace NConcurrency;
using NScheduler::TUserJobSpecPtr;

////////////////////////////////////////////////////////////////////////////////

class TAlertManager
    : public IAlertManager
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TAlertManager() = default;

    explicit TAlertManager(IAlertManagerHost* host)
        : Host_(host)
        , Config_(Host_->GetConfig()->AlertManager)
        , Logger(Host_->GetLogger())
        , AnalyzeExecutor_(New<TPeriodicExecutor>(
            Host_->GetCancelableInvoker(EOperationControllerQueue::Default),
            BIND(&TAlertManager::Analyze, MakeWeak(this)),
            Config_->Period))
    { }

    void StartPeriodicActivity() override
    {
        AnalyzeExecutor_->Start();
    }

    void Analyze() override
    {
        YT_LOG_DEBUG("Analyze operation alerts");

        AnalyzeMemoryAndTmpfsUsage();
        AnalyzeInputStatistics();
        AnalyzeIntermediateJobsStatistics();
        AnalyzePartitionHistogram();
        AnalyzeAbortedJobs();
        AnalyzeJobsIOUsage();
        AnalyzeJobsCpuUsage();
        AnalyzeJobsGpuUsage();
        AnalyzeGpuPowerUsageOnWindow();
        AnalyzeJobsDuration();
        AnalyzeOperationDuration();
        AnalyzeScheduleJobStatistics();
        AnalyzeControllerQueues();
        AnalyzeInvalidatedJobs();
    }

private:
    //! Raw pointer here avoids cyclic reference; alert manager cannot live longer than its host.
    IAlertManagerHost* Host_;
    TAlertManagerConfigPtr Config_;

    NLogging::TSerializableLogger Logger;

    NConcurrency::TPeriodicExecutorPtr AnalyzeExecutor_;

    using TControllerQueueStatistics = TEnumIndexedArray<EOperationControllerQueue, TDiagnosableInvokerPool::TInvokerStatistics>;
    TControllerQueueStatistics LastControllerQueueStatistics_;

    struct TGpuPowerUsageRecord
    {
        TInstant Time;
        i64 Value;

        void Persist(const TStreamPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, Time);
            Persist(context, Value);
        }
    };

    THashMap<TString, std::deque<TGpuPowerUsageRecord>> AnalyzeGpuPowerUsageOnWindowVertexDescriptorToRecords_;

    void AnalyzeProcessingUnitUsage(
        const std::vector<TString>& usageStatistics,
        const std::vector<EJobState>& jobStates,
        const std::function<double(const TUserJobSpecPtr&)>& getLimit,
        const std::function<bool(TDuration, double, i64, double)>& needSetAlert,
        const TString& name,
        EOperationAlertType alertType,
        const TString& message)
    {
        std::vector<TError> errors;
        for (const auto& task : Host_->GetTasks()) {
            const auto& userJobSpecPtr = task->GetUserJobSpec();
            if (!userJobSpecPtr) {
                continue;
            }

            auto taskName = task->GetVertexDescriptor();

            i64 totalExecutionTime = 0;
            i64 jobCount = 0;

            double limit = getLimit(userJobSpecPtr);
            if (limit == 0) {
                continue;
            }

            for (const auto& jobState : jobStates) {
                const auto& aggregatedStatistics = (jobState == EJobState::Running)
                    ? Host_->GetAggregatedRunningJobStatistics()
                    : Host_->GetAggregatedFinishedJobStatistics();
                auto summary = aggregatedStatistics
                    .FindSummaryByJobStateAndType("/time/exec", jobState, taskName)
                    .value_or(NYT::TSummary());
                totalExecutionTime += summary.GetSum();
                jobCount += summary.GetCount();
            }

            if (jobCount == 0 || totalExecutionTime == 0) {
                continue;
            }

            i64 usage = 0;
            for (const auto& stat : usageStatistics) {
                for (const auto& jobState : jobStates) {
                    const auto& aggregatedStatistics = (jobState == EJobState::Running)
                        ? Host_->GetAggregatedRunningJobStatistics()
                        : Host_->GetAggregatedFinishedJobStatistics();
                    usage += aggregatedStatistics.GetSumByJobStateAndType(stat, jobState, taskName);
                }
            }

            TDuration totalExecutionDuration = TDuration::MilliSeconds(totalExecutionTime);
            double ratio = static_cast<double>(usage) / (totalExecutionTime * limit);

            if (needSetAlert(totalExecutionDuration, limit, jobCount, ratio))
            {
                auto error = TError("Jobs of task %Qlv use %.2f%% of requested %s limit", taskName, 100 * ratio, name)
                    << TErrorAttribute(Format("%s_time", name), usage)
                    << TErrorAttribute("exec_time", totalExecutionDuration)
                    << TErrorAttribute(Format("%s_limit", name), limit);
                errors.push_back(error);
            }
        }

        TError error;
        if (!errors.empty()) {
            error = TError(message) << errors;
        }

        Host_->SetOperationAlert(alertType, error);
    }

    void AnalyzeMemoryAndTmpfsUsage()
    {
        struct TMemoryInfo
        {
            std::vector<std::optional<i64>> MaxTmpfsUsage;
            std::optional<i64> MaxMemoryUsage;
            i64 MemoryReserve = 0;

            TUserJobSpecPtr JobSpec;
        };

        THashMap<TTaskPtr, TMemoryInfo> memoryInfoPerTask;

        for (const auto& task : Host_->GetTasks()) {
            if (!task->IsSimpleTask()) {
                continue;
            }

            const auto& userJobSpec = task->GetUserJobSpec();
            if (!userJobSpec) {
                continue;
            }

            auto memoryInfoIt = memoryInfoPerTask.find(task);
            if (memoryInfoIt == memoryInfoPerTask.end()) {
                memoryInfoIt = memoryInfoPerTask.emplace(task, TMemoryInfo()).first;
            }

            auto& memoryInfo = memoryInfoIt->second;

            memoryInfo.JobSpec = userJobSpec;

            // Some approximation to actual memory reserve in jobs of given task.
            memoryInfo.MemoryReserve = userJobSpec->MemoryLimit *
                task->GetUserJobMemoryDigest()->GetQuantile(Host_->GetConfig()->UserJobMemoryReserveQuantile);

            for (const auto& jobState : { EJobState::Completed, EJobState::Failed }) {
                auto summary = Host_->GetAggregatedFinishedJobStatistics().FindSummaryByJobStateAndType(
                    "/user_job/max_memory",
                    jobState,
                    task->GetVertexDescriptor());
                if (summary) {
                    if (!memoryInfo.MaxMemoryUsage) {
                        memoryInfo.MaxMemoryUsage = 0;
                    }
                    memoryInfo.MaxMemoryUsage = std::max(*memoryInfo.MaxMemoryUsage, summary->GetMax());
                }
            }

            auto maxUsedTmpfsSizes = task->GetMaximumUsedTmpfsSizes();

            YT_VERIFY(userJobSpec->TmpfsVolumes.size() == maxUsedTmpfsSizes.size());

            if (memoryInfo.MaxTmpfsUsage.empty()) {
                memoryInfo.MaxTmpfsUsage.resize(maxUsedTmpfsSizes.size());
            }

            YT_VERIFY(memoryInfo.MaxTmpfsUsage.size() == maxUsedTmpfsSizes.size());

            for (int index = 0; index < std::ssize(maxUsedTmpfsSizes); ++index) {
                auto tmpfsSize = maxUsedTmpfsSizes[index];
                if (tmpfsSize) {
                    if (!memoryInfo.MaxTmpfsUsage[index]) {
                        memoryInfo.MaxTmpfsUsage[index] = 0;
                    }
                    memoryInfo.MaxTmpfsUsage[index] = std::max(*memoryInfo.MaxTmpfsUsage[index], *tmpfsSize);
                }
            }
        }

        std::vector<TError> tmpfsErrors;
        std::vector<TError> memoryErrors;

        double minUnusedSpaceRatio = 1.0 - Config_->TmpfsAlertMaxUnusedSpaceRatio;

        for (const auto& [task, memoryInfo] : memoryInfoPerTask) {
            const auto& jobSpec = memoryInfo.JobSpec;
            const auto& tmpfsVolumes = jobSpec->TmpfsVolumes;

            bool skipTmpfsCheck = false;
            if (memoryInfo.MaxMemoryUsage) {
                i64 memoryUsage = *memoryInfo.MaxMemoryUsage;

                for (int index = 0; index < std::ssize(tmpfsVolumes); ++index) {
                    auto maxTmpfsUsage = memoryInfo.MaxTmpfsUsage[index];
                    if (maxTmpfsUsage) {
                        memoryUsage += *maxTmpfsUsage;
                    }
                }

                auto memoryUsageRatio = static_cast<double>(memoryUsage) / memoryInfo.MemoryReserve;

                bool ratioViolated = memoryUsageRatio + Config_->MemoryUsageAlertMaxUnusedRatio < 1.0;
                bool sizeViolated = memoryUsage + Config_->MemoryUsageAlertMaxUnusedSize < memoryInfo.MemoryReserve;

                auto maxJobCount = Config_->MemoryUsageAlertMaxJobCount;
                bool maxJobCountViolated = maxJobCount && Host_->GetTotalJobCount() < *maxJobCount;
                if (ratioViolated && sizeViolated && !maxJobCountViolated) {
                    memoryErrors.push_back(TError(
                        "Jobs of type %Qlv use less than %.1f%% of requested memory",
                        task->GetVertexDescriptor(),
                        100.0 * (1.0 - Config_->MemoryUsageAlertMaxUnusedRatio))
                        << TErrorAttribute("memory_reserve", memoryInfo.MemoryReserve)
                        << TErrorAttribute("memory_usage", memoryUsage));
                }

                if (memoryInfo.JobSpec->MemoryReserveFactor &&
                    *memoryInfo.JobSpec->MemoryReserveFactor >= 1 &&
                    memoryUsageRatio < 1.0 - Config_->MemoryReserveFactorAlertMaxUnusedRatio) {
                    memoryErrors.push_back(TError(
                        "Jobs of type %Qlv use less than %.1f%% of requested memory with memory reserve factor set to 1",
                        task->GetVertexDescriptor(),
                        100.0 * (1.0 - Config_->MemoryReserveFactorAlertMaxUnusedRatio))
                        << TErrorAttribute("memory_reserve", memoryInfo.MemoryReserve)
                        << TErrorAttribute("memory_usage", memoryUsage));
                }

                if (memoryUsageRatio > Config_->TmpfsAlertMemoryUsageMuteRatio) {
                    skipTmpfsCheck = true;
                }
            }

            if (skipTmpfsCheck) {
                continue;
            }

            for (int index = 0; index < std::ssize(tmpfsVolumes); ++index) {
                auto maxTmpfsUsage = memoryInfo.MaxTmpfsUsage[index];
                if (!maxTmpfsUsage) {
                    continue;
                }

                auto requestedTmpfsSize = tmpfsVolumes[index]->Size;
                bool minUnusedSpaceThresholdOvercome = requestedTmpfsSize - *maxTmpfsUsage >
                    Config_->TmpfsAlertMinUnusedSpaceThreshold;
                bool minUnusedSpaceRatioViolated = *maxTmpfsUsage < minUnusedSpaceRatio * requestedTmpfsSize;

                if (minUnusedSpaceThresholdOvercome && minUnusedSpaceRatioViolated) {
                    auto error = TError(
                        "Jobs of type %Qlv use less than %.1f%% of requested tmpfs size in volume %Qv",
                        task->GetVertexDescriptor(),
                        minUnusedSpaceRatio * 100.0,
                        tmpfsVolumes[index]->Path)
                        << TErrorAttribute("max_used_tmpfs_size", *maxTmpfsUsage)
                        << TErrorAttribute("tmpfs_size", requestedTmpfsSize);
                    tmpfsErrors.push_back(error);
                }
            }
        }

        {
            TError error;
            if (!tmpfsErrors.empty()) {
                error = TError(
                    "Operation has jobs that use tmpfs inefficiently; "
                    "consider specifying tmpfs size closer to actual usage")
                    << tmpfsErrors;
            }

            Host_->SetOperationAlert(EOperationAlertType::UnusedTmpfsSpace, error);
        }

        {
            TError error;
            if (!memoryErrors.empty()) {
                error = TError(
                    "Operation has jobs that use memory inefficiently; "
                    "consider specifying memory limit closer to actual usage")
                    << memoryErrors;
            }

            Host_->SetOperationAlert(EOperationAlertType::UnusedMemory, error);
        }
    }

    void AnalyzeInputStatistics()
    {
        TError error;
        if (Host_->GetUnavailableInputChunkCount() > 0) {
            error = TError(
                "Some input chunks are not available; "
                "the relevant parts of computation will be suspended");
        }

        Host_->SetOperationAlert(EOperationAlertType::LostInputChunks, error);
    }

    void AnalyzeIntermediateJobsStatistics()
    {
        TError error;
        if (Host_->GetTotalJobCounter()->GetLost() > 0) {
            error = TError(
                "Some intermediate outputs were lost and will be regenerated; "
                "operation will take longer than usual");
        }

        Host_->SetOperationAlert(EOperationAlertType::LostIntermediateChunks, error);
    }

    void AnalyzePartitionHistogram()
    {
        TError error;

        auto sizeHistogram = Host_->ComputeFinalPartitionSizeHistogram();
        if (!sizeHistogram) {
            return;
        }

        auto view = sizeHistogram->GetHistogramView();

        i64 minIqr = Config_->IntermediateDataSkewAlertMinInterquartileRange;

        if (view.Max > Config_->IntermediateDataSkewAlertMinPartitionSize) {
            auto quartiles = ComputeHistogramQuartiles(view);
            i64 iqr = quartiles.Q75 - quartiles.Q25;
            if (iqr > minIqr && quartiles.Q50 + 2 * iqr < view.Max) {
                error = TError(
                    "Intermediate data skew is too high (see partitions histogram); "
                    "operation is likely to have stragglers");
            }
        }

        Host_->SetOperationAlert(EOperationAlertType::IntermediateDataSkew, error);
    }

    void AnalyzeAbortedJobs()
    {
        if (Host_->GetOperationType() == EOperationType::Vanilla) {
            return;
        }

        auto aggregateTimeForJobState = [&] (EJobState state) {
            i64 sum = 0;
            for (auto type : TEnumTraits<EJobType>::GetDomainValues()) {
                sum += Host_->GetAggregatedFinishedJobStatistics().GetSumByJobStateAndType(
                    "/time/total",
                    state,
                    FormatEnum(type));
            }

            return sum;
        };

        i64 completedJobsTime = aggregateTimeForJobState(EJobState::Completed);
        i64 abortedJobsTime = aggregateTimeForJobState(EJobState::Aborted);
        double abortedJobsTimeRatio = 1.0;
        if (completedJobsTime > 0) {
            abortedJobsTimeRatio = 1.0 * abortedJobsTime / completedJobsTime;
        }

        TError error;
        if (abortedJobsTime > Config_->AbortedJobsAlertMaxAbortedTime &&
            abortedJobsTimeRatio > Config_->AbortedJobsAlertMaxAbortedTimeRatio)
        {
            error = TError(
                "Aborted jobs time ratio is too high, scheduling is likely to be inefficient; "
                "consider increasing job count to make individual jobs smaller")
                    << TErrorAttribute("aborted_jobs_time_ratio", abortedJobsTimeRatio);
        }

        Host_->SetOperationAlert(EOperationAlertType::LongAbortedJobs, error);
    }

    void AnalyzeJobsIOUsage()
    {
        std::vector<TError> innerErrors;

        for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
            auto value = Host_->GetAggregatedFinishedJobStatistics().GetSumByJobStateAndType(
                "/user_job/woodpecker",
                EJobState::Completed,
                FormatEnum(jobType));

            if (value > 0) {
                innerErrors.emplace_back("Detected excessive disk IO in %Qlv jobs", jobType);
            }
        }

        TError error;
        if (!innerErrors.empty()) {
            error = TError("Detected excessive disk IO in jobs; consider optimizing disk usage")
                << innerErrors;
        }

        Host_->SetOperationAlert(EOperationAlertType::ExcessiveDiskUsage, error);
    }

    void AnalyzeJobsCpuUsage()
    {
        {
            auto getCpuLimit = [] (const TUserJobSpecPtr& jobSpec) {
                return jobSpec->CpuLimit;
            };

            auto needSetAlert = [&] (TDuration totalExecutionDuration, double /*cpuLimit*/, i64 jobCount, double ratio) {
                TDuration averageJobDuration = TDuration::MilliSeconds(totalExecutionDuration.MillisecondsFloat() / jobCount);
                return totalExecutionDuration > Config_->LowCpuUsageAlertMinExecTime &&
                       averageJobDuration > Config_->LowCpuUsageAlertMinAverageJobTime &&
                       ratio < Config_->LowCpuUsageAlertCpuUsageThreshold;
            };

            const TString alertMessage =
                "Average CPU usage of some of your job types is significantly lower than requested 'cpu_limit'. "
                "Consider decreasing cpu_limit in spec of your operation";

            AnalyzeProcessingUnitUsage(
                Config_->LowCpuUsageAlertStatistics,
                Config_->LowCpuUsageAlertJobStates,
                getCpuLimit,
                needSetAlert,
                "cpu",
                EOperationAlertType::LowCpuUsage,
                alertMessage);
        }

        {
            auto getCpuLimit = [] (const TUserJobSpecPtr& jobSpec) {
                return jobSpec->CpuLimit;
            };

            auto needSetAlert = [&] (TDuration totalExecutionDuration, double /*cpuLimit*/, i64 jobCount, double ratio) {
                TDuration averageJobDuration = TDuration::MilliSeconds(totalExecutionDuration.MillisecondsFloat() / jobCount);
                return averageJobDuration > Config_->HighCpuWaitAlertMinAverageJobTime &&
                       ratio > Config_->HighCpuWaitAlertThreshold;
            };

            const TString alertMessage = Format(
                "Average CPU wait time of some of your job types is significantly high (average CPU wait time ratio greater than %v%%). "
                "Investigate your process.",
                Config_->HighCpuWaitAlertThreshold);

            AnalyzeProcessingUnitUsage(
                Config_->HighCpuWaitAlertStatistics,
                Config_->HighCpuWaitAlertJobStates,
                getCpuLimit,
                needSetAlert,
                "cpu",
                EOperationAlertType::HighCpuWait,
                alertMessage);
        }
    }

    void AnalyzeJobsGpuUsage()
    {
        if (TInstant::Now() - Host_->GetStartTime() < Config_->LowGpuUsageAlertMinDuration && !Host_->IsCompleted()) {
            return;
        }

        auto getGpuLimit = [] (const TUserJobSpecPtr& jobSpec) {
            return jobSpec->GpuLimit;
        };

        {
            auto needSetAlert = [&] (TDuration totalExecutionDuration, double gpuCount, i64 /*jobCount*/, double ratio) {
                return totalExecutionDuration.SecondsFloat() * gpuCount > Config_->LowGpuUsageAlertMinTotalGpuDuration.SecondsFloat() &&
                    ratio < Config_->LowGpuUsageAlertGpuUsageThreshold;
            };

            static const TString alertMessage =
                "Average gpu usage of some of your job types is significantly lower than requested 'gpu_limit'. "
                "Consider optimizing your GPU utilization";

            AnalyzeProcessingUnitUsage(
                Config_->LowGpuUsageAlertStatistics,
                Config_->LowGpuUsageAlertJobStates,
                getGpuLimit,
                needSetAlert,
                "gpu",
                EOperationAlertType::LowGpuUsage,
                alertMessage);
        }

        {
            auto needSetAlert = [&] (TDuration totalExecutionDuration, double gpuCount, i64 /*jobCount*/, double ratio) {
                return totalExecutionDuration.SecondsFloat() * gpuCount > Config_->LowGpuUsageAlertMinTotalGpuDuration.SecondsFloat() &&
                    ratio < Config_->LowGpuUsageAlertGpuUtilizationPowerThreshold;
            };

            static const TString alertMessage = Format(
                "Average GPU power usage is significantly lower than %v percents. "
                "Consider optimizing your GPU process",
                Config_->LowGpuUsageAlertGpuUtilizationPowerThreshold * 100.0);

            AnalyzeProcessingUnitUsage(
                {"/user_job/gpu/cumulative_utilization_power"},
                Config_->LowGpuUsageAlertJobStates,
                getGpuLimit,
                needSetAlert,
                "gpu",
                EOperationAlertType::LowGpuPower,
                alertMessage);
        }

        {
            auto needSetAlert = [&] (TDuration totalExecutionDuration, double gpuCount, i64 /*jobCount*/, double ratio) {
                return totalExecutionDuration.SecondsFloat() * gpuCount > Config_->LowGpuUsageAlertMinTotalGpuDuration.SecondsFloat() &&
                    ratio < Config_->LowGpuUsageAlertGpuUtilizationSMThreshold;
            };

            static const TString alertMessage = Format(
                "Average GPU SM usage is significantly lower than %v percents. "
                "Consider optimizing your GPU process",
                Config_->LowGpuUsageAlertGpuUtilizationSMThreshold * 100.0);

            AnalyzeProcessingUnitUsage(
                {"/user_job/gpu/cumulative_sm_utilization"},
                Config_->LowGpuUsageAlertJobStates,
                getGpuLimit,
                needSetAlert,
                "gpu",
                EOperationAlertType::LowGpuSMUsage,
                alertMessage);
        }
    }

    void AnalyzeGpuPowerUsageOnWindow()
    {
        const auto& config = Config_->LowGpuPowerUsageOnWindow;
        const auto jobStates = {EJobState::Running, EJobState::Completed, EJobState::Failed, EJobState::Aborted};

        auto& descriptorToRecords = AnalyzeGpuPowerUsageOnWindowVertexDescriptorToRecords_;
        auto now = TInstant::Now();

        std::vector<TError> errors;
        for (const auto& task : Host_->GetTasks()) {
            const auto& userJobSpecPtr = task->GetUserJobSpec();
            if (!userJobSpecPtr) {
                continue;
            }

            auto taskName = task->GetVertexDescriptor();

            double limit = userJobSpecPtr->GpuLimit;
            if (limit == 0) {
                continue;
            }

            i64 totalExecutionTime = 0;
            i64 jobCount = 0;
            for (const auto& jobState : jobStates) {
                const auto& aggregatedStatistics = (jobState == EJobState::Running)
                    ? Host_->GetAggregatedRunningJobStatistics()
                    : Host_->GetAggregatedFinishedJobStatistics();
                auto summary = aggregatedStatistics
                    .FindSummaryByJobStateAndType("/time/exec", jobState, taskName)
                    .value_or(NYT::TSummary());
                totalExecutionTime += summary.GetSum();
                jobCount += summary.GetCount();
            }

            if (jobCount == 0 || totalExecutionTime == 0) {
                continue;
            }

            i64 usage = 0;
            for (const auto& jobState : jobStates) {
                const auto& aggregatedStatistics = (jobState == EJobState::Running)
                    ? Host_->GetAggregatedRunningJobStatistics()
                    : Host_->GetAggregatedFinishedJobStatistics();
                usage += aggregatedStatistics.GetSumByJobStateAndType("/user_job/gpu/cumulative_power", jobState, taskName);
            }

            auto& records = descriptorToRecords[taskName];

            // Generate alert and pop old records.
            if (!records.empty()) {
                auto frontRecord = records.front();
                if (now - frontRecord.Time > config->WindowSize) {
                    auto averageUsage = (usage - frontRecord.Value) / (now - frontRecord.Time).MilliSeconds();
                    YT_LOG_DEBUG("Checking average GPU power on window (TaskName: %v, AverageUsage: %v, EffectiveWindowSize: %v)",
                        taskName,
                        usage - frontRecord.Value,
                        now - frontRecord.Time);
                    if (averageUsage < config->Threshold) {
                        auto error = TError(
                                "Jobs of task %Qlv have average GPU power less than %v Watts during last %v minutes",
                                taskName,
                                config->Threshold,
                                config->WindowSize.Minutes())
                            << TErrorAttribute("average_usage", averageUsage)
                            << TErrorAttribute("threshold", config->Threshold)
                            << TErrorAttribute("task_name", taskName);
                        errors.push_back(error);
                    }
                }

                if (now - frontRecord.Time > config->WindowSize + 2 * config->RecordPeriod) {
                    records.pop_front();
                }
            }

            if (records.empty() || now - records.back().Time > config->RecordPeriod) {
                records.push_back(TGpuPowerUsageRecord{now, usage});
            }
        }

        TError error;
        if (!errors.empty()) {
            error = TError("Average GPU power is too low on window") << errors;
        }

        Host_->SetOperationAlert(EOperationAlertType::LowGpuPowerOnWindow, error);
    }

    void AnalyzeJobsDuration()
    {
        auto operationType = Host_->GetOperationType();
        if (operationType == EOperationType::RemoteCopy || operationType == EOperationType::Erase) {
            return;
        }

        auto operationDuration = TInstant::Now() - Host_->GetStartTime();

        std::vector<TError> innerErrors;

        for (auto jobType : Host_->GetSupportedJobTypesForJobsDurationAnalyzer()) {
            auto completedJobsSummary = Host_->GetAggregatedFinishedJobStatistics().FindSummaryByJobStateAndType(
                "/time/total",
                EJobState::Completed,
                FormatEnum(jobType));

            if (!completedJobsSummary) {
                continue;
            }

            auto maxJobDuration = TDuration::MilliSeconds(completedJobsSummary->GetMax());
            auto completedJobCount = completedJobsSummary->GetCount();
            auto avgJobDuration = TDuration::MilliSeconds(completedJobsSummary->GetSum() / completedJobCount);

            if (completedJobCount > Config_->ShortJobsAlertMinJobCount &&
                operationDuration > maxJobDuration * Config_->ShortJobsAlertMinAllowedOperationDurationToMaxJobDurationRatio &&
                avgJobDuration < Config_->ShortJobsAlertMinJobDuration &&
                Host_->GetDataWeightParameterNameForJob(jobType))
            {
                auto error = TError(
                    "Average duration of %Qlv jobs is less than %v seconds, try increasing %v in operation spec",
                    jobType,
                    Config_->ShortJobsAlertMinJobDuration.Seconds(),
                    Host_->GetDataWeightParameterNameForJob(jobType))
                        << TErrorAttribute("average_job_duration", avgJobDuration);

                innerErrors.push_back(error);
            }
        }

        TError error;
        if (!innerErrors.empty()) {
            error = TError(
                "Operation has jobs with duration is less than %v seconds, "
                "that leads to large overhead costs for scheduling",
                Config_->ShortJobsAlertMinJobDuration.Seconds())
                << innerErrors;
        }

        Host_->SetOperationAlert(EOperationAlertType::ShortJobsDuration, error);
    }

    void AnalyzeOperationDuration()
    {
        TError error;
        const auto& jobCounter = Host_->GetTotalJobCounter();
        for (const auto& task : Host_->GetTasks()) {
            if (!task->GetUserJobSpec()) {
                continue;
            }
            i64 completedAndRunning = jobCounter->GetCompletedTotal() + jobCounter->GetRunning();
            if (completedAndRunning == 0) {
                continue;
            }
            i64 pending = jobCounter->GetPending();
            TDuration wallTime = GetInstant() - Host_->GetStartTime();
            TDuration estimatedDuration = (wallTime / completedAndRunning) * pending;

            if (wallTime > Config_->OperationTooLongAlertMinWallTime &&
                estimatedDuration > Config_->OperationTooLongAlertEstimateDurationThreshold)
            {
                error = TError(
                    "Estimated duration of this operation is about %v days; "
                    "consider breaking operation into smaller ones",
                    estimatedDuration.Days()) << TErrorAttribute("estimated_duration", estimatedDuration);
                break;
            }
        }

        Host_->SetOperationAlert(EOperationAlertType::OperationTooLong, error);
    }

    void AnalyzeScheduleJobStatistics()
    {
        auto jobSpecThrottlerActivationCount = Host_->GetScheduleAllocationStatistics()->Failed()[EScheduleAllocationFailReason::JobSpecThrottling];
        auto activationCountThreshold = Config_->JobSpecThrottlingAlertActivationCountThreshold;

        TError error;
        if (jobSpecThrottlerActivationCount > activationCountThreshold) {
            error = TError(
                "Excessive job spec throttling is detected. Usage ratio of operation can be "
                "significantly less than fair share ratio")
                << TErrorAttribute("job_spec_throttler_activation_count", jobSpecThrottlerActivationCount);
        }

        Host_->SetOperationAlert(EOperationAlertType::ExcessiveJobSpecThrottling, error);
    }

    void AnalyzeControllerQueues()
    {
        TControllerQueueStatistics currentControllerQueueStatistics;
        THashMap<EOperationControllerQueue, TDuration> queueToTotalTimeEstimate;
        for (auto queue : TEnumTraits<EOperationControllerQueue>::GetDomainValues()) {
            auto statistics = Host_->GetInvokerStatistics(queue);
            const auto& lastStatistics = LastControllerQueueStatistics_[queue];

            auto deltaEnqueuedActionCount = statistics.EnqueuedActionCount - lastStatistics.EnqueuedActionCount;
            auto deltaExecutedActionCount = statistics.ExecutedActionCount - lastStatistics.ExecutedActionCount;

            YT_LOG_DEBUG(
                "Operation controller queue statistics (ControllerQueue: %v, DeltaEnqueuedActionCount: %v, "
                "DeltaExecutedActionCount: %v, WaitingActionCount: %v, TotalTimeEstimate: %v)",
                queue,
                deltaEnqueuedActionCount,
                deltaExecutedActionCount,
                statistics.WaitingActionCount,
                statistics.TotalTimeEstimate);

            if (statistics.TotalTimeEstimate > Config_->QueueTotalTimeEstimateThreshold) {
                queueToTotalTimeEstimate.emplace(queue, statistics.TotalTimeEstimate);
            }

            currentControllerQueueStatistics[queue] = std::move(statistics);
        }
        std::swap(LastControllerQueueStatistics_, currentControllerQueueStatistics);

        TError highQueueTotalTimeEstimateError;
        if (!queueToTotalTimeEstimate.empty()) {
            highQueueTotalTimeEstimateError = TError("Found action queues with high wait time estimate: %v",
                MakeFormattableView(queueToTotalTimeEstimate, [] (auto* builder, const auto& pair) {
                    const auto& [queue, averageWaitTime] = pair;
                    builder->AppendFormat("%Qlv", queue);
                }))
                << TErrorAttribute("queues_with_high_total_time_estimate", queueToTotalTimeEstimate);
        }
        Host_->SetOperationAlert(EOperationAlertType::HighQueueTotalTimeEstimate, highQueueTotalTimeEstimateError);
    }

    void AnalyzeInvalidatedJobs()
    {
        i64 invalidatedJobCount = 0;
        for (const auto& task : Host_->GetTasks()) {
            invalidatedJobCount += task->GetJobCounter()->GetInvalidated();
        }

        if (invalidatedJobCount > 0) {
            auto invalidatedJobCountError = TError("Operation has invalidated jobs")
                << TErrorAttribute("invalidated_job_count", invalidatedJobCount);
            Host_->SetOperationAlert(EOperationAlertType::InvalidatedJobsFound, invalidatedJobCountError);
        }
    }

    void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;

        Persist(context, Host_);
        Persist(context, Logger);
        Persist(context, AnalyzeGpuPowerUsageOnWindowVertexDescriptorToRecords_);

        if (context.IsLoad()) {
            Config_ = Host_->GetConfig()->AlertManager;
            AnalyzeExecutor_ = New<TPeriodicExecutor>(
                Host_->GetCancelableInvoker(EOperationControllerQueue::Default),
                BIND(&TAlertManager::Analyze, MakeWeak(this)),
                Config_->Period);
        }
    }

    DECLARE_DYNAMIC_PHOENIX_TYPE(TAlertManager, 0xf4e8bb36);
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TAlertManager);
DEFINE_REFCOUNTED_TYPE(TAlertManager)

////////////////////////////////////////////////////////////////////////////////

IAlertManagerPtr CreateAlertManager(IAlertManagerHost* host)
{
    return New<TAlertManager>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

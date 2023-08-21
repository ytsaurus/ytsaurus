#include "job_splitter.h"

#include <yt/yt/server/controller_agent/operation_controller.h>
#include <yt/yt/server/controller_agent/config.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/core/logging/serializable_logger.h>

#include <util/generic/cast.h>

#include <algorithm>

namespace NYT::NControllerAgent::NControllers {

using namespace NChunkPools;
using namespace NProfiling;
using namespace NYTree;
using namespace NYson;
using namespace NScheduler;
using namespace NPhoenix;

////////////////////////////////////////////////////////////////////////////////

class TJobSplitter
    : public IJobSplitter
{
public:
    //! Used only for persistence.
    TJobSplitter() = default;

    TJobSplitter(
        TJobSplitterConfigPtr config,
        IPersistentChunkPoolJobSplittingHost* chunkPool,
        const NLogging::TLogger& logger)
        : Config_(config)
        , CanSplitJobs_(config->EnableJobSplitting)
        , CanLaunchSpeculativeJobs_(config->EnableJobSpeculation)
        , JobTimeTracker_(std::move(config))
        , ChunkPool_(chunkPool)
        , Logger(logger)
    {
        YT_VERIFY(Config_);
    }

    EJobSplitterVerdict ExamineJob(TJobId jobId) override
    {
        auto& job = GetOrCrash(RunningJobs_, jobId);

        auto minJobExecTime = std::max(
            Config_->MinJobTime,
            job.GetPrepareWithoutDownloadDuration() * Config_->ExecToPrepareTimeRatio);

        auto isLongAmongRunning = JobTimeTracker_.IsLongJob(jobId);
        auto isResidual = IsResidual();

        auto now = GetInstant();
        if (CanLaunchSpeculativeJobs_ && job.GetSplitDeadline() && now >= job.GetSplitDeadline().value()) {
            YT_LOG_DEBUG("Split timeout expired, requesting speculative launch (JobId: %v)", jobId);
            return EJobSplitterVerdict::LaunchSpeculative;
        }

        if (job.GetExecDuration() > minJobExecTime) {
            if (job.GetRowCount() > 0 &&
                job.GetRemainingDuration() > minJobExecTime &&
                isLongAmongRunning)
            {
                YT_LOG_DEBUG("Job splitter detected long job among running (JobId: %v)", jobId);
                if (CanSplitJobs_ && job.IsSplittable() && job.GetTotalDataWeight() > Config_->MinTotalDataWeight) {
                    job.OnSplitRequested(Config_->SplitTimeoutBeforeSpeculate);
                    return EJobSplitterVerdict::Split;
                } else if (CanLaunchSpeculativeJobs_) {
                    return EJobSplitterVerdict::LaunchSpeculative;
                }
            }

            if (job.GetRowCount() > 0 &&
                job.GetRemainingDuration() > minJobExecTime &&
                isResidual)
            {
                YT_LOG_DEBUG("Job splitter detected residual job (JobId: %v)", jobId);
                if (CanSplitJobs_ && job.IsSplittable() && job.GetTotalDataWeight() > Config_->MinTotalDataWeight) {
                    job.OnSplitRequested(Config_->SplitTimeoutBeforeSpeculate);
                    return EJobSplitterVerdict::Split;
                } else if (CanLaunchSpeculativeJobs_) {
                    return EJobSplitterVerdict::LaunchSpeculative;
                }
            }
        }

        auto noProgressJobTimeLimit = GetAverageSuccessJobPrepareDuration() * Config_->NoProgressJobTimeToAveragePrepareTimeRatio;
        auto minJobTotalTime = std::max(noProgressJobTimeLimit, Config_->MinJobTime);
        TDuration totalDuration = job.GetPrepareDuration() + job.GetExecDuration();
        if (CanLaunchSpeculativeJobs_ &&
            totalDuration > minJobTotalTime &&
            job.GetRowCount() == 0 &&
            isResidual &&
            noProgressJobTimeLimit > TDuration::Zero())
        {
            YT_LOG_DEBUG("Job splitter detected long job without any progress (JobId: %v)", jobId);
            return EJobSplitterVerdict::LaunchSpeculative;
        }

        if (!job.GetNextLoggingTime().has_value()) {
            job.SetNextLoggingTime(now + Config_->JobLoggingPeriod);
        }

        if (job.GetNextLoggingTime() < now) {
            job.SetNextLoggingTime(now + Config_->JobLoggingPeriod);
            YT_LOG_DEBUG(
                "Job splitter detailed information (JobId: %v, PrepareDuration: %v, PrepareWithoutDownloadDuration: %v, "
                "ExecDuration: %v, RemainingDuration: %v, TotalDataWeight: %v, RowCount: %v, IsLongAmongRunning: %v, "
                "IsResidual: %v, IsInterruptible: %v, IsSplittable: %v, SplitDeadline: %v, SuccessJobPrepareDurationSum: %v, SuccessJobCount: %v)",
                jobId,
                job.GetPrepareDuration(),
                job.GetPrepareWithoutDownloadDuration(),
                job.GetExecDuration(),
                job.GetRemainingDuration(),
                job.GetTotalDataWeight(),
                job.GetRowCount(),
                isLongAmongRunning,
                isResidual,
                job.GetIsInterruptible(),
                job.IsSplittable(),
                job.GetSplitDeadline(),
                SuccessJobPrepareDurationSum_,
                SuccessJobCount_);
        }

        return EJobSplitterVerdict::DoNothing;
    }

    void OnJobStarted(
        TJobId jobId,
        const TChunkStripeListPtr& inputStripeList,
        TOutputCookie cookie,
        bool isInterruptible) override
    {
        RunningJobs_.emplace(jobId, TRunningJob(inputStripeList, cookie, this, isInterruptible));
        MaxRunningJobCount_ = std::max<i64>(MaxRunningJobCount_, RunningJobs_.size());
    }

    void OnJobRunning(const TJobSummary& summary) override
    {
        auto& job = GetOrCrash(RunningJobs_, summary.Id);
        job.Update(&JobTimeTracker_, summary);
    }

    void OnJobFailed(const TFailedJobSummary& summary) override
    {
        OnJobFinished(summary);
    }

    void OnJobAborted(const TAbortedJobSummary& summary) override
    {
        OnJobFinished(summary);
    }

    void OnJobCompleted(const TCompletedJobSummary& summary) override
    {
        OnJobFinished(summary);
        SuccessJobPrepareDurationSum_ += summary.TimeStatistics.PrepareDuration.value_or(TDuration());
        ++SuccessJobCount_;
    }

    int EstimateJobCount(
        const TCompletedJobSummary& summary,
        i64 unreadRowCount) const override
    {
        if (!Config_->EnableJobSplitting) {
            return 1;
        }

        if (summary.InterruptReason == EInterruptReason::UserRequest) {
            return 1;
        }

        double execDuration = summary.TimeStatistics.ExecDuration.value_or(TDuration()).SecondsFloat();
        YT_VERIFY(summary.TotalInputDataStatistics);
        i64 processedRowCount = summary.TotalInputDataStatistics->row_count();
        if (unreadRowCount <= 1 || processedRowCount == 0 || execDuration == 0.0) {
            return 1;
        }
        double prepareDuration = summary.TimeStatistics.PrepareDuration.value_or(TDuration()).SecondsFloat() -
            summary.TimeStatistics.ArtifactsDownloadDuration.value_or(TDuration()).SecondsFloat();
        double expectedExecDuration = execDuration / processedRowCount * unreadRowCount;

        auto getMedianCompletionDuration = [&] () {
            auto medianCompletionTime = JobTimeTracker_.GetMedianCompletionTime();
            if (!IsResidual() && medianCompletionTime) {
                return medianCompletionTime.SecondsFloat() - GetInstant().SecondsFloat();
            }

            // If running job count is small, we don't pay attention to median completion time
            // and rely only on MinJobTime.
            return 0.0;
        };

        double medianCompletionDuration = getMedianCompletionDuration();
        double minJobTime = std::max({
            Config_->MinJobTime.SecondsFloat(),
            Config_->ExecToPrepareTimeRatio * prepareDuration,
            medianCompletionDuration - prepareDuration});

        int jobCount = std::clamp<int>(
            std::min(static_cast<i64>(expectedExecDuration / minJobTime), unreadRowCount),
            1,
            Config_->MaxJobsPerSplit);

        YT_LOG_DEBUG("Estimated optimal job count for unread data slices "
            "(JobCount: %v, JobId: %v, PrepareDuration: %.6g, ExecDuration: %.6g, "
            "ProcessedRowCount: %v, MedianCompletionDuration: %.6g, MinJobTime: %v, "
            "ExecToPrepareTimeRatio: %v, UnreadRowCount: %v, ExpectedExecDuration: %.6g)",
            jobCount,
            summary.Id,
            prepareDuration,
            execDuration,
            processedRowCount,
            medianCompletionDuration,
            Config_->MinJobTime.SecondsFloat(),
            Config_->ExecToPrepareTimeRatio,
            unreadRowCount,
            expectedExecDuration);
        return jobCount;
    }

    void BuildJobSplitterInfo(TFluentMap fluent) const override
    {
        fluent
            .Item("build_time").Value(GetInstant())
            .Item("running_job_count").Value(RunningJobs_.size())
            .Item("max_running_job_count").Value(MaxRunningJobCount_)
            .DoIf(Config_->ShowRunningJobsInProgress, [&] (TFluentMap fluent) {
                fluent.Item("running_jobs").DoMapFor(RunningJobs_,
                    [&] (TFluentMap fluent, const std::pair<TJobId, TRunningJob>& pair) {
                        const auto& job = pair.second;
                        fluent
                            .Item(ToString(pair.first)).BeginMap()
                                .Do(BIND(&TRunningJob::BuildRunningJobInfo, &job))
                                .Item("candidate").Value(JobTimeTracker_.IsLongJob(pair.first))
                            .EndMap();
                    });
            })
            .Item("statistics").BeginMap()
                .Do(BIND(&TJobTimeTracker::BuildStatistics, &JobTimeTracker_))
            .EndMap()
            .Item("config").Value(Config_)
            .Item("can_split_jobs").Value(CanSplitJobs_)
            .Item("can_launch_speculative_jobs").Value(CanLaunchSpeculativeJobs_);
    }

    void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;

        Persist(context, Config_);
        Persist(context, CanSplitJobs_);
        Persist(context, CanLaunchSpeculativeJobs_);
        Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, RunningJobs_);
        Persist(context, JobTimeTracker_);
        Persist(context, MaxRunningJobCount_);
        Persist(context, Logger);
        Persist(context, SuccessJobPrepareDurationSum_);
        Persist(context, SuccessJobCount_);
        Persist(context, ChunkPool_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TJobSplitter, 0x1ddf34ff);

    class TJobTimeTracker
    {
    public:
        //! Used only for persistence.
        TJobTimeTracker() = default;

        explicit TJobTimeTracker(TJobSplitterConfigPtr config)
            : Config_(std::move(config))
        { }

        void SetSample(TInstant completionTime, TJobId jobId)
        {
            JobIdToCompletionTime_[jobId] = completionTime;
        }

        void RemoveSample(TJobId jobId)
        {
            JobIdToCompletionTime_.erase(jobId);
        }

        void Update()
        {
            constexpr double ExcessFactor = 2.1;

            if (JobIdToCompletionTime_.empty()) {
                MedianCompletionTime_ = TInstant::Zero();
                return;
            }

            auto now = GetInstant();
            if (now < NextUpdateTime_) {
                return;
            }

            NextUpdateTime_ = now + Config_->UpdatePeriod;

            std::vector<std::pair<TInstant, TJobId>> samples;
            samples.reserve(JobIdToCompletionTime_.size());
            for (const auto& [jobId, time] : JobIdToCompletionTime_) {
                samples.emplace_back(time, jobId);
            }

            int medianIndex = JobIdToCompletionTime_.size() / 2;
            std::nth_element(samples.begin(), samples.begin() + medianIndex, samples.end());
            MedianCompletionTime_ = samples[medianIndex].first;

            int percentileIndex = static_cast<int>(std::floor(static_cast<double>(JobIdToCompletionTime_.size()) * Config_->CandidatePercentile));
            percentileIndex = std::max(percentileIndex, medianIndex);
            std::nth_element(samples.begin() + medianIndex, samples.begin() + percentileIndex, samples.end());
            LongJobSet_.clear();
            const auto medianJobTimeRemaining = MedianCompletionTime_ - now;
            for (auto it = samples.begin() + percentileIndex; it < samples.end(); ++it) {
                auto jobTimeRemaining = it->first - now;
                if (jobTimeRemaining.SecondsFloat() / medianJobTimeRemaining.SecondsFloat() >= ExcessFactor) {
                    // If we are going to split job at least into 2 + epsilon parts.
                    LongJobSet_.insert(it->second);
                }
            }
        }

        TInstant GetMedianCompletionTime() const
        {
            return MedianCompletionTime_;
        }

        bool IsLongJob(TJobId jobId) const
        {
            return LongJobSet_.contains(jobId);
        }

        void BuildStatistics(TFluentMap fluent) const
        {
            fluent
                .Item("median_remaining_duration").Value(MedianCompletionTime_ - GetInstant())
                .Item("next_update_time").Value(NextUpdateTime_);
        }

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, Config_);
            Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, JobIdToCompletionTime_);
            Persist<TSetSerializer<TDefaultSerializer, TUnsortedTag>>(context, LongJobSet_);
            Persist(context, NextUpdateTime_);
            Persist(context, MedianCompletionTime_);
        }

    private:
        TJobSplitterConfigPtr Config_;
        THashMap<TJobId, TInstant> JobIdToCompletionTime_;
        THashSet<TJobId> LongJobSet_;
        TInstant NextUpdateTime_;
        TInstant MedianCompletionTime_ = GetInstant();
    };

    class TRunningJob
    {
    public:
        //! Used only for persistence.
        TRunningJob() = default;

        TRunningJob(
            const TChunkStripeListPtr& inputStripeList,
            TOutputCookie cookie,
            TJobSplitter* owner,
            bool isInterruptible)
            : TotalRowCount_(inputStripeList->TotalRowCount)
            , TotalDataWeight_(inputStripeList->TotalDataWeight)
            , IsInterruptible_(isInterruptible)
            , Owner_(owner)
            , Cookie_(cookie)
        { }

        void Update(TJobTimeTracker* jobTimeTracker, const TJobSummary& summary)
        {
            PrepareDuration_ = summary.TimeStatistics.PrepareDuration.value_or(TDuration());
            auto downloadDuration = summary.TimeStatistics.ArtifactsDownloadDuration.value_or(TDuration());
            PrepareWithoutDownloadDuration_ = PrepareDuration_ >= downloadDuration
                ? PrepareDuration_ - downloadDuration
                : TDuration();
            if (!summary.TimeStatistics.ExecDuration) {
                return;
            }
            ExecDuration_ = summary.TimeStatistics.ExecDuration.value_or(TDuration());
            YT_VERIFY(summary.Statistics);

            if (!summary.TotalInputDataStatistics) {
                return;
            }

            RowCount_ = summary.TotalInputDataStatistics->row_count();
            if (RowCount_ == 0) {
                return;
            }

            SecondsPerRow_ = std::max(ExecDuration_.SecondsFloat() / RowCount_, 1e-12);
            RemainingDuration_ = RowCount_ < TotalRowCount_
                ? TDuration::Seconds((TotalRowCount_ - RowCount_) * SecondsPerRow_)
                : TDuration::Zero();
            CompletionTime_ = GetInstant() + RemainingDuration_;

            jobTimeTracker->SetSample(CompletionTime_, summary.Id);
            jobTimeTracker->Update();
        }

        void OnSplitRequested(TDuration splitTimeout)
        {
            if (!SplitDeadline_) {
                SplitDeadline_ = GetInstant() + splitTimeout;
            }
        }

        void BuildRunningJobInfo(TFluentMap fluent) const
        {
            fluent
                .Item("row_count").Value(RowCount_)
                .Item("interruptible").Value(IsInterruptible_)
                .Item("splittable").Value(Owner_->ChunkPool_->IsSplittable(Cookie_))
                .Item("total_row_count").Value(TotalRowCount_)
                .Item("seconds_per_row").Value(SecondsPerRow_)
                .Item("remaining_duration").Value(CompletionTime_ - GetInstant())
                .Item("interrupt_deadline").Value(SplitDeadline_);
        }

        bool IsSplittable() const
        {
            return IsInterruptible_ && Owner_->ChunkPool_->IsSplittable(Cookie_);
        }

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, Owner_);
            Persist(context, TotalRowCount_);
            Persist(context, TotalDataWeight_);
            Persist(context, PrepareWithoutDownloadDuration_);
            Persist(context, ExecDuration_);
            Persist(context, RemainingDuration_);
            Persist(context, CompletionTime_);
            Persist(context, RowCount_);
            Persist(context, SecondsPerRow_);
            Persist(context, Cookie_);
            Persist(context, IsInterruptible_);
            Persist(context, SplitDeadline_);
            Persist(context, PrepareDuration_);
        }

        DEFINE_BYVAL_RO_PROPERTY(i64, RowCount, 0);
        DEFINE_BYVAL_RO_PROPERTY(i64, TotalRowCount, 1);
        DEFINE_BYVAL_RO_PROPERTY(i64, TotalDataWeight, 1);
        DEFINE_BYVAL_RO_PROPERTY(TDuration, PrepareWithoutDownloadDuration);
        DEFINE_BYVAL_RO_PROPERTY(TDuration, ExecDuration);
        DEFINE_BYVAL_RO_PROPERTY(TDuration, RemainingDuration);
        DEFINE_BYVAL_RO_PROPERTY(bool, IsInterruptible);
        DEFINE_BYVAL_RO_PROPERTY(std::optional<TInstant>, SplitDeadline);
        DEFINE_BYVAL_RO_PROPERTY(TDuration, PrepareDuration);
        DEFINE_BYVAL_RW_PROPERTY(std::optional<TInstant>, NextLoggingTime);

    private:
        TJobSplitter* Owner_ = nullptr;

        TOutputCookie Cookie_;
        TInstant CompletionTime_;
        double SecondsPerRow_ = 0;
    };

    TJobSplitterConfigPtr Config_;

    bool CanSplitJobs_ = false;
    bool CanLaunchSpeculativeJobs_ = false;

    THashMap<TJobId, TRunningJob> RunningJobs_;
    TJobTimeTracker JobTimeTracker_;
    i64 MaxRunningJobCount_ = 0;
    TDuration SuccessJobPrepareDurationSum_;
    int SuccessJobCount_ = 0;
    IPersistentChunkPoolJobSplittingHost* ChunkPool_;
    NLogging::TSerializableLogger Logger;

    void OnJobFinished(const TJobSummary& summary)
    {
        auto it = RunningJobs_.find(summary.Id);
        YT_VERIFY(it != RunningJobs_.end());
        JobTimeTracker_.RemoveSample(summary.Id);
        RunningJobs_.erase(it);
        JobTimeTracker_.Update();
    }

    bool IsResidual() const
    {
        i64 runningJobCount = RunningJobs_.size();
        int smallJobCount = std::max(Config_->ResidualJobCountMinThreshold, static_cast<int>(Config_->ResidualJobFactor * MaxRunningJobCount_));
        return runningJobCount <= smallJobCount;
    }

    TDuration GetAverageSuccessJobPrepareDuration()
    {
        return SuccessJobCount_ == 0
            ? TDuration::Zero()
            : SuccessJobPrepareDurationSum_ / SuccessJobCount_;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TJobSplitter);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobSplitter> CreateJobSplitter(
    TJobSplitterConfigPtr config,
    IPersistentChunkPoolJobSplittingHost* chunkPool,
    const NLogging::TLogger& logger)
{
    return std::make_unique<TJobSplitter>(std::move(config), chunkPool, logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

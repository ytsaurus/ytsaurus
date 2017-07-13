#include "job_splitter.h"
#include "private.h"
#include "chunk_pool.h"
#include "config.h"

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TJobSplitter
    : public IJobSplitter
{
public:
    TJobSplitter(const TJobSplitterConfigPtr& config, const TOperationId& operationId)
        : Config_(config)
        , Statistics_(config)
        , Logger(OperationLogger)
    {
        YCHECK(config);

        Logger.AddTag("OperationId: %v", operationId);
    }

    virtual bool IsJobSplittable(const TJobId& jobId) const override
    {
        auto it = RunningJobs_.find(jobId);
        YCHECK(it != RunningJobs_.end());
        auto& job = it->second;
        return (IsResidual() || Statistics_.GetInterruptHint(jobId)) && job.IsSplittable(Config_);
    }

    virtual void OnJobStarted(
        const TJobId& jobId,
        const TChunkStripeListPtr& inputStripeList) override
    {
        RunningJobs_.emplace(jobId, TRunningJob(inputStripeList));
        MaxRunningJobCount_ = std::max<i64>(MaxRunningJobCount_, RunningJobs_.size());
    }

    void OnJobRunning(const TJobSummary& summary) override
    {
        auto it = RunningJobs_.find(summary.Id);
        YCHECK(it != RunningJobs_.end());
        auto& job = it->second;
        job.UpdateCompletionTime(&Statistics_, summary);
    }

    virtual void OnJobFailed(const TFailedJobSummary& summary) override
    {
        OnJobFinished(summary);
    }

    virtual void OnJobAborted(const TAbortedJobSummary& summary) override
    {
        OnJobFinished(summary);
    }

    virtual void OnJobCompleted(const TCompletedJobSummary& summary) override
    {
        OnJobFinished(summary);
    }

    virtual int EstimateJobCount(
        const TCompletedJobSummary& summary,
        i64 unreadRowCount) const override
    {
        double execDuration = summary.ExecDuration.Get(TDuration()).SecondsFloat();
        i64 processedRowCount = GetNumericValue(summary.Statistics, "/data/input/row_count");
        if (unreadRowCount <= 1 || processedRowCount == 0 || execDuration == 0.0) {
            return 1;
        }
        double prepareDuration = summary.PrepareDuration.Get(TDuration()).SecondsFloat() -
            summary.DownloadDuration.Get(TDuration()).SecondsFloat();
        double expectedExecDuration = execDuration / processedRowCount * unreadRowCount;

        auto getMedianCompletionDuration = [&] () {
            auto medianCompletionTime = Statistics_.GetMedianCompletionTime();
            if (!IsResidual() && medianCompletionTime) {
                return CpuDurationToDuration(medianCompletionTime).SecondsFloat() -
                    CpuDurationToDuration(GetCpuInstant()).SecondsFloat();
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

        int jobCount = Clamp<int>(
            std::min(static_cast<i64>(expectedExecDuration / minJobTime), unreadRowCount),
            1,
            Config_->MaxJobsPerSplit);

        LOG_DEBUG("Estimated optimal job count for unread data slices "
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

    virtual void BuildJobSplitterInfo(NYson::IYsonConsumer* consumer) const override
    {
        BuildYsonMapFluently(consumer)
            .Item("build_time").Value(CpuInstantToInstant(GetCpuInstant()))
            .Item("running_job_count").Value(RunningJobs_.size())
            .Item("max_running_job_count").Value(MaxRunningJobCount_)
            .Item("running_jobs").DoMapFor(RunningJobs_,
                [&] (TFluentMap fluent, const std::pair<TJobId, TRunningJob>& pair) {
                    const auto& job = pair.second;
                    fluent
                        .Item(ToString(pair.first)).BeginMap()
                            .Do(BIND(&TRunningJob::BuildRunningJobInfo, &job))
                            .Item("candidate").Value(Statistics_.GetInterruptHint(pair.first))
                        .EndMap();
                })
            .Item("statistics").BeginMap()
                .Do(BIND(&TStatistics::BuildStatistics, &Statistics_))
            .EndMap()
            .Item("config").Value(Config_);
    }

private:
    class TStatistics
    {
    public:
        explicit TStatistics(const TJobSplitterConfigPtr& config)
            : Config_(config)
        { }

        void AddSample(TCpuInstant completionTime, const TJobId& jobId)
        {
            CompletionTimeSet_.insert({completionTime, jobId});
        }

        void RemoveSample(TCpuInstant completionTime, const TJobId& jobId)
        {
            if (completionTime != 0) {
                auto it = CompletionTimeSet_.find(std::make_pair(completionTime, jobId));
                YCHECK(it != CompletionTimeSet_.end());
                CompletionTimeSet_.erase(it);
            }
        }

        void Update()
        {
            if (CompletionTimeSet_.empty()) {
                MedianCompletionTime_ = 0;
                return;
            }
            if (GetCpuInstant() > NextUpdateTime_) {
                NextUpdateTime_ = GetCpuInstant() + DurationToCpuDuration(Config_->UpdatePeriod);

                int medianIndex = CompletionTimeSet_.size() / 2;
                int percentileIndex = static_cast<int>(std::floor(static_cast<double>(CompletionTimeSet_.size()) * Config_->CandidatePercentile));
                percentileIndex = std::max(percentileIndex, medianIndex);

                std::vector<std::pair<TCpuInstant, TJobId>> samples(CompletionTimeSet_.begin(), CompletionTimeSet_.end());
                std::nth_element(samples.begin(), samples.begin() + medianIndex, samples.end());
                MedianCompletionTime_ = samples[medianIndex].first;

                std::nth_element(samples.begin() + medianIndex, samples.begin() + percentileIndex, samples.end());
                InterruptCandidateSet_.clear();
                for (auto it = samples.begin() + percentileIndex; it < samples.end(); ++it) {
                    InterruptCandidateSet_.insert(it->second);
                }
            }
        }

        TCpuInstant GetMedianCompletionTime() const
        {
            return MedianCompletionTime_;
        }

        bool GetInterruptHint(const TJobId& jobId) const
        {
            return InterruptCandidateSet_.find(jobId) != InterruptCandidateSet_.end();
        }

        void BuildStatistics(NYson::IYsonConsumer* consumer) const
        {
            BuildYsonMapFluently(consumer)
                .Item("median_remaining_duration").Value(SecondsFromNow(MedianCompletionTime_))
                .Item("next_update_time").Value(CpuInstantToInstant(NextUpdateTime_));
        }

    private:
        TJobSplitterConfigPtr Config_;
        yhash_set<std::pair<TCpuInstant, TJobId>> CompletionTimeSet_;
        yhash_set<TJobId> InterruptCandidateSet_;
        TCpuInstant NextUpdateTime_ = 0;
        TCpuInstant MedianCompletionTime_ = GetCpuInstant();
    };

    class TRunningJob
    {
    public:
        explicit TRunningJob(const TChunkStripeListPtr& inputStripeList)
            : TotalRowCount_(inputStripeList->TotalRowCount)
            , TotalDataSize_(inputStripeList->TotalDataSize)
        { }

        void UpdateCompletionTime(TStatistics* statistics, const TJobSummary& summary)
        {
            PrepareWithoutDownloadDuration_ = summary.PrepareDuration.Get(TDuration()) - summary.DownloadDuration.Get(TDuration());
            if (!summary.ExecDuration) {
                return;
            }
            ExecDuration_ = summary.ExecDuration.Get(TDuration());
            RowCount_ = FindNumericValue(summary.Statistics, "/data/input/row_count").Get(0);
            if (RowCount_ == 0) {
                return;
            }
            SecondsPerRow_ = std::max(ExecDuration_.SecondsFloat() / RowCount_, 1e-12);
            if (RowCount_ < TotalRowCount_) {
                RemainingDuration_ = TDuration::Seconds((TotalRowCount_ - RowCount_) * SecondsPerRow_);
            } else {
                RemainingDuration_ = TDuration();
            }
            statistics->RemoveSample(CompletionTime_, summary.Id);
            CompletionTime_ = GetCpuInstant() + DurationToCpuDuration(RemainingDuration_);
            statistics->AddSample(CompletionTime_, summary.Id);
            statistics->Update();
        }

        bool IsSplittable(const TJobSplitterConfigPtr& config) const
        {
            auto minJobTime = std::max(
                config->MinJobTime,
                PrepareWithoutDownloadDuration_ * config->ExecToPrepareTimeRatio);
            return ExecDuration_ > minJobTime &&
                RemainingDuration_ > minJobTime &&
                TotalDataSize_ > config->MinTotalDataSize;
        }

        TCpuInstant GetCompletionTime() const
        {
            return CompletionTime_;
        }

        i64 GetTotalRowCount() const
        {
            return TotalRowCount_;
        }

        void BuildRunningJobInfo(NYson::IYsonConsumer* consumer) const
        {
            BuildYsonMapFluently(consumer)
                .Item("row_count").Value(RowCount_)
                .Item("total_row_count").Value(TotalRowCount_)
                .Item("seconds_per_row").Value(SecondsPerRow_)
                .Item("remaining_duration").Value(SecondsFromNow(CompletionTime_));
        }

    private:
        i64 TotalRowCount_ = 1.0;
        i64 TotalDataSize_ = 1.0;
        TDuration PrepareWithoutDownloadDuration_;
        TDuration ExecDuration_;
        TDuration RemainingDuration_;
        TCpuInstant CompletionTime_ = 0;
        i64 RowCount_ = 0;
        double SecondsPerRow_ = 0;
    };

    TJobSplitterConfigPtr Config_;
    yhash<TJobId, TRunningJob> RunningJobs_;
    TStatistics Statistics_;
    i64 MaxRunningJobCount_ = 0;
    NLogging::TLogger Logger;

    void OnJobFinished(const TJobSummary& summary)
    {
        auto it = RunningJobs_.find(summary.Id);
        YCHECK(it != RunningJobs_.end());
        const auto& job = it->second;
        Statistics_.RemoveSample(job.GetCompletionTime(), summary.Id);
        RunningJobs_.erase(it);
        Statistics_.Update();
    }

    bool IsResidual() const
    {
        i64 runningJobCount = RunningJobs_.size();
        i64 smallJobCount = static_cast<i64>(std::sqrt(static_cast<double>(MaxRunningJobCount_)));
        return runningJobCount <= smallJobCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobSplitter> CreateJobSplitter(const TJobSplitterConfigPtr& config, const TOperationId& operationId)
{
    return std::unique_ptr<IJobSplitter>(new TJobSplitter(config, operationId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


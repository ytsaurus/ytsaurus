#include "job_splitter.h"
#include "private.h"

#include <yt/server/chunk_pools/chunk_pool.h>

#include <yt/server/scheduler/config.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NControllerAgent {

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

    TJobSplitter(const TJobSplitterConfigPtr& config, const TOperationId& operationId)
        : Config_(config)
        , Statistics_(config)
        , OperationId_(operationId)
        , Logger(NLogging::TLogger(ControllerLogger)
            .AddTag("OperationId: %v", OperationId_))
    {
        YCHECK(Config_);
    }

    virtual bool IsJobSplittable(const TJobId& jobId) const override
    {
        auto it = RunningJobs_.find(jobId);
        YCHECK(it != RunningJobs_.end());
        const auto& job = it->second;
        auto residual = IsResidual();
        auto interruptHint = Statistics_.GetInterruptHint(jobId);
        auto isSplittable = job.IsSplittable(Config_);
        LOG_TRACE("Checking if job is splittable (Residual: %v, GetInterruptHint: %v, IsSplittable: %v)",
            residual,
            interruptHint,
            isSplittable);
        return (residual || interruptHint) && isSplittable;
    }

    virtual void OnJobStarted(
        const TJobId& jobId,
        const TChunkStripeListPtr& inputStripeList) override
    {
        RunningJobs_.emplace(jobId, TRunningJob(inputStripeList, this));
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
        YCHECK(summary.Statistics);
        i64 processedRowCount = GetNumericValue(*summary.Statistics, "/data/input/row_count");
        if (unreadRowCount <= 1 || processedRowCount == 0 || execDuration == 0.0) {
            return 1;
        }
        double prepareDuration = summary.PrepareDuration.Get(TDuration()).SecondsFloat() -
            summary.DownloadDuration.Get(TDuration()).SecondsFloat();
        double expectedExecDuration = execDuration / processedRowCount * unreadRowCount;

        auto getMedianCompletionDuration = [&] () {
            auto medianCompletionTime = Statistics_.GetMedianCompletionTime();
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

    virtual void BuildJobSplitterInfo(TFluentMap fluent) const override
    {
        fluent
            .Item("build_time").Value(GetInstant())
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

    virtual void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;

        Persist(context, Config_);
        Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, RunningJobs_);
        Persist(context, Statistics_);
        Persist(context, MaxRunningJobCount_);
        Persist(context, OperationId_);

        if (context.IsLoad()) {
            Logger = NLogging::TLogger(ControllerLogger)
                .AddTag("OperationId: %v", OperationId_);
        }
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TJobSplitter, 0x1ddf34ff);

    class TStatistics
    {
    public:
        //! Used only for persistence.
        TStatistics() = default;

        explicit TStatistics(const TJobSplitterConfigPtr& config)
            : Config_(config)
        { }

        void SetSample(TInstant completionTime, const TJobId& jobId)
        {
            JobIdToCompletionTime_[jobId] = completionTime;
        }

        void RemoveSample(const TJobId& jobId)
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

            if (GetInstant() < NextUpdateTime_) {
                return;
            }

            NextUpdateTime_ = GetInstant() + Config_->UpdatePeriod;

            int medianIndex = JobIdToCompletionTime_.size() / 2;
            int percentileIndex = static_cast<int>(std::floor(static_cast<double>(JobIdToCompletionTime_.size()) * Config_->CandidatePercentile));
            percentileIndex = std::max(percentileIndex, medianIndex);

            std::vector<std::pair<TInstant, TJobId>> samples;
            samples.reserve(JobIdToCompletionTime_.size());
            for (const auto& pair : JobIdToCompletionTime_) {
                samples.push_back({pair.second, pair.first});
            }
            std::nth_element(samples.begin(), samples.begin() + medianIndex, samples.end());
            MedianCompletionTime_ = samples[medianIndex].first;

            std::nth_element(samples.begin() + medianIndex, samples.begin() + percentileIndex, samples.end());
            InterruptCandidateSet_.clear();
            for (auto it = samples.begin() + percentileIndex; it < samples.end(); ++it) {
                if (it->first.SecondsFloat() / MedianCompletionTime_.SecondsFloat() >= ExcessFactor) {
                    // If we are going to split job at least into 2 + epsilon parts.
                    InterruptCandidateSet_.insert(it->second);
                }
            }
        }

        TInstant GetMedianCompletionTime() const
        {
            return MedianCompletionTime_;
        }

        bool GetInterruptHint(const TJobId& jobId) const
        {
            return InterruptCandidateSet_.find(jobId) != InterruptCandidateSet_.end();
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
            Persist<TSetSerializer<TDefaultSerializer, TUnsortedTag>>(context, InterruptCandidateSet_);
            Persist(context, NextUpdateTime_);
            Persist(context, MedianCompletionTime_);
        }

    private:
        TJobSplitterConfigPtr Config_;
        THashMap<TJobId, TInstant> JobIdToCompletionTime_;
        THashSet<TJobId> InterruptCandidateSet_;
        TInstant NextUpdateTime_;
        TInstant MedianCompletionTime_ = GetInstant();
    };

    class TRunningJob
    {
    public:
        //! Used only for persistence.
        TRunningJob() = default;

        TRunningJob(const TChunkStripeListPtr& inputStripeList, TJobSplitter* owner)
            : Owner_(owner)
            , TotalRowCount_(inputStripeList->TotalRowCount)
            , TotalDataWeight_(inputStripeList->TotalDataWeight)
            , IsSplittable_(inputStripeList->IsSplittable)
        { }

        void UpdateCompletionTime(TStatistics* statistics, const TJobSummary& summary)
        {
            PrepareWithoutDownloadDuration_ = summary.PrepareDuration.Get(TDuration()) - summary.DownloadDuration.Get(TDuration());
            if (!summary.ExecDuration) {
                return;
            }

            ExecDuration_ = summary.ExecDuration.Get(TDuration());
            YCHECK(summary.Statistics);

            RowCount_ = FindNumericValue(*summary.Statistics, "/data/input/row_count").Get(0);
            if (RowCount_ == 0) {
                return;
            }

            SecondsPerRow_ = std::max(ExecDuration_.SecondsFloat() / RowCount_, 1e-12);
            RemainingDuration_ = RowCount_ < TotalRowCount_
                ? TDuration::Seconds((TotalRowCount_ - RowCount_) * SecondsPerRow_)
                : TDuration::Zero();
            CompletionTime_ = GetInstant() + RemainingDuration_;

            statistics->SetSample(CompletionTime_, summary.Id);
            statistics->Update();
        }

        bool IsSplittable(const TJobSplitterConfigPtr& config) const
        {
            auto minJobTime = std::max(
                config->MinJobTime,
                PrepareWithoutDownloadDuration_ * config->ExecToPrepareTimeRatio);
            const auto& Logger = Owner_->Logger;
            LOG_TRACE("Checking if job is splittable (IsSplittable: %v, RowCount: %v, ExecDuration: %v, "
                "RemainingDuration: %v, MinJobTime: %v, TotalDataWeight: %v, MinTotalDataWeight: %v)",
                IsSplittable_,
                RowCount_,
                ExecDuration_,
                RemainingDuration_,
                minJobTime,
                TotalDataWeight_,
                config->MinTotalDataWeight);
            return IsSplittable_ &&
                RowCount_ > 0 &&                     // don't split job that hasn't read anything;
                ExecDuration_ > minJobTime &&
                RemainingDuration_ > minJobTime &&
                TotalDataWeight_ > config->MinTotalDataWeight;
        }

        TInstant GetCompletionTime() const
        {
            return CompletionTime_;
        }

        i64 GetTotalRowCount() const
        {
            return TotalRowCount_;
        }

        void BuildRunningJobInfo(TFluentMap fluent) const
        {
            fluent
                .Item("row_count").Value(RowCount_)
                .Item("splittable").Value(IsSplittable_)
                .Item("total_row_count").Value(TotalRowCount_)
                .Item("seconds_per_row").Value(SecondsPerRow_)
                .Item("remaining_duration").Value(CompletionTime_ - GetInstant());
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
            Persist(context, IsSplittable_);
        }

    private:
        TJobSplitter* Owner_ = nullptr;

        i64 TotalRowCount_ = 1;
        i64 TotalDataWeight_ = 1;
        TDuration PrepareWithoutDownloadDuration_;
        TDuration ExecDuration_;
        TDuration RemainingDuration_;
        TInstant CompletionTime_;
        i64 RowCount_ = 0;
        double SecondsPerRow_ = 0;
        bool IsSplittable_ = true;
    };

    TJobSplitterConfigPtr Config_;

    THashMap<TJobId, TRunningJob> RunningJobs_;
    TStatistics Statistics_;
    i64 MaxRunningJobCount_ = 0;

    TOperationId OperationId_;

    NLogging::TLogger Logger;

    void OnJobFinished(const TJobSummary& summary)
    {
        auto it = RunningJobs_.find(summary.Id);
        YCHECK(it != RunningJobs_.end());
        Statistics_.RemoveSample(summary.Id);
        RunningJobs_.erase(it);
        Statistics_.Update();
    }

    bool IsResidual() const
    {
        constexpr i64 MinSmallJobCount = 10;

        i64 runningJobCount = RunningJobs_.size();
        i64 smallJobCount = std::max(MinSmallJobCount, static_cast<i64>(std::sqrt(static_cast<double>(MaxRunningJobCount_))));
        return runningJobCount <= smallJobCount;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TJobSplitter);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobSplitter> CreateJobSplitter(const TJobSplitterConfigPtr& config, const TOperationId& operationId)
{
    return std::unique_ptr<IJobSplitter>(new TJobSplitter(config, operationId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT


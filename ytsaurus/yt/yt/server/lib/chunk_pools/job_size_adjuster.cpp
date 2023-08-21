#include "job_size_adjuster.h"
#include "config.h"

namespace NYT::NChunkPools {

using namespace NScheduler;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

class TJobSizeAdjuster
    : public IJobSizeAdjuster
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TJobSizeAdjuster() = default;

    TJobSizeAdjuster(
        i64 dataWeightPerJob,
        const TJobSizeAdjusterConfigPtr& config)
        : DataWeightPerJob_(static_cast<double>(dataWeightPerJob))
        , MinJobTime_(static_cast<double>(config->MinJobTime.MicroSeconds()))
        , MaxJobTime_(static_cast<double>(config->MaxJobTime.MicroSeconds()))
        , ExecToPrepareTimeRatio_(config->ExecToPrepareTimeRatio)
    { }

    void UpdateStatistics(const TCompletedJobSummary& summary) override
    {
        if (!summary.Abandoned) {
            YT_VERIFY(summary.TotalInputDataStatistics);
            UpdateStatistics(
                summary.TotalInputDataStatistics->data_weight(),
                summary.TimeStatistics.PrepareDuration.value_or(TDuration()) - summary.TimeStatistics.ArtifactsDownloadDuration.value_or(TDuration()),
                summary.TimeStatistics.ExecDuration.value_or(TDuration()));
        }
    }

    void UpdateStatistics(i64 jobDataWeight, TDuration prepareDuration, TDuration execDuration) override
    {
        Statistics_.AddSample(jobDataWeight, prepareDuration, execDuration);

        if (!Statistics_.IsEmpty()) {
            double idealExecTime = std::max(MinJobTime_, ExecToPrepareTimeRatio_ * Statistics_.GetMeanPrepareTime());
            idealExecTime = std::min(idealExecTime, MaxJobTime_);

            double idealDataWeight = idealExecTime / Statistics_.GetMeanExecTimePerByte();

            DataWeightPerJob_ = ClampVal(
                idealDataWeight,
                DataWeightPerJob_,
                DataWeightPerJob_ * JobSizeBoostFactor);
        }
    }

    i64 GetDataWeightPerJob() const override
    {
        return static_cast<i64>(DataWeightPerJob_);
    }

    void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;
        Persist(context, DataWeightPerJob_);
        Persist(context, MinJobTime_);
        Persist(context, MaxJobTime_);
        Persist(context, ExecToPrepareTimeRatio_);
        Persist(context, Statistics_);
    }

private:
    class TStatistics
    {
    public:
        void AddSample(i64 jobDataSize, TDuration prepareDuration, TDuration execDuration)
        {
            double dataSize = static_cast<double>(jobDataSize);
            double prepareTime = static_cast<double>(prepareDuration.MicroSeconds());
            double execTime = static_cast<double>(execDuration.MicroSeconds());

            if (dataSize > 0 && prepareTime > 0 && execTime > 0) {
                ++Count_;
                TotalPrepareTime_ += prepareTime;
                TotalExecTime_ += execTime;
                TotalDataWeight_ += dataSize;
                MaxDataWeight_ = std::max(MaxDataWeight_, dataSize);
            }
        }

        double GetMeanPrepareTime() const
        {
            return TotalPrepareTime_ / Count_;
        }

        double GetMeanExecTimePerByte() const
        {
            return std::max(TotalExecTime_ / TotalDataWeight_, 1e-12);
        }

        bool IsEmpty() const
        {
            return Count_ == 0;
        }

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Count_);
            Persist(context, TotalPrepareTime_);
            Persist(context, TotalExecTime_);
            Persist(context, TotalDataWeight_);
            Persist(context, MaxDataWeight_);
        }

    private:
        int Count_ = 0;
        double TotalPrepareTime_ = 0.0;
        double TotalExecTime_ = 0.0;
        double TotalDataWeight_ = 0.0;
        double MaxDataWeight_ = 0.0;
    };

    DECLARE_DYNAMIC_PHOENIX_TYPE(TJobSizeAdjuster, 0xf8338721);

    double DataWeightPerJob_ = 0.0;
    double MinJobTime_ = 0.0;
    double MaxJobTime_ = 0.0;
    double ExecToPrepareTimeRatio_ = 0.0;

    TStatistics Statistics_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TJobSizeAdjuster);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobSizeAdjuster> CreateJobSizeAdjuster(
    i64 dataWeightPerJob,
    const TJobSizeAdjusterConfigPtr& config)
{
    return std::make_unique<TJobSizeAdjuster>(
        dataWeightPerJob,
        config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools


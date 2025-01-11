#include "job_size_adjuster.h"
#include "config.h"

namespace NYT::NChunkPools {

using namespace NScheduler;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

class TJobSizeAdjuster
    : public IJobSizeAdjuster
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

    private:
        int Count_ = 0;
        double TotalPrepareTime_ = 0.0;
        double TotalExecTime_ = 0.0;
        double TotalDataWeight_ = 0.0;
        double MaxDataWeight_ = 0.0;

        PHOENIX_DECLARE_TYPE(TStatistics, 0xab66ec65);
    };

    double DataWeightPerJob_ = 0.0;
    double MinJobTime_ = 0.0;
    double MaxJobTime_ = 0.0;
    double ExecToPrepareTimeRatio_ = 0.0;

    TStatistics Statistics_;

    // COMPAT(coteeq): This field is not used, but I cannot simply drop it because of phoenix2's quirks.
    bool EnableJobShrinking_ = false;

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TJobSizeAdjuster, 0xf8338721);
};

void TJobSizeAdjuster::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, DataWeightPerJob_);
    PHOENIX_REGISTER_FIELD(2, MinJobTime_);
    PHOENIX_REGISTER_FIELD(3, MaxJobTime_);
    PHOENIX_REGISTER_FIELD(4, ExecToPrepareTimeRatio_);
    PHOENIX_REGISTER_FIELD(5, Statistics_);

    // COMPAT(coteeq)
    PHOENIX_REGISTER_FIELD(6, EnableJobShrinking_,
        .SinceVersion(ESnapshotVersion::DisableShrinkingJobs));
}

PHOENIX_DEFINE_TYPE(TJobSizeAdjuster);

////////////////////////////////////////////////////////////////////////////////

void TJobSizeAdjuster::TStatistics::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Count_);
    PHOENIX_REGISTER_FIELD(2, TotalPrepareTime_);
    PHOENIX_REGISTER_FIELD(3, TotalExecTime_);
    PHOENIX_REGISTER_FIELD(4, TotalDataWeight_);
    PHOENIX_REGISTER_FIELD(5, MaxDataWeight_);
}

PHOENIX_DEFINE_TYPE(TJobSizeAdjuster::TStatistics);

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

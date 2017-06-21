#include "job_size_adjuster.h"

#include "config.h"
#include "private.h"

namespace NYT {
namespace NControllerAgent {

using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TJobSizeAdjuster
    : public IJobSizeAdjuster
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TJobSizeAdjuster()
    { }

    TJobSizeAdjuster(
        i64 dataSizePerJob,
        const TJobSizeAdjusterConfigPtr& config)
        : DataSizePerJob_(static_cast<double>(dataSizePerJob))
        , MinJobTime_(static_cast<double>(config->MinJobTime.MicroSeconds()))
        , MaxJobTime_(static_cast<double>(config->MaxJobTime.MicroSeconds()))
        , ExecToPrepareTimeRatio_(config->ExecToPrepareTimeRatio)
    { }

    virtual void UpdateStatistics(const TCompletedJobSummary& summary) override
    {
        if (!summary.Abandoned) {
            YCHECK(summary.Statistics);
            UpdateStatistics(
                GetNumericValue(*summary.Statistics, "/data/input/uncompressed_data_size"),
                summary.PrepareDuration.Get(TDuration()) - summary.DownloadDuration.Get(TDuration()),
                summary.ExecDuration.Get(TDuration()));
        }
    }

    virtual void UpdateStatistics(i64 jobDataSize, TDuration prepareDuration, TDuration execDuration) override
    {
        Statistics_.AddSample(jobDataSize, prepareDuration, execDuration);

        if (!Statistics_.IsEmpty()) {
            double idealExecTime = std::max(MinJobTime_, ExecToPrepareTimeRatio_ * Statistics_.GetMeanPrepareTime());
            idealExecTime = std::min(idealExecTime, MaxJobTime_);

            double idealDataSize = idealExecTime / Statistics_.GetMeanExecTimePerByte();

            DataSizePerJob_ = ClampVal(
                idealDataSize,
                DataSizePerJob_,
                DataSizePerJob_ * JobSizeBoostFactor);
        }
    }

    virtual i64 GetDataSizePerJob() const override
    {
        return static_cast<i64>(DataSizePerJob_);
    }

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, DataSizePerJob_);
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
                PrepareTimeTotal_ += prepareTime;
                ExecTimeTotal_ += execTime;
                DataSizeTotal_ += dataSize;
                DataSizeMax_ = std::max(DataSizeMax_, dataSize);
            }
        }

        double GetMeanPrepareTime() const
        {
            return PrepareTimeTotal_ / Count_;
        }

        double GetMeanExecTimePerByte() const
        {
            return std::max(ExecTimeTotal_ / DataSizeTotal_, 1e-12);
        }

        bool IsEmpty() const
        {
            return Count_ == 0;
        }

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;
            Persist(context, Count_);
            Persist(context, PrepareTimeTotal_);
            Persist(context, ExecTimeTotal_);
            Persist(context, DataSizeTotal_);
            Persist(context, DataSizeMax_);
        }

    private:
        int Count_ = 0;
        double PrepareTimeTotal_ = 0.0;
        double ExecTimeTotal_ = 0.0;
        double DataSizeTotal_ = 0.0;
        double DataSizeMax_ = 0.0;
    };

    DECLARE_DYNAMIC_PHOENIX_TYPE(TJobSizeAdjuster, 0xf8338721);

    double DataSizePerJob_ = 0.0;
    double MaxDataSizePerJob_ = 0.0;
    double MinJobTime_ = 0.0;
    double MaxJobTime_ = 0.0;
    double ExecToPrepareTimeRatio_ = 0.0;

    TStatistics Statistics_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TJobSizeAdjuster);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IJobSizeAdjuster> CreateJobSizeAdjuster(
    i64 dataSizePerJob,
    const TJobSizeAdjusterConfigPtr& config)
{
    return std::unique_ptr<IJobSizeAdjuster>(new TJobSizeAdjuster(
        dataSizePerJob,
        config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT


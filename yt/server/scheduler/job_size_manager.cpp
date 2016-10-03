#include "job_size_manager.h"
#include "private.h"
#include "config.h"

namespace NYT {
namespace NScheduler {

using namespace NLogging;

////////////////////////////////////////////////////////////////////

class TJobSizeManager
    : public IJobSizeManager
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TJobSizeManager()
    { }

    TJobSizeManager(
        i64 dataSizePerJob,
        i64 maxDataSizePerJob,
        TJobSizeManagerConfigPtr config)
        : IdealDataSizePerJob_(static_cast<double>(dataSizePerJob))
        , MaxDataSizePerJob_(static_cast<double>(maxDataSizePerJob))
        , DesiredMinJobTime_(static_cast<double>(config->MinJobTime.MicroSeconds()))
        , DesiredExecToPrepareTimeRatio_(config->ExecToPrepareTimeRatio)
    { }

    virtual void OnJobCompleted(const TCompletedJobSummary& summary) override
    {
        if (!summary.Abandoned) {
            OnJobCompleted(
                GetNumericValue(summary.Statistics, "/data/input/uncompressed_data_size"),
                summary.PrepareDuration.Get(TDuration()) - summary.DownloadDuration.Get(TDuration()),
                summary.ExecDuration.Get(TDuration()));
        }
    }

    virtual void OnJobCompleted(i64 jobDataSize, TDuration prepareDuration, TDuration execDuration) override
    {
        Statistics_.AddSample(jobDataSize, prepareDuration, execDuration);

        if (!Statistics_.IsEmpty()) {
            double idealExecTime = std::max(DesiredMinJobTime_, DesiredExecToPrepareTimeRatio_ * Statistics_.GetMeanPrepareTime());
            double idealDataSize = idealExecTime / Statistics_.GetMeanExecTimePerByte();
            IdealDataSizePerJob_ = ClampVal(
                idealDataSize,
                IdealDataSizePerJob_,
                std::min(IdealDataSizePerJob_ * JobSizeBoostFactor, MaxDataSizePerJob_));
        }
    }

    virtual i64 GetIdealDataSizePerJob() const
    {
        return static_cast<i64>(IdealDataSizePerJob_);
    }

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;
        Persist(context, IdealDataSizePerJob_);
        Persist(context, MaxDataSizePerJob_);
        Persist(context, DesiredMinJobTime_);
        Persist(context, DesiredExecToPrepareTimeRatio_);
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

    DECLARE_DYNAMIC_PHOENIX_TYPE(TJobSizeManager, 0xf8338721);

    double IdealDataSizePerJob_ = 0.0;
    double MaxDataSizePerJob_ = 0.0;
    double DesiredMinJobTime_ = 0.0;
    double DesiredExecToPrepareTimeRatio_ = 0.0;
    TStatistics Statistics_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TJobSizeManager);

std::unique_ptr<IJobSizeManager> CreateJobSizeManager(
    i64 dataSizePerJob,
    i64 maxDataSizePerJob,
    TJobSizeManagerConfigPtr config)
{
    return std::unique_ptr<IJobSizeManager>(new TJobSizeManager(
        dataSizePerJob,
        maxDataSizePerJob,
        config));
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

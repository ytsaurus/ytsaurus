#include "job_size_constraints.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TExplicitJobSizeConstraints
    : public IJobSizeConstraints
{
public:
    //! Used only for persistence.
    TExplicitJobSizeConstraints()
    { }

    TExplicitJobSizeConstraints(
        bool canAdjustDataWeightPerJob,
        bool isExplicitJobCount,
        int jobCount,
        i64 dataWeightPerJob,
        i64 primaryDataWeightPerJob,
        i64 maxDataSlicesPerJob,
        i64 maxDataWeightPerJob,
        i64 maxPrimaryDataWeightPerJob,
        i64 inputSliceDataWeight,
        i64 inputSliceRowCount,
        std::optional<double> samplingRate,
        i64 samplingDataWeightPerJob,
        i64 samplingPrimaryDataWeightPerJob,
        i64 maxBuildRetryCount,
        double dataWeightPerJobRetryFactor)
        : CanAdjustDataWeightPerJob_(canAdjustDataWeightPerJob)
        , IsExplicitJobCount_(isExplicitJobCount)
        , JobCount_(jobCount)
        , DataWeightPerJob_(dataWeightPerJob)
        , PrimaryDataWeightPerJob_(primaryDataWeightPerJob)
        , MaxDataSlicesPerJob_(maxDataSlicesPerJob)
        , MaxDataWeightPerJob_(maxDataWeightPerJob)
        , MaxPrimaryDataWeightPerJob_(maxPrimaryDataWeightPerJob)
        , InputSliceDataWeight_(inputSliceDataWeight)
        , InputSliceRowCount_(inputSliceRowCount)
        , SamplingRate_(samplingRate)
        , SamplingDataWeightPerJob_(samplingDataWeightPerJob)
        , SamplingPrimaryDataWeightPerJob_(samplingPrimaryDataWeightPerJob)
        , MaxBuildRetryCount_(maxBuildRetryCount)
        , DataWeightPerJobRetryFactor_(dataWeightPerJobRetryFactor)
    {
        // COMPAT(max42): remove this after YT-10666 (and put YT_VERIFY about job having non-empty
        // input somewhere in controller).
        MaxDataWeightPerJob_ = std::max<i64>(1, MaxDataWeightPerJob_);
        DataWeightPerJob_ = std::max<i64>(1, DataWeightPerJob_);
        PrimaryDataWeightPerJob_ = std::max<i64>(1, PrimaryDataWeightPerJob_);
    }

    virtual bool CanAdjustDataWeightPerJob() const override
    {
        return CanAdjustDataWeightPerJob_;
    }

    virtual bool IsExplicitJobCount() const override
    {
        return IsExplicitJobCount_;
    }

    virtual int GetJobCount() const override
    {
        return JobCount_;
    }

    virtual i64 GetDataWeightPerJob() const override
    {
        return DataWeightPerJob_;
    }

    virtual i64 GetMaxDataSlicesPerJob() const override
    {
        return MaxDataSlicesPerJob_;
    }

    virtual i64 GetPrimaryDataWeightPerJob() const override
    {
        return PrimaryDataWeightPerJob_;
    }

    virtual i64 GetMaxDataWeightPerJob() const override
    {
        return MaxDataWeightPerJob_;
    }

    virtual i64 GetMaxPrimaryDataWeightPerJob() const override
    {
        return MaxPrimaryDataWeightPerJob_;
    }

    virtual i64 GetInputSliceDataWeight() const override
    {
        return InputSliceDataWeight_;
    }

    virtual i64 GetInputSliceRowCount() const override
    {
        return InputSliceRowCount_;
    }

    virtual std::optional<double> GetSamplingRate() const override
    {
        return SamplingRate_;
    }

    virtual i64 GetSamplingDataWeightPerJob() const override
    {
        YT_VERIFY(SamplingRate_);
        return SamplingDataWeightPerJob_;
    }

    virtual i64 GetSamplingPrimaryDataWeightPerJob() const override
    {
        YT_VERIFY(SamplingRate_);
        return SamplingPrimaryDataWeightPerJob_;
    }

    virtual double GetDataWeightPerJobRetryFactor() const override
    {
        return DataWeightPerJobRetryFactor_;
    }

    virtual i64 GetMaxBuildRetryCount() const override
    {
        return MaxBuildRetryCount_;
    }

    virtual void UpdateInputDataWeight(i64 inputDataWeight) override
    {
        // Do nothing. Explicit job size constraints do not care about input data weight.
    }

    virtual void Persist(const NPhoenix::TPersistenceContext& context) override
    {
        using NYT::Persist;
        Persist(context, CanAdjustDataWeightPerJob_);
        Persist(context, IsExplicitJobCount_);
        Persist(context, JobCount_);
        Persist(context, DataWeightPerJob_);
        Persist(context, PrimaryDataWeightPerJob_);
        Persist(context, MaxDataSlicesPerJob_);
        Persist(context, MaxDataWeightPerJob_);
        Persist(context, MaxPrimaryDataWeightPerJob_);
        Persist(context, InputSliceDataWeight_);
        Persist(context, InputSliceRowCount_);
        Persist(context, SamplingRate_);
        Persist(context, SamplingDataWeightPerJob_);
        Persist(context, SamplingPrimaryDataWeightPerJob_);
        Persist(context, MaxBuildRetryCount_);
        Persist(context, DataWeightPerJobRetryFactor_);

        // COMPAT(max42): remove this after YT-10666 (and put YT_VERIFY about job having non-empty
        // input somewhere in controller).
        if (context.IsLoad()) {
            MaxDataWeightPerJob_ = std::max<i64>(1, MaxDataWeightPerJob_);
            DataWeightPerJob_ = std::max<i64>(1, DataWeightPerJob_);
            PrimaryDataWeightPerJob_ = std::max<i64>(1, PrimaryDataWeightPerJob_);
        }
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TExplicitJobSizeConstraints, 0xab6bc389);

    bool CanAdjustDataWeightPerJob_;
    bool IsExplicitJobCount_;
    int JobCount_;
    i64 DataWeightPerJob_;
    i64 PrimaryDataWeightPerJob_;
    i64 MaxDataSlicesPerJob_;
    i64 MaxDataWeightPerJob_;
    i64 MaxPrimaryDataWeightPerJob_;
    i64 InputSliceDataWeight_;
    i64 InputSliceRowCount_;
    std::optional<double> SamplingRate_;
    i64 SamplingDataWeightPerJob_;
    i64 SamplingPrimaryDataWeightPerJob_;
    i64 MaxBuildRetryCount_;
    double DataWeightPerJobRetryFactor_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TExplicitJobSizeConstraints);
DEFINE_REFCOUNTED_TYPE(TExplicitJobSizeConstraints);

////////////////////////////////////////////////////////////////////////////////

IJobSizeConstraintsPtr CreateExplicitJobSizeConstraints(
    bool canAdjustDataSizePerJob,
    bool isExplicitJobCount,
    int jobCount,
    i64 dataSizePerJob,
    i64 primaryDataSizePerJob,
    i64 maxDataSlicesPerJob,
    i64 maxDataWeightPerJob,
    i64 maxPrimaryDataWeightPerJob,
    i64 inputSliceDataSize,
    i64 inputSliceRowCount,
    std::optional<double> samplingRate,
    i64 samplingDataWeightPerJob,
    i64 samplingPrimaryDataWeightPerJob,
    i64 maxBuildRetryCount,
    double dataWeightPerJobRetryFactor)
{
    return New<TExplicitJobSizeConstraints>(
        canAdjustDataSizePerJob,
        isExplicitJobCount,
        jobCount,
        dataSizePerJob,
        primaryDataSizePerJob,
        maxDataSlicesPerJob,
        maxDataWeightPerJob,
        maxPrimaryDataWeightPerJob,
        inputSliceDataSize,
        inputSliceRowCount,
        samplingRate,
        samplingDataWeightPerJob,
        samplingPrimaryDataWeightPerJob,
        maxBuildRetryCount,
        dataWeightPerJobRetryFactor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

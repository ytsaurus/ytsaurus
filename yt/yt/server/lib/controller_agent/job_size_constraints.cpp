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
        std::optional<i64> batchRowCount,
        i64 foreignSliceDataWeight,
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
        , BatchRowCount_(batchRowCount)
        , ForeignSliceDataWeight_(foreignSliceDataWeight)
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

    bool CanAdjustDataWeightPerJob() const override
    {
        return CanAdjustDataWeightPerJob_;
    }

    bool IsExplicitJobCount() const override
    {
        return IsExplicitJobCount_;
    }

    int GetJobCount() const override
    {
        return JobCount_;
    }

    i64 GetDataWeightPerJob() const override
    {
        return DataWeightPerJob_;
    }

    i64 GetMaxDataSlicesPerJob() const override
    {
        return MaxDataSlicesPerJob_;
    }

    i64 GetPrimaryDataWeightPerJob() const override
    {
        return PrimaryDataWeightPerJob_;
    }

    i64 GetMaxDataWeightPerJob() const override
    {
        return MaxDataWeightPerJob_;
    }

    i64 GetMaxPrimaryDataWeightPerJob() const override
    {
        return MaxPrimaryDataWeightPerJob_;
    }

    i64 GetInputSliceDataWeight() const override
    {
        return InputSliceDataWeight_;
    }

    i64 GetInputSliceRowCount() const override
    {
        return InputSliceRowCount_;
    }

    std::optional<i64> GetBatchRowCount() const override
    {
        return BatchRowCount_;
    }

    i64 GetForeignSliceDataWeight() const override
    {
        return ForeignSliceDataWeight_;
    }

    std::optional<double> GetSamplingRate() const override
    {
        return SamplingRate_;
    }

    i64 GetSamplingDataWeightPerJob() const override
    {
        YT_VERIFY(SamplingRate_);
        return SamplingDataWeightPerJob_;
    }

    i64 GetSamplingPrimaryDataWeightPerJob() const override
    {
        YT_VERIFY(SamplingRate_);
        return SamplingPrimaryDataWeightPerJob_;
    }

    double GetDataWeightPerJobRetryFactor() const override
    {
        return DataWeightPerJobRetryFactor_;
    }

    i64 GetMaxBuildRetryCount() const override
    {
        return MaxBuildRetryCount_;
    }

    void UpdateInputDataWeight(i64 /*inputDataWeight*/) override
    {
        // Do nothing. Explicit job size constraints do not care about input data weight.
    }

    void UpdatePrimaryInputDataWeight(i64 /*inputDataWeight*/) override
    {
        // Do nothing. Explicit job size constraints do not care about primary input data weight.
    }

    void Persist(const TPersistenceContext& context) override
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
        // NB: ESnapshotVersion::NodeJobStartTimeInJoblet is the first 24.1 snapshot version.
        if ((context.GetVersion() >= ESnapshotVersion::BatchRowCount_23_2 && context.GetVersion() < ESnapshotVersion::NodeJobStartTimeInJoblet) ||
            context.GetVersion() >= ESnapshotVersion::BatchRowCount_24_1)
        {
            Persist(context, BatchRowCount_);
        }
        Persist(context, ForeignSliceDataWeight_);
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
    std::optional<i64> BatchRowCount_;
    i64 ForeignSliceDataWeight_;
    std::optional<double> SamplingRate_;
    i64 SamplingDataWeightPerJob_;
    i64 SamplingPrimaryDataWeightPerJob_;
    i64 MaxBuildRetryCount_;
    double DataWeightPerJobRetryFactor_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TExplicitJobSizeConstraints);
DEFINE_REFCOUNTED_TYPE(TExplicitJobSizeConstraints)

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
    i64 inputSliceDataWeight,
    i64 inputSliceRowCount,
    std::optional<i64> batchRowCount,
    i64 foreignSliceDataWeight,
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
        inputSliceDataWeight,
        inputSliceRowCount,
        batchRowCount,
        foreignSliceDataWeight,
        samplingRate,
        samplingDataWeightPerJob,
        samplingPrimaryDataWeightPerJob,
        maxBuildRetryCount,
        dataWeightPerJobRetryFactor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

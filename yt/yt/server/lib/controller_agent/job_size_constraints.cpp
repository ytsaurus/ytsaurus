#include "job_size_constraints.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

void IJobSizeConstraints::RegisterMetadata(auto&& /*registrar*/)
{ }

PHOENIX_DEFINE_TYPE(IJobSizeConstraints);

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
        double dataWeightPerJobRetryFactor,
        bool forceAllowJobInterruption)
        : CanAdjustDataWeightPerJob_(canAdjustDataWeightPerJob)
        , ForceAllowJobInterruption_(forceAllowJobInterruption)
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

    bool ForceAllowJobInterruption() const override
    {
        return ForceAllowJobInterruption_;
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

private:
    bool CanAdjustDataWeightPerJob_;
    bool ForceAllowJobInterruption_;
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

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TExplicitJobSizeConstraints, 0xab6bc389);
};

void TExplicitJobSizeConstraints::RegisterMetadata(auto&& registrar)
{
    registrar.template Field<1, &TThis::CanAdjustDataWeightPerJob_>("can_adjust_data_weight_per_job")();
    registrar.template Field<2, &TThis::IsExplicitJobCount_>("is_explicit_job_count")();
    registrar.template Field<3, &TThis::JobCount_>("job_count")();
    registrar.template Field<4, &TThis::DataWeightPerJob_>("data_weight_per_job")();
    registrar.template Field<5, &TThis::PrimaryDataWeightPerJob_>("primary_data_weight_per_job")();
    registrar.template Field<6, &TThis::MaxDataSlicesPerJob_>("max_data_slices_per_job")();
    registrar.template Field<7, &TThis::MaxDataWeightPerJob_>("max_data_weight_per_job")();
    registrar.template Field<8, &TThis::MaxPrimaryDataWeightPerJob_>("max_primary_data_weight_per_job")();
    registrar.template Field<9, &TThis::InputSliceDataWeight_>("input_slice_data_weight")();
    registrar.template Field<10, &TThis::InputSliceRowCount_>("input_slice_row_count")();
    // NB: ESnapshotVersion::BumpTo_24_1 is the first 24.1 snapshot version.
    registrar.template Field<11, &TThis::BatchRowCount_>("batch_row_count")
        .InVersions([] (ESnapshotVersion version) {
            return ((version >= ESnapshotVersion::BatchRowCount_23_2 && version < ESnapshotVersion::BumpTo_24_1) ||
                version >= ESnapshotVersion::BatchRowCount_24_1);
        })();
    registrar.template Field<12, &TThis::ForeignSliceDataWeight_>("foreign_slice_data_weight")();
    registrar.template Field<13, &TThis::SamplingRate_>("sampling_rate")();
    registrar.template Field<14, &TThis::SamplingDataWeightPerJob_>("sampling_data_weight_per_job")();
    registrar.template Field<15, &TThis::SamplingPrimaryDataWeightPerJob_>("sampling_primary_data_weight_per_job")();
    registrar.template Field<16, &TThis::MaxBuildRetryCount_>("max_build_retry_count")();
    registrar.template Field<17, &TThis::DataWeightPerJobRetryFactor_>("data_weight_per_job_retry_factor")();

    // COMPAT(galtsev)
    registrar.template Field<18, &TThis::ForceAllowJobInterruption_>("force_allow_job_interruption")
        .SinceVersion(ESnapshotVersion::ForceAllowJobInterruption)
        .WhenMissing([] (TThis* this_, auto& /*context*/) {
            this_->ForceAllowJobInterruption_ = false;
        })();

    // COMPAT(max42): remove this after YT-10666 (and put YT_VERIFY about job having non-empty
    // input somewhere in controller).
    registrar.AfterLoad([] (TThis* this_, auto& /*context*/) {
        this_->MaxDataWeightPerJob_ = std::max<i64>(1, this_->MaxDataWeightPerJob_);
        this_->DataWeightPerJob_ = std::max<i64>(1, this_->DataWeightPerJob_);
        this_->PrimaryDataWeightPerJob_ = std::max<i64>(1, this_->PrimaryDataWeightPerJob_);
    });
}

PHOENIX_DEFINE_TYPE(TExplicitJobSizeConstraints);

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
    double dataWeightPerJobRetryFactor,
    bool forceAllowJobInterruption)
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
        dataWeightPerJobRetryFactor,
        forceAllowJobInterruption);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

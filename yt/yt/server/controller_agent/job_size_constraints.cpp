#include "job_size_constraints.h"

#include "config.h"

#include <yt/yt/server/lib/controller_agent/job_size_constraints.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/core/logging/serializable_logger.h>

#include <library/cpp/yt/misc/numeric_helpers.h>

#include <algorithm>

namespace NYT::NControllerAgent {

using namespace NLogging;
using namespace NScheduler;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 InfiniteSize = std::numeric_limits<i64>::max() / 4;
static constexpr i64 InfiniteCount = std::numeric_limits<i64>::max() / 4;

////////////////////////////////////////////////////////////////////////////////

//! When neither data_size_per_job nor job_count are specified,
//! we need to get a hint about job size from another source.
DEFINE_ENUM(EDataSizePerMergeJobHint,
    //! Use the value from T${OperationType}OperationOptions
    //! from the dynamic config. Useful when DesiredChunkSize is meaningless
    //! (for example, when remote_copying files).
    (OperationOptions)
    //! Try to optimize writer's memory using
    //! #TMultiChunkWriterConfig::DesiredChunkSize.
    (DesiredChunkSize)
);

////////////////////////////////////////////////////////////////////////////////


class TJobSizeConstraintsBase
    : public IJobSizeConstraints
{
public:
    //! Used only for persistence.
    TJobSizeConstraintsBase() = default;

    TJobSizeConstraintsBase(
        i64 inputDataWeight,
        i64 inputCompressedDataSize,
        i64 primaryInputDataWeight,
        i64 primaryInputCompressedDataSize,
        TOperationSpecBasePtr spec,
        TOperationOptionsPtr options,
        TLogger logger,
        i64 inputRowCount = -1,
        i64 inputChunkCount = InfiniteCount,
        int mergeInputTableCount = 1,
        int mergePrimaryInputTableCount = 1,
        TSamplingConfigPtr samplingConfig = nullptr)
        : InputDataWeight_(inputDataWeight)
        , InputCompressedDataSize_(inputCompressedDataSize)
        , PrimaryInputDataWeight_(primaryInputDataWeight)
        , PrimaryInputCompressedDataSize_(primaryInputCompressedDataSize)
        , ForeignInputDataWeight_(InputDataWeight_ - PrimaryInputDataWeight_)
        , InputChunkCount_(inputChunkCount)
        , InputRowCount_(inputRowCount)
        , MergeInputTableCount_(mergeInputTableCount)
        , MergePrimaryInputTableCount_(mergePrimaryInputTableCount)
        , Logger(std::move(logger))
        , InitialInputDataWeight_(inputDataWeight)
        , InitialPrimaryInputDataWeight_(primaryInputDataWeight)
        , Options_(std::move(options))
        , Spec_(std::move(spec))
        , SamplingConfig_(std::move(samplingConfig))
    {
        YT_VERIFY(ForeignInputDataWeight_ >= 0);

        if (GetSamplingRate()) {
            InitializeSampling();
        }
    }

    std::optional<double> GetSamplingRate() const override
    {
        return SamplingConfig_ ? SamplingConfig_->SamplingRate : std::nullopt;
    }

    i64 GetSamplingDataWeightPerJob() const override
    {
        YT_VERIFY(SamplingDataWeightPerJob_);
        return *SamplingDataWeightPerJob_;
    }

    i64 GetSamplingPrimaryDataWeightPerJob() const override
    {
        YT_VERIFY(SamplingPrimaryDataWeightPerJob_);
        return *SamplingPrimaryDataWeightPerJob_;
    }

    i64 GetMaxBuildRetryCount() const override
    {
        return Options_->MaxBuildRetryCount;
    }

    double GetDataWeightPerJobRetryFactor() const override
    {
        return Options_->DataWeightPerJobRetryFactor;
    }

    i64 GetInputSliceDataWeight() const override
    {
        auto dataWeightPerJob = GetSamplingRate()
            ? GetSamplingDataWeightPerJob()
            : GetDataWeightPerJob();

        i64 sliceDataSize = std::clamp<i64>(
            Options_->SliceDataWeightMultiplier * dataWeightPerJob,
            1,
            Options_->MaxSliceDataWeight);

        if (sliceDataSize < Options_->MinSliceDataWeight) {
            // Non-trivial multiplier should be used only if input data size is large enough.
            // Otherwise we do not want to have more slices than job count.

            sliceDataSize = dataWeightPerJob;
        }

        return std::max<i64>(1, sliceDataSize);
    }

    i64 GetForeignSliceDataWeight() const override
    {
        auto jobCount = GetJobCount();
        auto foreignDataWeightPerJob = jobCount > 0
            ? std::max<i64>(1, DivCeil<i64>(ForeignInputDataWeight_, jobCount))
            : 1;

        auto foreignSliceDataWeight = std::clamp<i64>(
            Options_->SliceDataWeightMultiplier * foreignDataWeightPerJob,
            Options_->MinSliceDataWeight,
            Options_->MaxSliceDataWeight);

        return std::max<i64>(1, foreignSliceDataWeight);
    }

    i64 GetMaxDataWeightPerJob() const override
    {
        return Spec_->MaxDataWeightPerJob;
    }

    i64 GetMaxPrimaryDataWeightPerJob() const override
    {
        return Spec_->MaxPrimaryDataWeightPerJob;
    }

    i64 GetMaxCompressedDataSizePerJob() const override
    {
        return Spec_->MaxCompressedDataSizePerJob;
    }

    i64 GetInputSliceRowCount() const override
    {
        if (GetJobCount() == 0) {
            return 1;
        }
        return std::max<i64>(InputRowCount_ / GetJobCount(), 1);
    }

    std::optional<i64> GetBatchRowCount() const override
    {
        return Spec_->BatchRowCount;
    }

    int GetJobCount() const override
    {
        return std::max(JobCountByDataWeight_, JobCountByCompressedDataSize_);
    }

    bool ForceAllowJobInterruption() const override
    {
        return false;
    }

    void UpdateInputDataWeight(i64 inputDataWeight) override
    {
        YT_LOG_DEBUG("Job size constraints input data weight updated (OldInputDataWeight: %v, NewInputDataWeight: %v)",
            InputDataWeight_,
            inputDataWeight);
        InputDataWeight_ = inputDataWeight;
    }

    void UpdatePrimaryInputDataWeight(i64 primaryInputDataWeight) override
    {
        YT_LOG_DEBUG("Job size constraints primary input data weight updated (OldPrimaryInputDataWeight: %v, NewPrimaryInputDataWeight: %v)",
            PrimaryInputDataWeight_,
            primaryInputDataWeight);
        PrimaryInputDataWeight_ = primaryInputDataWeight;
    }

protected:
    i64 InputDataWeight_ = -1;
    i64 InputCompressedDataSize_ = -1;
    i64 PrimaryInputDataWeight_ = -1;
    i64 PrimaryInputCompressedDataSize_ = -1;
    i64 ForeignInputDataWeight_ = -1;
    i64 InputChunkCount_ = -1;
    i64 InputRowCount_ = -1;
    int MergeInputTableCount_ = -1;
    int MergePrimaryInputTableCount_ = -1;

    i64 JobCountByDataWeight_ = -1;
    i64 JobCountByCompressedDataSize_ = -1;

    TSerializableLogger Logger;

private:
    i64 InitialInputDataWeight_ = -1;
    i64 InitialPrimaryInputDataWeight_ = -1;
    TOperationOptionsPtr Options_;
    TOperationSpecBasePtr Spec_;
    std::optional<i64> SamplingDataWeightPerJob_;
    std::optional<i64> SamplingPrimaryDataWeightPerJob_;
    TSamplingConfigPtr SamplingConfig_;

    void InitializeSampling()
    {
        YT_VERIFY(SamplingConfig_->MaxTotalSliceCount);
        YT_VERIFY(InputCompressedDataSize_ >= 0);
        YT_VERIFY(PrimaryInputCompressedDataSize_ >= 0);
        // Replace input data weight and input row count with their expected values after the sampling.
        InputDataWeight_ *= *SamplingConfig_->SamplingRate;
        PrimaryInputDataWeight_ *= *SamplingConfig_->SamplingRate;
        InputRowCount_ *= *SamplingConfig_->SamplingRate;
        InputCompressedDataSize_ *= *SamplingConfig_->SamplingRate;
        PrimaryInputCompressedDataSize_ *= *SamplingConfig_->SamplingRate;
        // We do not want jobs to read less than `ioBlockSize` from each table.
        i64 minSamplingJobDataWeightForIOEfficiency = MergeInputTableCount_ * SamplingConfig_->IOBlockSize;
        i64 minSamplingJobPrimaryDataWeightForIOEfficiency = MergePrimaryInputTableCount_ * SamplingConfig_->IOBlockSize;
        // Each sampling job increases number of slices by InputTableCount_ in worst-case.
        i64 maxJobCountForSliceCountFit =
            std::max<i64>(1, 1 + (*SamplingConfig_->MaxTotalSliceCount - InputChunkCount_) / MergeInputTableCount_);
        i64 minSamplingJobDataWeightForSliceCountFit = InitialInputDataWeight_ / maxJobCountForSliceCountFit;
        i64 minSamplingJobPrimaryDataWeightForSliceCountFit = InitialPrimaryInputDataWeight_ / maxJobCountForSliceCountFit;
        SamplingDataWeightPerJob_ = std::max<i64>({1, minSamplingJobDataWeightForIOEfficiency, minSamplingJobDataWeightForSliceCountFit});
        SamplingPrimaryDataWeightPerJob_ = std::max<i64>({1, minSamplingJobPrimaryDataWeightForIOEfficiency, minSamplingJobPrimaryDataWeightForSliceCountFit});
        YT_LOG_INFO(
            "Sampling parameters calculated (InitialInputDataWeight: %v, SamplingRate: %v, InputDataWeight: %v, "
            "PrimaryInputDataWeight: %v, MinSamplingJobDataWeightForIOEfficiency: %v, "
            "MinSamplingJobPrimaryDataWeightForIOEfficiency: %v, MaxJobCountForSliceCountFit: %v, "
            "MinSamplingJobDataWeightForSliceCountFit: %v, SamplingDataWeightPerJob: %v, SamplingPrimaryDataWeightPerJob: %v)",
            InitialInputDataWeight_,
            SamplingConfig_->SamplingRate,
            InputDataWeight_,
            PrimaryInputDataWeight_,
            minSamplingJobDataWeightForIOEfficiency,
            minSamplingJobPrimaryDataWeightForIOEfficiency,
            maxJobCountForSliceCountFit,
            minSamplingJobDataWeightForSliceCountFit,
            SamplingDataWeightPerJob_,
            SamplingPrimaryDataWeightPerJob_);
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TJobSizeConstraintsBase, 0x1272e58a);
};

void TJobSizeConstraintsBase::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Options_);
    PHOENIX_REGISTER_FIELD(2, Spec_);
    PHOENIX_REGISTER_FIELD(3, InputDataWeight_);
    PHOENIX_REGISTER_FIELD(4, PrimaryInputDataWeight_);
    PHOENIX_REGISTER_FIELD(5, ForeignInputDataWeight_);
    PHOENIX_REGISTER_FIELD(6, InitialInputDataWeight_);
    PHOENIX_REGISTER_FIELD(7, InitialPrimaryInputDataWeight_);
    PHOENIX_REGISTER_FIELD(8, InputChunkCount_);
    PHOENIX_REGISTER_FIELD(9, JobCountByDataWeight_);
    PHOENIX_REGISTER_FIELD(10, InputRowCount_);
    PHOENIX_REGISTER_FIELD(11, Logger);
    PHOENIX_REGISTER_FIELD(12, MergeInputTableCount_);
    PHOENIX_REGISTER_FIELD(13, MergePrimaryInputTableCount_);
    PHOENIX_REGISTER_FIELD(14, SamplingDataWeightPerJob_);
    PHOENIX_REGISTER_FIELD(15, SamplingPrimaryDataWeightPerJob_);
    PHOENIX_REGISTER_FIELD(16, SamplingConfig_);
    PHOENIX_REGISTER_FIELD(17, InputCompressedDataSize_,
        .SinceVersion(ESnapshotVersion::MaxCompressedDataSizePerJob));
    PHOENIX_REGISTER_FIELD(18, PrimaryInputCompressedDataSize_,
        .SinceVersion(ESnapshotVersion::MaxCompressedDataSizePerJob));
    PHOENIX_REGISTER_FIELD(19, JobCountByCompressedDataSize_,
        .SinceVersion(ESnapshotVersion::CompressedDataSizePerJob)
        .WhenMissing([] (TThis* this_, auto& /*context*/) {
            this_->JobCountByCompressedDataSize_ = 1;
        }));
}

PHOENIX_DEFINE_TYPE(TJobSizeConstraintsBase);

////////////////////////////////////////////////////////////////////////////////

class TUserJobSizeConstraints
    : public TJobSizeConstraintsBase
{
public:
    TUserJobSizeConstraints() = default;

    TUserJobSizeConstraints(
        const TSimpleOperationSpecBasePtr& spec,
        const TSimpleOperationOptionsPtr& options,
        TLogger logger,
        int outputTableCount,
        double dataWeightRatio,
        i64 inputChunkCount,
        i64 primaryInputDataWeight,
        i64 primaryInputCompressedDataSize,
        i64 inputRowCount,
        i64 foreignInputDataWeight,
        i64 foreignInputCompressedDataSize,
        int inputTableCount,
        int primaryInputTableCount)
        : TJobSizeConstraintsBase(
            primaryInputDataWeight + foreignInputDataWeight,
            primaryInputCompressedDataSize + foreignInputCompressedDataSize,
            primaryInputDataWeight,
            primaryInputCompressedDataSize,
            spec,
            options,
            std::move(logger),
            inputRowCount,
            inputChunkCount,
            inputTableCount,
            primaryInputTableCount,
            spec->Sampling)
        , Spec_(spec)
        , Options_(options)
    {
        i64 dataWeightPerJob = [&] {
            if (Spec_->DataWeightPerJob.has_value() || !Spec_->CompressedDataSizePerJob.has_value()) {
                i64 dataWeightPerJob = Spec_->DataWeightPerJob.value_or(Options_->DataWeightPerJob);
                if (dataWeightRatio < 1) {
                    // This means that uncompressed data size is larger than data weight,
                    // which may happen for very sparse data.
                    dataWeightPerJob = std::max(static_cast<i64>(dataWeightPerJob * dataWeightRatio), (i64)1);
                }
                return dataWeightPerJob;
            }
            return Spec_->MaxDataWeightPerJob;
        }();

        JobCountByDataWeight_ = ComputeJobCount(
            dataWeightPerJob,
            Spec_->MaxDataWeightPerJob,
            PrimaryInputDataWeight_,
            InputDataWeight_,
            outputTableCount);

        JobCountByCompressedDataSize_ = ComputeJobCount(
            Spec_->CompressedDataSizePerJob.value_or(InfiniteSize),
            Spec_->MaxCompressedDataSizePerJob,
            PrimaryInputCompressedDataSize_,
            InputCompressedDataSize_,
            outputTableCount);
    }

    bool CanAdjustDataWeightPerJob() const override
    {
        if (Spec_->JobCount) {
            return false;
        }
        if (Spec_->DataWeightPerJob) {
            return Spec_->ForceJobSizeAdjuster;
        }

        return true;
    }

    bool IsExplicitJobCount() const override
    {
        // If #DataWeightPerJob == 1, we guarantee #JobCount == #RowCount (if row count doesn't exceed #MaxJobCount).
        return static_cast<bool>(Spec_->JobCount) ||
            (static_cast<bool>(Spec_->DataWeightPerJob) && *Spec_->DataWeightPerJob == 1);
    }

    bool ForceAllowJobInterruption() const override
    {
        return Spec_->ForceAllowJobInterruption;
    }

    i64 GetDataWeightPerJob() const override
    {
        return GetSizePerJob(JobCountByDataWeight_, PrimaryInputDataWeight_, InputDataWeight_, GetPrimaryDataWeightPerJob());
    }

    i64 GetCompressedDataSizePerJob() const override
    {
        return GetSizePerJob(JobCountByCompressedDataSize_, PrimaryInputCompressedDataSize_, InputCompressedDataSize_, GetPrimaryCompressedDataSizePerJob());
    }

    i64 GetPrimaryDataWeightPerJob() const override
    {
        return JobCountByDataWeight_ > 0
            ? std::max<i64>(1, DivCeil(PrimaryInputDataWeight_, JobCountByDataWeight_))
            : 1;
    }

    i64 GetMaxDataSlicesPerJob() const override
    {
        auto maxDataSlicesPerJob = std::min(
            Spec_->MaxDataSlicesPerJob.value_or(Options_->MaxDataSlicesPerJob),
            Options_->MaxDataSlicesPerJobLimit);
        return std::max<i64>(maxDataSlicesPerJob, Spec_->JobCount && *Spec_->JobCount > 0
            ? DivCeil<i64>(InputChunkCount_, *Spec_->JobCount)
            : 1);
    }

    i64 GetMaxCompressedDataSizePerJob() const override
    {
        return Spec_->MaxCompressedDataSizePerJob;
    }

private:
    TSimpleOperationSpecBasePtr Spec_;
    TSimpleOperationOptionsPtr Options_;

    i64 GetPrimaryCompressedDataSizePerJob() const
    {
        return JobCountByCompressedDataSize_ > 0
            ? std::max<i64>(1, DivCeil(PrimaryInputCompressedDataSize_, JobCountByCompressedDataSize_))
            : 1;
    }

    i64 GetSizePerJob(i64 jobCount, i64 primaryInputSize, i64 inputSize, i64 primarySizePerJob) const
    {
        if (jobCount == 0) {
            return 1;
        } else if (IsSmallForeignRatio(primaryInputSize, inputSize)) {
            return std::min(
                DivCeil(inputSize, jobCount),
                // We don't want to have much more that primary data weight per job, since that is
                // what we calculated given data_weight_per_job.
                2 * primarySizePerJob);
        } else {
            return DivCeil(inputSize, jobCount);
        }
    }

    i64 ComputeJobCount(
        i64 sizePerJob,
        i64 maxSizePerJob,
        i64 primaryInputSize,
        i64 inputSize,
        int outputTableCount) const
    {
        i64 jobCount = 0;
        if (Spec_->JobCount) {
            jobCount = *Spec_->JobCount;
        } else if (primaryInputSize > 0) {
            if (IsSmallForeignRatio(primaryInputSize, inputSize)) {
                // Since foreign tables are quite small, we use primary table to estimate job count.
                jobCount = std::max(
                    DivCeil(primaryInputSize, sizePerJob),
                    DivCeil(inputSize, DivCeil<i64>(maxSizePerJob, 2)));
            } else {
                jobCount = DivCeil(inputSize, sizePerJob);
            }
        }

        i64 maxJobCount = Options_->MaxJobCount;

        if (Spec_->MaxJobCount) {
            maxJobCount = std::min(maxJobCount, static_cast<i64>(*Spec_->MaxJobCount));
        }

        jobCount = std::min({jobCount, maxJobCount, InputRowCount_});

        if (jobCount * outputTableCount > Options_->MaxOutputTablesTimesJobsCount) {
            // ToDo(psushin): register alert if explicit job count or data size per job were given.
            jobCount = DivCeil(Options_->MaxOutputTablesTimesJobsCount, outputTableCount);
        }

        YT_VERIFY(jobCount >= 0);

        return jobCount;
    }

    static double GetForeignDataRatio(i64 primaryInputSize, i64 inputSize)
    {
        if (primaryInputSize > 0) {
            return (inputSize - primaryInputSize) / static_cast<double>(primaryInputSize);
        } else {
            return 0;
        }
    }

    static bool IsSmallForeignRatio(i64 primaryInputSize, i64 inputSize)
    {
        // ToDo(psushin): make configurable.
        constexpr double SmallForeignRatio = 0.2;

        return GetForeignDataRatio(primaryInputSize, inputSize) < SmallForeignRatio;
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TUserJobSizeConstraints, 0xb45cfe0d);
};

void TUserJobSizeConstraints::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TJobSizeConstraintsBase>();

    PHOENIX_REGISTER_FIELD(1, Spec_);
    PHOENIX_REGISTER_FIELD(2, Options_);

    registrar
        .template VirtualField<3>("SortedOperation_", [] (TThis* /*this_*/, auto& context) {
            bool sortedOperation;
            NYT::Load(context, sortedOperation);
        })
        .BeforeVersion(ESnapshotVersion::DropUnusedFieldInJobSizeConstraints)();
}

PHOENIX_DEFINE_TYPE(TUserJobSizeConstraints);

DEFINE_REFCOUNTED_TYPE(TUserJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

class TMergeJobSizeConstraints
    : public TJobSizeConstraintsBase
{
public:
    //! Used only for persistence.
    TMergeJobSizeConstraints() = default;

    TMergeJobSizeConstraints(
        const TSimpleOperationSpecBasePtr& spec,
        const TSimpleOperationOptionsPtr& options,
        TLogger logger,
        i64 inputChunkCount,
        i64 inputDataWeight,
        i64 inputCompressedDataSize,
        double dataWeightRatio,
        double compressionRatio,
        int mergeInputTableCount,
        int mergePrimaryInputTableCount,
        EDataSizePerMergeJobHint dataSizeHint)
        : TJobSizeConstraintsBase(
            inputDataWeight,
            inputCompressedDataSize,
            inputDataWeight,
            inputCompressedDataSize,
            spec,
            options,
            std::move(logger),
            /*inputRowCount*/ std::numeric_limits<i64>::max() / 4,
            inputChunkCount,
            mergeInputTableCount,
            mergePrimaryInputTableCount,
            spec->Sampling)
        , Spec_(spec)
        , Options_(options)
    {
        JobCountByDataWeight_ = ComputeJobCount(ComputeDataWeightPerJob(dataWeightRatio, compressionRatio, dataSizeHint), InputDataWeight_);
        JobCountByCompressedDataSize_ = ComputeJobCount(Spec_->CompressedDataSizePerJob.value_or(InfiniteSize), InputCompressedDataSize_);
    }

    bool CanAdjustDataWeightPerJob() const override
    {
        if (Spec_->JobCount) {
            return false;
        }
        if (Spec_->DataWeightPerJob) {
            return Spec_->ForceJobSizeAdjuster;
        }

        return true;
    }

    bool IsExplicitJobCount() const override
    {
        return false;
    }

    i64 GetDataWeightPerJob() const override
    {
        i64 dataWeightPerJob = JobCountByDataWeight_ > 0
            ? DivCeil(InputDataWeight_, JobCountByDataWeight_)
            : 1;

        return std::min(dataWeightPerJob, DivCeil<i64>(TJobSizeConstraintsBase::GetMaxDataWeightPerJob(), 2));
    }

    i64 GetPrimaryDataWeightPerJob() const override
    {
        return GetDataWeightPerJob();
    }

    i64 GetMaxDataSlicesPerJob() const override
    {
        return std::max<i64>(
            Options_->MaxDataSlicesPerJob,
            Spec_->JobCount && *Spec_->JobCount > 0
                ? DivCeil<i64>(InputChunkCount_, *Spec_->JobCount)
                : 1);
    }

    i64 GetCompressedDataSizePerJob() const override
    {
        i64 compressedDataSizePerJob = JobCountByCompressedDataSize_ > 0
            ? DivCeil(InputCompressedDataSize_, JobCountByCompressedDataSize_)
            : 1;

        return std::min(compressedDataSizePerJob, DivCeil<i64>(GetMaxCompressedDataSizePerJob(), 2));
    }

    i64 GetMaxCompressedDataSizePerJob() const override
    {
        return Spec_->MaxCompressedDataSizePerJob;
    }

    i64 GetInputSliceRowCount() const override
    {
        return std::numeric_limits<i64>::max() / 4;
    }

private:
    TSimpleOperationSpecBasePtr Spec_;
    TSimpleOperationOptionsPtr Options_;

    i64 ComputeDataWeightPerJob(double dataWeightRatio, double compressionRatio, EDataSizePerMergeJobHint dataSizeHint) const
    {
        if (Spec_->DataWeightPerJob) {
            i64 dataWeightPerJob = *Spec_->DataWeightPerJob;
            if (dataWeightRatio < 1.0 / 2) {
                // This means that uncompressed data size is larger than 2x data weight,
                // which may happen for very sparse data. Than, adjust data weight accordingly.
                dataWeightPerJob = std::max(static_cast<i64>(dataWeightPerJob * dataWeightRatio * 2), (i64)1);
            }
            return dataWeightPerJob;
        }

        if (Spec_->CompressedDataSizePerJob) {
            return Spec_->MaxCompressedDataSizePerJob;
        }

        i64 dataWeightPerJob;
        switch (dataSizeHint) {
            case EDataSizePerMergeJobHint::DesiredChunkSize:
                dataWeightPerJob = Spec_->JobIO->TableWriter->DesiredChunkSize / compressionRatio;
                break;
            case EDataSizePerMergeJobHint::OperationOptions:
                dataWeightPerJob = Options_->DataWeightPerJob;
                break;
            default:
                YT_ABORT();
        }

        if (dataWeightPerJob / dataWeightRatio > Options_->DataWeightPerJob) {
            // This means that compression ration w.r.t data weight is very small,
            // so we would like to limit uncompressed data size per job.
            dataWeightPerJob = Options_->DataWeightPerJob * dataWeightRatio;
        }

        return dataWeightPerJob;
    }

    i64 ComputeJobCount(i64 sizePerJob, i64 inputSize) const
    {
        i64 jobCount;
        if (Spec_->JobCount) {
            jobCount = *Spec_->JobCount;
        } else {
            jobCount = DivCeil(inputSize, sizePerJob);
        }

        i64 maxJobCount = Options_->MaxJobCount;
        if (Spec_->MaxJobCount) {
            maxJobCount = std::min(maxJobCount, static_cast<i64>(*Spec_->MaxJobCount));
        }
        jobCount = std::min(jobCount, maxJobCount);

        YT_VERIFY(jobCount >= 0);
        YT_VERIFY(jobCount != 0 || inputSize == 0);

        return jobCount;
    }

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TMergeJobSizeConstraints, 0x3f1caf80);
};

void TMergeJobSizeConstraints::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TJobSizeConstraintsBase>();

    PHOENIX_REGISTER_FIELD(1, Spec_);
    PHOENIX_REGISTER_FIELD(2, Options_);
}

PHOENIX_DEFINE_TYPE(TMergeJobSizeConstraints);

DEFINE_REFCOUNTED_TYPE(TMergeJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

class TSimpleSortJobSizeConstraints
    : public TJobSizeConstraintsBase
{
public:
    TSimpleSortJobSizeConstraints() = default;

    TSimpleSortJobSizeConstraints(
        const TSortOperationSpecBasePtr& spec,
        const TSortOperationOptionsBasePtr& options,
        TLogger logger,
        i64 inputDataWeight,
        i64 inputCompressedDataSize)
        : TJobSizeConstraintsBase(
            inputDataWeight,
            inputCompressedDataSize,
            inputDataWeight,
            inputCompressedDataSize,
            spec,
            options,
            std::move(logger))
        , Spec_(spec)
        , Options_(options)
    {
        JobCountByDataWeight_ = DivCeil(InputDataWeight_, Spec_->DataWeightPerShuffleJob);
        YT_VERIFY(JobCountByDataWeight_ >= 0);
        YT_VERIFY(JobCountByDataWeight_ != 0 || InputDataWeight_ == 0);

        JobCountByCompressedDataSize_ = 0;
    }

    bool CanAdjustDataWeightPerJob() const override
    {
        return false;
    }

    bool IsExplicitJobCount() const override
    {
        return false;
    }

    i64 GetDataWeightPerJob() const override
    {
        return JobCountByDataWeight_ > 0
            ? DivCeil(InputDataWeight_, JobCountByDataWeight_)
            : 1;
    }

    i64 GetCompressedDataSizePerJob() const override
    {
        return InfiniteSize;
    }

    i64 GetMaxCompressedDataSizePerJob() const override
    {
        return InfiniteSize;
    }

    i64 GetPrimaryDataWeightPerJob() const override
    {
        YT_ABORT();
    }

    i64 GetMaxDataSlicesPerJob() const override
    {
        return Options_->MaxDataSlicesPerJob;
    }

    i64 GetInputSliceRowCount() const override
    {
        return std::numeric_limits<i64>::max() / 4;
    }

private:
    TSortOperationSpecBasePtr Spec_;
    TSortOperationOptionsBasePtr Options_;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TSimpleSortJobSizeConstraints, 0xef270530);
};

void TSimpleSortJobSizeConstraints::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TJobSizeConstraintsBase>();

    PHOENIX_REGISTER_FIELD(1, Spec_);
    PHOENIX_REGISTER_FIELD(2, Options_);
}

PHOENIX_DEFINE_TYPE(TSimpleSortJobSizeConstraints);

DEFINE_REFCOUNTED_TYPE(TSimpleSortJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

class TPartitionJobSizeConstraints
    : public TJobSizeConstraintsBase
{
public:
    TPartitionJobSizeConstraints() = default;

    TPartitionJobSizeConstraints(
        const TSortOperationSpecBasePtr& spec,
        const TSortOperationOptionsBasePtr& options,
        TLogger logger,
        i64 inputUncompressedDataSize,
        i64 inputDataWeight,
        i64 inputRowCount,
        double compressionRatio)
        : TJobSizeConstraintsBase(
            inputDataWeight,
            inputUncompressedDataSize * compressionRatio,
            inputDataWeight,
            inputUncompressedDataSize * compressionRatio,
            spec,
            options,
            std::move(logger),
            inputRowCount,
            /*inputChunkCount*/ InfiniteCount,
            /*mergeInputTableCount*/ 1,
            /*primaryMergeInputTableCount*/ 1,
            spec->Sampling)
        , Spec_(spec)
        , Options_(options)
    {
        if (Spec_->PartitionJobCount) {
            JobCountByDataWeight_ = *Spec_->PartitionJobCount;
        } else if (Spec_->DataWeightPerPartitionJob) {
            i64 dataWeightPerJob = *Spec_->DataWeightPerPartitionJob;
            JobCountByDataWeight_ = DivCeil(InputDataWeight_, dataWeightPerJob);
        } else {
            // Rationale and details are on the wiki.
            // https://wiki.yandex-team.ru/yt/design/partitioncount/
            i64 uncompressedBlockSize = static_cast<i64>(Options_->CompressedBlockSize / compressionRatio);
            uncompressedBlockSize = std::min(uncompressedBlockSize, Spec_->PartitionJobIO->TableWriter->BlockSize);

            // Just in case compression ratio is very large.
            uncompressedBlockSize = std::max(i64(1), uncompressedBlockSize);

            // Product may not fit into i64.
            auto partitionJobDataWeight = [&] {
                double partitionJobDataWeight = sqrt(InputDataWeight_) * sqrt(uncompressedBlockSize);
                partitionJobDataWeight = std::min(partitionJobDataWeight, static_cast<double>(Spec_->PartitionJobIO->TableWriter->MaxBufferSize));
                return static_cast<i64>(partitionJobDataWeight);
            }();

            JobCountByDataWeight_ = DivCeil(InputDataWeight_, std::max<i64>(partitionJobDataWeight, 1));
        }

        YT_VERIFY(JobCountByDataWeight_ >= 0);
        YT_VERIFY(JobCountByDataWeight_ != 0 || InputDataWeight_ == 0);

        if (JobCountByDataWeight_ > 0 && inputUncompressedDataSize / JobCountByDataWeight_ > Spec_->MaxDataWeightPerJob) {
            // NB(apollo1321): There are no tests for this scenario.
            // Sometimes (but rarely) data weight can be smaller than data size. Let's protect from
            // unreasonable huge jobs.
            JobCountByDataWeight_ = DivCeil(inputUncompressedDataSize, 2 * Spec_->MaxDataWeightPerJob);
        }

        JobCountByDataWeight_ = std::min({JobCountByDataWeight_, static_cast<i64>(Options_->MaxPartitionJobCount), InputRowCount_});
        JobCountByCompressedDataSize_ = 0;
    }

    bool CanAdjustDataWeightPerJob() const override
    {
        if (Spec_->PartitionJobCount) {
            return false;
        }
        if (Spec_->DataWeightPerPartitionJob) {
            return Spec_->ForceJobSizeAdjuster;
        }

        return true;
    }

    bool IsExplicitJobCount() const override
    {
        return static_cast<bool>(Spec_->PartitionJobCount);
    }

    i64 GetDataWeightPerJob() const override
    {
        return JobCountByDataWeight_ > 0
            ? DivCeil(InputDataWeight_, JobCountByDataWeight_)
            : 1;
    }

    i64 GetPrimaryDataWeightPerJob() const override
    {
        YT_ABORT();
    }

    i64 GetMaxDataSlicesPerJob() const override
    {
        return Options_->MaxDataSlicesPerJob;
    }

    i64 GetCompressedDataSizePerJob() const override
    {
        return InfiniteSize;
    }

    i64 GetMaxCompressedDataSizePerJob() const override
    {
        return InfiniteSize;
    }

private:
    TSortOperationSpecBasePtr Spec_;
    TSortOperationOptionsBasePtr Options_;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TPartitionJobSizeConstraints, 0xeea00714);
};

void TPartitionJobSizeConstraints::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TJobSizeConstraintsBase>();

    PHOENIX_REGISTER_FIELD(1, Spec_);
    PHOENIX_REGISTER_FIELD(2, Options_);
}

PHOENIX_DEFINE_TYPE(TPartitionJobSizeConstraints);

DEFINE_REFCOUNTED_TYPE(TPartitionJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

IJobSizeConstraintsPtr CreateUserJobSizeConstraints(
    const TSimpleOperationSpecBasePtr& spec,
    const TSimpleOperationOptionsPtr& options,
    TLogger logger,
    int outputTableCount,
    double dataWeightRatio,
    i64 inputChunkCount,
    i64 primaryInputDataWeight,
    i64 primaryInputCompressedDataSize,
    i64 inputRowCount,
    i64 foreignInputDataWeight,
    i64 foreignInputCompressedDataSize,
    int inputTableCount,
    int primaryInputTableCount)
{
    return New<TUserJobSizeConstraints>(
        spec,
        options,
        std::move(logger),
        outputTableCount,
        dataWeightRatio,
        inputChunkCount,
        primaryInputDataWeight,
        primaryInputCompressedDataSize,
        inputRowCount,
        foreignInputDataWeight,
        foreignInputCompressedDataSize,
        inputTableCount,
        primaryInputTableCount);
}

IJobSizeConstraintsPtr CreateMergeJobSizeConstraints(
    const TSimpleOperationSpecBasePtr& spec,
    const TSimpleOperationOptionsPtr& options,
    TLogger logger,
    i64 inputChunkCount,
    i64 inputDataWeight,
    i64 inputCompressedDataSize,
    double dataWeightRatio,
    double compressionRatio,
    int mergeInputTableCount,
    int mergePrimaryInputTableCount)
{
    return New<TMergeJobSizeConstraints>(
        spec,
        options,
        std::move(logger),
        inputChunkCount,
        inputDataWeight,
        inputCompressedDataSize,
        dataWeightRatio,
        compressionRatio,
        mergeInputTableCount,
        mergePrimaryInputTableCount,
        EDataSizePerMergeJobHint::DesiredChunkSize);
}

IJobSizeConstraintsPtr CreateRemoteCopyJobSizeConstraints(
    const TSimpleOperationSpecBasePtr& spec,
    const TSimpleOperationOptionsPtr& options,
    TLogger logger,
    i64 inputChunkCount,
    i64 inputDataWeight,
    i64 inputCompressedDataSize,
    double dataWeightRatio,
    double compressionRatio)
{
    return New<TMergeJobSizeConstraints>(
        spec,
        options,
        std::move(logger),
        inputChunkCount,
        inputDataWeight,
        inputCompressedDataSize,
        dataWeightRatio,
        compressionRatio,
        /*inputTableCount*/ 1,
        /*primaryInputTableCount*/ 1,
        EDataSizePerMergeJobHint::OperationOptions);
}

IJobSizeConstraintsPtr CreateSimpleSortJobSizeConstraints(
    const TSortOperationSpecBasePtr& spec,
    const TSortOperationOptionsBasePtr& options,
    TLogger logger,
    i64 inputDataWeight,
    i64 inputCompressedDataSize)
{
    return New<TSimpleSortJobSizeConstraints>(
        spec,
        options,
        std::move(logger),
        inputDataWeight,
        inputCompressedDataSize);
}

IJobSizeConstraintsPtr CreatePartitionJobSizeConstraints(
    const TSortOperationSpecBasePtr& spec,
    const TSortOperationOptionsBasePtr& options,
    TLogger logger,
    i64 inputUncompressedDataSize,
    i64 inputDataWeight,
    i64 inputRowCount,
    double compressionRatio)
{
    return New<TPartitionJobSizeConstraints>(
        spec,
        options,
        std::move(logger),
        inputUncompressedDataSize,
        inputDataWeight,
        inputRowCount,
        compressionRatio);
}

IJobSizeConstraintsPtr CreatePartitionBoundSortedJobSizeConstraints(
    const TSortOperationSpecBasePtr& spec,
    const TSortOperationOptionsBasePtr& options,
    int outputTableCount)
{
    // NB(psushin): I don't know real partition size at this point,
    // but I assume at least 2 sort jobs per partition.
    // Also I don't know exact partition count, so I take the worst case scenario.
    i64 jobsPerPartition = DivCeil(
        options->MaxOutputTablesTimesJobsCount,
        outputTableCount * options->MaxPartitionCount);
    i64 estimatedDataSizePerPartition = 2 * spec->DataWeightPerSortedJob.value_or(spec->DataWeightPerShuffleJob);

    i64 minDataWeightPerJob = std::max(estimatedDataSizePerPartition / jobsPerPartition, (i64)1);
    i64 dataWeightPerJob = std::max(minDataWeightPerJob, spec->DataWeightPerSortedJob.value_or(spec->DataWeightPerShuffleJob));

    return CreateExplicitJobSizeConstraints(
        /*canAdjustDataSizePerJob*/ spec->ForceJobSizeAdjuster,
        /*isExplicitJobCount*/ false,
        /*jobCount*/ 0,
        /*dataWeightPerJob*/ dataWeightPerJob,
        /*primaryDataWeightPerJob*/ dataWeightPerJob,
        /*compressedDataSizePerJob*/ InfiniteSize,
        options->MaxDataSlicesPerJob,
        spec->MaxDataWeightPerJob,
        spec->MaxPrimaryDataWeightPerJob,
        /*maxCompressedDataSizePerJob*/ InfiniteSize,
        /*inputSliceDataSize*/ InfiniteSize,
        /*inputSliceRowCount*/ InfiniteCount,
        /*batchRowCount*/ {},
        /*foreignSliceDataWeight*/ 0,
        /*samplingRate*/ std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

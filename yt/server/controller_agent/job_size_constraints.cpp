#include "job_size_constraints.h"

#include "config.h"

#include <yt/server/scheduler/config.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NControllerAgent {

using namespace NScheduler;
using namespace NTableClient;
using namespace NLogging;
using NPhoenix::TPersistenceContext;

////////////////////////////////////////////////////////////////////////////////

class TJobSizeConstraintsBase
    : public IJobSizeConstraints
{
public:
    //! Used only for persistence.
    TJobSizeConstraintsBase() = default;

    TJobSizeConstraintsBase(
        i64 inputDataWeight,
        i64 primaryInputDataWeight,
        TOperationSpecBasePtr spec,
        TOperationOptionsPtr options,
        TLogger logger,
        i64 inputRowCount = -1,
        i64 inputChunkCount = std::numeric_limits<i64>::max(),
        int mergeInputTableCount = 1,
        int mergePrimaryInputTableCount = 1,
        TSamplingConfigPtr samplingConfig = nullptr)
        : InputDataWeight_(inputDataWeight)
        , PrimaryInputDataWeight_(primaryInputDataWeight)
        , InputChunkCount_(inputChunkCount)
        , InputRowCount_(inputRowCount)
        , Logger(logger)
        , MergeInputTableCount_(mergeInputTableCount)
        , MergePrimaryInputTableCount_(mergePrimaryInputTableCount)
        , InitialInputDataWeight_(inputDataWeight)
        , Options_(std::move(options))
        , Spec_(std::move(spec))
        , SamplingConfig_(std::move(samplingConfig))
    {
        if (SamplingConfig_ && SamplingConfig_->SamplingRate) {
            InitializeSampling();
        }
    }

    virtual std::optional<double> GetSamplingRate() const override
    {
        return SamplingConfig_ ? SamplingConfig_->SamplingRate : std::nullopt;
    }

    virtual i64 GetSamplingDataWeightPerJob() const override
    {
        YCHECK(SamplingDataWeightPerJob_);
        return *SamplingDataWeightPerJob_;
    }

    virtual i64 GetSamplingPrimaryDataWeightPerJob() const override
    {
        YCHECK(SamplingPrimaryDataWeightPerJob_);
        return *SamplingPrimaryDataWeightPerJob_;
    }

    virtual i64 GetMaxBuildRetryCount() const override
    {
        return Options_->MaxBuildRetryCount;
    }

    virtual double GetDataWeightPerJobRetryFactor() const override
    {
        return Options_->DataWeightPerJobRetryFactor;
    }

    virtual i64 GetInputSliceDataWeight() const override
    {
        if (JobCount_ == 0 || InitialInputDataWeight_ == 0) {
            return 1;
        }

        auto dataWeightPerJob = (SamplingConfig_ && SamplingConfig_->SamplingRate)
            ? GetSamplingDataWeightPerJob()
            : GetDataWeightPerJob();

        i64 sliceDataSize = Clamp<i64>(
            Options_->SliceDataWeightMultiplier * dataWeightPerJob,
            1,
            Options_->MaxSliceDataWeight);

        if (sliceDataSize < Options_->MinSliceDataWeight) {
            // Non-trivial multiplier should be used only if input data size is large enough.
            // Otherwise we do not want to have more slices than job count.

            sliceDataSize = dataWeightPerJob;
        }

        return sliceDataSize;
    }

    virtual i64 GetMaxDataWeightPerJob() const override
    {
        return Spec_->MaxDataWeightPerJob;
    }

    virtual i64 GetMaxPrimaryDataWeightPerJob() const override
    {
        return Spec_->MaxPrimaryDataWeightPerJob;
    }

    virtual i64 GetInputSliceRowCount() const override
    {
        return JobCount_ > 0
            ? DivCeil(InputRowCount_, JobCount_)
            : 1;
    }

    virtual int GetJobCount() const override
    {
        return JobCount_;
    }

    virtual void UpdateInputDataWeight(i64 inputDataWeight) override
    {
        LOG_DEBUG("Job size constraints input data weight updated (OldInputDataWeight: %v, NewInputDataWeight: %v)",
            InputDataWeight_,
            inputDataWeight);
        InputDataWeight_ = inputDataWeight;

    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;

        Persist(context, Options_);
        Persist(context, Spec_);
        Persist(context, InputDataWeight_);
        Persist(context, PrimaryInputDataWeight_);
        Persist(context, InitialInputDataWeight_);
        Persist(context, InputChunkCount_);
        Persist(context, JobCount_);
        Persist(context, InputRowCount_);
        Persist(context, Logger);
        Persist(context, MergeInputTableCount_);
        Persist(context, MergePrimaryInputTableCount_);
        Persist(context, SamplingDataWeightPerJob_);
        Persist(context, SamplingPrimaryDataWeightPerJob_);
        Persist(context, SamplingConfig_);
    }

protected:
    i64 InputDataWeight_ = -1;
    i64 PrimaryInputDataWeight_ = -1;
    i64 InputChunkCount_ = -1;
    i64 JobCount_ = -1;
    i64 InputRowCount_ = -1;
    TLogger Logger;

private:
    int MergeInputTableCount_ = -1;
    int MergePrimaryInputTableCount_ = -1;
    i64 InitialInputDataWeight_ = -1;
    TOperationOptionsPtr Options_;
    TOperationSpecBasePtr Spec_;
    std::optional<i64> SamplingDataWeightPerJob_;
    std::optional<i64> SamplingPrimaryDataWeightPerJob_;
    TSamplingConfigPtr SamplingConfig_;

    void InitializeSampling()
    {
        YCHECK(SamplingConfig_->MaxTotalSliceCount);
        // Replace input data weight and input row count with their expected values after the sampling.
        InputDataWeight_ *= *SamplingConfig_->SamplingRate;
        PrimaryInputDataWeight_ *= *SamplingConfig_->SamplingRate;
        InputRowCount_ *= *SamplingConfig_->SamplingRate;
        // We do not want jobs to read less than `ioBlockSize` from each table.
        i64 minSamplingJobDataWeightForIOEfficiency = MergeInputTableCount_ * SamplingConfig_->IOBlockSize;
        i64 minSamplingJobPrimaryDataWeightForIOEfficiency = MergePrimaryInputTableCount_ * SamplingConfig_->IOBlockSize;
        // Each sampling job increases number of slices by InputTableCount_ in worst-case.
        i64 maxJobCountForSliceCountFit =
            std::max<i64>(1, 1 + (*SamplingConfig_->MaxTotalSliceCount - InputChunkCount_) / MergeInputTableCount_);
        i64 minSamplingJobDataWeightForSliceCountFit = InitialInputDataWeight_ / maxJobCountForSliceCountFit;
        SamplingDataWeightPerJob_ = std::max<i64>({1, minSamplingJobDataWeightForIOEfficiency, minSamplingJobDataWeightForSliceCountFit});
        SamplingPrimaryDataWeightPerJob_ = std::max<i64>(1, minSamplingJobPrimaryDataWeightForIOEfficiency);
        LOG_INFO(
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
};

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
        i64 inputRowCount,
        i64 foreignInputDataWeight,
        int inputTableCount,
        int primaryInputTableCount)
        : TJobSizeConstraintsBase(
            primaryInputDataWeight + foreignInputDataWeight,
            primaryInputDataWeight,
            spec,
            options,
            logger,
            inputRowCount,
            inputChunkCount,
            inputTableCount,
            primaryInputTableCount,
            spec->Sampling)
        , Spec_(spec)
        , Options_(options)
    {
        if (Spec_->JobCount) {
            JobCount_ = *Spec_->JobCount;
        } else if (PrimaryInputDataWeight_ > 0) {
            i64 dataWeightPerJob = Spec_->DataWeightPerJob.value_or(Options_->DataWeightPerJob);

            if (dataWeightRatio < 1) {
                // This means that uncompressed data size is larger than data weight,
                // which may happen for very sparse data.
                dataWeightPerJob = std::max(static_cast<i64>(dataWeightPerJob * dataWeightRatio), (i64)1);
            }

            if (IsSmallForeignRatio()) {
                // Since foreign tables are quite small, we use primary table to estimate job count.
                JobCount_ = std::max(
                    DivCeil(PrimaryInputDataWeight_, dataWeightPerJob),
                    DivCeil(InputDataWeight_, DivCeil<i64>(Spec_->MaxDataWeightPerJob, 2)));
            } else {
                JobCount_ = DivCeil(InputDataWeight_, dataWeightPerJob);
            }
        } else {
            JobCount_ = 0;
        }

        i64 maxJobCount = Options_->MaxJobCount;

        if (Spec_->MaxJobCount) {
            maxJobCount = std::min(maxJobCount, static_cast<i64>(*Spec_->MaxJobCount));
        }

        JobCount_ = std::min(JobCount_, maxJobCount);
        JobCount_ = std::min(JobCount_, InputRowCount_);

        if (JobCount_ * outputTableCount > Options_->MaxOutputTablesTimesJobsCount) {
            // ToDo(psushin): register alert if explicit job count or data size per job were given.
            JobCount_ = DivCeil(Options_->MaxOutputTablesTimesJobsCount, outputTableCount);
        }

        YCHECK(JobCount_ >= 0);
    }

    virtual bool CanAdjustDataWeightPerJob() const override
    {
        return !Spec_->DataWeightPerJob && !Spec_->JobCount;
    }

    virtual bool IsExplicitJobCount() const override
    {
        // If #DataWeightPerJob == 1, we guarantee #JobCount == #RowCount (if row count doesn't exceed #MaxJobCount).
        return static_cast<bool>(Spec_->JobCount) ||
            (static_cast<bool>(Spec_->DataWeightPerJob) && *Spec_->DataWeightPerJob == 1);
    }

    virtual i64 GetDataWeightPerJob() const override
    {
        if (JobCount_ == 0) {
            return 1;
        } else if (IsSmallForeignRatio()) {
            return std::min(
                DivCeil(InputDataWeight_, JobCount_),
                // We don't want to have much more that primary data weight per job, since that is
                // what we calculated given data_weight_per_job.
                2 * GetPrimaryDataWeightPerJob());
        } else {
            return DivCeil(InputDataWeight_, JobCount_);
        }
    }

    virtual i64 GetPrimaryDataWeightPerJob() const override
    {
        return JobCount_ > 0
            ? std::max<i64>(1, DivCeil(PrimaryInputDataWeight_, JobCount_))
            : 1;
    }

    virtual i64 GetMaxDataSlicesPerJob() const override
    {
        return std::max<i64>(Options_->MaxDataSlicesPerJob, Spec_->JobCount && *Spec_->JobCount > 0
            ? DivCeil<i64>(InputChunkCount_, *Spec_->JobCount)
            : 1);
    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        TJobSizeConstraintsBase::Persist(context);

        using NYT::Persist;

        Persist(context, Spec_);
        Persist(context, Options_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUserJobSizeConstraints, 0xb45cfe0d);

    TSimpleOperationSpecBasePtr Spec_;
    TSimpleOperationOptionsPtr Options_;

    double GetForeignDataRatio() const
    {
        if (PrimaryInputDataWeight_ > 0) {
            return (InputDataWeight_ - PrimaryInputDataWeight_) / static_cast<double>(PrimaryInputDataWeight_);
        } else {
            return 0;
        }
    }

    bool IsSmallForeignRatio() const
    {
        // ToDo(psushin): make configurable.
        constexpr double SmallForeignRatio = 0.2;

        return GetForeignDataRatio() < SmallForeignRatio;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TUserJobSizeConstraints);
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
        double dataWeightRatio,
        double compressionRatio,
        int mergeInputTableCount,
        int mergePrimaryInputTableCount)
        : TJobSizeConstraintsBase(
            inputDataWeight,
            inputDataWeight,
            spec,
            options,
            logger,
            std::numeric_limits<i64>::max() /* inputRowCount */,
            inputChunkCount,
            mergeInputTableCount,
            mergePrimaryInputTableCount,
            spec->Sampling)
        , Spec_(spec)
        , Options_(options)
    {
        if (Spec_->JobCount) {
            JobCount_ = *Spec_->JobCount;
        } else if (Spec_->DataWeightPerJob) {
            i64 dataWeightPerJob = *Spec_->DataWeightPerJob;
            if (dataWeightRatio < 1.0 / 2) {
                // This means that uncompressed data size is larger than 2x data weight,
                // which may happen for very sparse data. Than, adjust data weight accordingly.
                dataWeightPerJob = std::max(static_cast<i64>(dataWeightPerJob * dataWeightRatio * 2), (i64)1);
            }
            JobCount_ = DivCeil(InputDataWeight_, dataWeightPerJob);
        } else {
            i64 dataWeightPerJob = Spec_->JobIO->TableWriter->DesiredChunkSize / compressionRatio;

            if (dataWeightPerJob / dataWeightRatio > Options_->DataWeightPerJob) {
                // This means that compression ration w.r.t data weight is very small,
                // so we would like to limit uncompressed data size per job.
                dataWeightPerJob = Options_->DataWeightPerJob * dataWeightRatio;
            }
            JobCount_ = DivCeil(InputDataWeight_, dataWeightPerJob);
        }

        i64 maxJobCount = Options_->MaxJobCount;
        if (Spec_->MaxJobCount) {
            maxJobCount = std::min(maxJobCount, static_cast<i64>(*Spec_->MaxJobCount));
        }
        JobCount_ = std::min(JobCount_, maxJobCount);

        YCHECK(JobCount_ >= 0);
        YCHECK(JobCount_ != 0 || InputDataWeight_ == 0);
    }

    virtual bool CanAdjustDataWeightPerJob() const override
    {
        return !Spec_->DataWeightPerJob && !Spec_->JobCount;
    }

    virtual bool IsExplicitJobCount() const override
    {
        return false;
    }

    virtual i64 GetDataWeightPerJob() const override
    {
        auto dataWeightPerJob = JobCount_ > 0
            ? DivCeil(InputDataWeight_, JobCount_)
            : 1;

        return std::min(dataWeightPerJob, DivCeil<i64>(GetMaxDataWeightPerJob() , 2));
    }

    virtual i64 GetPrimaryDataWeightPerJob() const override
    {
        return GetDataWeightPerJob();
    }

    virtual i64 GetMaxDataSlicesPerJob() const override
    {
        return std::max<i64>(Options_->MaxDataSlicesPerJob, Spec_->JobCount && *Spec_->JobCount > 0
            ? DivCeil<i64>(InputChunkCount_, *Spec_->JobCount)
            : 1);
    }

    virtual i64 GetInputSliceRowCount() const override
    {
        return std::numeric_limits<i64>::max();
    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        TJobSizeConstraintsBase::Persist(context);

        using NYT::Persist;

        Persist(context, Spec_);
        Persist(context, Options_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMergeJobSizeConstraints, 0x3f1caf80);

    TSimpleOperationSpecBasePtr Spec_;
    TSimpleOperationOptionsPtr Options_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMergeJobSizeConstraints);
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
        i64 inputDataWeight)
        : TJobSizeConstraintsBase(inputDataWeight, inputDataWeight, spec, options, logger)
        , Spec_(spec)
        , Options_(options)
    {
        JobCount_ = DivCeil(InputDataWeight_, Spec_->DataWeightPerShuffleJob);
        YCHECK(JobCount_ >= 0);
        YCHECK(JobCount_ != 0 || InputDataWeight_ == 0);
    }

    virtual bool CanAdjustDataWeightPerJob() const override
    {
        return false;
    }

    virtual bool IsExplicitJobCount() const override
    {
        return false;
    }

    virtual i64 GetDataWeightPerJob() const override
    {
        return JobCount_ > 0
            ? DivCeil(InputDataWeight_, JobCount_)
            : 1;
    }

    virtual i64 GetPrimaryDataWeightPerJob() const override
    {
        Y_UNREACHABLE();
    }

    virtual i64 GetMaxDataSlicesPerJob() const override
    {
        return Options_->MaxDataSlicesPerJob;
    }

    virtual i64 GetInputSliceRowCount() const override
    {
        return std::numeric_limits<i64>::max();
    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        TJobSizeConstraintsBase::Persist(context);

        using NYT::Persist;

        Persist(context, Spec_);
        Persist(context, Options_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSimpleSortJobSizeConstraints, 0xef270530);

    TSortOperationSpecBasePtr Spec_;
    TSortOperationOptionsBasePtr Options_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSimpleSortJobSizeConstraints);
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
        i64 inputDataSize,
        i64 inputDataWeight,
        i64 inputRowCount,
        double compressionRatio)
        : TJobSizeConstraintsBase(
            inputDataWeight,
            inputDataWeight,
            spec,
            options,
            logger,
            inputRowCount,
            std::numeric_limits<i64>::max() /* inputChunkCount */,
            1 /* mergeInputTableCount */,
            1 /* primaryMergeInputTableCount */,
            spec->Sampling)
        , Spec_(spec)
        , Options_(options)
    {
        if (Spec_->PartitionJobCount) {
            JobCount_ = *Spec_->PartitionJobCount;
        } else if (Spec_->DataWeightPerPartitionJob) {
            i64 dataWeightPerJob = *Spec_->DataWeightPerPartitionJob;
            JobCount_ = DivCeil(InputDataWeight_, dataWeightPerJob);
        } else {
            // Rationale and details are on the wiki.
            // https://wiki.yandex-team.ru/yt/design/partitioncount/
            i64 uncompressedBlockSize = static_cast<i64>(Options_->CompressedBlockSize / compressionRatio);
            uncompressedBlockSize = std::min(uncompressedBlockSize, Spec_->PartitionJobIO->TableWriter->BlockSize);

            // Just in case compression ratio is very large.
            uncompressedBlockSize = std::max(i64(1), uncompressedBlockSize);

            // Product may not fit into i64.
            double partitionJobDataWeight = sqrt(InputDataWeight_) * sqrt(uncompressedBlockSize);
            partitionJobDataWeight = std::min(partitionJobDataWeight, static_cast<double>(Spec_->PartitionJobIO->TableWriter->MaxBufferSize));

            JobCount_ = DivCeil(InputDataWeight_, static_cast<i64>(partitionJobDataWeight));
        }

        YCHECK(JobCount_ >= 0);
        YCHECK(JobCount_ != 0 || InputDataWeight_ == 0);

        if (JobCount_ > 0 && inputDataSize / JobCount_ > Spec_->MaxDataWeightPerJob) {
            // Sometimes (but rarely) data weight can be smaller than data size. Let's protect from
            // unreasonable huge jobs.
            JobCount_ = DivCeil(inputDataSize, 2 * Spec_->MaxDataWeightPerJob);
        }


        JobCount_ = std::min(JobCount_, static_cast<i64>(Options_->MaxPartitionJobCount));
        JobCount_ = std::min(JobCount_, InputRowCount_);
    }

    virtual bool CanAdjustDataWeightPerJob() const override
    {
        return !Spec_->DataWeightPerPartitionJob && !Spec_->PartitionJobCount;
    }

    virtual bool IsExplicitJobCount() const override
    {
        return static_cast<bool>(Spec_->PartitionJobCount);
    }

    virtual i64 GetDataWeightPerJob() const override
    {
        return JobCount_ > 0
            ? DivCeil(InputDataWeight_, JobCount_)
            : 1;
    }

    virtual i64 GetPrimaryDataWeightPerJob() const override
    {
        Y_UNREACHABLE();
    }

    virtual i64 GetMaxDataSlicesPerJob() const override
    {
        return Options_->MaxDataSlicesPerJob;
    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        TJobSizeConstraintsBase::Persist(context);

        using NYT::Persist;

        Persist(context, Spec_);
        Persist(context, Options_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TPartitionJobSizeConstraints, 0xeea00714);

    TSortOperationSpecBasePtr Spec_;
    TSortOperationOptionsBasePtr Options_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TPartitionJobSizeConstraints);
DEFINE_REFCOUNTED_TYPE(TPartitionJobSizeConstraints)

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
    { }

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
        YCHECK(SamplingRate_);
        return SamplingDataWeightPerJob_;
    }

    virtual i64 GetSamplingPrimaryDataWeightPerJob() const override
    {
        YCHECK(SamplingRate_);
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

    virtual void Persist(const TPersistenceContext& context) override
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

IJobSizeConstraintsPtr CreateUserJobSizeConstraints(
    const TSimpleOperationSpecBasePtr& spec,
    const TSimpleOperationOptionsPtr& options,
    TLogger logger,
    int outputTableCount,
    double dataWeightRatio,
    i64 inputChunkCount,
    i64 primaryInputDataSize,
    i64 inputRowCount,
    i64 foreignInputDataSize,
    int mergeInputTableCount,
    int mergePrimaryInputTableCount)
{
    return New<TUserJobSizeConstraints>(
        spec,
        options,
        logger,
        outputTableCount,
        dataWeightRatio,
        inputChunkCount,
        primaryInputDataSize,
        inputRowCount,
        foreignInputDataSize,
        mergeInputTableCount,
        mergePrimaryInputTableCount);
}

IJobSizeConstraintsPtr CreateMergeJobSizeConstraints(
    const NScheduler::TSimpleOperationSpecBasePtr& spec,
    const TSimpleOperationOptionsPtr& options,
    TLogger logger,
    i64 inputChunkCount,
    i64 inputDataWeight,
    double dataWeightRatio,
    double compressionRatio,
    int mergeInputTableCount,
    int mergePrimaryInputTableCount)
{
    return New<TMergeJobSizeConstraints>(
        spec,
        options,
        logger,
        inputChunkCount,
        inputDataWeight,
        dataWeightRatio,
        compressionRatio,
        mergeInputTableCount,
        mergePrimaryInputTableCount);
}

IJobSizeConstraintsPtr CreateSimpleSortJobSizeConstraints(
    const TSortOperationSpecBasePtr& spec,
    const TSortOperationOptionsBasePtr& options,
    TLogger logger,
    i64 inputDataWeight)
{
    return New<TSimpleSortJobSizeConstraints>(spec, options, logger, inputDataWeight);
}

IJobSizeConstraintsPtr CreatePartitionJobSizeConstraints(
    const TSortOperationSpecBasePtr& spec,
    const TSortOperationOptionsBasePtr& options,
    TLogger logger,
    i64 inputDataSize,
    i64 inputDataWeight,
    i64 inputRowCount,
    double compressionRatio)
{
    return New<TPartitionJobSizeConstraints>(spec, options, logger, inputDataSize, inputDataWeight, inputRowCount, compressionRatio);
}

IJobSizeConstraintsPtr CreatePartitionBoundSortedJobSizeConstraints(
    const TSortOperationSpecBasePtr& spec,
    const TSortOperationOptionsBasePtr& options,
    TLogger logger,
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
        false /* canAdjustDataSizePerJob */,
        false /* isExplicitJobCount */,
        0 /* jobCount */,
        dataWeightPerJob /* dataWeightPerJob */,
        dataWeightPerJob /* primaryDataWeightPerJob */,
        options->MaxDataSlicesPerJob /* maxDataSlicesPerJob */,
        std::numeric_limits<i64>::max() /* maxDataWeightPerJob */,
        std::numeric_limits<i64>::max() /* maxPrimaryDataWeightPerJob */,
        std::numeric_limits<i64>::max() /* inputSliceDataSize */,
        std::numeric_limits<i64>::max() /* inputSliceRowCount */,
        std::nullopt /* samplingRate */);
}

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

} // namespace NControllerAgent
} // namespace NYT

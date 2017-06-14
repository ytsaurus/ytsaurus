#include "helpers.h"

#include "serialize.h"

#include <yt/server/scheduler/config.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NControllerAgent {

using namespace NObjectClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobSizeConstraints
    : public IJobSizeConstraints
{
public:
    TSimpleJobSizeConstraints()
        : InputDataSize_(-1)
        , InputRowCount_(-1)
    { }

    TSimpleJobSizeConstraints(
        const TSimpleOperationSpecBasePtr& spec,
        const TSimpleOperationOptionsPtr& options,
        int outputTableCount,
        i64 primaryInputDataSize,
        i64 inputRowCount,
        i64 foreignInputDataSize)
        : Spec_(spec)
        , Options_(options)
        , InputDataSize_(primaryInputDataSize + foreignInputDataSize)
        , PrimaryInputDataSize_(primaryInputDataSize)
        , InputRowCount_(inputRowCount)
    {
        if (Spec_->JobCount) {
            JobCount_ = *Spec_->JobCount;
        } else {
            i64 dataSizePerJob = Spec_->DataSizePerJob.Get(Options_->DataSizePerJob);
            JobCount_ = DivCeil(PrimaryInputDataSize_, dataSizePerJob);
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
        YCHECK(JobCount_ != 0 || InputDataSize_ == 0);
    }

    virtual bool CanAdjustDataSizePerJob() const override
    {
        return !Spec_->DataSizePerJob && !Spec_->JobCount;
    }

    virtual bool IsExplicitJobCount() const override
    {
        // If #DataSizePerJob == 1, we guarantee #JobCount == #RowCount (if row count doesn't exceed #MaxJobCount).
        return static_cast<bool>(Spec_->JobCount) ||
            (static_cast<bool>(Spec_->DataSizePerJob) && Spec_->DataSizePerJob.Get() == 1);
    }

    virtual int GetJobCount() const override
    {
        return JobCount_;
    }

    virtual i64 GetDataSizePerJob() const override
    {
        return JobCount_ > 0
            ? DivCeil(InputDataSize_, JobCount_)
            : 1;
    }

    virtual i64 GetPrimaryDataSizePerJob() const override
    {
        return JobCount_ > 0
            ? DivCeil(PrimaryInputDataSize_, JobCount_)
            : 1;
    }

    virtual i64 GetMaxDataSlicesPerJob() const override
    {
        return Options_->MaxDataSlicesPerJob;
    }

    virtual i64 GetMaxDataSizePerJob() const override
    {
        return Spec_->MaxDataSizePerJob;
    }

    virtual i64 GetInputSliceDataSize() const override
    {
        if (JobCount_ == 0 || InputDataSize_ == 0) {
            return 1;
        }

        i64 sliceDataSize = Clamp<i64>(
            Options_->SliceDataSizeMultiplier * InputDataSize_ / JobCount_,
            1,
            Options_->MaxSliceDataSize);

        if (sliceDataSize < Options_->MinSliceDataSize) {
            // Non-trivial multiplier should be used only if input data size is large enough.
            // Otherwise we do not want to have more slices than job count.

            sliceDataSize = DivCeil(InputDataSize_, JobCount_);
        }

        return sliceDataSize;
    }

    virtual i64 GetInputSliceRowCount() const override
    {
        return JobCount_ > 0
            ? DivCeil(InputRowCount_, JobCount_)
            : 1;
    }

    virtual void Persist(const NPhoenix::TPersistenceContext& context) override
    {
        using NYT::Persist;
        Persist(context, Spec_);
        Persist(context, Options_);
        Persist(context, InputDataSize_);
        Persist(context, InputRowCount_);
        Persist(context, PrimaryInputDataSize_);
        Persist(context, JobCount_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSimpleJobSizeConstraints, 0xb45cfe0d);

    TSimpleOperationSpecBasePtr Spec_;
    TSimpleOperationOptionsPtr Options_;

    i64 InputDataSize_;
    i64 PrimaryInputDataSize_;
    i64 InputRowCount_;

    i64 JobCount_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSimpleJobSizeConstraints);
DEFINE_REFCOUNTED_TYPE(TSimpleJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

class TSimpleSortJobSizeConstraints
    : public IJobSizeConstraints
{
public:
    TSimpleSortJobSizeConstraints()
        : InputDataSize_(-1)
    { }

    TSimpleSortJobSizeConstraints(
        const TSortOperationSpecBasePtr& spec,
        const TSortOperationOptionsBasePtr& options,
        i64 inputDataSize)
        : Spec_(spec)
        , Options_(options)
        , InputDataSize_(inputDataSize)
    {
        JobCount_ = DivCeil(InputDataSize_, Spec_->DataSizePerShuffleJob);
        YCHECK(JobCount_ >= 0);
        YCHECK(JobCount_ != 0 || InputDataSize_ == 0);
    }

    virtual bool CanAdjustDataSizePerJob() const override
    {
        return false;
    }

    virtual bool IsExplicitJobCount() const override
    {
        return false;
    }

    virtual int GetJobCount() const override
    {
        return JobCount_;
    }

    virtual i64 GetDataSizePerJob() const override
    {
        return JobCount_ > 0
            ? DivCeil(InputDataSize_, JobCount_)
            : 1;
    }

    virtual i64 GetPrimaryDataSizePerJob() const override
    {
        Y_UNREACHABLE();
    }

    virtual i64 GetMaxDataSlicesPerJob() const override
    {
        return Options_->MaxDataSlicesPerJob;
    }

    virtual i64 GetMaxDataSizePerJob() const override
    {
        return Spec_->MaxDataSizePerJob;
    }

    virtual i64 GetInputSliceDataSize() const override
    {
        if (JobCount_ == 0 || InputDataSize_ == 0) {
            return 1;
        }

        i64 sliceDataSize = Clamp<i64>(
            Options_->SliceDataSizeMultiplier * InputDataSize_ / JobCount_,
            1,
            Options_->MaxSliceDataSize);

        if (sliceDataSize < Options_->MinSliceDataSize) {
            // Non-trivial multiplier should be used only if input data size is large enough.
            // Otherwise we do not want to have more slices than job count.

            sliceDataSize = DivCeil(InputDataSize_, JobCount_);
        }

        return sliceDataSize;
    }

    virtual i64 GetInputSliceRowCount() const override
    {
        return std::numeric_limits<i64>::max();
    }

    virtual void Persist(const NPhoenix::TPersistenceContext& context) override
    {
        using NYT::Persist;
        Persist(context, Spec_);
        Persist(context, Options_);
        Persist(context, InputDataSize_);
        Persist(context, JobCount_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSimpleSortJobSizeConstraints, 0xef270530);

    TSortOperationSpecBasePtr Spec_;
    TSortOperationOptionsBasePtr Options_;

    i64 InputDataSize_;

    i64 JobCount_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TSimpleSortJobSizeConstraints);
DEFINE_REFCOUNTED_TYPE(TSimpleSortJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

class TPartitionJobSizeConstraints
    : public IJobSizeConstraints
{
public:
    TPartitionJobSizeConstraints()
        : InputDataSize_(-1)
        , InputRowCount_(-1)
    { }

    TPartitionJobSizeConstraints(
        const TSortOperationSpecBasePtr& spec,
        const TSortOperationOptionsBasePtr& options,
        i64 inputDataSize,
        i64 inputRowCount,
        double compressionRatio)
        : Spec_(spec)
        , Options_(options)
        , InputDataSize_(inputDataSize)
        , InputRowCount_(inputRowCount)
    {
        if (Spec_->PartitionJobCount) {
            JobCount_ = *Spec_->PartitionJobCount;
        } else if (Spec_->DataSizePerPartitionJob) {
            i64 dataSizePerJob = *Spec_->DataSizePerPartitionJob;
            JobCount_ = DivCeil(InputDataSize_, dataSizePerJob);
        } else {
            // Rationale and details are on the wiki.
            // https://wiki.yandex-team.ru/yt/design/partitioncount/
            i64 uncompressedBlockSize = static_cast<i64>(Options_->CompressedBlockSize / compressionRatio);
            uncompressedBlockSize = std::min(uncompressedBlockSize, Spec_->PartitionJobIO->TableWriter->BlockSize);

            // Product may not fit into i64.
            double partitionJobDataSize = sqrt(InputDataSize_) * sqrt(uncompressedBlockSize);
            partitionJobDataSize = std::min(partitionJobDataSize, static_cast<double>(Spec_->PartitionJobIO->TableWriter->MaxBufferSize));

            JobCount_ = DivCeil(InputDataSize_, static_cast<i64>(partitionJobDataSize));
        }

        YCHECK(JobCount_ >= 0);
        YCHECK(JobCount_ != 0 || InputDataSize_ == 0);

        JobCount_ = std::min(JobCount_, static_cast<i64>(Options_->MaxPartitionJobCount));
        JobCount_ = std::min(JobCount_, InputRowCount_);
    }

    virtual bool CanAdjustDataSizePerJob() const override
    {
        return !Spec_->DataSizePerPartitionJob && !Spec_->PartitionJobCount;
    }

    virtual bool IsExplicitJobCount() const override
    {
        return static_cast<bool>(Spec_->PartitionJobCount);
    }

    virtual int GetJobCount() const override
    {
        return JobCount_;
    }

    virtual i64 GetDataSizePerJob() const override
    {
        return JobCount_ > 0
            ? DivCeil(InputDataSize_, JobCount_)
            : 1;
    }

    virtual i64 GetPrimaryDataSizePerJob() const override
    {
        Y_UNREACHABLE();
    }

    virtual i64 GetMaxDataSlicesPerJob() const override
    {
        return Options_->MaxDataSlicesPerJob;
    }

    virtual i64 GetMaxDataSizePerJob() const override
    {
        return Spec_->MaxDataSizePerJob;
    }

    virtual i64 GetInputSliceDataSize() const override
    {
        if (JobCount_ == 0 || InputDataSize_ == 0) {
            return 1;
        }

        i64 sliceDataSize = Clamp<i64>(
            Options_->SliceDataSizeMultiplier * InputDataSize_ / JobCount_,
            1,
            Options_->MaxSliceDataSize);

        if (sliceDataSize < Options_->MinSliceDataSize) {
            // Non-trivial multiplier should be used only if input data size is large enough.
            // Otherwise we do not want to have more slices than job count.

            sliceDataSize = DivCeil(InputDataSize_, JobCount_);
        }

        return sliceDataSize;
    }

    virtual i64 GetInputSliceRowCount() const override
    {
        return JobCount_ > 0
            ? DivCeil(InputRowCount_, JobCount_)
            : 1;
    }

    virtual void Persist(const NPhoenix::TPersistenceContext& context) override
    {
        using NYT::Persist;
        Persist(context, Spec_);
        Persist(context, Options_);
        Persist(context, InputDataSize_);
        Persist(context, InputRowCount_);
        Persist(context, JobCount_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TPartitionJobSizeConstraints, 0xeea00714);

    TSortOperationSpecBasePtr Spec_;
    TSortOperationOptionsBasePtr Options_;

    i64 InputDataSize_;
    i64 InputRowCount_;

    i64 JobCount_;
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
        bool canAdjustDataSizePerJob,
        bool isExplicitJobCount,
        int jobCount,
        i64 dataSizePerJob,
        i64 primaryDataSizePerJob,
        i64 maxDataSlicesPerJob,
        i64 maxDataSizePerJob,
        i64 inputSliceDataSize,
        i64 inputSliceRowCount)
        : CanAdjustDataSizePerJob_(canAdjustDataSizePerJob)
        , IsExplicitJobCount_(isExplicitJobCount)
        , JobCount_(jobCount)
        , DataSizePerJob_(dataSizePerJob)
        , PrimaryDataSizePerJob_(primaryDataSizePerJob)
        , MaxDataSlicesPerJob_(maxDataSlicesPerJob)
        , MaxDataSizePerJob_(maxDataSizePerJob)
        , InputSliceDataSize_(inputSliceDataSize)
        , InputSliceRowCount_(inputSliceRowCount)
    { }

    virtual bool CanAdjustDataSizePerJob() const override
    {
        return CanAdjustDataSizePerJob_;
    }

    virtual bool IsExplicitJobCount() const override
    {
        return IsExplicitJobCount_;
    }

    virtual int GetJobCount() const override
    {
        return JobCount_;
    }

    virtual i64 GetDataSizePerJob() const override
    {
        return DataSizePerJob_;
    }

    virtual i64 GetMaxDataSlicesPerJob() const override
    {
        return MaxDataSlicesPerJob_;
    }

    virtual i64 GetPrimaryDataSizePerJob() const override
    {
        return PrimaryDataSizePerJob_;
    }

    virtual i64 GetMaxDataSizePerJob() const override
    {
        return MaxDataSizePerJob_;
    }

    virtual i64 GetInputSliceDataSize() const override
    {
        return InputSliceDataSize_;
    }

    virtual i64 GetInputSliceRowCount() const override
    {
        return InputSliceRowCount_;
    }

    virtual void Persist(const NPhoenix::TPersistenceContext& context) override
    {
        using NYT::Persist;
        Persist(context, CanAdjustDataSizePerJob_);
        Persist(context, IsExplicitJobCount_);
        Persist(context, JobCount_);
        Persist(context, DataSizePerJob_);
        Persist(context, PrimaryDataSizePerJob_);
        Persist(context, MaxDataSlicesPerJob_);
        Persist(context, MaxDataSizePerJob_);
        Persist(context, InputSliceDataSize_);
        Persist(context, InputSliceRowCount_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TExplicitJobSizeConstraints, 0xab6bc389);

    bool CanAdjustDataSizePerJob_;
    bool IsExplicitJobCount_;
    int JobCount_;
    i64 DataSizePerJob_;
    i64 PrimaryDataSizePerJob_;
    i64 MaxDataSlicesPerJob_;
    i64 MaxDataSizePerJob_;
    i64 InputSliceDataSize_;
    i64 InputSliceRowCount_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TExplicitJobSizeConstraints);
DEFINE_REFCOUNTED_TYPE(TExplicitJobSizeConstraints);

////////////////////////////////////////////////////////////////////////////////

IJobSizeConstraintsPtr CreateSimpleJobSizeConstraints(
    const TSimpleOperationSpecBasePtr& spec,
    const TSimpleOperationOptionsPtr& options,
    int outputTableCount,
    i64 primaryInputDataSize,
    i64 inputRowCount,
    i64 foreignInputDataSize)
{
    return New<TSimpleJobSizeConstraints>(spec, options, outputTableCount, primaryInputDataSize, inputRowCount, foreignInputDataSize);
}

IJobSizeConstraintsPtr CreateSimpleSortJobSizeConstraints(
    const TSortOperationSpecBasePtr& spec,
    const TSortOperationOptionsBasePtr& options,
    i64 inputDataSize)
{
    return New<TSimpleSortJobSizeConstraints>(spec, options, inputDataSize);
}

IJobSizeConstraintsPtr CreatePartitionJobSizeConstraints(
    const TSortOperationSpecBasePtr& spec,
    const TSortOperationOptionsBasePtr& options,
    i64 inputDataSize,
    i64 inputRowCount,
    double compressionRatio)
{
    return New<TPartitionJobSizeConstraints>(spec, options, inputDataSize, inputRowCount, compressionRatio);
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
    i64 estimatedDataSizePerPartition = 2 * spec->DataSizePerSortedJob.Get(spec->DataSizePerShuffleJob);

    i64 minDataSizePerJob = std::max(estimatedDataSizePerPartition / jobsPerPartition, (i64)1);
    i64 dataSizePerJob = std::max(minDataSizePerJob, spec->DataSizePerSortedJob.Get(spec->DataSizePerShuffleJob));

    return CreateExplicitJobSizeConstraints(
        false /* canAdjustDataSizePerJob */,
        false /* isExplicitJobCount */,
        0 /* jobCount */,
        dataSizePerJob /* dataSizePerJob */,
        dataSizePerJob /* dataSizePerJob */,
        options->MaxDataSlicesPerJob /* maxDataSlicesPerJob */,
        std::numeric_limits<i64>::max() /* maxDataSizePerJob */,
        std::numeric_limits<i64>::max() /* inputSliceDataSize */,
        std::numeric_limits<i64>::max() /* inputSliceRowCount */);
}

IJobSizeConstraintsPtr CreateExplicitJobSizeConstraints(
    bool canAdjustDataSizePerJob,
    bool isExplicitJobCount,
    int jobCount,
    i64 dataSizePerJob,
    i64 primaryDataSizePerJob,
    i64 maxDataSlicesPerJob,
    i64 maxDataSizePerJob,
    i64 inputSliceDataSize,
    i64 inputSliceRowCount)
{
    return New<TExplicitJobSizeConstraints>(
        canAdjustDataSizePerJob,
        isExplicitJobCount,
        jobCount,
        dataSizePerJob,
        primaryDataSizePerJob,
        maxDataSlicesPerJob,
        maxDataSizePerJob,
        inputSliceDataSize,
        inputSliceRowCount);
}

////////////////////////////////////////////////////////////////////////////////

TString TrimCommandForBriefSpec(const TString& command)
{
    const int MaxBriefSpecCommandLength = 256;
    return
        command.length() <= MaxBriefSpecCommandLength
        ? command
        : command.substr(0, MaxBriefSpecCommandLength) + "...";
}

////////////////////////////////////////////////////////////////////////////////

TString TLockedUserObject::GetPath() const
{
    return FromObjectId(ObjectId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT


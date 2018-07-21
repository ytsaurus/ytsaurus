#include "helpers.h"
#include "config.h"

#include "serialize.h"
#include "table.h"

#include <yt/server/scheduler/config.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/table_client/row_buffer.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/scheduler/proto/output_result.pb.h>
#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/hive/cluster_directory.h>

#include <yt/core/misc/numeric_helpers.h>

#include <yt/core/ytree/helpers.h>

namespace NYT {
namespace NControllerAgent {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NScheduler;
using namespace NTableClient;
using namespace NYTree;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

class TUserJobSizeConstraints
    : public IJobSizeConstraints
{
public:
    TUserJobSizeConstraints()
        : InputDataWeight_(-1)
        , InputRowCount_(-1)
    { }

    TUserJobSizeConstraints(
        const TSimpleOperationSpecBasePtr& spec,
        const TSimpleOperationOptionsPtr& options,
        int outputTableCount,
        double dataWeightRatio,
        i64 inputChunkCount,
        i64 primaryInputDataWeight,
        i64 inputRowCount,
        i64 foreignInputDataWeight)
        : Spec_(spec)
        , Options_(options)
        , InputChunkCount_(inputChunkCount)
        , InputDataWeight_(primaryInputDataWeight + foreignInputDataWeight)
        , PrimaryInputDataWeight_(primaryInputDataWeight)
        , InputRowCount_(inputRowCount)
    {
        if (Spec_->JobCount) {
            JobCount_ = *Spec_->JobCount;
        } else if (PrimaryInputDataWeight_ > 0) {
            i64 dataWeightPerJob = Spec_->DataWeightPerJob.Get(Options_->DataWeightPerJob);

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
            (static_cast<bool>(Spec_->DataWeightPerJob) && Spec_->DataWeightPerJob.Get() == 1);
    }

    virtual int GetJobCount() const override
    {
        return JobCount_;
    }

    virtual i64 GetDataWeightPerJob() const override
    {
        if (JobCount_ == 0 ){
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
            ? DivCeil(PrimaryInputDataWeight_, JobCount_)
            : 1;
    }

    virtual i64 GetMaxDataSlicesPerJob() const override
    {
        return std::max<i64>(Options_->MaxDataSlicesPerJob, Spec_->JobCount && Spec_->JobCount.Get() > 0
            ? DivCeil<i64>(InputChunkCount_, Spec_->JobCount.Get())
            : 1);
    }

    virtual i64 GetMaxDataWeightPerJob() const override
    {
        return Spec_->MaxDataWeightPerJob;
    }

    virtual i64 GetInputSliceDataWeight() const override
    {
        if (JobCount_ == 0 || InputDataWeight_ == 0) {
            return 1;
        }

        i64 sliceDataWeight = Clamp<i64>(
            Options_->SliceDataWeightMultiplier * PrimaryInputDataWeight_ / JobCount_,
            1,
            Options_->MaxSliceDataWeight);

        if (sliceDataWeight < Options_->MinSliceDataWeight) {
            // Non-trivial multiplier should be used only if input data size is large enough.
            // Otherwise we do not want to have more slices than job count.

            sliceDataWeight = DivCeil(InputDataWeight_, JobCount_);
        }

        return sliceDataWeight;
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
        Persist(context, InputDataWeight_);
        Persist(context, InputRowCount_);
        Persist(context, PrimaryInputDataWeight_);
        Persist(context, JobCount_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUserJobSizeConstraints, 0xb45cfe0d);

    TSimpleOperationSpecBasePtr Spec_;
    TSimpleOperationOptionsPtr Options_;

    i64 InputChunkCount_;
    i64 InputDataWeight_;
    i64 PrimaryInputDataWeight_;
    i64 InputRowCount_;

    i64 JobCount_;

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
    : public IJobSizeConstraints
{
public:
    TMergeJobSizeConstraints()
        : InputDataWeight_(-1)
    { }

    TMergeJobSizeConstraints(
        const TSimpleOperationSpecBasePtr& spec,
        const TSimpleOperationOptionsPtr& options,
        i64 inputChunkCount,
        i64 inputDataWeight,
        double dataWeightRatio,
        double compressionRatio)
        : Spec_(spec)
        , Options_(options)
        , InputChunkCount_(inputChunkCount)
        , InputDataWeight_(inputDataWeight)
    {
        if (Spec_->JobCount) {
            JobCount_ = *Spec_->JobCount;
        } else if (Spec_->DataWeightPerJob) {
            i64 dataWeightPerJob = *Spec_->DataWeightPerJob;
            if (dataWeightRatio < 1.0 / 2) {
                // This means that uncompressed data size is larger than 2x data weight,
                // which may happen for very sparse data. Than, adjust data weight accordingly.
                dataWeightPerJob = std::max<i64>(dataWeightPerJob * dataWeightRatio * 2, 1);
            }
            JobCount_ = DivCeil(InputDataWeight_, dataWeightPerJob);
        } else {
            i64 dataWeightPerJob = std::max<i64>(Spec_->JobIO->TableWriter->DesiredChunkSize / compressionRatio, 1);

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

    virtual int GetJobCount() const override
    {
        return JobCount_;
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
        return std::max<i64>(Options_->MaxDataSlicesPerJob, Spec_->JobCount && Spec_->JobCount.Get() > 0
            ? DivCeil<i64>(InputChunkCount_, Spec_->JobCount.Get())
            : 1);
    }

    virtual i64 GetMaxDataWeightPerJob() const override
    {
        return Spec_->MaxDataWeightPerJob;
    }

    virtual i64 GetInputSliceDataWeight() const override
    {
        if (JobCount_ == 0 || InputDataWeight_ == 0) {
            return 1;
        }

        i64 sliceDataWeight = Clamp<i64>(
            Options_->SliceDataWeightMultiplier * InputDataWeight_ / JobCount_,
            1,
            Options_->MaxSliceDataWeight);

        if (sliceDataWeight < Options_->MinSliceDataWeight) {
            // Non-trivial multiplier should be used only if input data size is large enough.
            // Otherwise we do not want to have more slices than job count.

            sliceDataWeight = DivCeil(InputDataWeight_, JobCount_);
        }

        return sliceDataWeight;
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
        Persist(context, InputDataWeight_);
        Persist(context, JobCount_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMergeJobSizeConstraints, 0x3f1caf80);

    TSimpleOperationSpecBasePtr Spec_;
    TSimpleOperationOptionsPtr Options_;

    i64 InputChunkCount_;
    i64 InputDataWeight_;
    i64 JobCount_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMergeJobSizeConstraints);
DEFINE_REFCOUNTED_TYPE(TMergeJobSizeConstraints)

////////////////////////////////////////////////////////////////////////////////

class TSimpleSortJobSizeConstraints
    : public IJobSizeConstraints
{
public:
    TSimpleSortJobSizeConstraints()
        : InputDataWeight_(-1)
    { }

    TSimpleSortJobSizeConstraints(
        const TSortOperationSpecBasePtr& spec,
        const TSortOperationOptionsBasePtr& options,
        i64 inputDataWeight)
        : Spec_(spec)
        , Options_(options)
        , InputDataWeight_(inputDataWeight)
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

    virtual int GetJobCount() const override
    {
        return JobCount_;
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

    virtual i64 GetMaxDataWeightPerJob() const override
    {
        return Spec_->MaxDataWeightPerJob;
    }

    virtual i64 GetInputSliceDataWeight() const override
    {
        if (JobCount_ == 0 || InputDataWeight_ == 0) {
            return 1;
        }

        i64 sliceDataWeight = Clamp<i64>(
            Options_->SliceDataWeightMultiplier * InputDataWeight_ / JobCount_,
            1,
            Options_->MaxSliceDataWeight);

        if (sliceDataWeight < Options_->MinSliceDataWeight) {
            // Non-trivial multiplier should be used only if input data size is large enough.
            // Otherwise we do not want to have more slices than job count.

            sliceDataWeight = DivCeil(InputDataWeight_, JobCount_);
        }

        return sliceDataWeight;
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
        Persist(context, InputDataWeight_);
        Persist(context, JobCount_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TSimpleSortJobSizeConstraints, 0xef270530);

    TSortOperationSpecBasePtr Spec_;
    TSortOperationOptionsBasePtr Options_;

    i64 InputDataWeight_;

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
        : InputDataWeight_(-1)
        , InputRowCount_(-1)
    { }

    TPartitionJobSizeConstraints(
        const TSortOperationSpecBasePtr& spec,
        const TSortOperationOptionsBasePtr& options,
        i64 inputDataSize,
        i64 inputDataWeight,
        i64 inputRowCount,
        double compressionRatio)
        : Spec_(spec)
        , Options_(options)
        , InputDataWeight_(inputDataWeight)
        , InputRowCount_(inputRowCount)
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

    virtual int GetJobCount() const override
    {
        return JobCount_;
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

    virtual i64 GetMaxDataWeightPerJob() const override
    {
        return Spec_->MaxDataWeightPerJob;
    }

    virtual i64 GetInputSliceDataWeight() const override
    {
        if (JobCount_ == 0 || InputDataWeight_ == 0) {
            return 1;
        }

        i64 sliceDataSize = Clamp<i64>(
            Options_->SliceDataWeightMultiplier * InputDataWeight_ / JobCount_,
            1,
            Options_->MaxSliceDataWeight);

        if (sliceDataSize < Options_->MinSliceDataWeight) {
            // Non-trivial multiplier should be used only if input data size is large enough.
            // Otherwise we do not want to have more slices than job count.

            sliceDataSize = DivCeil(InputDataWeight_, JobCount_);
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
        Persist(context, InputDataWeight_);
        Persist(context, InputRowCount_);
        Persist(context, JobCount_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TPartitionJobSizeConstraints, 0xeea00714);

    TSortOperationSpecBasePtr Spec_;
    TSortOperationOptionsBasePtr Options_;

    i64 InputDataWeight_;
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
        bool canAdjustDataWeightPerJob,
        bool isExplicitJobCount,
        int jobCount,
        i64 dataWeightPerJob,
        i64 primaryDataWeightPerJob,
        i64 maxDataSlicesPerJob,
        i64 maxDataWeightPerJob,
        i64 inputSliceDataWeight,
        i64 inputSliceRowCount)
        : CanAdjustDataWeightPerJob_(canAdjustDataWeightPerJob)
        , IsExplicitJobCount_(isExplicitJobCount)
        , JobCount_(jobCount)
        , DataWeightPerJob_(dataWeightPerJob)
        , PrimaryDataWeightPerJob_(primaryDataWeightPerJob)
        , MaxDataSlicesPerJob_(maxDataSlicesPerJob)
        , MaxDataWeightPerJob_(maxDataWeightPerJob)
        , InputSliceDataWeight_(inputSliceDataWeight)
        , InputSliceRowCount_(inputSliceRowCount)
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

    virtual i64 GetInputSliceDataWeight() const override
    {
        return InputSliceDataWeight_;
    }

    virtual i64 GetInputSliceRowCount() const override
    {
        return InputSliceRowCount_;
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
        Persist(context, InputSliceDataWeight_);
        Persist(context, InputSliceRowCount_);
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
    i64 InputSliceDataWeight_;
    i64 InputSliceRowCount_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TExplicitJobSizeConstraints);
DEFINE_REFCOUNTED_TYPE(TExplicitJobSizeConstraints);

////////////////////////////////////////////////////////////////////////////////

IJobSizeConstraintsPtr CreateUserJobSizeConstraints(
    const TSimpleOperationSpecBasePtr& spec,
    const TSimpleOperationOptionsPtr& options,
    int outputTableCount,
    double dataWeightRatio,
    i64 inputChunkCount,
    i64 primaryInputDataSize,
    i64 inputRowCount,
    i64 foreignInputDataSize)
{
    return New<TUserJobSizeConstraints>(
        spec,
        options,
        outputTableCount,
        dataWeightRatio,
        inputChunkCount,
        primaryInputDataSize,
        inputRowCount,
        foreignInputDataSize);
}

IJobSizeConstraintsPtr CreateMergeJobSizeConstraints(
    const NScheduler::TSimpleOperationSpecBasePtr& spec,
    const TSimpleOperationOptionsPtr& options,
    i64 inputChunkCount,
    i64 inputDataWeight,
    double dataWeightRatio,
    double compressionRatio)
{
    return New<TMergeJobSizeConstraints>(
        spec,
        options,
        inputChunkCount,
        inputDataWeight,
        dataWeightRatio,
        compressionRatio);
}

IJobSizeConstraintsPtr CreateSimpleSortJobSizeConstraints(
    const TSortOperationSpecBasePtr& spec,
    const TSortOperationOptionsBasePtr& options,
    i64 inputDataWeight)
{
    return New<TSimpleSortJobSizeConstraints>(spec, options, inputDataWeight);
}

IJobSizeConstraintsPtr CreatePartitionJobSizeConstraints(
    const TSortOperationSpecBasePtr& spec,
    const TSortOperationOptionsBasePtr& options,
    i64 inputDataSize,
    i64 inputDataWeight,
    i64 inputRowCount,
    double compressionRatio)
{
    return New<TPartitionJobSizeConstraints>(spec, options, inputDataSize, inputDataWeight, inputRowCount, compressionRatio);
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
    i64 estimatedDataSizePerPartition = 2 * spec->DataWeightPerSortedJob.Get(spec->DataWeightPerShuffleJob);

    i64 minDataSizePerJob = std::max(estimatedDataSizePerPartition / jobsPerPartition, (i64)1);
    i64 dataSizePerJob = std::max(minDataSizePerJob, spec->DataWeightPerSortedJob.Get(spec->DataWeightPerShuffleJob));

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

NYTree::INodePtr UpdateSpec(NYTree::INodePtr templateSpec, NYTree::INodePtr originalSpec)
{
    if (!templateSpec) {
        return originalSpec;
    }
    return PatchNode(templateSpec, originalSpec);
}

////////////////////////////////////////////////////////////////////////////////

TString TLockedUserObject::GetPath() const
{
    return FromObjectId(ObjectId);
}

////////////////////////////////////////////////////////////////////////////////

void TUserFile::Persist(const TPersistenceContext& context)
{
    TUserObject::Persist(context);

    using NYT::Persist;
    Persist<TAttributeDictionaryRefSerializer>(context, Attributes);
    Persist(context, FileName);
    Persist(context, ChunkSpecs);
    Persist(context, ChunkCount);
    Persist(context, Type);
    Persist(context, Executable);
    Persist(context, Format);
    Persist(context, Schema);
    Persist(context, IsDynamic);
    if (context.GetVersion() >= 202000) {
        Persist(context, IsLayer);
    }
}

////////////////////////////////////////////////////////////////////////////////

TBoundaryKeys BuildBoundaryKeysFromOutputResult(
    const NScheduler::NProto::TOutputResult& boundaryKeys,
    const TEdgeDescriptor& edgeDescriptor,
    const TRowBufferPtr& rowBuffer)
{
    YCHECK(!boundaryKeys.empty());
    YCHECK(boundaryKeys.sorted());
    YCHECK(!edgeDescriptor.TableWriterOptions->ValidateUniqueKeys || boundaryKeys.unique_keys());

    auto trimAndCaptureKey = [&] (const TOwningKey& key) {
        int limit = edgeDescriptor.TableUploadOptions.TableSchema.GetKeyColumnCount();
        if (key.GetCount() > limit) {
            // NB: This can happen for a teleported chunk from a table with a wider key in sorted (but not unique_keys) mode.
            YCHECK(!edgeDescriptor.TableWriterOptions->ValidateUniqueKeys);
            return rowBuffer->Capture(key.Begin(), limit);
        } else {
            return rowBuffer->Capture(key.Begin(), key.GetCount());
        }
    };

    return TBoundaryKeys {
        trimAndCaptureKey(FromProto<TOwningKey>(boundaryKeys.min())),
        trimAndCaptureKey(FromProto<TOwningKey>(boundaryKeys.max())),
    };
}

void BuildFileSpecs(NScheduler::NProto::TUserJobSpec* jobSpec, const std::vector<TUserFile>& files)
{
    for (const auto& file : files) {
        auto* descriptor = file.IsLayer
            ? jobSpec->add_layers()
            : jobSpec->add_files();

        ToProto(descriptor->mutable_chunk_specs(), file.ChunkSpecs);

        if (file.Type == EObjectType::Table && file.IsDynamic && file.Schema.IsSorted()) {
            auto dataSource = MakeVersionedDataSource(
                file.GetPath(),
                file.Schema,
                file.Path.GetColumns(),
                file.Path.GetTimestamp().Get(AsyncLastCommittedTimestamp));

            ToProto(descriptor->mutable_data_source(), dataSource);
        } else {
            auto dataSource = file.Type == EObjectType::File
                ? MakeFileDataSource(file.GetPath())
                : MakeUnversionedDataSource(file.GetPath(), file.Schema, file.Path.GetColumns());

            ToProto(descriptor->mutable_data_source(), dataSource);
        }

        if (!file.IsLayer) {
            descriptor->set_file_name(file.FileName);
            switch (file.Type) {
                case EObjectType::File:
                    descriptor->set_executable(file.Executable);
                    break;
                case EObjectType::Table:
                    descriptor->set_format(file.Format.GetData());
                    break;
                default:
                    Y_UNREACHABLE();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

NNative::IConnectionPtr FindRemoteConnection(
    const NNative::IConnectionPtr& connection,
    TCellTag cellTag)
{
    if (cellTag == connection->GetCellTag()) {
        return connection;
    }

    auto remoteConnection = connection->GetClusterDirectory()->FindConnection(cellTag);
    if (!remoteConnection) {
        return nullptr;
    }

    return dynamic_cast<NNative::IConnection*>(remoteConnection.Get());
}

NNative::IConnectionPtr GetRemoteConnectionOrThrow(
    const NNative::IConnectionPtr& connection,
    TCellTag cellTag)
{
    auto remoteConnection = FindRemoteConnection(connection, cellTag);
    if (!remoteConnection) {
        THROW_ERROR_EXCEPTION("Cannot find cluster with cell tag %v", cellTag);
    }
    return remoteConnection;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT


#include "helpers.h"
#include "public.h"
#include "exec_node.h"
#include "config.h"
#include "job.h"
#include "operation.h"
#include "operation_controller.h"
#include "chunk_pool.h"

#include <yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/ytlib/core_dump/core_info.pb.h>
#include <yt/ytlib/core_dump/helpers.h>

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/ypath/rich.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

using namespace NProto;
using namespace NYTree;
using namespace NYPath;
using namespace NCoreDump::NProto;
using namespace NYson;
using namespace NObjectClient;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

static const TString InputRowCountPath = "/data/input/row_count";
static const TString InputUncompressedDataSizePath = "/data/input/uncompressed_data_size";
static const TString InputCompressedDataSizePath = "/data/input/compressed_data_size";
static const TString InputPipeIdleTimePath = "/user_job/pipes/input/idle_time";
static const TString JobProxyCpuUsagePath = "/job_proxy/cpu/user";

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
        Persist(context, PrimaryInputDataSize_);
        Persist(context, InputRowCount_);
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

    virtual i64 GetPrimaryDataSizePerJob() const override
    {
        return PrimaryDataSizePerJob_;
    }

    virtual i64 GetMaxDataSlicesPerJob() const override
    {
        return MaxDataSlicesPerJob_;
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
    return New<TSimpleJobSizeConstraints>(
        spec,
        options,
        outputTableCount,
        primaryInputDataSize,
        inputRowCount,
        foreignInputDataSize);
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
        dataSizePerJob /* primaryDataSizePerJob */,
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

void BuildInitializingOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("operation_type").Value(operation->GetType())
        .Item("start_time").Value(operation->GetStartTime())
        .Item("spec").Value(operation->GetSpec())
        .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
        .Item("mutation_id").Value(operation->GetMutationId())
        .Do(BIND(&BuildRunningOperationAttributes, operation));
}

void BuildRunningOperationAttributes(TOperationPtr operation, NYson::IYsonConsumer* consumer)
{
    auto controller = operation->GetController();
    auto userTransaction = operation->GetUserTransaction();
    BuildYsonMapFluently(consumer)
        .Item("state").Value(operation->GetState())
        .Item("suspended").Value(operation->GetSuspended())
        .Item("user_transaction_id").Value(userTransaction ? userTransaction->GetId() : NullTransactionId)
        .Item("events").Value(operation->GetEvents())
        .Item("slot_index").Value(operation->GetSlotIndex())
        .DoIf(static_cast<bool>(controller), BIND(&IOperationController::BuildOperationAttributes, controller));
}

void BuildJobAttributes(TJobPtr job, NYson::IYsonConsumer* consumer)
{
    auto state = job->GetState();
    BuildYsonMapFluently(consumer)
        .Item("job_type").Value(FormatEnum(job->GetType()))
        .Item("state").Value(FormatEnum(state))
        .Item("address").Value(job->GetNode()->GetDefaultAddress())
        .Item("start_time").Value(job->GetStartTime())
        .Item("account").Value(job->GetAccount())
        .Item("progress").Value(job->GetProgress())
        .DoIf(static_cast<bool>(job->GetBriefStatistics()), [=] (TFluentMap fluent) {
            fluent.Item("brief_statistics").Value(*job->GetBriefStatistics());
        })
        .Item("suspicious").Value(job->GetSuspicious())
        .DoIf(job->GetFinishTime().HasValue(), [=] (TFluentMap fluent) {
            fluent.Item("finish_time").Value(job->GetFinishTime().Get());
        })
        .DoIf(state == EJobState::Failed, [=] (TFluentMap fluent) {
            auto error = FromProto<TError>(job->Status().result().error());
            fluent.Item("error").Value(error);
        })
        // NB: This extension is missing while job is running. It appears only when job is complete,
        // i.e. this item will be presented in Cypress but it will never appear when accessing
        // scheduler via Orchid.
        .DoIf(job->Status().result().HasExtension(TSchedulerJobResultExt::scheduler_job_result_ext),
            [=] (TFluentMap fluent)
        {
            const auto& schedulerResultExt = job->Status().result().GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            fluent.Item("core_infos").Value(schedulerResultExt.core_infos());
        });
}

void BuildExecNodeAttributes(TExecNodePtr node, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("state").Value(node->GetMasterState())
        .Item("resource_usage").Value(node->GetResourceUsage())
        .Item("resource_limits").Value(node->GetResourceLimits());
}

static void BuildInputSliceLimit(
    const TInputDataSlicePtr& slice,
    const TInputSliceLimit& limit,
    TNullable<i64> rowIndex,
    NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf((limit.RowIndex.operator bool() || rowIndex) && slice->IsTrivial(), [&] (TFluentMap fluent) {
                fluent
                    .Item("row_index").Value(
                        limit.RowIndex.Get(rowIndex.Get(0)) + slice->GetSingleUnversionedChunkOrThrow()->GetTableRowIndex());
            })
            .DoIf(limit.Key.operator bool(), [&] (TFluentMap fluent) {
                fluent
                    .Item("key").Value(limit.Key);
            })
        .EndMap();
}

TYsonString BuildInputPaths(
    const std::vector<TRichYPath>& inputPaths,
    const TChunkStripeListPtr& inputStripeList,
    EOperationType operationType,
    EJobType jobType)
{
    bool hasSlices = false;
    std::vector<std::vector<TInputDataSlicePtr>> slicesByTable(inputPaths.size());
    for (const auto& stripe : inputStripeList->Stripes) {
        for (const auto& slice : stripe->DataSlices) {
            auto tableIndex = slice->GetTableIndex();
            if (tableIndex >= 0) {
                slicesByTable[tableIndex].push_back(slice);
                hasSlices = true;
            }
        }
    }
    if (!hasSlices) {
        return TYsonString();
    }

    std::vector<char> isForeignTable(inputPaths.size());
    std::transform(
        inputPaths.begin(),
        inputPaths.end(),
        isForeignTable.begin(),
        [](const TRichYPath& path) { return path.GetForeign(); });

    std::vector<std::vector<std::pair<TInputDataSlicePtr, TInputDataSlicePtr>>> rangesByTable(inputPaths.size());
    bool mergeByRows = !(
        operationType == EOperationType::Reduce ||
        (operationType == EOperationType::Merge && jobType == EJobType::SortedMerge));
    for (int tableIndex = 0; tableIndex < static_cast<int>(slicesByTable.size()); ++tableIndex) {
        auto& tableSlices = slicesByTable[tableIndex];

        std::sort(tableSlices.begin(), tableSlices.end(), &CompareDataSlicesByLowerLimit);

        int firstSlice = 0;
        while (firstSlice < static_cast<int>(tableSlices.size())) {
            int lastSlice = firstSlice + 1;
            while (lastSlice < static_cast<int>(tableSlices.size())) {
                if (mergeByRows && !isForeignTable[tableIndex] &&
                    !CanMergeSlices(tableSlices[lastSlice - 1], tableSlices[lastSlice]))
                {
                    break;
                }
                ++lastSlice;
            }
            rangesByTable[tableIndex].emplace_back(tableSlices[firstSlice], tableSlices[lastSlice - 1]);
            firstSlice = lastSlice;
        }
    }

    return BuildYsonStringFluently()
        .DoListFor(rangesByTable, [&] (TFluentList fluent, const std::vector<std::pair<TInputDataSlicePtr, TInputDataSlicePtr>>& tableRanges) {
            fluent
                .DoIf(!tableRanges.empty(), [&] (TFluentList fluent) {
                    int tableIndex = tableRanges[0].first->GetTableIndex();
                    fluent
                        .Item()
                        .BeginAttributes()
                            .DoIf(isForeignTable[tableIndex], [&] (TFluentAttributes fluent) {
                                fluent
                                    .Item("foreign").Value(true);
                            })
                            .Item("ranges")
                            .DoListFor(tableRanges, [&] (TFluentList fluent, const std::pair<TInputDataSlicePtr, TInputDataSlicePtr>& range) {
                                fluent
                                    .Item()
                                    .BeginMap()
                                        .Item("lower_limit")
                                            .Do(BIND(
                                                &BuildInputSliceLimit,
                                                range.first,
                                                range.first->LowerLimit(),
                                                TNullable<i64>(mergeByRows && !isForeignTable[tableIndex], 0)))
                                        .Item("upper_limit")
                                            .Do(BIND(
                                                &BuildInputSliceLimit,
                                                range.second,
                                                range.second->UpperLimit(),
                                                TNullable<i64>(mergeByRows && !isForeignTable[tableIndex], range.second->GetRowCount())))
                                    .EndMap();
                            })
                        .EndAttributes()
                        .Value(inputPaths[tableIndex].GetPath());
                });
        });
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

EAbortReason GetAbortReason(const NJobTrackerClient::NProto::TJobResult& result)
{
    auto error = FromProto<TError>(result.error());
    try {
        return error.Attributes().Get<EAbortReason>("abort_reason", EAbortReason::Scheduler);
    } catch (const std::exception& ex) {
        // Process unknown abort reason from node.
        LOG_WARNING(ex, "Found unknown abort_reason in job result");
        return EAbortReason::Unknown;
    }
}

////////////////////////////////////////////////////////////////////////////////

TString MakeOperationCodicilString(const TOperationId& operationId)
{
    return Format("OperationId: %v", operationId);
}

TCodicilGuard MakeOperationCodicilGuard(const TOperationId& operationId)
{
    return TCodicilGuard(MakeOperationCodicilString(operationId));
}

////////////////////////////////////////////////////////////////////////////////

TString TLockedUserObject::GetPath() const
{
    return FromObjectId(ObjectId);
}

////////////////////////////////////////////////////////////////////////////////

TBriefJobStatisticsPtr BuildBriefStatistics(const TYsonString& statisticsYson)
{
    auto statistics = ConvertTo<NJobTrackerClient::TStatistics>(statisticsYson);

    auto briefStatistics = New<TBriefJobStatistics>();
    briefStatistics->ProcessedInputRowCount = GetNumericValue(statistics, InputRowCountPath);
    briefStatistics->ProcessedInputUncompressedDataSize = GetNumericValue(statistics, InputUncompressedDataSizePath);
    briefStatistics->ProcessedInputCompressedDataSize = GetNumericValue(statistics, InputCompressedDataSizePath);
    briefStatistics->InputPipeIdleTime = FindNumericValue(statistics, InputPipeIdleTimePath);
    briefStatistics->JobProxyCpuUsage = FindNumericValue(statistics, JobProxyCpuUsagePath);
    briefStatistics->Timestamp = statistics.GetTimestamp().Get(TInstant::Now());

    // TODO(max42): GetTotalOutputDataStatistics is implemented very inefficiently (it creates yhash containing
    // output data statistics per output table and then aggregates them). Rewrite it without any new allocations.
    auto outputDataStatistics = GetTotalOutputDataStatistics(statistics);
    briefStatistics->ProcessedOutputUncompressedDataSize = outputDataStatistics.uncompressed_data_size();
    briefStatistics->ProcessedOutputCompressedDataSize = outputDataStatistics.compressed_data_size();
    briefStatistics->ProcessedOutputRowCount = outputDataStatistics.row_count();

    return briefStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


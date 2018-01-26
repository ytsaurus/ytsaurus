#include "unordered_controller.h"

#include "auto_merge_task.h"
#include "chunk_list_pool.h"
#include "helpers.h"
#include "job_info.h"
#include "job_memory.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "task.h"
#include "controller_agent.h"

#include <yt/server/chunk_pools/unordered_chunk_pool.h>
#include <yt/server/chunk_pools/chunk_pool.h>

#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/query_client/query.h>

#include <yt/core/concurrency/periodic_yielder.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NControllerAgent {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NChunkServer;
using namespace NJobProxy;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NChunkClient::NProto;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NTableClient;
using namespace NConcurrency;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

static const NProfiling::TProfiler Profiler("/operations/unordered");

////////////////////////////////////////////////////////////////////////////////

class TUnorderedControllerBase
    : public TOperationControllerBase
{
public:
    class TUnorderedTaskBase
        : public TTask
    {
    public:
        //! For persistence only.
        TUnorderedTaskBase()
            : Controller(nullptr)
        { }

        TUnorderedTaskBase(TUnorderedControllerBase* controller, std::vector<TEdgeDescriptor> edgeDescriptors)
            : TTask(controller, std::move(edgeDescriptors))
            , Controller(controller)
        { }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller->UnorderedTaskGroup;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->IsLocalityEnabled()
                ? Controller->Spec->LocalityTimeout
                : TDuration::Zero();
        }

        virtual TExtendedJobResources GetNeededResources(const TJobletPtr& joblet) const override
        {
            auto result = Controller->GetUnorderedOperationResources(
                joblet->InputStripeList->GetStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return Controller->UnorderedPool.get();
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return Controller->UnorderedPool.get();
        }

        virtual void Persist(const TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller);
        }

        virtual TUserJobSpecPtr GetUserJobSpec() const override
        {
            return Controller->GetUserJobSpec();
        }

        virtual EJobType GetJobType() const override
        {
            return Controller->GetJobType();
        }

        virtual void OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
        {
            TTask::OnJobCompleted(joblet, jobSummary);

            RegisterOutput(&jobSummary.Result, joblet->ChunkListIds, joblet);

            if (jobSummary.InterruptReason != EInterruptReason::None) {
                SplitByRowsAndReinstall(jobSummary.UnreadInputDataSlices, jobSummary.SplitJobCount);
            }
        }

        virtual bool SupportsInputPathYson() const override
        {
            return true;
        }

    private:
        TUnorderedControllerBase* Controller;

        virtual TExtendedJobResources GetMinNeededResourcesHeavy() const override
        {
            auto result = Controller->GetUnorderedOperationResources(
                Controller->UnorderedPool->GetApproximateStripeStatistics());
            AddFootprintAndUserJobResources(result);
            return result;
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->JobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet);
            AddOutputTableSpecs(jobSpec, joblet);
        }

        void SplitByRowsAndReinstall(
            const std::vector<TInputDataSlicePtr>& dataSlices,
            int jobCount)
        {
            i64 unreadRowCount = GetCumulativeRowCount(dataSlices);
            i64 rowsPerJob = DivCeil<i64>(unreadRowCount, jobCount);
            i64 rowsToAdd = rowsPerJob;
            int sliceIndex = 0;
            auto currentDataSlice = dataSlices[0];
            std::vector<TInputDataSlicePtr> jobSlices;
            while (true) {
                i64 sliceRowCount = currentDataSlice->GetRowCount();
                if (currentDataSlice->Type == EDataSourceType::UnversionedTable && sliceRowCount > rowsToAdd) {
                    auto split = currentDataSlice->SplitByRowIndex(rowsToAdd);
                    jobSlices.emplace_back(std::move(split.first));
                    rowsToAdd = 0;
                    currentDataSlice = std::move(split.second);
                } else {
                    jobSlices.emplace_back(std::move(currentDataSlice));
                    rowsToAdd -= sliceRowCount;
                    ++sliceIndex;
                    if (sliceIndex == static_cast<int>(dataSlices.size())) {
                        break;
                    }
                    currentDataSlice = dataSlices[sliceIndex];
                }
                if (rowsToAdd <= 0) {
                    ReinstallInputDataSlices(jobSlices);
                    jobSlices.clear();
                    rowsToAdd = rowsPerJob;
                }
            }
            if (!jobSlices.empty()) {
                ReinstallInputDataSlices(jobSlices);
            }
        }

        void ReinstallInputDataSlices(const std::vector<TInputDataSlicePtr>& inputDataSlices)
        {
            std::vector<TChunkStripePtr> stripes;
            auto chunkStripe = New<TChunkStripe>(false /*foreign*/, true /*solid*/);
            for (const auto& slice : inputDataSlices) {
                chunkStripe->DataSlices.push_back(slice);
            }
            stripes.emplace_back(std::move(chunkStripe));
            AddInput(stripes);

            GetChunkPoolInput()->Finish();
            AddPendingHint();
            CheckCompleted();
        }
    };

    INHERIT_DYNAMIC_PHOENIX_TYPE(TUnorderedTaskBase, TUnorderedTask, 0x8ab75ee7);
    INHERIT_DYNAMIC_PHOENIX_TYPE_TEMPLATED(TAutoMergeableOutputMixin, TAutoMergeableUnorderedTask, 0x9a9bcee3, TUnorderedTaskBase);

    typedef TIntrusivePtr<TUnorderedTaskBase> TUnorderedTaskPtr;

    TUnorderedControllerBase(
        TUnorderedOperationSpecBasePtr spec,
        TSimpleOperationOptionsPtr options,
        TControllerAgentPtr controllerAgent,
        TOperation* operation)
        : TOperationControllerBase(spec, options, controllerAgent, operation)
        , Spec(spec)
        , Options(options)
    { }

    // Persistence.
    virtual void Persist(const TPersistenceContext& context) override
    {
        TOperationControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, JobIOConfig);
        Persist(context, JobSpecTemplate);
        Persist(context, IsExplicitJobCount);
        Persist(context, UnorderedPool);
        Persist(context, UnorderedTask);
        Persist(context, UnorderedTaskGroup);
    }

protected:
    TUnorderedOperationSpecBasePtr Spec;
    TSimpleOperationOptionsPtr Options;

    //! Customized job IO config.
    TJobIOConfigPtr JobIOConfig;

    //! The template for starting new jobs.
    TJobSpec JobSpecTemplate;

    //! Flag set when job count was explicitly specified.
    bool IsExplicitJobCount;

    std::unique_ptr<IChunkPool> UnorderedPool;

    TUnorderedTaskPtr UnorderedTask;
    TTaskGroupPtr UnorderedTaskGroup;

    // Custom bits of preparation pipeline.
    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual void DoInitialize() override
    {
        TOperationControllerBase::DoInitialize();

        UnorderedTaskGroup = New<TTaskGroup>();
        UnorderedTaskGroup->MinNeededResources.SetCpu(GetCpuLimit());
        RegisterTaskGroup(UnorderedTaskGroup);
    }

    void InitUnorderedPool(IJobSizeConstraintsPtr jobSizeConstraints, TJobSizeAdjusterConfigPtr jobSizeAdjusterConfig)
    {
        UnorderedPool = CreateUnorderedChunkPool(jobSizeConstraints, jobSizeAdjusterConfig);
    }

    virtual bool IsCompleted() const override
    {
        // Unordered task may be null, if all chunks were teleported.
        return TOperationControllerBase::IsCompleted() && (!UnorderedTask || UnorderedTask->IsCompleted());
    }

    virtual void CustomPrepare() override
    {
        // NB: this call should be after TotalEstimatedOutputChunkCount is calculated.
        TOperationControllerBase::CustomPrepare();

        // The total data size for processing (except teleport chunks).
        i64 totalDataWeight = 0;
        i64 totalRowCount = 0;

        // The number of output partitions generated so far.
        // Each partition either corresponds to a teleport chunk.
        int currentPartitionIndex = 0;

        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            std::vector<TInputChunkPtr> mergedChunks;

            TPeriodicYielder yielder(PrepareYieldPeriod);
            for (const auto& chunk : CollectPrimaryUnversionedChunks()) {
                yielder.TryYield();
                if (IsTeleportChunk(chunk)) {
                    // Chunks not requiring merge go directly to the output chunk list.
                    LOG_TRACE("Teleport chunk added (ChunkId: %v, Partition: %v)",
                        chunk->ChunkId(),
                        currentPartitionIndex);

                    // Place the chunk directly to the output table.
                    RegisterTeleportChunk(chunk, currentPartitionIndex, 0);
                    ++currentPartitionIndex;
                } else {
                    mergedChunks.push_back(chunk);
                    totalDataWeight += chunk->GetDataWeight();
                    totalRowCount += chunk->GetRowCount();
                }
            }

            auto versionedInputStatistics = CalculatePrimaryVersionedChunksStatistics();
            totalDataWeight += versionedInputStatistics.first;
            totalRowCount += versionedInputStatistics.second;

            // Create the task, if any data.
            if (totalDataWeight > 0) {
                auto createJobSizeConstraints = [&] () -> IJobSizeConstraintsPtr {
                    switch (OperationType) {
                        case EOperationType::Merge:
                            return CreateMergeJobSizeConstraints(
                                Spec,
                                Options,
                                totalDataWeight,
                                DataWeightRatio,
                                InputCompressionRatio);

                        default:
                            return CreateUserJobSizeConstraints(
                                Spec,
                                Options,
                                GetOutputTablePaths().size(),
                                DataWeightRatio,
                                totalDataWeight,
                                totalRowCount);
                    }
                };

                auto jobSizeConstraints = createJobSizeConstraints();
                IsExplicitJobCount = jobSizeConstraints->IsExplicitJobCount();
                InitAutoMerge(jobSizeConstraints->GetJobCount(), DataWeightRatio);

                std::vector<TChunkStripePtr> stripes;
                SliceUnversionedChunks(mergedChunks, jobSizeConstraints, &stripes);
                SlicePrimaryVersionedChunks(jobSizeConstraints, &stripes);

                InitUnorderedPool(
                    std::move(jobSizeConstraints),
                    GetJobSizeAdjusterConfig());


                bool requiresAutoMerge = false;

                auto edgeDescriptors = GetStandardEdgeDescriptors();
                if (GetAutoMergeDirector()) {
                    YCHECK(AutoMergeTasks.size() == edgeDescriptors.size());
                    for (int index = 0; index < edgeDescriptors.size(); ++index) {
                        if (AutoMergeTasks[index]) {
                            edgeDescriptors[index].DestinationPool = AutoMergeTasks[index]->GetChunkPoolInput();
                            edgeDescriptors[index].ChunkMapping = AutoMergeTasks[index]->GetChunkMapping();
                            edgeDescriptors[index].ImmediatelyUnstageChunkLists = true;
                            edgeDescriptors[index].RequiresRecoveryInfo = true;
                            edgeDescriptors[index].IsFinalOutput = false;
                            requiresAutoMerge = true;
                        }
                    }
                }
                if (requiresAutoMerge) {
                    UnorderedTask = New<TAutoMergeableUnorderedTask>(this, std::move(edgeDescriptors));
                } else {
                    UnorderedTask = New<TUnorderedTask>(this, std::move(edgeDescriptors));
                }
                RegisterTask(UnorderedTask);

                UnorderedTask->AddInput(stripes);
                FinishTaskInput(UnorderedTask);
                for (int index = 0; index < AutoMergeTasks.size(); ++index) {
                    if (AutoMergeTasks[index]) {
                        AutoMergeTasks[index]->FinishInput(UnorderedTask->GetVertexDescriptor());
                    }
                }

                LOG_INFO("Inputs processed (JobCount: %v, IsExplicitJobCount: %v)",
                    UnorderedTask->GetPendingJobCount(),
                    IsExplicitJobCount);
            } else {
                LOG_INFO("Inputs processed, all chunks were teleported");
            }
        }

        InitJobIOConfig();
        InitJobSpecTemplate();
    }

    // Resource management.
    TExtendedJobResources GetUnorderedOperationResources(
        const TChunkStripeStatisticsVector& statistics) const
    {
        TExtendedJobResources result;
        result.SetUserSlots(1);
        result.SetCpu(GetCpuLimit());
        result.SetJobProxyMemory(GetFinalIOMemorySize(Spec->JobIO, AggregateStatistics(statistics)));
        return result;
    }

    virtual void OnChunksReleased(int chunkCount) override
    {
        TOperationControllerBase::OnChunksReleased(chunkCount);

        if (const auto& autoMergeDirector = GetAutoMergeDirector()) {
            autoMergeDirector->OnMergeJobFinished(chunkCount /* unregisteredIntermediateChunkCount */);
        }
    }

    virtual EIntermediateChunkUnstageMode GetIntermediateChunkUnstageMode() const override
    {
        auto mapperSpec = GetUserJobSpec();
        // We could get here only if this is an unordered map and auto-merge is enabled.
        YCHECK(mapperSpec);
        YCHECK(Spec->AutoMerge->Mode != EAutoMergeMode::Disabled);

        if (Spec->AutoMerge->Mode != EAutoMergeMode::Relaxed && mapperSpec->Deterministic) {
            return EIntermediateChunkUnstageMode::OnJobCompleted;
        } else {
            return EIntermediateChunkUnstageMode::OnSnapshotCompleted;
        }
    }

    // Unsorted helpers.
    virtual EJobType GetJobType() const = 0;

    virtual TJobSizeAdjusterConfigPtr GetJobSizeAdjusterConfig() const = 0;

    virtual TUserJobSpecPtr GetUserJobSpec() const
    {
        return nullptr;
    }

    virtual TCpuResource GetCpuLimit() const
    {
        return 1;
    }

    void InitJobIOConfig()
    {
        JobIOConfig = CloneYsonSerializable(Spec->JobIO);
    }

    //! Returns |true| if the chunk can be included into the output as-is.
    virtual bool IsTeleportChunk(const TInputChunkPtr& chunkSpec) const
    {
        return false;
    }

    virtual void PrepareInputQuery() override
    {
        if (Spec->InputQuery) {
            ParseInputQuery(*Spec->InputQuery, Spec->InputSchema);
        }
    }

    virtual void InitJobSpecTemplate()
    {
        JobSpecTemplate.set_type(static_cast<int>(GetJobType()));
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        schedulerJobSpecExt->set_table_reader_options(ConvertToYsonString(CreateTableReaderOptions(Spec->JobIO)).GetData());

        SetInputDataSources(schedulerJobSpecExt);

        if (Spec->InputQuery) {
            WriteInputQueryToJobSpec(schedulerJobSpecExt);
        }

        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).GetData());
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TUnorderedControllerBase::TUnorderedTask);
DEFINE_DYNAMIC_PHOENIX_TYPE(TUnorderedControllerBase::TAutoMergeableUnorderedTask);

////////////////////////////////////////////////////////////////////////////////

class TMapController
    : public TUnorderedControllerBase
{
public:
    TMapController(
        TMapOperationSpecPtr spec,
        TMapOperationOptionsPtr options,
        TControllerAgentPtr controllerAgent,
        TOperation* operation)
        : TUnorderedControllerBase(spec, options, controllerAgent, operation)
        , Spec(spec)
        , Options(options)
    { }

    virtual void BuildBriefSpec(TFluentMap fluent) const override
    {
        TUnorderedControllerBase::BuildBriefSpec(fluent);
        fluent
            .Item("mapper").BeginMap()
                .Item("command").Value(TrimCommandForBriefSpec(Spec->Mapper->Command))
            .EndMap();
    }

    // Persistence.
    virtual void Persist(const TPersistenceContext& context) override
    {
        TUnorderedControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, StartRowIndex);
    }

protected:
    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        return STRINGBUF("data_weight_per_job");
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::Map};
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMapController, 0xbac5fd82);

    TMapOperationSpecPtr Spec;
    TMapOperationOptionsPtr Options;

    i64 StartRowIndex = 0;

    // Custom bits of preparation pipeline.

    virtual EJobType GetJobType() const override
    {
        return EJobType::Map;
    }

    virtual TJobSizeAdjusterConfigPtr GetJobSizeAdjusterConfig() const override
    {
        return Config->EnableMapJobSizeAdjustment
            ? Options->JobSizeAdjuster
            : nullptr;
    }

    virtual TJobSplitterConfigPtr GetJobSplitterConfig() const override
    {
        return IsJobInterruptible() && Config->EnableJobSplitting && Spec->EnableJobSplitting
            ? Options->JobSplitter
            : nullptr;
    }

    virtual TUserJobSpecPtr GetUserJobSpec() const override
    {
        return Spec->Mapper;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    virtual TNullable<TRichYPath> GetStderrTablePath() const override
    {
        return Spec->StderrTablePath;
    }

    virtual TBlobTableWriterConfigPtr GetStderrTableWriterConfig() const override
    {
        return Spec->StderrTableWriterConfig;
    }

    virtual TNullable<TRichYPath> GetCoreTablePath() const override
    {
        return Spec->CoreTablePath;
    }

    virtual TBlobTableWriterConfigPtr GetCoreTableWriterConfig() const override
    {
        return Spec->CoreTableWriterConfig;
    }

    virtual std::vector<TUserJobSpecPtr> GetUserJobSpecs() const override
    {
        return {Spec->Mapper};
    }

    virtual void DoInitialize() override
    {
        TUnorderedControllerBase::DoInitialize();

        ValidateUserFileCount(Spec->Mapper, "mapper");
    }

    virtual bool IsOutputLivePreviewSupported() const override
    {
        return true;
    }

    // Unsorted helpers.
    virtual TCpuResource GetCpuLimit() const override
    {
        return Spec->Mapper->CpuLimit;
    }

    virtual void InitJobSpecTemplate() override
    {
        TUnorderedControllerBase::InitJobSpecTemplate();
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        InitUserJobSpecTemplate(
            schedulerJobSpecExt->mutable_user_job_spec(),
            Spec->Mapper,
            UserJobFiles_[Spec->Mapper],
            Spec->JobNodeAccount);
    }

    virtual void CustomizeJoblet(const TJobletPtr& joblet) override
    {
        joblet->StartRowIndex = StartRowIndex;
        StartRowIndex += joblet->InputStripeList->TotalRowCount;
    }

    virtual bool IsInputDataSizeHistogramSupported() const override
    {
        return true;
    }

    virtual bool IsJobInterruptible() const override
    {
        // We don't let jobs to be interrupted if MaxOutputTablesTimesJobCount is too much overdrafted.
        return
            !IsExplicitJobCount &&
            2 * Options->MaxOutputTablesTimesJobsCount > JobCounter->GetTotal() * GetOutputTablePaths().size() &&
            2 * Options->MaxJobCount > JobCounter->GetTotal() &&
            TOperationControllerBase::IsJobInterruptible();
    }

    virtual TYsonSerializablePtr GetTypedSpec() const override
    {
        return Spec;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMapController);

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateUnorderedMapController(
    TControllerAgentPtr controllerAgent,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TMapOperationSpec>(operation->GetSpec());
    return New<TMapController>(spec, controllerAgent->GetConfig()->MapOperationOptions, controllerAgent, operation);
}

////////////////////////////////////////////////////////////////////////////////

class TUnorderedMergeController
    : public TUnorderedControllerBase
{
public:
    TUnorderedMergeController(
        TUnorderedMergeOperationSpecPtr spec,
        TUnorderedMergeOperationOptionsPtr options,
        TControllerAgentPtr controllerAgent,
        TOperation* operation)
        : TUnorderedControllerBase(spec, options, controllerAgent, operation)
        , Spec(spec)
    { }

protected:
    virtual TStringBuf GetDataWeightParameterNameForJob(EJobType jobType) const override
    {
        return STRINGBUF("data_weight_per_job");
    }

    virtual std::vector<EJobType> GetSupportedJobTypesForJobsDurationAnalyzer() const override
    {
        return {EJobType::UnorderedMerge};
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeController, 0x9a17a41f);

    TUnorderedMergeOperationSpecPtr Spec;

    // Custom bits of preparation pipeline.
    virtual EJobType GetJobType() const override
    {
        return EJobType::UnorderedMerge;
    }

    virtual TJobSizeAdjusterConfigPtr GetJobSizeAdjusterConfig() const override
    {
        return nullptr;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        std::vector<TRichYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    // Unsorted helpers.
    virtual bool IsRowCountPreserved() const override
    {
        return !Spec->InputQuery;
    }

    //! Returns |true| if the chunk can be included into the output as-is.
    //! A typical implementation of #IsTeleportChunk that depends on whether chunks must be combined or not.
    virtual bool IsTeleportChunk(const TInputChunkPtr& chunkSpec) const override
    {
        bool isSchemaCompatible =
            ValidateTableSchemaCompatibility(
                InputTables[chunkSpec->GetTableIndex()].Schema,
                OutputTables_[0].TableUploadOptions.TableSchema,
                false)
            .IsOK();

        if (Spec->ForceTransform ||
            Spec->InputQuery ||
            !isSchemaCompatible ||
            InputTables[chunkSpec->GetTableIndex()].Path.GetColumns())
        {
            return false;
        }

        return Spec->CombineChunks
            ? chunkSpec->IsLargeCompleteChunk(Spec->JobIO->TableWriter->DesiredChunkSize)
            : chunkSpec->IsCompleteChunk();
    }

    virtual void PrepareInputQuery() override
    {
        if (Spec->InputQuery) {
            ParseInputQuery(*Spec->InputQuery, Spec->InputSchema);
        }
    }

    virtual void PrepareOutputTables() override
    {
        auto& table = OutputTables_[0];

        auto validateOutputNotSorted = [&] () {
            if (table.TableUploadOptions.TableSchema.IsSorted()) {
                THROW_ERROR_EXCEPTION("Cannot perform unordered merge into a sorted table in a \"strong\" schema mode")
                    << TErrorAttribute("schema", table.TableUploadOptions.TableSchema);
            }
        };

        auto inferFromInput = [&] () {
            if (Spec->InputQuery) {
                table.TableUploadOptions.TableSchema = InputQuery->Query->GetTableSchema();
            } else {
                InferSchemaFromInput();
            }
        };

        switch (Spec->SchemaInferenceMode) {
            case ESchemaInferenceMode::Auto:
                if (table.TableUploadOptions.SchemaMode == ETableSchemaMode::Weak) {
                    inferFromInput();
                } else {
                    validateOutputNotSorted();

                    if (!Spec->InputQuery) {
                        ValidateOutputSchemaCompatibility(true);
                    }
                }
                break;

            case ESchemaInferenceMode::FromInput:
                inferFromInput();
                break;

            case ESchemaInferenceMode::FromOutput:
                validateOutputNotSorted();
                break;

            default:
                Y_UNREACHABLE();
        }
    }

    virtual TYsonSerializablePtr GetTypedSpec() const override
    {
        return Spec;
    }

    virtual bool IsJobInterruptible() const override
    {
        return false;
    }
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TUnorderedMergeController);

////////////////////////////////////////////////////////////////////////////////

IOperationControllerPtr CreateUnorderedMergeController(
    TControllerAgentPtr controllerAgent,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TUnorderedMergeOperationSpec>(operation->GetSpec());
    return New<TUnorderedMergeController>(spec, controllerAgent->GetConfig()->UnorderedMergeOperationOptions, controllerAgent, operation);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT


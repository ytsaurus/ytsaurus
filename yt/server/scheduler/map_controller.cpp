#include "stdafx.h"
#include "map_controller.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "job_resources.h"

#include <ytlib/ytree/fluent.h>

#include <ytlib/table_client/schema.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/table_client/key.h>

#include <server/job_proxy/config.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYPath;
using namespace NChunkServer;
using namespace NJobProxy;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OperationLogger);
static NProfiling::TProfiler Profiler("/operations/map");

////////////////////////////////////////////////////////////////////

class TMapController
    : public TOperationControllerBase
{
public:
    TMapController(
        TSchedulerConfigPtr config,
        TMapOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, spec, host, operation)
        , Spec(spec)
        , StartRowIndex(0)
    { }

private:
    TMapOperationSpecPtr Spec;
    i64 StartRowIndex;

    class TMapTask
        : public TTask
    {
    public:
        explicit TMapTask(TMapController* controller)
            : TTask(controller)
            , Controller(controller)
        {
            ChunkPool = CreateUnorderedChunkPool(Controller->JobCounter.GetTotal());
        }

        virtual Stroka GetId() const override
        {
            return "Map";
        }

        virtual TTaskGroup* GetGroup() const override
        {
            return &Controller->MapTaskGroup;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->Spec->LocalityTimeout;
        }

        virtual TNodeResources GetMinNeededResourcesHeavy() const override
        {
            return GetMapResources(ChunkPool->GetApproximateStripeStatistics());
        }

        virtual TNodeResources GetNeededResources(TJobletPtr joblet) const override
        {
            return GetMapResources(joblet->InputStripeList->GetStatistics());
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ~ChunkPool;
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ~ChunkPool;
        }

    private:
        TMapController* Controller;

        TAutoPtr<IChunkPool> ChunkPool;

        TNodeResources GetMapResources(const TChunkStripeStatisticsVector& statistics) const
        {
            TNodeResources result;
            result.set_slots(1);
            result.set_cpu(Controller->Spec->Mapper->CpuLimit);
            result.set_memory(
                GetIOMemorySize(
                    Controller->Spec->JobIO,
                    Controller->Spec->OutputTablePaths.size(),
                    AggregateStatistics(statistics)) +
                GetFootprintMemorySize() +
                Controller->Spec->Mapper->MemoryLimit);
            return result;
        }

        virtual int GetChunkListCountPerJob() const override
        {
            return Controller->OutputTables.size();
        }

        virtual EJobType GetJobType() const override
        {
            return EJobType(Controller->JobSpecTemplate.type());
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->JobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet, Controller->Spec->Mapper->EnableTableIndex);
            AddFinalOutputSpecs(jobSpec, joblet);

            auto* jobSpecExt = jobSpec->MutableExtension(TMapJobSpecExt::map_job_spec_ext);
            Controller->AddUserJobEnvironment(jobSpecExt->mutable_mapper_spec(), joblet);
        }

        virtual void OnJobCompleted(TJobletPtr joblet) override
        {
            TTask::OnJobCompleted(joblet);

            RegisterOutput(joblet, joblet->JobIndex);
        }

    };

    typedef TIntrusivePtr<TMapTask> TMapTaskPtr;

    TMapTaskPtr MapTask;
    TTaskGroup MapTaskGroup;
    TJobIOConfigPtr JobIOConfig;
    TJobSpec JobSpecTemplate;


    // Custom bits of preparation pipeline.

    virtual void DoInitialize() override
    {
        TOperationControllerBase::DoInitialize();

        RegisterTaskGroup(&MapTaskGroup);
    }

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    virtual std::vector<TPathWithStage> GetFilePaths() const override
    {
        std::vector<TPathWithStage> result;
        FOREACH (const auto& path, Spec->Mapper->FilePaths) {
            result.push_back(std::make_pair(path, EOperationStage::Map));
        }
        return result;
    }

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline) override
    {
        return pipeline->Add(BIND(&TMapController::ProcessInputs, MakeStrong(this)));
    }

    TFuture<void> ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            auto jobCount = SuggestJobCount(
                TotalInputDataSize,
                Spec->DataSizePerJob,
                Spec->JobCount);

            auto stripes = SliceInputChunks(Config->MapJobMaxSliceDataSize, &jobCount);

            JobCounter.Set(jobCount);

            MapTask = New<TMapTask>(this);
            MapTask->AddInput(stripes);
            MapTask->FinishInput();

            InitJobIOConfig();
            InitJobSpecTemplate();

            LOG_INFO("Inputs processed (JobCount: %" PRId64 ")",
                JobCounter.GetTotal());

            // Kick-start the map task.
            AddTaskPendingHint(MapTask);
        }

        return MakeFuture();
    }

    virtual void CustomizeJoblet(TJobletPtr joblet) override
    {
        joblet->StartRowIndex = StartRowIndex;
        StartRowIndex += joblet->InputStripeList->TotalRowCount;
    }


    // Progress reporting.

    virtual Stroka GetLoggingProgress() override
    {
        return Sprintf(
            "Jobs = {T: %" PRId64", R: %" PRId64", C: %" PRId64", P: %d, F: %" PRId64", A: %" PRId64"}",
            JobCounter.GetTotal(),
            JobCounter.GetRunning(),
            JobCounter.GetCompleted(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAborted());
    }


    // Unsorted helpers.

    virtual bool IsSortedOutputSupported() const override
    {
        return true;
    }

    void InitJobIOConfig()
    {
        JobIOConfig = CloneYsonSerializable(Spec->JobIO);
        InitFinalOutputConfig(JobIOConfig);
    }

    void InitJobSpecTemplate()
    {
        JobSpecTemplate.set_type(EJobType::Map);
        JobSpecTemplate.set_lfalloc_buffer_size(GetLFAllocBufferSize());

        auto* jobSpecExt = JobSpecTemplate.MutableExtension(TMapJobSpecExt::map_job_spec_ext);

        InitUserJobSpec(
            jobSpecExt->mutable_mapper_spec(),
            Spec->Mapper,
            RegularFiles,
            TableFiles);

        *JobSpecTemplate.mutable_output_transaction_id() = Operation->GetOutputTransaction()->GetId().ToProto();

        JobSpecTemplate.set_io_config(ConvertToYsonString(JobIOConfig).Data());
    }

};

IOperationControllerPtr CreateMapController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TMapOperationSpec>(operation, config->MapOperationSpec);
    return New<TMapController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


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
        : TOperationControllerBase(config, host, operation)
        , Spec(spec)
    { }

    virtual TNodeResources GetMinNeededResources() override
    {
        return MapTask
            ? MapTask->GetMinNeededResources()
            : InfiniteNodeResources();
    }


private:
    TMapOperationSpecPtr Spec;

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

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->Spec->LocalityTimeout;
        }

        virtual TNodeResources GetMinNeededResources() const override
        {
            TNodeResources result;
            result.set_slots(1);
            result.set_cpu(Controller->Spec->Mapper->CpuLimit);
            result.set_memory(
                GetIOMemorySize(
                Controller->Spec->JobIO,
                1,
                Controller->Spec->OutputTablePaths.size()) +
                GetFootprintMemorySize());
            return result;
        }

    private:
        TMapController* Controller;
        i64 StartRowIndex;

        TAutoPtr<IChunkPool> ChunkPool;

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ~ChunkPool;
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ~ChunkPool;
        }

        virtual int GetChunkListCountPerJob() const override
        {
            return Controller->OutputTables.size();
        }

        virtual void BuildJobSpec(TJobletPtr joblet, TJobSpec* jobSpec) override
        {
            jobSpec->CopyFrom(Controller->JobSpecTemplate);
            AddSequentialInputSpec(jobSpec, joblet, Controller->Spec->EnableTableIndex);
            AddOutputSpecs(jobSpec, joblet);

            joblet->StartRowIndex = StartRowIndex;
            StartRowIndex += joblet->InputStripeList->TotalRowCount;
            
            auto* jobSpecExt = jobSpec->MutableExtension(TMapJobSpecExt::map_job_spec_ext);
            Controller->AddUserJobEnvironment(jobSpecExt->mutable_mapper_spec(), joblet);
        }

        virtual void OnJobCompleted(TJobletPtr joblet) override
        {
            TTask::OnJobCompleted(joblet);

            Controller->RegisterOutputChunkTrees(joblet, 0);
        }
    };
    
    typedef TIntrusivePtr<TMapTask> TMapTaskPtr;

    TMapTaskPtr MapTask;
    TJobIOConfigPtr JobIOConfig;
    TJobSpec JobSpecTemplate;


    // Custom bits of preparation pipeline.

    virtual std::vector<TRichYPath> GetInputTablePaths() const override
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TRichYPath> GetOutputTablePaths() const override
    {
        return Spec->OutputTablePaths;
    }

    virtual std::vector<TRichYPath> GetFilePaths() const override
    {
        return Spec->Mapper->FilePaths;
    }

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline) override
    {
        return pipeline->Add(BIND(&TMapController::ProcessInputs, MakeStrong(this)));
    }

    TFuture<void> ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            auto stripes = SliceInputChunks(
                Spec->JobCount,
                Spec->JobSliceDataSize);

            JobCounter.Set(SuggestJobCount(
                TotalInputDataSize,
                Spec->MinDataSizePerJob,
                Spec->MaxDataSizePerJob,
                Spec->JobCount,
                static_cast<int>(stripes.size())));

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

    // Progress reporting.

    virtual void LogProgress() override
    {
        LOG_DEBUG("Progress: "
            "Jobs = {T: %"PRId64", R: %"PRId64", C: %"PRId64", P: %d, F: %"PRId64", A: %"PRId64"}",
            JobCounter.GetTotal(),
            JobCounter.GetRunning(),
            JobCounter.GetCompleted(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAborted());
    }


    // Unsorted helpers.

    void InitJobIOConfig() 
    {
        JobIOConfig = CloneYsonSerializable(Spec->JobIO);
        InitFinalOutputConfig(JobIOConfig);
    }

    void InitJobSpecTemplate()
    {
        JobSpecTemplate.set_type(EJobType::Map);

        auto* jobSpecExt = JobSpecTemplate.MutableExtension(TMapJobSpecExt::map_job_spec_ext);
        
        InitUserJobSpec(
            jobSpecExt->mutable_mapper_spec(),
            Spec->Mapper,
            Files);

        *JobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

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


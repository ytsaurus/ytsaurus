#include "stdafx.h"
#include "map_controller.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "job_resources.h"
#include "helpers.h"

#include <core/ytree/fluent.h>

#include <ytlib/chunk_client/schema.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <server/job_proxy/config.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NChunkServer;
using namespace NJobProxy;
using namespace NChunkClient;
using namespace NScheduler::NProto;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;

////////////////////////////////////////////////////////////////////

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

    virtual void BuildBriefSpec(IYsonConsumer* consumer) const override
    {
        TOperationControllerBase::BuildBriefSpec(consumer);
        BuildYsonMapFluently(consumer)
            .DoIf(Spec->Mapper, [&] (TFluentMap fluent) {
                fluent
                    .Item("mapper").BeginMap()
                        .Item("command").Value(TrimCommandForBriefSpec(Spec->Mapper->Command))
                    .EndMap();
            });
    }
    
    // Persistence.

    virtual void Persist(TPersistenceContext& context) override
    {
        TOperationControllerBase::Persist(context);

        using NYT::Persist;
        Persist(context, StartRowIndex);
        Persist(context, MapTask);
        Persist(context, MapTaskGroup);
        Persist(context, JobIOConfig);
        Persist(context, JobSpecTemplate);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMapController, 0xbac5fd82);

    TMapOperationSpecPtr Spec;

    i64 StartRowIndex;


    class TMapTask
        : public TTask
    {
    public:
        //! For persistence only.
        TMapTask()
            : Controller(nullptr)
        { }

        explicit TMapTask(TMapController* controller, int jobCount)
            : TTask(controller)
            , Controller(controller)
            , ChunkPool(CreateUnorderedChunkPool(
                Controller->NodeDirectory,
                jobCount))
        { }

        virtual Stroka GetId() const override
        {
            return "Map";
        }

        virtual TTaskGroupPtr GetGroup() const override
        {
            return Controller->MapTaskGroup;
        }

        virtual TDuration GetLocalityTimeout() const override
        {
            return Controller->Spec->LocalityTimeout;
        }

        virtual TNodeResources GetNeededResources(TJobletPtr joblet) const override
        {
            return GetMapResources(
                joblet->InputStripeList->GetStatistics(),
                joblet->MemoryReserveEnabled);
        }

        virtual IChunkPoolInput* GetChunkPoolInput() const override
        {
            return ChunkPool.get();
        }

        virtual IChunkPoolOutput* GetChunkPoolOutput() const override
        {
            return ChunkPool.get();
        }

        virtual void Persist(TPersistenceContext& context) override
        {
            TTask::Persist(context);

            using NYT::Persist;
            Persist(context, Controller);
            Persist(context, ChunkPool);
        }

    private:
        DECLARE_DYNAMIC_PHOENIX_TYPE(TMapTask, 0x87bacfe3);

        TMapController* Controller;

        std::unique_ptr<IChunkPool> ChunkPool;

        virtual bool IsMemoryReserveEnabled() const override
        {
            return Controller->IsMemoryReserveEnabled(Controller->JobCounter);
        }

        virtual TNodeResources GetMinNeededResourcesHeavy() const override
        {
            return GetMapResources(
                ChunkPool->GetApproximateStripeStatistics(),
                IsMemoryReserveEnabled());
        }

        TNodeResources GetMapResources(const TChunkStripeStatisticsVector& statistics, bool isReserveEnabled) const
        {
            TNodeResources result;
            result.set_user_slots(1);
            result.set_cpu(Controller->Spec->Mapper->CpuLimit);
            result.set_memory(
                Controller->GetFinalIOMemorySize(
                    Controller->Spec->JobIO,
                    AggregateStatistics(statistics)) +
                GetFootprintMemorySize() +
                Controller->GetMemoryReserve(isReserveEnabled, Controller->Spec->Mapper));
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
            AddSequentialInputSpec(jobSpec, joblet);
            AddFinalOutputSpecs(jobSpec, joblet);

            auto* schedulerJobSpecExt = jobSpec->MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);

            Controller->InitUserJobSpec(
                schedulerJobSpecExt->mutable_user_job_spec(),
                joblet,
                Controller->GetMemoryReserve(joblet->MemoryReserveEnabled, Controller->Spec->Mapper));
        }

        virtual void OnJobCompleted(TJobletPtr joblet) override
        {
            TTask::OnJobCompleted(joblet);

            RegisterOutput(joblet, joblet->JobIndex);
        }

        virtual void OnJobAborted(TJobletPtr joblet) override
        {
            TTask::OnJobAborted(joblet);
            Controller->UpdateAllTasksIfNeeded(Controller->JobCounter);
        }

    };

    typedef TIntrusivePtr<TMapTask> TMapTaskPtr;

    TMapTaskPtr MapTask;
    TTaskGroupPtr MapTaskGroup;


    TJobIOConfigPtr JobIOConfig;
    TJobSpec JobSpecTemplate;


    // Custom bits of preparation pipeline.

    virtual void DoInitialize() override
    {
        TOperationControllerBase::DoInitialize();

        if (Spec->Mapper && Spec->Mapper->FilePaths.size() > Config->MaxUserFileCount) {
            THROW_ERROR_EXCEPTION("Too many user files in mapper: maximum allowed %d, actual %" PRISZT,
                Config->MaxUserFileCount,
                Spec->Mapper->FilePaths.size());
        }

        MapTaskGroup = New<TTaskGroup>();
        RegisterTaskGroup(MapTaskGroup);
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
        for (const auto& path : Spec->Mapper->FilePaths) {
            result.push_back(std::make_pair(path, EOperationStage::Map));
        }
        return result;
    }

    virtual void CustomPrepare() override
    {
        TOperationControllerBase::CustomPrepare();

        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");

            auto jobCount = SuggestJobCount(
                TotalInputDataSize,
                Spec->DataSizePerJob,
                Spec->JobCount);

            auto stripes = SliceInputChunks(Config->MapJobMaxSliceDataSize, jobCount);
            jobCount = std::min(jobCount, static_cast<int>(stripes.size()));

            MapTask = New<TMapTask>(this, jobCount);
            MapTask->Initialize();
            MapTask->AddInput(stripes);
            MapTask->FinishInput();
            RegisterTask(MapTask);

            LOG_INFO("Inputs processed (JobCount: %v)",
                jobCount);
        }

        InitJobIOConfig();
        InitJobSpecTemplate();
    }

    virtual void CustomizeJoblet(TJobletPtr joblet) override
    {
        joblet->StartRowIndex = StartRowIndex;
        StartRowIndex += joblet->InputStripeList->TotalRowCount;
    }

    virtual bool IsOutputLivePreviewSupported() const override
    {
        return true;
    }

    virtual bool IsCompleted() const override
    {
        return MapTask->IsCompleted();
    }

    // Progress reporting.

    virtual Stroka GetLoggingProgress() const override
    {
        return Format(
            "Jobs = {T: %v, R: %v, C: %v, P: %v, F: %v, A: %v}, "
            "UnavailableInputChunks: %v",
            JobCounter.GetTotal(),
            JobCounter.GetRunning(),
            JobCounter.GetCompleted(),
            GetPendingJobCount(),
            JobCounter.GetFailed(),
            JobCounter.GetAborted(),
            UnavailableInputChunkCount);
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
        auto* schedulerJobSpecExt = JobSpecTemplate.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        schedulerJobSpecExt->set_lfalloc_buffer_size(GetLFAllocBufferSize());

        InitUserJobSpecTemplate(
            schedulerJobSpecExt->mutable_user_job_spec(),
            Spec->Mapper,
            RegularFiles,
            TableFiles);

        ToProto(schedulerJobSpecExt->mutable_output_transaction_id(), Operation->GetOutputTransaction()->GetId());

        schedulerJobSpecExt->set_io_config(ConvertToYsonString(JobIOConfig).Data());
    }

};

DEFINE_DYNAMIC_PHOENIX_TYPE(TMapController);
DEFINE_DYNAMIC_PHOENIX_TYPE(TMapController::TMapTask);

IOperationControllerPtr CreateMapController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TMapOperationSpec>(operation->GetSpec());
    return New<TMapController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


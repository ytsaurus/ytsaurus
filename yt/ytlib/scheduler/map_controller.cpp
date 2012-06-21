#include "stdafx.h"
#include "map_controller.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/table_client/schema.h>
#include <ytlib/job_proxy/config.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/transaction_client/transaction.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NChunkServer;
using namespace NScheduler::NProto;
using namespace NChunkHolder::NProto;
using namespace NTableClient::NProto;

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
        , Config(config)
        , Spec(spec)
        , TotalJobCount(0)
        , MapTask(New<TMapTask>(this))
    { }

private:
    TSchedulerConfigPtr Config;
    TMapOperationSpecPtr Spec;

    // Counters.
    int TotalJobCount;

    // Map task.

    class TMapTask
        : public TTask
    {
    public:
        explicit TMapTask(TMapController* controller)
            : TTask(controller)
            , Controller(controller)
        {
            ChunkPool = CreateUnorderedChunkPool();
        }

        virtual Stroka GetId() const
        {
            return "Map";
        }

        virtual int GetPendingJobCount() const
        {
            return
                IsPending()
                ? Controller->TotalJobCount - Controller->RunningJobCount - Controller->CompletedJobCount
                : 0;
        }

        virtual TDuration GetMaxLocalityDelay() const
        {
            // TODO(babenko): make configurable
            return TDuration::Seconds(5);
        }

    private:
        friend class TMapController;

        TMapController* Controller;

        virtual int GetChunkListCountPerJob() const 
        {
            return static_cast<int>(Controller->OutputTables.size());
        }

        virtual TNullable<i64> GetJobWeightThreshold() const
        {
            return GetJobWeightThresholdGeneric(
                GetPendingJobCount(),
                WeightCounter().GetPending());
        }

        virtual TJobSpec GetJobSpec(TJobInProgress* jip)
        {
            auto jobSpec = Controller->JobSpecTemplate;
            AddSequentialInputSpec(&jobSpec, jip);
            FOREACH (const auto& table, Controller->OutputTables) {
                AddTabularOutputSpec(&jobSpec, jip, table);
            }
            return jobSpec;
        }

        virtual void OnJobCompleted(TJobInProgress* jip)
        {
            TTask::OnJobCompleted(jip);

            for (int index = 0; index < static_cast<int>(Controller->OutputTables.size()); ++index) {
                auto chunkListId = jip->ChunkListIds[index];
                Controller->OutputTables[index].PartitionTreeIds.push_back(chunkListId);
            }
        }
    };
    
    typedef TIntrusivePtr<TMapTask> TMapTaskPtr;

    TMapTaskPtr MapTask;
    TJobSpec JobSpecTemplate;


    // Custom bits of preparation pipeline.

    virtual std::vector<TYPath> GetInputTablePaths()
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TYPath> GetOutputTablePaths()
    {
        return Spec->OutputTablePaths;
    }

    virtual std::vector<TYPath> GetFilePaths()
    {
        return Spec->Mapper->FilePaths;
    }

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline)
    {
        return pipeline->Add(BIND(&TMapController::ProcessInputs, MakeStrong(this)));
    }

    void ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");
            
            // Compute statistics and populate the pool.
            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];

                TNullable<TYson> rowAttributes;
                if (InputTables.size() > 1) {
                    rowAttributes = BuildYsonFluently()
                        .BeginMap()
                            .Item("table_index").Scalar(tableIndex)
                        .EndMap();
                }

                FOREACH (auto& inputChunk, *table.FetchResponse->mutable_chunks()) {
                    // Currently fetch never returns row attributes.
                    YCHECK(!inputChunk.has_row_attributes());

                    if (rowAttributes) {
                        inputChunk.set_row_attributes(rowAttributes.Get());
                    }

                    // TODO(babenko): make customizable
                    auto miscExt = GetProtoExtension<NChunkHolder::NProto::TMiscExt>(inputChunk.extensions());
                    i64 weight = miscExt->data_weight();

                    auto stripe = New<TChunkStripe>(inputChunk, weight);
                    MapTask->AddStripe(stripe);
                }
            }

            // Check for empty inputs.
            if (MapTask->IsCompleted()) {
                LOG_INFO("Empty input");
                OnOperationCompleted();
                return;
            }

            TotalJobCount = GetJobCount(
                MapTask->WeightCounter().GetTotal(),
                Config->MapJobIO->ChunkSequenceWriter->DesiredChunkSize,
                Spec->JobCount,
                MapTask->ChunkCounter().GetTotal());
            
            // Allocate some initial chunk lists.
            ChunkListPool->Allocate(OutputTables.size() * TotalJobCount + Config->SpareChunkListCount);

            InitJobSpecTemplate();

            LOG_INFO("Inputs processed (Weight: %" PRId64 ", ChunkCount: %" PRId64 ", JobCount: %d)",
                MapTask->WeightCounter().GetTotal(),
                MapTask->ChunkCounter().GetTotal(),
                TotalJobCount);

            // Kick-start the map task.
            AddTaskPendingHint(MapTask);
        }
    }


    // Progress reporting.

    virtual void LogProgress()
    {
        LOG_DEBUG("Progress: "
            "Jobs = {T: %d, R: %d, C: %d, P: %d, F: %d}, "
            "Chunks = {%s}, "
            "Weight = {%s}",
            TotalJobCount,
            RunningJobCount,
            CompletedJobCount,
            GetPendingJobCount(),
            FailedJobCount,
            ~ToString(MapTask->ChunkCounter()),
            ~ToString(MapTask->WeightCounter()));
    }

    virtual void DoGetProgress(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("chunks").Do(BIND(&TProgressCounter::ToYson, &MapTask->ChunkCounter()))
            .Item("weight").Do(BIND(&TProgressCounter::ToYson, &MapTask->WeightCounter()));
    }


    // Unsorted helpers.

    void InitJobSpecTemplate()
    {
        JobSpecTemplate.set_type(EJobType::Map);

        auto* jobSpecExt = JobSpecTemplate.MutableExtension(TMapJobSpecExt::map_job_spec_ext);
        
        InitUserJobSpec(
            jobSpecExt->mutable_mapper_spec(),
            Spec->Mapper,
            Files);

        *JobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        JobSpecTemplate.set_io_config(SerializeToYson(Config->MapJobIO));
    }

};

IOperationControllerPtr CreateMapController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = ParseOperationSpec<TMapOperationSpec>(operation);
    return New<TMapController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


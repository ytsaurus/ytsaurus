#include "stdafx.h"
#include "map_controller.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"

#include <ytlib/formats/format.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/table_client/schema.h>
#include <ytlib/job_proxy/config.h>
#include <ytlib/chunk_holder/chunk_meta_extensions.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NChunkServer;
using namespace NFormats;
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
        , TotalWeight(0)
        , PendingWeight(0)
        , CompletedWeight(0)
        , TotalChunkCount(0)
        , PendingChunkCount(0)
        , CompletedChunkCount(0)
        , MapTask(New<TMapTask>(this))
    { }

private:
    TSchedulerConfigPtr Config;
    TMapOperationSpecPtr Spec;

    // Counters.
    int TotalJobCount;
    i64 TotalWeight;
    i64 PendingWeight;
    i64 CompletedWeight;
    int TotalChunkCount;
    int PendingChunkCount;
    int CompletedChunkCount;

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
                Controller->PendingWeight == 0
                ? 0
                : Controller->TotalJobCount - Controller->RunningJobCount - Controller->CompletedJobCount;
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
                Controller->PendingWeight);
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

        virtual void OnJobStarted(TJobInProgress* jip)
        {
            TTask::OnJobStarted(jip);

            Controller->PendingChunkCount -= jip->PoolResult->LocalChunkCount;
            Controller->PendingWeight -= jip->PoolResult->TotalChunkWeight;
        }

        virtual void OnJobCompleted(TJobInProgress* jip)
        {
            TTask::OnJobCompleted(jip);

            Controller->CompletedChunkCount += jip->PoolResult->TotalChunkCount;
            Controller->CompletedWeight += jip->PoolResult->TotalChunkWeight;

            for (int index = 0; index < static_cast<int>(Controller->OutputTables.size()); ++index) {
                auto chunkListId = jip->ChunkListIds[index];
                Controller->OutputTables[index].PartitionTreeIds.push_back(chunkListId);
            }
        }

        virtual void OnJobFailed(TJobInProgress* jip)
        {
            TTask::OnJobFailed(jip);

            Controller->PendingChunkCount += jip->PoolResult->TotalChunkCount;
            Controller->PendingWeight += jip->PoolResult->TotalChunkWeight;
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
        return Spec->FilePaths;
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

                    ++TotalChunkCount;
                    TotalWeight += stripe->Weight;
                }
            }

            // Check for empty inputs.
            if (TotalWeight == 0) {
                LOG_INFO("Empty input");
                FinalizeOperation();
                return;
            }

            TotalJobCount = GetJobCount(
                TotalWeight,
                Config->MapJobIO->ChunkSequenceWriter->DesiredChunkSize,
                Spec->JobCount,
                TotalChunkCount);
            PendingWeight = TotalWeight;
            PendingChunkCount = TotalChunkCount;
            
            // Allocate some initial chunk lists.
            ChunkListPool->Allocate(OutputTables.size() * TotalJobCount + Config->SpareChunkListCount);

            InitJobSpecTemplate();

            LOG_INFO("Inputs processed (Weight: %" PRId64 ", ChunkCount: %d, JobCount: %d)",
                TotalWeight,
                TotalChunkCount,
                TotalJobCount);
        }
    }


    // Progress reporting.

    virtual void LogProgress()
    {
        LOG_DEBUG("Progress: "
            "Jobs = {T: %d, R: %d, C: %d, P: %d, F: %d}, "
            "Chunks = {T: %d, C: %d, P: %d}, "
            "Weight = {T: %" PRId64 ", C: %" PRId64 ", P: %" PRId64 "}",
            TotalJobCount,
            RunningJobCount,
            CompletedJobCount,
            GetPendingJobCount(),
            FailedJobCount,
            TotalChunkCount,
            CompletedChunkCount,
            PendingChunkCount,
            TotalWeight,
            CompletedWeight,
            PendingWeight);
    }

    virtual void DoGetProgress(IYsonConsumer* consumer)
    {
        BuildYsonMapFluently(consumer)
            .Item("chunks").BeginMap()
                .Item("total").Scalar(TotalChunkCount)
                .Item("completed").Scalar(CompletedChunkCount)
                .Item("pending").Scalar(PendingChunkCount)
            .EndMap()
            .Item("weight").BeginMap()
                .Item("total").Scalar(TotalWeight)
                .Item("completed").Scalar(CompletedWeight)
                .Item("pending").Scalar(PendingWeight)
            .EndMap();
    }


    // Unsorted helpers.

    void InitJobSpecTemplate()
    {
        JobSpecTemplate.set_type(EJobType::Map);

        auto* userJobSpecExt = JobSpecTemplate.MutableExtension(TUserJobSpecExt::user_job_spec_ext);
        userJobSpecExt->set_shell_command(Spec->Mapper);

        {
            // Set input and output format.
            TFormat inputFormat(EFormatType::Yson);
            TFormat outputFormat(EFormatType::Yson);

            if (Spec->Format) {
                inputFormat = TFormat::FromYson(Spec->Format);
                outputFormat = inputFormat;
            }

            if (Spec->InputFormat) {
                inputFormat = TFormat::FromYson(Spec->InputFormat);
            }

            if (Spec->OutputFormat) {
                outputFormat = TFormat::FromYson(Spec->OutputFormat);
            }

            userJobSpecExt->set_input_format(inputFormat.ToYson());
            userJobSpecExt->set_output_format(outputFormat.ToYson());
        }

        FOREACH (const auto& file, Files) {
            *userJobSpecExt->add_files() = *file.FetchResponse;
        }

        *JobSpecTemplate.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        JobSpecTemplate.set_io_config(SerializeToYson(Config->MapJobIO));

        // TODO(babenko): stderr
    }

};

IOperationControllerPtr CreateMapController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = New<TMapOperationSpec>();
    try {
        spec->Load(~operation->GetSpec());
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing operation spec\n%s", ex.what());
    }

    return New<TMapController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


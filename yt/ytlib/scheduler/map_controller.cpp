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
    { }

private:
    typedef TMapController TThis;

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
    
    TUnorderedChunkPool ChunkPool;

    // A template for starting new jobs.
    TJobSpec JobSpecTemplate;

    // Init/finish.

    virtual int GetPendingJobCount()
    {
        return PendingWeight == 0
            ? 0
            : TotalJobCount - RunningJobCount - CompletedJobCount;
    }


    // Job scheduling and outcome handling.

    struct TMapJobInProgress
        : public TJobInProgress
    {
        TPoolExtractionResultPtr PoolResult;
        std::vector<TChunkListId> ChunkListIds;
    };

    virtual TJobPtr DoScheduleJob(TExecNodePtr node)
    {
        // Check if we have enough chunk lists in the pool.
        if (!CheckChunkListsPoolSize(OutputTables.size())) {
            return NULL;
        }

        // We've got a job to do! :)

        // Allocate chunks for the job.
        auto jip = New<TMapJobInProgress>();
        i64 weightThreshold = GetJobWeightThreshold(GetPendingJobCount(), PendingWeight);
        jip->PoolResult = ChunkPool.Extract(
            node->GetAddress(),
            weightThreshold,
            false);

        LOG_DEBUG("Extracted %d chunks for map at node %s (LocalChunkCount: %d, ExtractedWeight: %" PRId64 ", WeightThreshold: %" PRId64 ")",
            jip->PoolResult->LocalChunkCount,
            ~node->GetAddress(),
            jip->PoolResult->LocalChunkCount,
            jip->PoolResult->TotalChunkWeight,
            weightThreshold);

        // Make a copy of the generic spec and customize it.
        auto jobSpec = JobSpecTemplate;
        {
            auto* inputSpec = jobSpec.add_input_specs();
            FOREACH (const auto& stripe, jip->PoolResult->Stripes) {
                *inputSpec->add_chunks() = stripe->InputChunks[0];
            }

            FOREACH (const auto& table, OutputTables) {
                auto* outputSpec = jobSpec.add_output_specs();
                outputSpec->set_channels(table.Channels);
                auto chunkListId = ChunkListPool->Extract();
                jip->ChunkListIds.push_back(chunkListId);
                *outputSpec->mutable_chunk_list_id() = chunkListId.ToProto();
            }
        }

        // Update running counters.
        PendingChunkCount -= jip->PoolResult->LocalChunkCount;
        PendingWeight -= jip->PoolResult->TotalChunkWeight;

        return CreateJob(
            jip,
            node,
            jobSpec,
            BIND(&TThis::OnJobCompleted, MakeWeak(this)),
            BIND(&TThis::OnJobFailed, MakeWeak(this)));
    }

    void OnJobCompleted(TMapJobInProgress* jip)
    {
        CompletedChunkCount += jip->PoolResult->TotalChunkCount;
        CompletedWeight += jip->PoolResult->TotalChunkWeight;

        ChunkPool.OnCompleted(jip->PoolResult);

        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            auto chunkListId = jip->ChunkListIds[index];
            OutputTables[index].PartitionTreeIds.push_back(chunkListId);
        }
    }

    void OnJobFailed(TMapJobInProgress* jip)
    {
        PendingChunkCount += jip->PoolResult->TotalChunkCount;
        PendingWeight += jip->PoolResult->TotalChunkWeight;

        LOG_DEBUG("Returned %d chunks into pool", jip->PoolResult->TotalChunkCount);
        ChunkPool.OnFailed(jip->PoolResult);

        ReleaseChunkLists(jip->ChunkListIds);
    }


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
        return pipeline->Add(BIND(&TThis::ProcessInputs, MakeStrong(this)));
    }

    void ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            LOG_INFO("Processing inputs");
            
            // Compute statistics and populate the pool.
            i64 totalRowCount = 0;

            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];

                TNullable<TYson> rowAttributes;
                if (InputTables.size() > 1) {
                    rowAttributes = BuildYsonFluently()
                        .BeginMap()
                            .Item("table_index").Scalar(tableIndex)
                        .EndMap();
                }

                FOREACH (auto& chunk, *table.FetchResponse->mutable_chunks()) {
                    // Currently fetch never returns row attributes.
                    YCHECK(!chunk.has_row_attributes());

                    if (rowAttributes) {
                        chunk.set_row_attributes(rowAttributes.Get());
                    }

                    auto miscExt = GetProtoExtension<NChunkHolder::NProto::TMiscExt>(chunk.extensions());

                    i64 rowCount = miscExt->row_count();
                    // TODO(babenko): make customizable
                    i64 weight = miscExt->data_weight();

                    totalRowCount += rowCount;
                    ++TotalChunkCount;
                    TotalWeight += weight;

                    auto stripe = New<TChunkStripe>(chunk, weight);
                    ChunkPool.Add(stripe);
                }
            }

            // Check for empty inputs.
            if (totalRowCount == 0) {
                LOG_INFO("Empty input");
                FinalizeOperation();
                return;
            }

            // Init counters.
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

            LOG_INFO("Inputs processed (RowCount: %" PRId64 ", Weight: %" PRId64 ", ChunkCount: %d, JobCount: %d)",
                totalRowCount,
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
                inputFormat = outputFormat;
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


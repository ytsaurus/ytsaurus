#include "stdafx.h"
#include "map_controller.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/table_client/schema.h>
#include <ytlib/job_proxy/config.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NChunkServer;
using namespace NScheduler::NProto;
using namespace NChunkHolder::NProto;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OperationsLogger);
static NProfiling::TProfiler Profiler("/operations/map");

////////////////////////////////////////////////////////////////////

class TMapController
    : public TOperationControllerBase
{
public:
    TMapController(
        TMapControllerConfigPtr config,
        IOperationHost* host,
        TOperation* operation)
        : Config(config)
        , TOperationControllerBase(config, host, operation)
    { }

private:
    typedef TMapController TThis;

    TMapControllerConfigPtr Config;

    TMapOperationSpecPtr Spec;

    // Running counters.
    TRunningCounter ChunkCounter;
    TRunningCounter WeightCounter;

    ::THolder<TUnorderedChunkPool> ChunkPool;

    // The template for starting new jobs.
    TJobSpec JobSpecTemplate;

    // Init/finish.

    virtual void DoInitialize()
    {
        Spec = New<TMapOperationSpec>();
        try {
            Spec->Load(~Operation->GetSpec());
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing operation spec\n%s", ex.what());
        }
    }

    virtual bool HasPendingJobs()
    {
        // Use chunk counter not job counter since the latter one may be inaccurate.
        return ChunkCounter.GetPending() > 0;
    }


    // Job scheduling and outcome handling.

    struct TJobInProgress
        : public TIntrinsicRefCounted
    {
        std::vector<TPooledChunkPtr> Chunks;
        i64 Weight;
        std::vector<TChunkListId> ChunkListIds;
    };

    typedef TIntrusivePtr<TJobInProgress> TJobInProgressPtr;

    virtual TJobPtr DoScheduleJob(TExecNodePtr node)
    {
        // Check if we have enough chunk lists in the pool.
        if (!CheckChunkListsPoolSize(OutputTables.size())) {
            return NULL;
        }

        // We've got a job to do! :)

        // Allocate chunks for the job.
        auto jip = New<TJobInProgress>();
        i64 weightThreshold = GetJobWeightThreshold(JobCounter.GetPending(), WeightCounter.GetPending());
        i64 extractedWeight;
        int localCount;
        int remoteCount;
        ChunkPool->Extract(
            node->GetAddress(),
            weightThreshold,
            false,
            &jip->Chunks,
            &jip->Weight,
            &localCount,
            &remoteCount);
        YASSERT(!jip->Chunks.empty());

        LOG_DEBUG("Extracted %d chunks for node %s (ExtractedWeight: %" PRId64 ", WeightThreshold: %" PRId64 ", LocalCount: %d, RemoteCount: %d)",
            static_cast<int>(jip->Chunks.size()),
            ~node->GetAddress(),
            jip->Weight,
            weightThreshold,
            localCount,
            remoteCount);

        // Make a copy of the generic spec and customize it.
        auto jobSpec = JobSpecTemplate;
        auto* mapJobSpec = jobSpec.MutableExtension(TMapJobSpec::map_job_spec);
        FOREACH (const auto& chunk, jip->Chunks) {
            *mapJobSpec->mutable_input_spec()->add_chunks() = chunk->InputChunk;
        }
        FOREACH (auto& outputSpec, *mapJobSpec->mutable_output_specs()) {
            auto chunkListId = ChunkListPool->Extract();
            jip->ChunkListIds.push_back(chunkListId);
            *outputSpec.mutable_chunk_list_id() = chunkListId.ToProto();
        }

        // Update running counters.
        ChunkCounter.Start(jip->Chunks.size());
        WeightCounter.Start(jip->Weight);

        return CreateJob(
            Operation,
            node,
            jobSpec,
            BIND(&TThis::OnJobCompleted, MakeWeak(this), jip),
            BIND(&TThis::OnJobFailed, MakeWeak(this), jip));
    }

    void OnJobCompleted(TJobInProgressPtr jip)
    {
        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            auto chunkListId = jip->ChunkListIds[index];
            OutputTables[index].OutputChildrenIds.push_back(chunkListId);
        }

        ChunkCounter.Completed(jip->Chunks.size());
        WeightCounter.Completed(jip->Weight);
    }

    void OnJobFailed(TJobInProgressPtr jip)
    {
        LOG_DEBUG("%d chunks are back in the pool", static_cast<int>(jip->Chunks.size()));
        ChunkPool->Put(jip->Chunks);

        ChunkCounter.Failed(jip->Chunks.size());
        WeightCounter.Failed(jip->Weight);

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

    virtual void DoCompletePreparation()
    {
        PROFILE_TIMING ("input_processing_time") {
            LOG_INFO("Processing inputs");
            
            // Compute statistics and populate the pool.
            i64 totalRowCount = 0;
            i64 totalDataSize = 0;
            i64 totalWeight = 0;
            i64 totalChunkCount = 0;

            ChunkPool.Reset(new TUnorderedChunkPool());

            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];

                TNullable<TYson> rowAttributes;
                if (InputTables.size() > 1) {
                    // TODO(babenko): think of a proper name
                    rowAttributes = BuildYsonFluently()
                        .BeginMap()
                        .Item("table_index").Scalar(tableIndex)
                        .EndMap();
                }

                auto fetchRsp = table.FetchResponse;
                FOREACH (auto& inputChunk, *fetchRsp->mutable_chunks()) {
                    // Currently fetch never returns row attributes.
                    YASSERT(!inputChunk.has_row_attributes());

                    if (rowAttributes) {
                        inputChunk.set_row_attributes(rowAttributes.Get());
                    }

                    i64 rowCount = inputChunk.approximate_row_count();
                    i64 dataSize = inputChunk.approximate_data_size();
                    // TODO(babenko): make customizable
                    // Plus one is to ensure that weights are positive.
                    i64 weight = inputChunk.approximate_data_size() + 1;

                    totalRowCount += rowCount;
                    totalDataSize += dataSize;
                    totalChunkCount += 1;
                    totalWeight += weight;

                    auto pooledChunk = New<TPooledChunk>(inputChunk, weight);
                    ChunkPool->Put(pooledChunk);
                }
            }

            // Check for empty inputs.
            if (totalRowCount == 0) {
                LOG_INFO("Empty input");
                FinalizeOperation();
                return;
            }

            // Choose job count.
            // TODO(babenko): refactor, generalize, and improve.
            i64 jobCount = ExecNodeCount;
            if (Spec->JobCount) {
                jobCount = Spec->JobCount.Get();
            }
            jobCount = std::min(jobCount, static_cast<i64>(totalChunkCount));
            YASSERT(totalWeight > 0);
            YASSERT(jobCount > 0);

            // Init running counters.
            JobCounter.Init(jobCount);
            ChunkCounter.Init(totalChunkCount);
            WeightCounter.Init(totalWeight);

            // Allocate some initial chunk lists.
            ChunkListPool->Allocate(OutputTables.size() * jobCount + Config->SpareChunkListCount);

            InitJobSpecTemplate();

            LOG_INFO("Inputs processed (TotalRowCount: %" PRId64 ", TotalDataSize: %" PRId64 ", TotalWeight: %" PRId64 ", TotalChunkCount: %" PRId64 ", JobCount: %" PRId64 ")",
                totalRowCount,
                totalDataSize,
                totalWeight,
                totalChunkCount,
                jobCount);
        }
    }


    // Unsorted helpers.

    virtual void DumpProgress()
    {
        LOG_DEBUG("Progress: Jobs = {%s}, Chunks = {%s}, Weight = {%s}",
            ~ToString(JobCounter),
            ~ToString(ChunkCounter),
            ~ToString(WeightCounter));
    }

    void InitJobSpecTemplate()
    {
        JobSpecTemplate.set_type(EJobType::Map);

        TUserJobSpec userJobSpec;
        userJobSpec.set_shell_command(Spec->Mapper);
        FOREACH (const auto& file, Files) {
            *userJobSpec.add_files() = *file.FetchResponse;
        }
        *JobSpecTemplate.MutableExtension(TUserJobSpec::user_job_spec) = userJobSpec;

        TMapJobSpec mapJobSpec;
        *mapJobSpec.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();
        FOREACH (const auto& table, OutputTables) {
            auto* outputSpec = mapJobSpec.add_output_specs();
            outputSpec->set_schema(table.Schema);
        }
        *JobSpecTemplate.MutableExtension(TMapJobSpec::map_job_spec) = mapJobSpec;

        JobSpecTemplate.set_io_config(SerializeToYson(Spec->JobIO));

        // TODO(babenko): stderr
    }
};

IOperationControllerPtr CreateMapController(
    TMapControllerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    return New<TMapController>(config, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


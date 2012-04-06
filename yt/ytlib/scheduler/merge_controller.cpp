#include "stdafx.h"
#include "merge_controller.h"
#include "operation_controller.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NChunkServer;
using namespace NScheduler::NProto;
using namespace NChunkHolder::NProto;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OperationsLogger);
static NProfiling::TProfiler Profiler("/operations/merge");

////////////////////////////////////////////////////////////////////

class TMergeController
    : public TOperationControllerBase
{
public:
    TMergeController(
        TSchedulerConfigPtr config,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, host, operation)
    { }

private:
    typedef TMergeController TThis;

    TMergeOperationSpecPtr Spec;

    // Running counters.
    TRunningCounter ChunkCounter;
    TRunningCounter WeightCounter;

    ::THolder<TUnorderedChunkPool> ChunkPool;

    // The template for starting new jobs.
    TJobSpec JobSpecTemplate;

    // Init/finish.

    virtual void DoInitialize()
    {
        Spec = New<TMergeOperationSpec>();
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
        TChunkListId ChunkListId;
    };

    typedef TIntrusivePtr<TJobInProgress> TJobInProgressPtr;

    virtual TJobPtr DoScheduleJob(TExecNodePtr node)
    {
        // Check if we have enough chunk lists in the pool.
        if (!CheckChunkListsPoolSize(1)) {
            return NULL;
        }

        // We've got a job to do! :)

        // Allocate chunks for the job.
        auto jip = New<TJobInProgress>();
        i64 weightThreshold = GetJobWeightThreshold(JobCounter.GetPending(), WeightCounter.GetPending());
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

        LOG_DEBUG("Extracted %d chunks for node %s (jip->Weight: %" PRId64 ", WeightThreshold: %" PRId64 ", LocalCount: %d, RemoteCount: %d)",
            static_cast<int>(jip->Chunks.size()),
            ~node->GetAddress(),
            jip->Weight,
            weightThreshold,
            localCount,
            remoteCount);

        // Make a copy of the generic spec and customize it.
        auto jobSpec = JobSpecTemplate;
        auto* mergeJobSpec = jobSpec.MutableExtension(TMergeJobSpec::merge_job_spec);
        FOREACH (const auto& chunk, jip->Chunks) {
            *mergeJobSpec->mutable_input_spec()->add_chunks() = chunk->InputChunk;
        }
        jip->ChunkListId = ChunkListPool->Extract();
        *mergeJobSpec->mutable_output_spec()->mutable_chunk_list_id() = jip->ChunkListId.ToProto();

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

    virtual void OnJobCompleted(TJobInProgressPtr jip)
    {
        OutputTables[0].OutputChildrenIds.push_back(jip->ChunkListId);

        ChunkCounter.Completed(jip->Chunks.size());
        WeightCounter.Completed(jip->Weight);
    }

    virtual void OnJobFailed(TJobInProgressPtr jip)
    {
        LOG_DEBUG("%d chunks are back in the pool", static_cast<int>(jip->Chunks.size()));
        ChunkPool->Put(jip->Chunks);

        ChunkCounter.Failed(jip->Chunks.size());
        WeightCounter.Failed(jip->Weight);

        ReleaseChunkList(jip->ChunkListId);
    }


    // Custom bits of preparation pipeline.

    virtual std::vector<TYPath> GetInputTablePaths()
    {
        return Spec->InputTablePaths;
    }

    virtual std::vector<TYPath> GetOutputTablePaths()
    {
        std::vector<TYPath> result;
        result.push_back(Spec->OutputTablePath);
        return result;
    }

    virtual std::vector<TYPath> GetFilePaths()
    {
        return std::vector<TYPath>();
    }

    virtual void DoCompletePreparation()
    {
        PROFILE_TIMING ("input_processing_time") {
            LOG_INFO("Processing inputs");

            // Compute statistics and populate the pool.
            i64 totalRowCount = 0;
            i64 mergeRowCount = 0;
            i64 totalDataSize = 0;
            i64 mergeDataSize = 0;
            i64 totalChunkCount = 0;
            i64 mergeChunkCount = 0;

            ChunkPool.Reset(new TUnorderedChunkPool());

            for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
                const auto& table = InputTables[tableIndex];
                auto fetchRsp = table.FetchResponse;
                FOREACH (auto& inputChunk, *fetchRsp->mutable_chunks()) {
                    auto chunkId = TChunkId::FromProto(inputChunk.slice().chunk_id());

                    i64 rowCount = inputChunk.approximate_row_count();
                    i64 dataSize = inputChunk.approximate_data_size();

                    totalRowCount += rowCount;
                    totalDataSize += dataSize;
                    totalChunkCount += 1;

                    if (IsMergableChunk(inputChunk)) {
                        mergeRowCount += rowCount;
                        mergeDataSize += dataSize;
                        mergeChunkCount += 1;

                        // Merge is IO-bound, use data size as weight.
                        auto pooledChunk = New<TPooledChunk>(inputChunk, dataSize);
                        ChunkPool->Put(pooledChunk);
                    } else {
                        // Chunks not requiring merge go directly to the output chunk list.
                        OutputTables[0].OutputChildrenIds.push_back(chunkId);
                    }
                }
            }

            // Check for empty and trivial inputs.
            if (totalRowCount == 0) {
                LOG_INFO("Empty input");
                FinalizeOperation();
                return;
            }

            if (mergeRowCount == 0) {
                LOG_INFO("Trivial merge");
                FinalizeOperation();
                return;
            }

            // Choose job count.
            // TODO(babenko): refactor, generalize, and improve.
            i64 jobCount = (i64) ceil((double) mergeDataSize / Spec->JobIO->ChunkSequenceWriter->DesiredChunkSize);
            if (Spec->JobCount) {
                jobCount = Spec->JobCount.Get();
            }
            jobCount = std::min(jobCount, static_cast<i64>(mergeChunkCount));
            YASSERT(jobCount > 0);

            // Init running counters.
            JobCounter.Init(jobCount);
            ChunkCounter.Init(mergeChunkCount);
            WeightCounter.Init(mergeDataSize);

            // Allocate some initial chunk lists.
            // TOOD(babenko): make configurable
            ChunkListPool->Allocate(jobCount + Config->SpareChunkListCount);

            InitJobSpecTemplate();

            LOG_INFO("Inputs processed (TotalRowCount: %" PRId64 ", MergeRowCount: %" PRId64 ", TotalDataSize: %" PRId64 ", MergeDataSize: %" PRId64 ", TotalChunkCount: %" PRId64 ", MergeChunkCount: %" PRId64 ", JobCount: %" PRId64 ")",
                totalRowCount,
                mergeRowCount,
                totalDataSize,
                mergeDataSize,
                totalChunkCount,
                mergeChunkCount,
                jobCount);
        }
    }


    // Unsorted helpers.

    //! Returns True iff the chunk has nontrivial limits.
    //! Such chunks are always pooled.
    static bool IsCompleteChunk(const TInputChunk& chunk)
    {
        return
            !chunk.slice().start_limit().has_row_index() &&
            !chunk.slice().start_limit().has_key () &&
            !chunk.slice().end_limit().has_row_index() &&
            !chunk.slice().end_limit().has_key ();
    }

    //! Returns True iff the chunk must be pooled.
    bool IsMergableChunk(const TInputChunk& chunk)
    {
        if (!IsCompleteChunk(chunk)) {
            // Incomplete chunks are always pooled.
            return true;
        }

        // Completed chunks are pooled depending on their sizes and the spec.
        return
            chunk.approximate_data_size() < Spec->JobIO->ChunkSequenceWriter->DesiredChunkSize &&
            Spec->CombineChunks;
    }

    virtual void DumpProgress()
    {
        LOG_DEBUG("Progress: Jobs = {%s}, Chunks = {%s}, Weight = {%s}",
            ~ToString(JobCounter),
            ~ToString(ChunkCounter),
            ~ToString(WeightCounter));
    }

    void InitJobSpecTemplate()
    {
        JobSpecTemplate.set_type(Spec->Mode == EMergeMode::Sorted ? EJobType::SortedMerge : EJobType::OrderedMerge);

        TMergeJobSpec mergeJobSpec;
        *mergeJobSpec.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();

        mergeJobSpec.mutable_output_spec()->set_schema(OutputTables[0].Schema);
        *JobSpecTemplate.MutableExtension(TMergeJobSpec::merge_job_spec) = mergeJobSpec;

        JobSpecTemplate.set_io_config(SerializeToYson(Spec->JobIO));
    }
};

IOperationControllerPtr CreateMergeController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    return New<TMergeController>(config, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


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
static NProfiling::TProfiler Profiler("operations/map");

////////////////////////////////////////////////////////////////////

class TMapController
    : public TOperationControllerBase
{
public:
    TMapController(IOperationHost* host, TOperation* operation)
        : TOperationControllerBase(host, operation)
    { }

    virtual void Initialize()
    {
        TOperationControllerBase::Initialize();

        Spec = New<TMapOperationSpec>();
        try {
            Spec->Load(~Operation->GetSpec());
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing operation spec\n%s", ex.what());
        }
    }


    virtual void OnJobCompleted(TJobPtr job)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Job %s completed\n%s",
            ~job->GetId().ToString(),
            ~TError::FromProto(job->Result().error()).ToString());

        auto jobInfo = GetJobInfo(job);

        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            auto chunkListId = jobInfo->OutputChunkListIds[index];
            OutputTables[index].ChunkTreeIds.push_back(chunkListId);
        }

        JobCounter.Completed(1);
        ChunkCounter.Completed(jobInfo->Chunks.size());
        WeightCounter.Completed(jobInfo->Weight);

        RemoveJobInfo(job);

        DumpStatistics();

        if (JobCounter.GetRunning() == 0 && ChunkCounter.GetPending() == 0) {
            CompleteOperation();
        }
    }

    virtual void OnJobFailed(TJobPtr job)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto jobInfo = GetJobInfo(job);

        LOG_INFO("Job %s failed, %d chunks are back in the pool\n%s",
            ~job->GetId().ToString(),
            static_cast<int>(jobInfo->Chunks.size()),
            ~TError::FromProto(job->Result().error()).ToString());

        ChunkPool->Put(jobInfo->Chunks);

        JobCounter.Failed(1);
        ChunkCounter.Failed(jobInfo->Chunks.size());
        WeightCounter.Failed(jobInfo->Weight);

        ReleaseChunkLists(jobInfo->OutputChunkListIds);
        RemoveJobInfo(job);

        DumpStatistics();

        // TODO(babenko): make configurable
        if (JobCounter.GetFailed() > 10) {
            OnOperationFailed(TError("%d jobs failed, aborting operation",
                JobCounter.GetFailed()));
        }
    }


    virtual void ScheduleJobs(
        TExecNodePtr node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        
        // Check if we have any unassigned chunks left.
        if (ChunkCounter.GetPending() == 0) {
            LOG_DEBUG("No pending chunks left, ignoring scheduling request");
            return;
        }

        // Check if we have enough chunk lists in the pool.
        if (ChunkListPool->GetSize() < OutputTables.size()) {
            LOG_DEBUG("No pooled chunk lists left, ignoring scheduling request");
            // TODO(babenko): make configurable
            ChunkListPool->Allocate(OutputTables.size() * 5);
            return;
        }

        // We've got a job to do! :)
        
        // Make a copy of the generic spec and customize it.
        auto jobSpec = JobSpecTemplate;
        auto* mapJobSpec = jobSpec.MutableExtension(TMapJobSpec::map_job_spec);

        i64 pendingJobs = JobCounter.GetPending();
        YASSERT(pendingJobs > 0);
        i64 pendingWeight = WeightCounter.GetPending();
        YASSERT(pendingWeight > 0);
        i64 weightPerJob = (pendingWeight + pendingJobs - 1) / pendingJobs;

        auto jobInfo = New<TJobInfo>();

        // Allocate chunks for the job.
        auto& extractedChunks = jobInfo->Chunks;
        i64 extractedWeight;
        int localCount;
        int remoteCount;
        ChunkPool->Extract(
            node->GetAddress(),
            weightPerJob,
            false,
            &extractedChunks,
            &extractedWeight,
            &localCount,
            &remoteCount);

        LOG_DEBUG("Extracted %d chunks for node %s (ExtractedWeight: %" PRId64 ", WeightPerJob: %" PRId64 ", LocalCount: %d, RemoteCount: %d)",
            static_cast<int>(extractedChunks.size()),
            ~node->GetAddress(),
            extractedWeight,
            weightPerJob,
            localCount,
            remoteCount);

        YASSERT(!extractedChunks.empty());

        FOREACH (const auto& chunk, extractedChunks) {
            *mapJobSpec->mutable_input_spec()->add_chunks() = chunk->InputChunk;
        }

        FOREACH (auto& outputSpec, *mapJobSpec->mutable_output_specs()) {
            auto chunkListId = ChunkListPool->Extract();
            jobInfo->OutputChunkListIds.push_back(chunkListId);
            *outputSpec.mutable_chunk_list_id() = chunkListId.ToProto();
        }

        jobInfo->Weight = extractedWeight;
        
        auto job = Host->CreateJob(
            Operation,
            node,
            jobSpec);

        PutJobInfo(job, jobInfo);

        JobCounter.Start(1);
        ChunkCounter.Start(extractedChunks.size());
        WeightCounter.Start(extractedWeight);

        DumpStatistics();

        jobsToStart->push_back(job);
    }

    virtual i64 GetPendingJobCount()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return JobCounter.GetPending();
    }

private:
    TMapOperationSpecPtr Spec;

    // Running counters.
    TRunningCounter JobCounter;
    TRunningCounter ChunkCounter;
    TRunningCounter WeightCounter;

    // Size estimates.
    i64 TotalRowCount;
    i64 TotalDataSize;
    i64 TotalWeight;

    ::THolder<TUnorderedChunkPool> ChunkPool;
    TChunkListPoolPtr ChunkListPool;

    // The template for starting new jobs.
    TJobSpec JobSpecTemplate;

    // Job scheduled so far.
    // TOOD(babenko): consider keeping this in job's attributes
    struct TJobInfo
        : public TIntrinsicRefCounted
    {
        // Chunks assigned to this job.
        std::vector<TPooledChunkPtr> Chunks;

        // Chunk lists allocated to store the output (one per each output table).
        std::vector<TChunkListId> OutputChunkListIds;

        // Total weight of allocated chunks.
        i64 Weight;
    };

    typedef TIntrusivePtr<TJobInfo> TJobInfoPtr;
    yhash_map<TJobPtr, TJobInfoPtr> JobInfos;


    // Scheduled jobs info management.

    void PutJobInfo(TJobPtr job, TJobInfoPtr jobInfo)
    {
        YVERIFY(JobInfos.insert(MakePair(job, jobInfo)).second);
    }

    TJobInfoPtr GetJobInfo(TJobPtr job)
    {
        auto it = JobInfos.find(job);
        YASSERT(it != JobInfos.end());
        return it->second;
    }

    void RemoveJobInfo(TJobPtr job)
    {
        YVERIFY(JobInfos.erase(job) == 1);
    }

    
    // Custom bits of preparation pipeline.

    virtual std::vector<TYPath> GetInputTablePaths()
    {
        return Spec->In;
    }

    virtual std::vector<TYPath> GetOutputTablePaths()
    {
        return Spec->Out;
    }

    virtual std::vector<TYPath> GetFilePaths()
    {
        return Spec->Files;
    }

    virtual void DoCompletePreparation()
    {
        ChunkPool.Reset(new TUnorderedChunkPool());
        ChunkListPool = New<TChunkListPool>(
            Host->GetMasterChannel(),
            Host->GetControlInvoker(),
            Operation,
            PrimaryTransaction->GetId());

        // Compute statistics and populate the pools.

        TotalRowCount = 0;
        TotalDataSize = 0;
        TotalWeight = 0;

        i64 totalChunkCount = 0;

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

                TChunkAttributes chunkAttributes;
                YVERIFY(DeserializeFromProto(&chunkAttributes, TRef::FromString(inputChunk.chunk_attributes())));

                YASSERT(chunkAttributes.HasExtension(TTableChunkAttributes::table_attributes));
                const auto& tableChunkAttributes = chunkAttributes.GetExtension(TTableChunkAttributes::table_attributes);

                // TODO(babenko): compute more accurately
                i64 rowCount = tableChunkAttributes.row_count();
                i64 dataSize = tableChunkAttributes.uncompressed_size();

                TotalRowCount += rowCount;
                TotalDataSize += dataSize;

                // TODO(babenko): make customizable
                // Plus one is to ensure that weights are positive.
                i64 weight = dataSize + 1;

                TotalWeight += weight;

                auto pooledChunk = New<TPooledChunk>(inputChunk, weight);
                ChunkPool->Put(pooledChunk);

                ++totalChunkCount;
            }
        }

        // Choose job count.
        i64 totalJobCount = Spec->JobCount.Get(ExecNodeCount);
        totalJobCount = std::min(totalJobCount, static_cast<i64>(totalChunkCount));

        // Init running counters.
        JobCounter.Init(totalJobCount);
        ChunkCounter.Init(totalChunkCount);
        WeightCounter.Init(TotalWeight);

        // Check for empty inputs.
        if (TotalRowCount == 0) {
            LOG_INFO("Empty input");
            CompleteOperation();
            return;
        }

        YASSERT(TotalWeight > 0);
        YASSERT(totalJobCount > 0);

        // Allocate some initial chunk lists.
        // TOOD(babenko): make configurable
        ChunkListPool->Allocate(OutputTables.size() * totalJobCount + 10);

        InitJobSpecTemplate();

        LOG_INFO("Preparation completed (RowCount: %" PRId64 ", DataSize: %" PRId64 ", Weight: %" PRId64 ", ChunkCount: %" PRId64 ", JobCount: %" PRId64 ")",
            TotalRowCount,
            TotalDataSize,
            TotalWeight,
            totalChunkCount,
            totalJobCount);
    }


    // Unsorted helpers.

    void DumpStatistics()
    {
        LOG_DEBUG("Running statistics: Jobs = {%s}, Chunks = {%s}, Weight = {%s}",
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

        JobSpecTemplate.set_io_config(SerializeToYson(Spec->JobIOConfig));


        // TODO(babenko): stderr
    }

};

IOperationControllerPtr CreateMapController(
    IOperationHost* host,
    TOperation* operation)
{
    return New<TMapController>(host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


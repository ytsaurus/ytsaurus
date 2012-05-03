#include "stdafx.h"
#include "map_controller.h"
#include "private.h"
#include "operation_controller_detail.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "samples_fetcher.h"

#include <ytlib/ytree/fluent.h>
#include <ytlib/table_client/schema.h>
#include <ytlib/job_proxy/config.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NChunkServer;
using namespace NScheduler::NProto;
using namespace NChunkHolder::NProto;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OperationsLogger);
static NProfiling::TProfiler Profiler("/operations/sort");

////////////////////////////////////////////////////////////////////

class TSortController
    : public TOperationControllerBase
{
public:
    TSortController(
        TSchedulerConfigPtr config,
        TSortOperationSpecPtr spec,
        IOperationHost* host,
        TOperation* operation)
        : TOperationControllerBase(config, host, operation)
        , Config(config)
        , Spec(spec)
    { }

private:
    typedef TSortController TThis;

    TSchedulerConfigPtr Config;
    TSortOperationSpecPtr Spec;

    //// Running counters.
    //TProgressCounter ChunkCounter;
    //TProgressCounter WeightCounter;

    //TAutoPtr<IChunkPool> ChunkPool;

    //// The template for starting new jobs.
    //TJobSpec JobSpecTemplate;

    //// Init/finish.

    virtual bool HasPendingJobs()
    {
        return false;
        // Use chunk counter not job counter since the latter one may be inaccurate.
        //return ChunkCounter.GetPending() > 0;
    }


    //// Job scheduling and outcome handling.

    //struct TJobInProgress
    //    : public TIntrinsicRefCounted
    //{
    //    IChunkPool::TExtractResultPtr ExtractResult;
    //    std::vector<TChunkListId> ChunkListIds;
    //};

    //typedef TIntrusivePtr<TJobInProgress> TJobInProgressPtr;

    virtual TJobPtr DoScheduleJob(TExecNodePtr node)
    {
        return NULL;
    //    // Check if we have enough chunk lists in the pool.
    //    if (!CheckChunkListsPoolSize(OutputTables.size())) {
    //        return NULL;
    //    }

    //    // We've got a job to do! :)

    //    // Allocate chunks for the job.
    //    auto jip = New<TJobInProgress>();
    //    i64 weightThreshold = GetJobWeightThreshold(JobCounter.GetPending(), WeightCounter.GetPending());
    //    jip->ExtractResult = ChunkPool->Extract(
    //        node->GetAddress(),
    //        weightThreshold,
    //        std::numeric_limits<int>::max(),
    //        false);
    //    YASSERT(jip->ExtractResult);

    //    LOG_DEBUG("Extracted %d chunks, %d local for node %s (ExtractedWeight: %" PRId64 ", WeightThreshold: %" PRId64 ")",
    //        static_cast<int>(jip->ExtractResult->Chunks.size()),
    //        jip->ExtractResult->LocalCount,
    //        ~node->GetAddress(),
    //        jip->ExtractResult->Weight,
    //        weightThreshold);

    //    // Make a copy of the generic spec and customize it.
    //    auto jobSpec = JobSpecTemplate;
    //    auto* mapJobSpec = jobSpec.MutableExtension(TMapJobSpec::map_job_spec);
    //    FOREACH (const auto& chunk, jip->ExtractResult->Chunks) {
    //        *mapJobSpec->mutable_input_spec()->add_chunks() = chunk->InputChunk;
    //    }
    //    FOREACH (auto& outputSpec, *mapJobSpec->mutable_output_specs()) {
    //        auto chunkListId = ChunkListPool->Extract();
    //        jip->ChunkListIds.push_back(chunkListId);
    //        *outputSpec.mutable_chunk_list_id() = chunkListId.ToProto();
    //    }

    //    // Update running counters.
    //    ChunkCounter.Start(jip->ExtractResult->Chunks.size());
    //    WeightCounter.Start(jip->ExtractResult->Weight);

    //    return CreateJob(
    //        Operation,
    //        node,
    //        jobSpec,
    //        BIND(&TThis::OnJobCompleted, MakeWeak(this), jip),
    //        BIND(&TThis::OnJobFailed, MakeWeak(this), jip));
    }

    //void OnJobCompleted(TJobInProgressPtr jip)
    //{
    //    for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
    //        auto chunkListId = jip->ChunkListIds[index];
    //        OutputTables[index].PartitionTreeIds.push_back(chunkListId);
    //    }

    //    ChunkCounter.Completed(jip->ExtractResult->Chunks.size());
    //    WeightCounter.Completed(jip->ExtractResult->Weight);
    //}

    //void OnJobFailed(TJobInProgressPtr jip)
    //{
    //    ChunkCounter.Failed(jip->ExtractResult->Chunks.size());
    //    WeightCounter.Failed(jip->ExtractResult->Weight);

    //    LOG_DEBUG("Returned %d chunks into the pool",
    //        static_cast<int>(jip->ExtractResult->Chunks.size()));
    //    ChunkPool->PutBack(jip->ExtractResult);

    //    ReleaseChunkLists(jip->ChunkListIds);
    //}


    //// Custom bits of preparation pipeline.

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

    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline)
    {
        return pipeline
            ->Add(BIND(&TThis::FetchSamples, MakeStrong(this)))
            ->Add(BIND(&TThis::ProcessInputs, MakeStrong(this)));
    }

    TFuture< TValueOrError<void> > FetchSamples()
    {
        auto samplesFetcher = New<TSamplesFetcher>(
            Config,
            Spec,
            Host->GetBackgroundInvoker(),
            Operation->GetOperationId());

        // Compute statistics and prepare the fetcher.
        i64 totalRowCount = 0;
        i64 totalDataSize = 0;
        i64 totalChunkCount = 0;

        for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
            const auto& table = InputTables[tableIndex];

            auto fetchRsp = table.FetchResponse;
            FOREACH (const auto& chunk, *fetchRsp->mutable_chunks()) {
                i64 rowCount = chunk.approximate_row_count();
                i64 dataSize = chunk.approximate_data_size();

                totalRowCount += rowCount;
                totalDataSize += dataSize;
                totalChunkCount += 1;

                samplesFetcher->AddChunk(chunk);
            }
        }

        // Check for empty inputs.
        if (totalRowCount == 0) {
            LOG_INFO("Empty input");
            FinalizeOperation();
            return MakeFuture(TValueOrError<void>());
        }

        LOG_INFO("Inputs processed (TotalRowCount: %" PRId64 ", TotalDataSize: %" PRId64 ", TotalChunkCount: %" PRId64 ")",
            totalRowCount,
            totalDataSize,
            totalChunkCount);

        return samplesFetcher->Run();
    }

    void ProcessInputs()
    {
        PROFILE_TIMING ("/input_processing_time") {
            //LOG_INFO("Processing inputs");
    //        
    //        // Compute statistics and populate the pool.
    //        i64 totalRowCount = 0;
    //        i64 totalDataSize = 0;
    //        i64 totalWeight = 0;
    //        i64 totalChunkCount = 0;

    //        ChunkPool = CreateUnorderedChunkPool();

    //        for (int tableIndex = 0; tableIndex < static_cast<int>(InputTables.size()); ++tableIndex) {
    //            const auto& table = InputTables[tableIndex];

    //            TNullable<TYson> rowAttributes;
    //            if (InputTables.size() > 1) {
    //                rowAttributes = BuildYsonFluently()
    //                    .BeginMap()
    //                        .Item("table_index").Scalar(tableIndex)
    //                    .EndMap();
    //            }

    //            auto fetchRsp = table.FetchResponse;
    //            FOREACH (auto& chunk, *fetchRsp->mutable_chunks()) {
    //                // Currently fetch never returns row attributes.
    //                YASSERT(!chunk.has_row_attributes());

    //                if (rowAttributes) {
    //                    chunk.set_row_attributes(rowAttributes.Get());
    //                }

    //                i64 rowCount = chunk.approximate_row_count();
    //                i64 dataSize = chunk.approximate_data_size();
    //                // TODO(babenko): make customizable
    //                // Plus one is to ensure that weights are positive.
    //                i64 weight = chunk.approximate_data_size() + 1;

    //                totalRowCount += rowCount;
    //                totalDataSize += dataSize;
    //                totalChunkCount += 1;
    //                totalWeight += weight;

    //                auto pooledChunk = New<TPooledChunk>(chunk, weight);
    //                ChunkPool->Add(pooledChunk);
    //            }
    //        }

    //        // Check for empty inputs.
    //        if (totalRowCount == 0) {
    //            LOG_INFO("Empty input");
    //            FinalizeOperation();
    //            return;
    //        }

    //        // Init counters.
    //        ChunkCounter.Set(totalChunkCount);
    //        WeightCounter.Set(totalWeight);
    //        ChooseJobCount();

    //        // Allocate some initial chunk lists.
    //        ChunkListPool->Allocate(OutputTables.size() * JobCounter.GetPending() + Config->SpareChunkListCount);

    //        InitJobSpecTemplate();

    //        LOG_INFO("Inputs processed (TotalRowCount: %" PRId64 ", TotalDataSize: %" PRId64 ", TotalWeight: %" PRId64 ", TotalChunkCount: %" PRId64 ", JobCount: %" PRId64 ")",
    //            totalRowCount,
    //            totalDataSize,
    //            totalWeight,
    //            totalChunkCount,
    //            JobCounter.GetPending());
        }
    }

    //void ChooseJobCount()
    //{
    //    // Choose job count.
    //    // TODO(babenko): refactor, generalize, and improve.
    //    // TODO(babenko): this currently assumes that weight is just size
    //    i64 jobCount = (i64) std::ceil((double) WeightCounter.GetPending() / Spec->JobIO->ChunkSequenceWriter->DesiredChunkSize);
    //    if (Spec->JobCount) {
    //        jobCount = Spec->JobCount.Get();
    //    }
    //    jobCount = std::min(jobCount, ChunkCounter.GetPending());
    //    YASSERT(jobCount > 0);
    //    JobCounter.Set(jobCount);
    //}

    //// Progress reporting.

    virtual void LogProgress()
    {
    //    LOG_DEBUG("Progress: Jobs = {%s}, Chunks = {%s}, Weight = {%s}",
    //        ~ToString(JobCounter),
    //        ~ToString(ChunkCounter),
    //        ~ToString(WeightCounter));
    }

    virtual void DoGetProgress(IYsonConsumer* consumer)
    {
    //    BuildYsonMapFluently(consumer)
    //        .Item("chunks").Do(BIND(&TProgressCounter::ToYson, &ChunkCounter))
    //        .Item("weight").Do(BIND(&TProgressCounter::ToYson, &WeightCounter));
    }

    //// Unsorted helpers.

    //void InitJobSpecTemplate()
    //{
    //    JobSpecTemplate.set_type(EJobType::Map);

    //    TUserJobSpec userJobSpec;
    //    userJobSpec.set_shell_command(Spec->Mapper);
    //    FOREACH (const auto& file, Files) {
    //        *userJobSpec.add_files() = *file.FetchResponse;
    //    }
    //    *JobSpecTemplate.MutableExtension(TUserJobSpec::user_job_spec) = userJobSpec;

    //    TMapJobSpec mapJobSpec;
    //    *mapJobSpec.mutable_output_transaction_id() = OutputTransaction->GetId().ToProto();
    //    FOREACH (const auto& table, OutputTables) {
    //        auto* outputSpec = mapJobSpec.add_output_specs();
    //        outputSpec->set_schema(table.Schema);
    //    }
    //    *JobSpecTemplate.MutableExtension(TMapJobSpec::map_job_spec) = mapJobSpec;

    //    JobSpecTemplate.set_io_config(SerializeToYson(Spec->JobIO));

    //    // TODO(babenko): stderr
    //}
};

IOperationControllerPtr CreateSortController(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
{
    auto spec = New<TSortOperationSpec>();
    try {
        spec->Load(~operation->GetSpec());
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing operation spec\n%s", ex.what());
    }

    return New<TSortController>(config, spec, host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


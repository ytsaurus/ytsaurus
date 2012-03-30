#include "stdafx.h"
#include "map_controller.h"
#include "operation_controller.h"
#include "operation_controller_detail.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"
#include "private.h"

#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/fluent.h>

#include <ytlib/misc/string.h>

#include <ytlib/transaction_server/transaction_ypath_proxy.h>

#include <ytlib/table_server/table_ypath_proxy.h>
#include <ytlib/table_client/table_reader.pb.h>

#include <ytlib/object_server/object_ypath_proxy.h>

#include <ytlib/file_server/file_ypath_proxy.h>

#include <ytlib/cypress/cypress_service_proxy.h>

#include <ytlib/chunk_server/public.h>
#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>

#include <ytlib/table_client/schema.h>

namespace NYT {
namespace NScheduler {

using namespace NProto;
using namespace NYTree;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NTableServer;
using namespace NTableServer::NProto;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NCypress;
using namespace NChunkServer;
using namespace NChunkHolder::NProto;
using namespace NFileServer;

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
        VERIFY_THREAD_AFFINITY(ControlThread);

        Spec = New<TMapOperationSpec>();
        try {
            Spec->Load(~Operation->GetSpec());
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing operation spec\n%s", ex.what());
        }
    }

    virtual TFuture<TVoid>::TPtr Prepare()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return StartAsyncPipeline(Host->GetBackgroundInvoker())
            ->Add(FromMethod(&TThis::StartPrimaryTransaction, MakeStrong(this)))
            ->Add(FromMethod(&TThis::OnPrimaryTransactionStarted, MakeStrong(this)))
            ->Add(FromMethod(&TThis::StartSeconaryTransactions, MakeStrong(this)))
            ->Add(FromMethod(&TThis::OnSecondaryTransactionsStarted, MakeStrong(this)))
            ->Add(FromMethod(&TThis::RequestInputs, MakeStrong(this)))
            ->Add(FromMethod(&TThis::OnInputsReceived, MakeStrong(this)))
            ->Add(FromMethod(&TThis::CompletePreparation, MakeStrong(this)))
            ->Run()
            ->Apply(FromMethod(&TThis::OnInitComplete, MakeStrong(this)));
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
            OutputTables[index].DoneChunkListIds.push_back(chunkListId);
        }

        JobCounter.Completed(1);
        ChunkCounter.Completed(jobInfo->ChunkIndexes.size());
        WeightCounter.Completed(jobInfo->Weight);

        RemoveJobInfo(job);

        DumpStatistics();

        if (JobCounter.GetRunning() == 0 && JobCounter.GetPending() == 0) {
            CompleteOperation();
        }
    }

    virtual void OnJobFailed(TJobPtr job)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Job %s failed\n%s",
            ~job->GetId().ToString(),
            ~TError::FromProto(job->Result().error()).ToString());

        auto jobInfo = GetJobInfo(job);

        ChunkPool->DeallocateChunks(jobInfo->ChunkIndexes);

        JobCounter.Failed(1);
        ChunkCounter.Failed(jobInfo->ChunkIndexes.size());
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


    virtual void OnOperationAborted()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        AbortOperation();
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
        i64 maxWeightPerJob = (pendingWeight + pendingJobs - 1) / pendingJobs;

        auto jobInfo = New<TJobInfo>();

        // Allocate chunks for the job.
        auto& chunkIndexes = jobInfo->ChunkIndexes;
        i64 allocatedWeight;
        int localCount;
        int remoteCount;
        ChunkPool->AllocateChunks(
            node->GetAddress(),
            maxWeightPerJob,
            &chunkIndexes,
            &allocatedWeight,
            &localCount,
            &remoteCount);

        LOG_DEBUG("Allocated %d chunks for node %s (AllocatedWeight: %" PRId64 ", MaxWeight: %" PRId64 ", LocalCount: %d, RemoteCount: %d)",
            static_cast<int>(chunkIndexes.size()),
            ~node->GetAddress(),
            allocatedWeight,
            maxWeightPerJob,
            localCount,
            remoteCount);

        YASSERT(!chunkIndexes.empty());

        FOREACH (int chunkIndex, chunkIndexes) {
            const auto& chunk = ChunkPool->GetChunk(chunkIndex);
            *mapJobSpec->mutable_input_spec()->add_chunks() = chunk;
        }

        FOREACH (auto& outputSpec, *mapJobSpec->mutable_output_specs()) {
            auto chunkListId = ChunkListPool->Extract();
            jobInfo->OutputChunkListIds.push_back(chunkListId);
            outputSpec.set_chunk_list_id(chunkListId.ToProto());
        }

        jobInfo->Weight = allocatedWeight;
        
        auto job = Host->CreateJob(
            Operation,
            node,
            jobSpec);

        PutJobInfo(job, jobInfo);

        JobCounter.Start(1);
        ChunkCounter.Start(chunkIndexes.size());
        WeightCounter.Start(allocatedWeight);

        DumpStatistics();

        jobsToStart->push_back(job);
    }

    virtual i64 GetPendingJobCount()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return JobCounter.GetPending();
    }

private:
    typedef TMapController TThis;

    TMapOperationSpecPtr Spec;

    // The primary transaction for the whole operation (nested inside operation's transaction).
    ITransaction::TPtr PrimaryTransaction;
    // The transaction for reading input tables (nested inside the primary one).
    // These tables are locked with Snapshot mode.
    ITransaction::TPtr InputTransaction;
    // The transaction for writing output tables (nested inside the primary one).
    // These tables are locked with Shared mode.
    ITransaction::TPtr OutputTransaction;

    // Input tables.
    struct TInputTable
    {
        TTableYPathProxy::TRspFetch::TPtr FetchResponse;
    };

    std::vector<TInputTable> InputTables;

    // Output tables.
    struct TOutputTable
    {
        TYson Schema;
        TChunkListId OutputChunkListId;
        std::vector<TChunkListId> DoneChunkListIds;
    };

    std::vector<TOutputTable> OutputTables;

    // Files.
    struct TFile
    {
        TFileYPathProxy::TRspFetch::TPtr FetchResponse;
    };

    std::vector<TFile> Files;

    // Running counters.
    TRunningCounter JobCounter;
    TRunningCounter ChunkCounter;
    TRunningCounter WeightCounter;

    // Size estimates.
    i64 TotalRowCount;
    i64 TotalDataSize;
    i64 TotalWeight;

    // Fixed during init time, used to compute job count.
    int ExecNodeCount;

    ::THolder<TChunkPool> ChunkPool;
    TChunkListPoolPtr ChunkListPool;

    // The template for starting new jobs.
    TJobSpec JobSpecTemplate;

    // Job scheduled so far.
    // TOOD(babenko): consider keeping this in job's attributes
    struct TJobInfo
        : public TIntrinsicRefCounted
    {
        // Chunk indexes assigned to this job.
        std::vector<int> ChunkIndexes;

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
        userJobSpec.set_shell_command(Spec->ShellCommand);
        FOREACH (const auto& file, Files) {
            *userJobSpec.add_files() = *file.FetchResponse;
        }
        *JobSpecTemplate.MutableExtension(TUserJobSpec::user_job_spec) = userJobSpec;

        TMapJobSpec mapJobSpec;
        mapJobSpec.set_input_transaction_id(InputTransaction->GetId().ToProto());
        mapJobSpec.set_output_transaction_id(OutputTransaction->GetId().ToProto());
        FOREACH (const auto& table, OutputTables) {
            auto* outputSpec = mapJobSpec.add_output_specs();
            outputSpec->set_schema(table.Schema);
        }
        *JobSpecTemplate.MutableExtension(TMapJobSpec::map_job_spec) = mapJobSpec;


        // TODO(babenko): stderr
    }

    void ReleaseChunkLists(const std::vector<TChunkListId>& ids)
    {
        auto batchReq = CypressProxy.ExecuteBatch();
        FOREACH (const auto& id, ids) {
            auto req = TTransactionYPathProxy::ReleaseObject();
            req->set_object_id(id.ToProto());
            batchReq->AddRequest(req);
        }
        // Fire-and-forget.
        // TODO(babenko): log result
        batchReq->Invoke();
    }

    // TODO(babenko): YPath and RPC responses currently share no base class.
    template <class TResponse>
    void CheckResponse(TResponse response, const Stroka& failureMessage) 
    {
        if (response->IsOK())
            return;

        ythrow yexception() << failureMessage + "\n" + response->GetError().ToString();
    }


    // Here comes the preparation pipeline.

    // Round 1:
    // - Start primary transaction.

    TCypressServiceProxy::TInvExecuteBatch::TPtr StartPrimaryTransaction(TVoid)
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        auto batchReq = CypressProxy.ExecuteBatch();

        {
            auto req = TTransactionYPathProxy::CreateObject(FromObjectId(Operation->GetTransactionId()));
            req->set_type(EObjectType::Transaction);
            batchReq->AddRequest(req, "start_primary_tx");
        }

        return batchReq->Invoke();
    }

    TVoid OnPrimaryTransactionStarted(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        CheckResponse(batchRsp, "Error creating primary transaction");

        {
            auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_primary_tx");
            CheckResponse(rsp, "Error creating primary transaction");
            auto id = TTransactionId::FromProto(rsp->object_id());
            LOG_INFO("Primary transaction is %s", ~id.ToString());
            PrimaryTransaction = Host->GetTransactionManager()->Attach(id);
        }

        return TVoid();
    }

    // Round 2:
    // - Start input transaction.
    // - Start output transaction.

    TCypressServiceProxy::TInvExecuteBatch::TPtr StartSeconaryTransactions(TVoid)
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        auto batchReq = CypressProxy.ExecuteBatch();

        {
            auto req = TTransactionYPathProxy::CreateObject(FromObjectId(PrimaryTransaction->GetId()));
            req->set_type(EObjectType::Transaction);
            batchReq->AddRequest(req, "start_input_tx");
        }

        {
            auto req = TTransactionYPathProxy::CreateObject(FromObjectId(PrimaryTransaction->GetId()));
            req->set_type(EObjectType::Transaction);
            batchReq->AddRequest(req, "start_output_tx");
        }

        return batchReq->Invoke();
    }

    TVoid OnSecondaryTransactionsStarted(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        CheckResponse(batchRsp, "Error creating secondary transactions");

        {
            auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_input_tx");
            CheckResponse(rsp, "Error creating input transaction");
            auto id = TTransactionId::FromProto(rsp->object_id());
            LOG_INFO("Input transaction is %s", ~id.ToString());
            InputTransaction = Host->GetTransactionManager()->Attach(id);
        }

        {
            auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_output_tx");
            CheckResponse(rsp, "Error creating output transaction");
            auto id = TTransactionId::FromProto(rsp->object_id());
            LOG_INFO("Output transaction is %s", ~id.ToString());
            OutputTransaction = Host->GetTransactionManager()->Attach(id);
        }

        return TVoid();
    }

    // Round 3: 
    // - Fetch input tables.
    // - Lock input tables.
    // - Lock output tables.
    // - Fetch files.
    // - Get output tables schemata.
    // - Get output chunk lists.

    TCypressServiceProxy::TInvExecuteBatch::TPtr RequestInputs(TVoid)
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        LOG_INFO("Requesting inputs");

        auto batchReq = CypressProxy.ExecuteBatch();

        FOREACH (const auto& path, Spec->In) {
            auto req = TTableYPathProxy::Fetch(WithTransaction(path, PrimaryTransaction->GetId()));
            req->set_fetch_holder_addresses(true);
            req->set_fetch_chunk_attributes(true);
            batchReq->AddRequest(req, "fetch_in_tables");
        }

        FOREACH (const auto& path, Spec->In) {
            auto req = TCypressYPathProxy::Lock(WithTransaction(path, InputTransaction->GetId()));
            req->set_mode(ELockMode::Snapshot);
            batchReq->AddRequest(req, "lock_in_tables");
        }

        FOREACH (const auto& path, Spec->Out) {
            auto req = TCypressYPathProxy::Lock(WithTransaction(path, OutputTransaction->GetId()));
            req->set_mode(ELockMode::Shared);
            batchReq->AddRequest(req, "lock_out_tables");
        }

        FOREACH (const auto& path, Spec->Out) {
            auto req = TYPathProxy::Get(CombineYPaths(
                WithTransaction(path, Operation->GetTransactionId()),
                "@schema"));
            batchReq->AddRequest(req, "get_out_tables_schemata");
        }

        FOREACH (const auto& path, Spec->Files) {
            auto req = TFileYPathProxy::Fetch(WithTransaction(path, Operation->GetTransactionId()));
            batchReq->AddRequest(req, "fetch_files");
        }

        FOREACH (const auto& path, Spec->Out) {
            auto req = TTableYPathProxy::GetChunkListForUpdate(WithTransaction(path, OutputTransaction->GetId()));
            batchReq->AddRequest(req, "get_chunk_lists");
        }

        return batchReq->Invoke();
    }

    TVoid OnInputsReceived(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        CheckResponse(batchRsp, "Error requesting inputs");

        {
            InputTables.resize(Spec->In.size());
            TotalRowCount = 0;
            auto fetchInTablesRsps = batchRsp->GetResponses<TTableYPathProxy::TRspFetch>("fetch_in_tables");
            auto lockInTablesRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_in_tables");
            for (int index = 0; index < static_cast<int>(Spec->In.size()); ++index) {
                auto lockInTableRsp = lockInTablesRsps[index];
                CheckResponse(lockInTableRsp, "Error locking input table");

                auto fetchInTableRsp = fetchInTablesRsps[index];
                CheckResponse(fetchInTableRsp, "Error fetching input input table");

                auto& table = InputTables[index];
                table.FetchResponse = fetchInTableRsp;
            }
        }

        {
            OutputTables.resize(Spec->Out.size());
            auto lockOutTablesRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_out_tables");
            auto getChunkListsRsps = batchRsp->GetResponses<TTableYPathProxy::TRspGetChunkListForUpdate>("get_chunk_lists");
            auto getOutTablesSchemataRsps = batchRsp->GetResponses<TTableYPathProxy::TRspGetChunkListForUpdate>("get_out_tables_schemata");
            for (int index = 0; index < static_cast<int>(Spec->Out.size()); ++index) {
                auto lockOutTablesRsp = lockOutTablesRsps[index];
                CheckResponse(lockOutTablesRsp, "Error fetching input input table");

                auto getChunkListRsp = getChunkListsRsps[index];
                CheckResponse(getChunkListRsp, "Error getting output chunk list");

                auto getOutTableSchemaRsp = getOutTablesSchemataRsps[index];

                auto& table = OutputTables[index];
                table.OutputChunkListId = TChunkListId::FromProto(getChunkListRsp->chunk_list_id());
                // TODO(babenko): fill output schema
                table.Schema = "{}";
            }
        }

        {
            auto fetchFilesRsps = batchRsp->GetResponses<TFileYPathProxy::TRspFetch>("fetch_files");
            FOREACH (auto fetchFileRsp, fetchFilesRsps) {
                CheckResponse(fetchFileRsp, "Error fetching files");

                TFile file;
                file.FetchResponse = fetchFileRsp;
                Files.push_back(file);
            }
        }

        LOG_INFO("Inputs received");

        return TVoid();
    }

    // Round 4.
    // - Compute various row counts, sizes, weights etc.
    // - Construct input chunks and put them into the pool.
    // - Choose job count.
    // - Preallocate chunk lists.
    // - Initialize running counters.

    TVoid CompletePreparation(TVoid)
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        LOG_INFO("Completing preparation");

        ChunkPool.Reset(new TChunkPool(Operation));
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
                YVERIFY(DeserializeFromProtobuf(&chunkAttributes, TRef::FromString(inputChunk.chunk_attributes())));

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

                ChunkPool->PutChunk(inputChunk, weight);

                ++totalChunkCount;
            }
        }

        // Choose job count.
        i64 totalJobCount = Spec->JobCount.Get(ExecNodeCount);
        totalJobCount = std::min(totalJobCount, static_cast<i64>(totalChunkCount));
        totalJobCount = std::min(totalJobCount, TotalRowCount);

        // Check for empty inputs.
        if (totalJobCount == 0) {
            LOG_INFO("Empty input");
            CompleteOperation();
            return TVoid();
        }

        // Allocate some initial chunk lists.
        // TOOD(babenko): make configurable
        ChunkListPool->Allocate(OutputTables.size() * totalJobCount + 10);

        // Init running counters.
        JobCounter.Init(totalJobCount);
        ChunkCounter.Init(totalChunkCount);
        WeightCounter.Init(TotalWeight);

        InitJobSpecTemplate();

        LOG_INFO("Preparation completed (RowCount: %" PRId64 ", DataSize: %" PRId64 ", Weight: %" PRId64 ", ChunkCount: %" PRId64 ", JobCount: %" PRId64 ")",
            TotalRowCount,
            TotalDataSize,
            TotalWeight,
            totalChunkCount,
            totalJobCount);

        return TVoid();
    }


    // Here comes the completion pipeline.

    void CompleteOperation()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_INFO("Completing operation");

        auto this_ = MakeStrong(this);
        StartAsyncPipeline(Host->GetBackgroundInvoker())
            ->Add(FromMethod(&TThis::CommitOutputs, MakeStrong(this)))
            ->Add(FromMethod(&TThis::OnOutputsCommitted, MakeStrong(this)))
            ->Run()
            ->Subscribe(FromFunctor([=] (TValueOrError<TVoid> result) {
                if (result.IsOK()) {
                    this_->OnOperationCompleted();
                } else {
                    this_->OnOperationFailed(result);
                }
            }));

    }

    // Round 1.
    // - Attach output chunk lists.
    // - Commit input transaction.
    // - Commit output transaction.
    // - Commit primary transaction.

    TCypressServiceProxy::TInvExecuteBatch::TPtr CommitOutputs(TVoid)
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        LOG_INFO("Committing %" PRId64 " output chunks", ChunkCounter.GetDone());

        // We don't need pings any longer, detach the transactions.
        PrimaryTransaction->Detach();
        InputTransaction->Detach();
        OutputTransaction->Detach();

        auto batchReq = CypressProxy.ExecuteBatch();

        FOREACH (const auto& table, OutputTables) {
            auto req = TChunkListYPathProxy::Attach(FromObjectId(table.OutputChunkListId));
            FOREACH (const auto& childId, table.DoneChunkListIds) {
                req->add_children_ids(childId.ToProto());
            }
            batchReq->AddRequest(req, "attach_chunk_lists");
        }

        {
            auto req = TTransactionYPathProxy::Commit(FromObjectId(InputTransaction->GetId()));
            batchReq->AddRequest(req, "commit_input_tx");
        }

        {
            auto req = TTransactionYPathProxy::Commit(FromObjectId(OutputTransaction->GetId()));
            batchReq->AddRequest(req, "commit_output_tx");
        }

        {
            auto req = TTransactionYPathProxy::Commit(FromObjectId(PrimaryTransaction->GetId()));
            batchReq->AddRequest(req, "commit_primary_tx");
        }

        return batchReq->Invoke();
    }

    TVoid OnOutputsCommitted(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        CheckResponse(batchRsp, "Error committing outputs");

        {
            auto rsps = batchRsp->GetResponses("attach_chunk_lists");
            FOREACH (auto rsp, rsps) {
                CheckResponse(rsp, "Error attaching chunk lists");
            }
        }

        {
            auto rsp = batchRsp->GetResponse("commit_input_tx");
            CheckResponse(rsp, "Error committing input transaction");
        }

        {
            auto rsp = batchRsp->GetResponse("commit_output_tx");
            CheckResponse(rsp, "Error committing output transaction");
        }

        {
            auto rsp = batchRsp->GetResponse("commit_primary_tx");
            CheckResponse(rsp, "Error committing primary transaction");
        }

        LOG_INFO("Outputs committed");

        OnOperationCompleted();

        return TVoid();
    }


    // Abort is not a pipeline really :)

    virtual void AbortOperation()
    {
        TOperationControllerBase::AbortOperation();

        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Aborting operation");

        AbortTransactions();

        LOG_INFO("Operation aborted");
    }

    void AbortTransactions()
    {
        LOG_INFO("Aborting transactions")

        if (PrimaryTransaction) {
            // This method is async, no problem in using it here.
            PrimaryTransaction->Abort();
        }

        // No need to abort the others.
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


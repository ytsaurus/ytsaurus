#include "stdafx.h"
#include "operation_controller_detail.h"
#include "private.h"

#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>

namespace NYT {
namespace NScheduler {

using namespace NCypress;
using namespace NTransactionServer;
using namespace NFileServer;
using namespace NTableServer;
using namespace NChunkServer;
using namespace NYTree;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////

TIntrusivePtr< TAsyncPipeline<TVoid> > StartAsyncPipeline(IInvoker::TPtr invoker)
{
    return New< TAsyncPipeline<TVoid> >(
        invoker,
        BIND([=] () -> TIntrusivePtr< TFuture< TValueOrError<TVoid> > > {
            return MakeFuture(TValueOrError<TVoid>(TVoid()));
    }));
}

////////////////////////////////////////////////////////////////////

TChunkListPool::TChunkListPool(
    NRpc::IChannel::TPtr masterChannel,
    IInvoker::TPtr controlInvoker,
    TOperationPtr operation,
    const TTransactionId& transactionId)
    : MasterChannel(masterChannel)
    , ControlInvoker(controlInvoker)
    , Operation(operation)
    , TransactionId(transactionId)
    , Logger(OperationsLogger)
    , RequestInProgress(false)
{
    Logger.AddTag(Sprintf("OperationId: %s", ~operation->GetOperationId().ToString()));
}

int TChunkListPool::GetSize() const
{
    return static_cast<int>(Ids.size());
}

NYT::NChunkServer::TChunkListId TChunkListPool::Extract()
{
    YASSERT(!Ids.empty());
    auto id = Ids.back();
    Ids.pop_back();

    LOG_DEBUG("Extracted chunk list %s from the pool, %d remaining",
        ~id.ToString(),
        static_cast<int>(Ids.size()));

    return id;
}

void TChunkListPool::Allocate(int count)
{
    if (RequestInProgress) {
        LOG_DEBUG("Cannot allocate more chunk lists, another request is in progress");
        return;
    }

    LOG_INFO("Allocating %d chunk lists for pool", count);

    TCypressServiceProxy cypressProxy(MasterChannel);
    auto batchReq = cypressProxy.ExecuteBatch();

    for (int index = 0; index < count; ++index) {
        auto req = TTransactionYPathProxy::CreateObject(FromObjectId(TransactionId));
        req->set_type(EObjectType::ChunkList);
        batchReq->AddRequest(req);
    }

    batchReq->Invoke()->Subscribe(
        BIND(&TChunkListPool::OnChunkListsCreated, MakeWeak(this))
        .Via(ControlInvoker));

    RequestInProgress = true;
}

void TChunkListPool::OnChunkListsCreated(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
{
    YASSERT(RequestInProgress);
    RequestInProgress = false;

    if (!batchRsp->IsOK()) {
        LOG_ERROR("Error allocating chunk lists\n%s", ~batchRsp->GetError().ToString());
        // TODO(babenko): backoff time?
        return;
    }

    LOG_INFO("Chunk lists allocated");

    YASSERT(Ids.empty());

    FOREACH (auto rsp, batchRsp->GetResponses<TTransactionYPathProxy::TRspCreateObject>()) {
        Ids.push_back(TChunkListId::FromProto(rsp->object_id()));
    }
}

////////////////////////////////////////////////////////////////////

TRunningCounter::TRunningCounter()
    : Total_(-1)
    , Running_(-1)
    , Done_(-1)
    , Pending_(-1)
    , Failed_(-1)
{ }

void TRunningCounter::Init(i64 total)
{
    Total_ = total;
    Running_ = 0;
    Done_ = 0;
    Pending_ = total;
    Failed_ = 0;
}

i64 TRunningCounter::GetTotal() const
{
    return Total_;
}

i64 TRunningCounter::GetRunning() const
{
    return Running_;
}

i64 TRunningCounter::GetDone() const
{
    return Done_;
}

i64 TRunningCounter::GetPending() const
{
    return Pending_;
}

i64 TRunningCounter::GetFailed() const
{
    return Failed_;
}

void TRunningCounter::Start(i64 count)
{
    YASSERT(Pending_ >= count);
    Running_ += count;
    Pending_ -= count;
}

void TRunningCounter::Completed(i64 count)
{
    YASSERT(Running_ >= count);
    Running_ -= count;
    Done_ += count;
}

void TRunningCounter::Failed(i64 count)
{
    YASSERT(Running_ >= count);
    Running_ -= count;
    Pending_ += count;
    Failed_ += count;
}

Stroka ToString(const TRunningCounter& counter)
{
    return Sprintf("T: %" PRId64 ", R: %" PRId64 ", D: %" PRId64 ", P: %" PRId64 ", F: %" PRId64,
        counter.GetTotal(),
        counter.GetRunning(),
        counter.GetDone(),
        counter.GetPending(),
        counter.GetFailed());
}

////////////////////////////////////////////////////////////////////

TOperationControllerBase::TOperationControllerBase(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
    : Config(config)
    , Host(host)
    , Operation(operation)
    , CypressProxy(host->GetMasterChannel())
    , Logger(OperationsLogger)
    , Active(false)
    , Running(false)
{
    Logger.AddTag(Sprintf("OperationId: %s", ~operation->GetOperationId().ToString()));
}

void TOperationControllerBase::Initialize()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    
    LOG_INFO("Initializing operation");

    Active = true;
    ExecNodeCount = Host->GetExecNodeCount();

    try {
        DoInitialize();
    } catch (const std::exception& ex) {
        LOG_INFO("Operation has failed to initialize\n%s", ex.what());
        Active = false;
        throw;
    }

    LOG_INFO("Operation initialized");
}

TFuture<TVoid>::TPtr TOperationControllerBase::Prepare()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto this_ = MakeStrong(this);
    return StartAsyncPipeline(Host->GetBackgroundInvoker())
        ->Add(BIND(&TThis::StartPrimaryTransaction, MakeStrong(this)))
        ->Add(BIND(&TThis::OnPrimaryTransactionStarted, MakeStrong(this)))
        ->Add(BIND(&TThis::StartSeconaryTransactions, MakeStrong(this)))
        ->Add(BIND(&TThis::OnSecondaryTransactionsStarted, MakeStrong(this)))
        ->Add(BIND(&TThis::RequestInputs, MakeStrong(this)))
        ->Add(BIND(&TThis::OnInputsReceived, MakeStrong(this)))
        ->Add(BIND(&TThis::CompletePreparation, MakeStrong(this)))
        ->Run()
        ->Apply(BIND([=] (TValueOrError<TVoid> result) -> TFuture<TVoid>::TPtr {
            if (result.IsOK()) {
                if (this_->Active) {
                    this_->Running = true;
                }
                return MakeFuture(TVoid());
            } else {
                LOG_WARNING("Operation preparation failed\n%s", ~result.ToString());
                this_->Active = false;
                this_->Host->OnOperationFailed(this_->Operation, result);
                return New< TFuture<TVoid> >();
            }
        }));
}

TFuture<TVoid>::TPtr TOperationControllerBase::Revive()
{
    try {
        Initialize();
    } catch (const std::exception& ex) {
        FailOperation(TError("Operation has failed to initialize\n%s",
            ex.what()));
        // This promise is never fulfilled.
        return New< TFuture<TVoid> >();
    }
    return Prepare();
}

void TOperationControllerBase::OnJobRunning(TJobPtr job)
{
    LOG_DEBUG("Job %s is running", ~job->GetId().ToString());
}

void TOperationControllerBase::OnJobCompleted(TJobPtr job)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Job %s completed\n%s",
        ~job->GetId().ToString(),
        ~TError::FromProto(job->Result().error()).ToString());

    JobCounter.Completed(1);

    auto jobInProgress = GetJobHandlers(job);
    jobInProgress->OnCompleted.Run();
    
    RemoveJobHandlers(job);

    DumpProgress();

    if (JobCounter.GetRunning() == 0 && !HasPendingJobs()) {
        FinalizeOperation();
    }
}

void TOperationControllerBase::OnJobFailed(TJobPtr job)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Job %s failed\n%s",
        ~job->GetId().ToString(),
        ~TError::FromProto(job->Result().error()).ToString());

    JobCounter.Failed(1);

    auto jobInProgress = GetJobHandlers(job);
    jobInProgress->OnFailed.Run();

    RemoveJobHandlers(job);

    DumpProgress();

    if (JobCounter.GetFailed() > Config->MaxFailedJobCount) {
        FailOperation(TError("%d jobs failed, aborting operation",
            JobCounter.GetFailed()));
    }
}

void TOperationControllerBase::OnOperationAborted()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    AbortOperation();
}

TJobPtr TOperationControllerBase::ScheduleJob(TExecNodePtr node)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
 
    if (!Running) {
        LOG_DEBUG("Operation is not running, scheduling request ignored");
        return NULL;
    }

    if (!HasPendingJobs()) {
        LOG_DEBUG("No pending jobs left, scheduling request ignored");
        return NULL;
    }

    auto job = DoScheduleJob(node);
    if (job) {
        LOG_INFO("Scheduled job %s", ~job->GetId().ToString());
        JobCounter.Start(1);
        DumpProgress();
    }

    return job;
}

i64 TOperationControllerBase::GetPendingJobCount()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return JobCounter.GetPending();
}

void TOperationControllerBase::FailOperation(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    LOG_INFO("Operation failed\n%s", ~error.ToString());

    Running = false;
    Active = false;

    Host->OnOperationFailed(Operation, error);
}

void TOperationControllerBase::AbortOperation()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Aborting operation");

    AbortTransactions();
    Running = false;
    Active = false;

    LOG_INFO("Operation aborted");
}

void TOperationControllerBase::AbortTransactions()
{
    LOG_INFO("Aborting transactions")

    if (PrimaryTransaction) {
        // This method is async, no problem in using it here.
        PrimaryTransaction->Abort();
    }

    // No need to abort the others.
}

void TOperationControllerBase::FinalizeOperation()
{
    VERIFY_THREAD_AFFINITY_ANY();

    LOG_INFO("Finalizing operation");

    Running = false;

    auto this_ = MakeStrong(this);
    StartAsyncPipeline(Host->GetBackgroundInvoker())
        ->Add(BIND(&TThis::CommitOutputs, MakeStrong(this)))
        ->Add(BIND(&TThis::OnOutputsCommitted, MakeStrong(this)))
        ->Run()
        ->Subscribe(BIND([=] (TValueOrError<TVoid> result) {
            Active = false;
            if (result.IsOK()) {
                LOG_INFO("Operation finalized and completed");
                this_->Host->OnOperationCompleted(this_->Operation);
            } else {
                LOG_WARNING("Operation has failed to finalize\n%s", ~result.ToString());
                this_->Host->OnOperationFailed(this_->Operation, result);
            }
    }));
}

TCypressServiceProxy::TInvExecuteBatch::TPtr TOperationControllerBase::CommitOutputs(TVoid)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Committing outputs");

    auto batchReq = CypressProxy.ExecuteBatch();

    FOREACH (const auto& table, OutputTables) {
        auto req = TChunkListYPathProxy::Attach(FromObjectId(table.OutputChunkListId));
        FOREACH (const auto& childId, table.OutputChildrenIds) {
            *req->add_children_ids() = childId.ToProto();
        }
        batchReq->AddRequest(req, "attach_chunk_trees");
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

    // We don't need pings any longer, detach the transactions.
    PrimaryTransaction->Detach();
    InputTransaction->Detach();
    OutputTransaction->Detach();

    return batchReq->Invoke();
}

TVoid TOperationControllerBase::OnOutputsCommitted(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    CheckResponse(batchRsp, "Error committing outputs");

    {
        auto rsps = batchRsp->GetResponses("attach_chunk_trees");
        FOREACH (auto rsp, rsps) {
            CheckResponse(rsp, "Error attaching chunk trees");
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

    return TVoid();
}

TCypressServiceProxy::TInvExecuteBatch::TPtr TOperationControllerBase::StartPrimaryTransaction(TVoid)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Starting primary transaction");

    auto batchReq = CypressProxy.ExecuteBatch();

    {
        auto req = TTransactionYPathProxy::CreateObject(FromObjectId(Operation->GetTransactionId()));
        req->set_type(EObjectType::Transaction);
        batchReq->AddRequest(req, "start_primary_tx");
    }

    return batchReq->Invoke();
}

TVoid TOperationControllerBase::OnPrimaryTransactionStarted(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    CheckResponse(batchRsp, "Error starting primary transaction");

    {
        auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_primary_tx");
        CheckResponse(rsp, "Error starting primary transaction");
        auto id = TTransactionId::FromProto(rsp->object_id());
        LOG_INFO("Primary transaction is %s", ~id.ToString());
        PrimaryTransaction = Host->GetTransactionManager()->Attach(id);
    }

    return TVoid();
}

TCypressServiceProxy::TInvExecuteBatch::TPtr TOperationControllerBase::StartSeconaryTransactions(TVoid)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Starting secondary transactions");

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

TVoid TOperationControllerBase::OnSecondaryTransactionsStarted(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    CheckResponse(batchRsp, "Error starting secondary transactions");

    {
        auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_input_tx");
        CheckResponse(rsp, "Error starting input transaction");
        auto id = TTransactionId::FromProto(rsp->object_id());
        LOG_INFO("Input transaction is %s", ~id.ToString());
        InputTransaction = Host->GetTransactionManager()->Attach(id);
    }

    {
        auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_output_tx");
        CheckResponse(rsp, "Error starting output transaction");
        auto id = TTransactionId::FromProto(rsp->object_id());
        LOG_INFO("Output transaction is %s", ~id.ToString());
        OutputTransaction = Host->GetTransactionManager()->Attach(id);
    }

    return TVoid();
}

TCypressServiceProxy::TInvExecuteBatch::TPtr TOperationControllerBase::RequestInputs(TVoid)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Requesting inputs");

    auto batchReq = CypressProxy.ExecuteBatch();

    auto inPaths = GetInputTablePaths();
    auto outPaths = GetOutputTablePaths();
    auto filePaths = GetFilePaths();

    // Lock everything first (fail-fast).
    FOREACH (const auto& path, inPaths) {
        TInputTable table;
        table.Path = path;
        InputTables.push_back(table);

        auto req = TCypressYPathProxy::Lock(WithTransaction(path, InputTransaction->GetId()));
        req->set_mode(ELockMode::Snapshot);
        batchReq->AddRequest(req, "lock_in_tables");
    }

    FOREACH (const auto& path, outPaths) {
        TOutputTable table;
        table.Path = path;
        OutputTables.push_back(table);

        auto req = TCypressYPathProxy::Lock(WithTransaction(path, OutputTransaction->GetId()));
        req->set_mode(ELockMode::Shared);
        batchReq->AddRequest(req, "lock_out_tables");
    }

    // Do the rest next.
    FOREACH (const auto& path, inPaths) {
        auto req = TTableYPathProxy::Fetch(WithTransaction(path, PrimaryTransaction->GetId()));
        req->set_fetch_holder_addresses(true);
        req->set_fetch_chunk_attributes(true);
        batchReq->AddRequest(req, "fetch_in_tables");
    }

    FOREACH (const auto& path, outPaths) {
        {
            auto req = TYPathProxy::Get(CombineYPaths(
                WithTransaction(path, Operation->GetTransactionId()),
                "@schema"));
            batchReq->AddRequest(req, "get_out_tables_schemata");
        }
        {
            auto req = TTableYPathProxy::GetChunkListForUpdate(WithTransaction(path, OutputTransaction->GetId()));
            batchReq->AddRequest(req, "get_chunk_lists");
        }
    }

    FOREACH (const auto& path, filePaths) {
        TFile file;
        file.Path = path;
        Files.push_back(file);

        auto req = TFileYPathProxy::Fetch(WithTransaction(path, Operation->GetTransactionId()));
        batchReq->AddRequest(req, "fetch_files");
    }

    return batchReq->Invoke();
}

TVoid TOperationControllerBase::OnInputsReceived(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    CheckResponse(batchRsp, "Error requesting inputs");

    {
        auto fetchInTablesRsps = batchRsp->GetResponses<TTableYPathProxy::TRspFetch>("fetch_in_tables");
        auto lockInTablesRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_in_tables");
        for (int index = 0; index < static_cast<int>(InputTables.size()); ++index) {
            auto lockInTableRsp = lockInTablesRsps[index];
            CheckResponse(lockInTableRsp, "Error locking input table");

            auto fetchInTableRsp = fetchInTablesRsps[index];
            CheckResponse(fetchInTableRsp, "Error fetching input input table");

            auto& table = InputTables[index];
            table.FetchResponse = fetchInTableRsp;
        }
    }

    {
        auto lockOutTablesRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_out_tables");
        auto getChunkListsRsps = batchRsp->GetResponses<TTableYPathProxy::TRspGetChunkListForUpdate>("get_chunk_lists");
        auto getOutTablesSchemataRsps = batchRsp->GetResponses<TTableYPathProxy::TRspGetChunkListForUpdate>("get_out_tables_schemata");
        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
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
        for (int index = 0; index < static_cast<int>(Files.size()); ++index) {
            auto fetchFileRsp = fetchFilesRsps[index];
            CheckResponse(fetchFileRsp, "Error fetching files");

            auto& file = Files[index];
            file.FetchResponse = fetchFileRsp;
        }
    }

    LOG_INFO("Inputs received");

    return TVoid();
}

TVoid TOperationControllerBase::CompletePreparation(TVoid)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Completing preparation");

    ChunkListPool = New<TChunkListPool>(
        Host->GetMasterChannel(),
        Host->GetControlInvoker(),
        Operation,
        PrimaryTransaction->GetId());

    DoCompletePreparation();

    if (Active) {
        LOG_INFO("Preparation completed");
    }

    return TVoid();
}

void TOperationControllerBase::ReleaseChunkList(const TChunkListId& id)
{
    std::vector<TChunkListId> ids;
    ids.push_back(id);
    ReleaseChunkLists(ids);
}

void TOperationControllerBase::ReleaseChunkLists(const std::vector<TChunkListId>& ids)
{
    auto batchReq = CypressProxy.ExecuteBatch();
    FOREACH (const auto& id, ids) {
        auto req = TTransactionYPathProxy::ReleaseObject();
        *req->mutable_object_id() = id.ToProto();
        batchReq->AddRequest(req);
    }

    // Fire-and-forget.
    // The subscriber is only needed to log the outcome.
    batchReq->Invoke()->Subscribe(BIND(&TThis::OnChunkListsReleased, MakeStrong(this)));
}

void TOperationControllerBase::OnChunkListsReleased(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
{
    if (batchRsp->IsOK()) {
        LOG_INFO("Chunk lists released successfully");
    } else {
        LOG_WARNING("Error releasing chunk lists\n%s", ~batchRsp->GetError().ToString());
    }
}

bool TOperationControllerBase::CheckChunkListsPoolSize(int count)
{
    if (ChunkListPool->GetSize() >= count) {
        return true;
    }

    int allocateCount = count * Config->ChunkListAllocationMultiplier;
    LOG_DEBUG("Insufficient pooled chunk lists left, allocating another %d", allocateCount);
    ChunkListPool->Allocate(allocateCount);
    return false;
}

i64 TOperationControllerBase::GetJobWeightThreshold(i64 pendingJobs, i64 pendingWeight)
{
    YASSERT(pendingJobs > 0);
    YASSERT(pendingWeight > 0);
    return (i64) ceil((double) pendingWeight / pendingJobs);
}

TJobPtr TOperationControllerBase::CreateJob(
    TOperationPtr operation,
    TExecNodePtr node,
    const NProto::TJobSpec& spec,
    TClosure onCompleted,
    TClosure onFailed)
{
    auto job = Host->CreateJob(operation, node, spec);
    auto handlers = New<TJobHandlers>();
    handlers->OnCompleted = onCompleted;
    handlers->OnFailed = onFailed;
    YVERIFY(JobHandlers.insert(MakePair(job, handlers)).second);
    return job;
}

TOperationControllerBase::TJobHandlersPtr TOperationControllerBase::GetJobHandlers(TJobPtr job)
{
    auto it = JobHandlers.find(job);
    YASSERT(it != JobHandlers.end());
    return it->second;
}

void TOperationControllerBase::RemoveJobHandlers(TJobPtr job)
{
    YVERIFY(JobHandlers.erase(job) == 1);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


#include "stdafx.h"
#include "operation_controller_detail.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"

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

TOperationControllerBase::TOperationControllerBase(IOperationHost* host, TOperation* operation)
    : Host(host)
    , Operation(operation)
    , CypressProxy(host->GetMasterChannel())
    , Logger(OperationsLogger)
{
    Logger.AddTag(Sprintf("OperationId: %s", ~operation->GetOperationId().ToString()));
}

void TOperationControllerBase::Initialize()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    
    ExecNodeCount = Host->GetExecNodeCount();
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
                return MakeFuture(TVoid());
            } else {
                this_->OnOperationFailed(result);
                return New< TFuture<TVoid> >();
            }
        }));
}

TFuture<TVoid>::TPtr TOperationControllerBase::Revive()
{
    try {
        Initialize();
    } catch (const std::exception& ex) {
        OnOperationFailed(TError("Operation has failed to initialize\n%s",
            ex.what()));
        // This promise is never fulfilled.
        return New< TFuture<TVoid> >();
    }
    return Prepare();
}

void TOperationControllerBase::OnJobRunning(TJobPtr job)
{
    UNUSED(job);
}

void TOperationControllerBase::OnJobCompleted(TJobPtr job)
{
    UNUSED(job);
}

void TOperationControllerBase::OnJobFailed(TJobPtr job)
{
    UNUSED(job);
}

void TOperationControllerBase::OnOperationAborted()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    AbortOperation();
}

void TOperationControllerBase::ScheduleJobs(
    TExecNodePtr node,
    std::vector<TJobPtr>* jobsToStart,
    std::vector<TJobPtr>* jobsToAbort)
{
    UNUSED(node);
    UNUSED(jobsToStart);
    UNUSED(jobsToAbort);
}

void TOperationControllerBase::OnOperationFailed(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    LOG_INFO("Operation failed\n%s", ~error.ToString());

    Host->OnOperationFailed(Operation, error);
}

void TOperationControllerBase::OnOperationCompleted()
{
    VERIFY_THREAD_AFFINITY_ANY();

    LOG_INFO("Operation completed");

    Host->OnOperationCompleted(Operation);
}

void TOperationControllerBase::AbortOperation()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Aborting operation");

    AbortTransactions();

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

void TOperationControllerBase::CompleteOperation()
{
    VERIFY_THREAD_AFFINITY_ANY();

    LOG_INFO("Completing operation");

    auto this_ = MakeStrong(this);
    StartAsyncPipeline(Host->GetBackgroundInvoker())
        ->Add(BIND(&TThis::CommitOutputs, MakeStrong(this)))
        ->Add(BIND(&TThis::OnOutputsCommitted, MakeStrong(this)))
        ->Run()
        ->Subscribe(BIND([=] (TValueOrError<TVoid> result) {
            if (result.IsOK()) {
                this_->OnOperationCompleted();
            } else {
                this_->OnOperationFailed(result);
            }
    }));
}

TCypressServiceProxy::TInvExecuteBatch::TPtr TOperationControllerBase::CommitOutputs(TVoid)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    auto batchReq = CypressProxy.ExecuteBatch();

    FOREACH (const auto& table, OutputTables) {
        auto req = TChunkListYPathProxy::Attach(FromObjectId(table.OutputChunkListId));
        FOREACH (const auto& childId, table.ChunkTreeIds) {
            req->add_children_ids(childId.ToProto());
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

    OnOperationCompleted();

    return TVoid();
}

TCypressServiceProxy::TInvExecuteBatch::TPtr TOperationControllerBase::StartPrimaryTransaction(TVoid)
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

TVoid TOperationControllerBase::OnPrimaryTransactionStarted(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
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

TCypressServiceProxy::TInvExecuteBatch::TPtr TOperationControllerBase::StartSeconaryTransactions(TVoid)
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

TVoid TOperationControllerBase::OnSecondaryTransactionsStarted(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
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

    DoCompletePreparation();

    return TVoid();
}

void TOperationControllerBase::ReleaseChunkLists(const std::vector<TChunkListId>& ids)
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

////////////////////////////////////////////////////////////////////

TChunkPool::TChunkPool(TOperationPtr operation)
    : Logger(OperationsLogger)
{
    Logger.AddTag(Sprintf("OperationId: %s", ~operation->GetOperationId().ToString()));
}

int TChunkPool::PutChunk(const TInputChunk& chunk, i64 weight)
{
    YASSERT(weight > 0);

    TChunkInfo info;
    info.Chunk = chunk;
    info.Weight = weight;
    int index = static_cast<int>(ChunkInfos.size());
    ChunkInfos.push_back(info);
    RegisterChunk(index);
    return index;
}

const TInputChunk& TChunkPool::GetChunk(int index)
{
    return ChunkInfos[index].Chunk;
}

void TChunkPool::AllocateChunks(const Stroka& address, i64 maxWeight, std::vector<int>* indexes, i64* allocatedWeight, int* localCount, int* remoteCount)
{
    *allocatedWeight = 0;

    // Take local chunks first.
    *localCount = 0;
    auto addressIt = AddressToIndexSet.find(address);
    if (addressIt != AddressToIndexSet.end()) {
        const auto& localIndexes = addressIt->second;
        FOREACH (int chunkIndex, localIndexes) {
            if (*allocatedWeight >= maxWeight) {
                break;
            }
            indexes->push_back(chunkIndex);
            ++*localCount;
            *allocatedWeight += ChunkInfos[chunkIndex].Weight;
        }
    }

    // Unregister taken local chunks.
    // We have to do this right away, otherwise we risk getting same chunks
    // in the next phase.
    for (int i = 0; i < *localCount; ++i) {
        UnregisterChunk((*indexes)[i]);
    }

    // Take remote chunks.
    *remoteCount = 0;
    FOREACH (int chunkIndex, UnallocatedIndexes) {
        if (*allocatedWeight >= maxWeight) {
            break;
        }
        indexes->push_back(chunkIndex);
        ++*remoteCount;
        *allocatedWeight += ChunkInfos[chunkIndex].Weight;
    }

    // Unregister taken remote chunks.
    for (int i = *localCount; i < *localCount + *remoteCount; ++i) {
        UnregisterChunk((*indexes)[i]);
    }

    LOG_DEBUG("Extracted chunks [%s] from the pool", ~JoinToString(*indexes));
}

void TChunkPool::DeallocateChunks(const std::vector<int>& indexes)
{
    FOREACH (auto index, indexes) {
        RegisterChunk(index);
    }

    LOG_DEBUG("Chunks [%s] are back in the pool", ~JoinToString(indexes));
}

void TChunkPool::RegisterChunk(int index)
{
    const auto& info = ChunkInfos[index];
    FOREACH (const auto& address, info.Chunk.holder_addresses()) {
        YVERIFY(AddressToIndexSet[address].insert(index).second);
    }
    YVERIFY(UnallocatedIndexes.insert(index).second);
}

void TChunkPool::UnregisterChunk(int index)
{
    const auto& info = ChunkInfos[index];
    FOREACH (const auto& address, info.Chunk.holder_addresses()) {
        YVERIFY(AddressToIndexSet[address].erase(index) == 1);
    }
    YVERIFY(UnallocatedIndexes.erase(index) == 1);
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

TIntrusivePtr< TAsyncPipeline<TVoid> > StartAsyncPipeline(IInvoker::TPtr invoker)
{
    return New< TAsyncPipeline<TVoid> >(
        invoker,
        BIND([=] () -> TIntrusivePtr< TFuture< TValueOrError<TVoid> > > {
            return MakeFuture(TValueOrError<TVoid>(TVoid()));
    }));
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

} // namespace NScheduler
} // namespace NYT


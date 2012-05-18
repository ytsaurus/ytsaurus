#include "stdafx.h"
#include "operation_controller_detail.h"
#include "private.h"
#include "chunk_list_pool.h"

#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>
#include <ytlib/object_server/object_ypath_proxy.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/ytree/fluent.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NCypress;
using namespace NTransactionServer;
using namespace NFileServer;
using namespace NTableServer;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NYTree;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////

TOperationControllerBase::TOperationControllerBase(
    TSchedulerConfigPtr config,
    IOperationHost* host,
    TOperation* operation)
    : Config(config)
    , Host(host)
    , Operation(operation)
    , ObjectProxy(host->GetMasterChannel())
    , Logger(OperationLogger)
    , Active(false)
    , Running(false)
    , ExecNodeCount(-1)
    , RunningJobCount(0)
    , CompletedJobCount(0)
    , FailedJobCount(0)
{
    Logger.AddTag(Sprintf("OperationId: %s", ~operation->GetOperationId().ToString()));
}

void TOperationControllerBase::Initialize()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    
    LOG_INFO("Initializing operation");

    ExecNodeCount = Host->GetExecNodeCount();
    if (ExecNodeCount == 0) {
        ythrow yexception() << "No online exec nodes";
    }

    FOREACH (const auto& path, GetInputTablePaths()) {
        TInputTable table;
        table.Path = path;
        InputTables.push_back(table);
    }

    FOREACH (const auto& path, GetOutputTablePaths()) {
        TOutputTable table;
        table.Path = path;
        OutputTables.push_back(table);
    }

    FOREACH (const auto& path, GetFilePaths()) {
        TFile file;
        file.Path = path;
        Files.push_back(file);
    }

    try {
        CustomInitialize();
    } catch (const std::exception& ex) {
        LOG_INFO("Operation has failed to initialize\n%s", ex.what());
        Active = false;
        throw;
    }

    Active = true;

    LOG_INFO("Operation initialized");
}

void TOperationControllerBase::CustomInitialize()
{ }

TFuture<void> TOperationControllerBase::Prepare()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto this_ = MakeStrong(this);
    auto pipeline = StartAsyncPipeline(Host->GetBackgroundInvoker())
        ->Add(BIND(&TThis::StartPrimaryTransaction, MakeStrong(this)))
        ->Add(BIND(&TThis::OnPrimaryTransactionStarted, MakeStrong(this)))
        ->Add(BIND(&TThis::StartSeconaryTransactions, MakeStrong(this)))
        ->Add(BIND(&TThis::OnSecondaryTransactionsStarted, MakeStrong(this)))
        ->Add(BIND(&TThis::GetObjectIds, MakeStrong(this)))
        ->Add(BIND(&TThis::OnObjectIdsReceived, MakeStrong(this)))
        ->Add(BIND(&TThis::RequestInputs, MakeStrong(this)))
        ->Add(BIND(&TThis::OnInputsReceived, MakeStrong(this)))
        ->Add(BIND(&TThis::CompletePreparation, MakeStrong(this)));
     pipeline = CustomizePreparationPipeline(pipeline);
     return pipeline
        ->Add(BIND(&TThis::OnPreparationCompleted, MakeStrong(this)))
        ->Run()
        .Apply(BIND([=] (TValueOrError<void> result) -> TFuture<void> {
            if (result.IsOK()) {
                if (this_->Active) {
                    this_->Running = true;
                }
                return MakeFuture();
            } else {
                LOG_WARNING("Operation preparation failed\n%s", ~result.ToString());
                this_->Active = false;
                this_->Host->OnOperationFailed(this_->Operation, result);
                // This promise is never fulfilled.
                return NewPromise<void>();
            }
        }));
}

TFuture<void> TOperationControllerBase::Revive()
{
    try {
        Initialize();
    } catch (const std::exception& ex) {
        FailOperation(TError("Operation has failed to initialize\n%s",
            ex.what()));
        // This promise is never fulfilled.
        return NewPromise<void>();
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

    --RunningJobCount;
    ++CompletedJobCount;

    auto jip = GetJobInProgress(job);
    jip->OnCompleted.Run();
    
    RemoveJobInProgress(job);

    LogProgress();

    if (RunningJobCount == 0 && GetPendingJobCount() == 0) {
        FinalizeOperation();
    }
}

void TOperationControllerBase::OnJobFailed(TJobPtr job)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Job %s failed\n%s",
        ~job->GetId().ToString(),
        ~TError::FromProto(job->Result().error()).ToString());

    --RunningJobCount;
    ++FailedJobCount;

    auto jip = GetJobInProgress(job);
    jip->OnFailed.Run();

    RemoveJobInProgress(job);

    LogProgress();

    if (FailedJobCount >= Config->FailedJobsLimit) {
        FailOperation(TError("Failed jobs limit %d has been reached",
            Config->FailedJobsLimit));
    }

    FOREACH (const auto& chunkId, job->Result().failed_chunk_ids()) {
        OnChunkFailed(TChunkId::FromProto(chunkId));
    }
}

void TOperationControllerBase::OnChunkFailed(const TChunkId& chunkId)
{
    if (InputChunkIds.find(chunkId) == InputChunkIds.end()) {
        LOG_WARNING("Intermediate chunk %s has failed", ~chunkId.ToString());
        OnIntermediateChunkFailed(chunkId);
    } else {
        LOG_WARNING("Input chunk %s has failed", ~chunkId.ToString());
        OnInputChunkFailed(chunkId);
    }
}

void TOperationControllerBase::OnInputChunkFailed(const TChunkId& chunkId)
{
    FailOperation(TError("Unable to read input chunk %s", ~chunkId.ToString()));
}

void TOperationControllerBase::OnIntermediateChunkFailed(const TChunkId& chunkId)
{
    FailOperation(TError("Unable to read intermediate chunk %s", ~chunkId.ToString()));
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

    if (GetPendingJobCount() == 0) {
        LOG_DEBUG("No pending jobs left, scheduling request ignored");
        return NULL;
    }

    auto job = DoScheduleJob(node);
    if (job) {
        LOG_INFO("Scheduled job %s", ~job->GetId().ToString());
        ++RunningJobCount;
        LogProgress();
    }

    return job;
}

void TOperationControllerBase::FailOperation(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!Active)
        return;

    LOG_INFO("Operation failed\n%s", ~error.ToString());

    Running = false;
    Active = false;

    Host->OnOperationFailed(Operation, error);
}

void TOperationControllerBase::AbortOperation()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Aborting operation");

    Running = false;
    Active = false;

    AbortTransactions();

    LOG_INFO("Operation aborted");
}

void TOperationControllerBase::AbortTransactions()
{
    LOG_INFO("Aborting transactions")

    if (PrimaryTransaction) {
        // The call is async.
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
        .Subscribe(BIND([=] (TValueOrError<void> result) {
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

TObjectServiceProxy::TInvExecuteBatch TOperationControllerBase::CommitOutputs()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Committing outputs");

    auto batchReq = ObjectProxy.ExecuteBatch();

    FOREACH (const auto& table, OutputTables) {
        auto ypath = FromObjectId(table.ObjectId);
        {
            auto req = TChunkListYPathProxy::Attach(FromObjectId(table.OutputChunkListId));
            FOREACH (const auto& childId, table.PartitionTreeIds) {
                if (childId != NullChunkTreeId) {
                    *req->add_children_ids() = childId.ToProto();
                }
            }
            batchReq->AddRequest(req, "attach_out");
        }
        if (table.SetSorted) {
            auto req = TTableYPathProxy::SetSorted(WithTransaction(ypath, OutputTransaction->GetId()));
            ToProto(req->mutable_key_columns(), table.KeyColumns);
            batchReq->AddRequest(req, "set_out_sorted");
        }
    }

    CommitCustomOutputs(batchReq);

    {
        auto req = TTransactionYPathProxy::Commit(FromObjectId(InputTransaction->GetId()));
        batchReq->AddRequest(req, "commit_in_tx");
    }

    {
        auto req = TTransactionYPathProxy::Commit(FromObjectId(OutputTransaction->GetId()));
        batchReq->AddRequest(req, "commit_out_tx");
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

void TOperationControllerBase::OnOutputsCommitted(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    CheckResponse(batchRsp, "Error committing outputs");

    {
        auto rsps = batchRsp->GetResponses("attach_out");
        FOREACH (auto rsp, rsps) {
            CheckResponse(rsp, "Error attaching chunk trees");
        }
    }

    OnCustomOutputsCommitted(batchRsp);

    {
        auto rsp = batchRsp->GetResponse("commit_in_tx");
        CheckResponse(rsp, "Error committing input transaction");
    }

    {
        auto rsp = batchRsp->GetResponse("commit_out_tx");
        CheckResponse(rsp, "Error committing output transaction");
    }

    {
        auto rsp = batchRsp->GetResponse("commit_primary_tx");
        CheckResponse(rsp, "Error committing primary transaction");
    }

    LOG_INFO("Outputs committed");
}

void TOperationControllerBase::CommitCustomOutputs(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
{
    UNUSED(batchReq);
}

void TOperationControllerBase::OnCustomOutputsCommitted(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    UNUSED(batchRsp);
}

TObjectServiceProxy::TInvExecuteBatch TOperationControllerBase::StartPrimaryTransaction()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Starting primary transaction");

    auto batchReq = ObjectProxy.ExecuteBatch();

    {
        auto req = TTransactionYPathProxy::CreateObject(
            Operation->GetTransactionId() == NullTransactionId
            ? RootTransactionPath
            : FromObjectId(Operation->GetTransactionId()));
        req->set_type(EObjectType::Transaction);
        batchReq->AddRequest(req, "start_primary_tx");
    }

    return batchReq->Invoke();
}

void TOperationControllerBase::OnPrimaryTransactionStarted(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
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
}

TObjectServiceProxy::TInvExecuteBatch TOperationControllerBase::StartSeconaryTransactions()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Starting secondary transactions");

    auto batchReq = ObjectProxy.ExecuteBatch();

    {
        auto req = TTransactionYPathProxy::CreateObject(FromObjectId(PrimaryTransaction->GetId()));
        req->set_type(EObjectType::Transaction);
        batchReq->AddRequest(req, "start_in_tx");
    }

    {
        auto req = TTransactionYPathProxy::CreateObject(FromObjectId(PrimaryTransaction->GetId()));
        req->set_type(EObjectType::Transaction);
        batchReq->AddRequest(req, "start_out_tx");
    }

    return batchReq->Invoke();
}

void TOperationControllerBase::OnSecondaryTransactionsStarted(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    CheckResponse(batchRsp, "Error starting secondary transactions");

    {
        auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_in_tx");
        CheckResponse(rsp, "Error starting input transaction");
        auto id = TTransactionId::FromProto(rsp->object_id());
        LOG_INFO("Input transaction is %s", ~id.ToString());
        InputTransaction = Host->GetTransactionManager()->Attach(id);
    }

    {
        auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_out_tx");
        CheckResponse(rsp, "Error starting output transaction");
        auto id = TTransactionId::FromProto(rsp->object_id());
        LOG_INFO("Output transaction is %s", ~id.ToString());
        OutputTransaction = Host->GetTransactionManager()->Attach(id);
    }
}

TObjectServiceProxy::TInvExecuteBatch TOperationControllerBase::GetObjectIds()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Getting object ids");

    auto batchReq = ObjectProxy.ExecuteBatch();

    FOREACH (const auto& table, InputTables) {
        auto req = TObjectYPathProxy::GetId(WithTransaction(table.Path, InputTransaction->GetId()));
        req->set_allow_nonempty_path_suffix(true);
        batchReq->AddRequest(req, "get_in_id");
    }

    FOREACH (const auto& table, OutputTables) {
        auto req = TObjectYPathProxy::GetId(WithTransaction(table.Path, InputTransaction->GetId()));
        // TODO(babenko): should we allow nonempty path suffixes for output tables as well?
        batchReq->AddRequest(req, "get_out_id");
    }

    return batchReq->Invoke();
}

void TOperationControllerBase::OnObjectIdsReceived(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    CheckResponse(batchRsp, "Error getting object ids");

    {
        auto getInIdRsps = batchRsp->GetResponses<TObjectYPathProxy::TRspGetId>("get_in_id");
        for (int index = 0; index < static_cast<int>(InputTables.size()); ++index) {
            auto& table = InputTables[index];
            {
                auto rsp = getInIdRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error getting id for input table %s", ~table.Path));
                table.ObjectId = TObjectId::FromProto(rsp->object_id());
            }
        }
    }

    {
        auto getOutIdRsps = batchRsp->GetResponses<TObjectYPathProxy::TRspGetId>("get_out_id");
        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            auto& table = OutputTables[index];
            {
                auto rsp = getOutIdRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error getting id for output table %s", ~table.Path));
                table.ObjectId = TObjectId::FromProto(rsp->object_id());
            }
        }
    }

    LOG_INFO("Object ids received");
}

TObjectServiceProxy::TInvExecuteBatch TOperationControllerBase::RequestInputs()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Requesting inputs");

    auto batchReq = ObjectProxy.ExecuteBatch();

    FOREACH (const auto& table, InputTables) {
        auto ypath = FromObjectId(table.ObjectId);
        {
            auto req = TCypressYPathProxy::Lock(WithTransaction(ypath, InputTransaction->GetId()));
            req->set_mode(ELockMode::Snapshot);
            batchReq->AddRequest(req, "lock_in");
        }
        {
            // NB: Use table.Path not ypath here, otherwise path suffix is ignored.
            auto req = TTableYPathProxy::Fetch(WithTransaction(table.Path, PrimaryTransaction->GetId()));
            req->set_fetch_holder_addresses(true);
            req->set_fetch_all_meta_extensions(true);
            req->set_negate(table.NegateFetch);
            batchReq->AddRequest(req, "fetch_in");
        }
        {
            auto req = TYPathProxy::Get(WithTransaction(ypath, PrimaryTransaction->GetId()) + "/@sorted");
            batchReq->AddRequest(req, "get_in_sorted");
        }
        {
            auto req = TYPathProxy::Get(WithTransaction(ypath, PrimaryTransaction->GetId()) + "/@key_columns");
            batchReq->AddRequest(req, "get_in_key_columns");
        }
    }

    FOREACH (const auto& table, OutputTables) {
        auto ypath = FromObjectId(table.ObjectId);
        {
            auto req = TCypressYPathProxy::Lock(WithTransaction(ypath, OutputTransaction->GetId()));
            req->set_mode(ELockMode::Shared);
            batchReq->AddRequest(req, "lock_out");
        }
        {
            auto req = TYPathProxy::Get(WithTransaction(ypath, Operation->GetTransactionId()) + "/@channels");
            batchReq->AddRequest(req, "get_out_channels");
        }
        {
            auto req = TTableYPathProxy::GetChunkListForUpdate(WithTransaction(ypath, OutputTransaction->GetId()));
            batchReq->AddRequest(req, "get_out_chunk_list");
        }
        {
            auto req = TYPathProxy::Get(WithTransaction(ypath, OutputTransaction->GetId()) + "/@row_count");
            batchReq->AddRequest(req, "get_out_row_count");
        }
    }

    FOREACH (const auto& file, Files) {
        auto ypath = file.Path;
        {
            auto req = TFileYPathProxy::Fetch(WithTransaction(ypath, Operation->GetTransactionId()));
            batchReq->AddRequest(req, "fetch_files");
        }
    }

    CustomRequestInputs(batchReq);

    return batchReq->Invoke();
}

void TOperationControllerBase::OnInputsReceived(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    CheckResponse(batchRsp, "Error requesting inputs");

    {
        auto fetchInRsps = batchRsp->GetResponses<TTableYPathProxy::TRspFetch>("fetch_in");
        auto lockInRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_in");
        auto getInSortedRsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_in_sorted");
        auto getInKeyColumns = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_in_key_columnns");
        for (int index = 0; index < static_cast<int>(InputTables.size()); ++index) {
            auto& table = InputTables[index];
            {
                auto rsp = lockInRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error locking input table %s", ~table.Path));
            }
            {
                auto rsp = fetchInRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error fetching input input table %s", ~table.Path));
                table.FetchResponse = rsp;
                FOREACH (const auto& chunk, rsp->chunks()) {
                    auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
                    if (chunk.holder_addresses_size() == 0) {
                        ythrow yexception() << Sprintf("Chunk %s in input table %s is lost",
                            ~chunkId.ToString(),
                            ~table.Path);
                    }
                    InputChunkIds.insert(chunkId);
                }
            }
            {
                auto rsp = getInSortedRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error getting \"sorted\" attribute for input table %s", ~table.Path));
                table.Sorted = DeserializeFromYson<bool>(rsp->value());
            }
            if (table.Sorted) {
                auto rsp = getInKeyColumns[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error getting \"key_columns\" attribute for input table %s", ~table.Path));
                table.KeyColumns = DeserializeFromYson< yvector<Stroka> >(rsp->value());
            }
        }
    }

    {
        auto lockOutRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_out");
        auto getOutChunkListRsps = batchRsp->GetResponses<TTableYPathProxy::TRspGetChunkListForUpdate>("get_out_chunk_list");
        auto getOutChannelsRsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_out_channels");
        auto getOutRowCountRsps = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_out_row_count");
        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            auto& table = OutputTables[index];
            {
                auto rsp = lockOutRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error locking output table %s", ~table.Path));
            }
            {
                auto rsp = getOutChunkListRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error getting output chunk list for table %s", ~table.Path));
                table.OutputChunkListId = TChunkListId::FromProto(rsp->chunk_list_id());
            }
            {
                auto rsp = getOutChannelsRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error getting channels for output table %s", ~table.Path));
                table.Channels = rsp->value();
            }
            {
                auto rsp = getOutRowCountRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error getting \"row_count\" attribute for output table %s", ~table.Path));
                table.InitialRowCount = DeserializeFromYson<i64>(rsp->value());
            }
        }
    }

    {
        auto fetchFilesRsps = batchRsp->GetResponses<TFileYPathProxy::TRspFetch>("fetch_files");
        for (int index = 0; index < static_cast<int>(Files.size()); ++index) {
            auto& file = Files[index];
            {
                auto rsp = fetchFilesRsps[index];
                CheckResponse(rsp, "Error fetching files");
                file.FetchResponse = rsp;
            }
        }
    }

    OnCustomInputsRecieved(batchRsp);

    LOG_INFO("Inputs received");
}

void TOperationControllerBase::CustomRequestInputs(TObjectServiceProxy::TReqExecuteBatchPtr batchReq)
{
    UNUSED(batchReq);
}

void TOperationControllerBase::OnCustomInputsRecieved(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    UNUSED(batchRsp);
}

void TOperationControllerBase::CompletePreparation()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Completing preparation");

    ChunkListPool = New<TChunkListPool>(
        Host->GetMasterChannel(),
        Host->GetControlInvoker(),
        Operation,
        PrimaryTransaction->GetId());
}

void TOperationControllerBase::OnPreparationCompleted()
{
    if (!Active)
        return;

    LOG_INFO("Preparation completed");
}

TAsyncPipeline<void>::TPtr TOperationControllerBase::CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline)
{
    return pipeline;
}

void TOperationControllerBase::ReleaseChunkList(const TChunkListId& id)
{
    std::vector<TChunkListId> ids;
    ids.push_back(id);
    ReleaseChunkLists(ids);
}

void TOperationControllerBase::ReleaseChunkLists(const std::vector<TChunkListId>& ids)
{
    auto batchReq = ObjectProxy.ExecuteBatch();
    FOREACH (const auto& id, ids) {
        auto req = TTransactionYPathProxy::ReleaseObject();
        *req->mutable_object_id() = id.ToProto();
        batchReq->AddRequest(req);
    }

    // Fire-and-forget.
    // The subscriber is only needed to log the outcome.
    batchReq->Invoke().Subscribe(
        BIND(&TThis::OnChunkListsReleased, MakeStrong(this)));
}

void TOperationControllerBase::OnChunkListsReleased(TObjectServiceProxy::TRspExecuteBatchPtr batchRsp)
{
    if (batchRsp->IsOK()) {
        LOG_INFO("Chunk lists released successfully");
    } else {
        LOG_WARNING("Error releasing chunk lists\n%s", ~batchRsp->GetError().ToString());
    }
}

void TOperationControllerBase::CheckInputTablesSorted()
{
    FOREACH (const auto& table, InputTables) {
        if (!table.Sorted) {
            ythrow yexception() << Sprintf("Input table %s is not sorted", ~table.Path);
        }
    }
}

void TOperationControllerBase::CheckOutputTablesEmpty()
{
    FOREACH (const auto& table, OutputTables) {
        if (table.InitialRowCount > 0) {
            ythrow yexception() << Sprintf("Output table %s is not empty", ~table.Path);
        }
    }
}

std::vector<Stroka> TOperationControllerBase::GetInputKeyColumns()
{
    YASSERT(!InputTables.empty());
    for (int index = 1; index < static_cast<int>(InputTables.size()); ++index) {
        if (InputTables[0].KeyColumns != InputTables[index].KeyColumns) {
            ythrow yexception() << Sprintf("Key columns mismatch in input tables %s and %s",
                ~InputTables[0].Path,
                ~InputTables[index].Path);
        }
    }
    return InputTables[0].KeyColumns;
}

void TOperationControllerBase::SetOutputTablesSorted(const std::vector<Stroka>& keyColumns)
{
    FOREACH (auto& table, OutputTables) {
        table.SetSorted = true;
        table.KeyColumns = keyColumns;
    }
}

bool TOperationControllerBase::CheckChunkListsPoolSize(int minSize)
{
    if (ChunkListPool->GetSize() >= minSize) {
        return true;
    }

    int allocateCount = minSize * Config->ChunkListAllocationMultiplier;
    LOG_DEBUG("Insufficient pooled chunk lists left, allocating another %d", allocateCount);
    ChunkListPool->Allocate(allocateCount);
    return false;
}

i64 TOperationControllerBase::GetJobWeightThreshold(int pendingJobCount, i64 pendingWeight)
{
    YASSERT(pendingJobCount > 0);
    YASSERT(pendingWeight > 0);
    return static_cast<i64>(std::ceil((double) pendingWeight / pendingJobCount));
}

TOperationControllerBase::TJobInProgressPtr TOperationControllerBase::GetJobInProgress(TJobPtr job)
{
    auto it = JobsInProgress.find(job);
    YASSERT(it != JobsInProgress.end());
    return it->second;
}

void TOperationControllerBase::RemoveJobInProgress(TJobPtr job)
{
    YVERIFY(JobsInProgress.erase(job) == 1);
}

void TOperationControllerBase::BuildProgressYson(IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("jobs").BeginMap()
                .Item("total").Scalar(CompletedJobCount + RunningJobCount + GetPendingJobCount())
                .Item("pending").Scalar(GetPendingJobCount())
                .Item("running").Scalar(RunningJobCount)
                .Item("completed").Scalar(CompletedJobCount)
                .Item("failed").Scalar(FailedJobCount)
            .EndMap()
            .Do(BIND(&TThis::DoGetProgress, Unretained(this)))
        .EndMap();
}

void TOperationControllerBase::BuildResultYson(IYsonConsumer* consumer)
{
    auto error = TError::FromProto(Operation->Result().error());
    // TODO(babenko): refactor
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("error").Do(BIND(&TError::ToYson, &error))
        .EndMap();
}

std::vector<TYPath> TOperationControllerBase::GetFilePaths()
{
    return std::vector<TYPath>();
}

int TOperationControllerBase::GetJobCount(
    i64 totalWeight,
    i64 weightPerJob,
    TNullable<int> configJobCount,
    int chunkCount)
{
    int result = configJobCount
        ? configJobCount.Get()
        : static_cast<int>(std::ceil((double) totalWeight / weightPerJob));
    result = std::min(result, chunkCount);
    YASSERT(result > 0);
    return result;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


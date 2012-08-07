#include "stdafx.h"
#include "operation_controller_detail.h"
#include "private.h"
#include "chunk_list_pool.h"
#include "chunk_pool.h"
#include "job_resources.h"

#include <ytlib/transaction_client/transaction.h>

#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>

#include <ytlib/object_server/object_ypath_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/convert.h>

#include <ytlib/formats/format.h>

#include <ytlib/chunk_holder/chunk_meta_extensions.h>

#include <ytlib/table_client/key.h>

#include <cmath>

namespace NYT {
namespace NScheduler {

using namespace NCypressClient;
using namespace NTransactionServer;
using namespace NFileServer;
using namespace NTableServer;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NYTree;
using namespace NFormats;
using namespace NTableClient;
using namespace NJobProxy;

////////////////////////////////////////////////////////////////////

TOperationControllerBase::TTask::TTask(TOperationControllerBase* controller)
    : Controller(controller)
    , Logger(Controller->Logger)
{ }

i64 TOperationControllerBase::TTask::GetLocality(const Stroka& address) const
{
    return ChunkPool->GetLocality(address);
}

int TOperationControllerBase::TTask::GetPriority() const
{
    return 0;
}

void TOperationControllerBase::TTask::AddStripe(TChunkStripePtr stripe)
{
    ChunkPool->Add(stripe);
    AddInputLocalityHint(stripe);
    AddPendingHint();
}

void TOperationControllerBase::TTask::AddStripes(const std::vector<TChunkStripePtr>& stripes)
{
    FOREACH (auto stripe, stripes) {
        AddStripe(stripe);
    }
}

TJobPtr TOperationControllerBase::TTask::ScheduleJob(TExecNodePtr node)
{
    using ::ToString;

    if (!Controller->CheckAvailableChunkLists(GetChunkListCountPerJob())) {
        return NULL;
    }

    auto jip = New<TJobInProgress>(this);
    auto dataSizeThreshold = GetJobDataSizeThreshold();
    jip->PoolResult = ChunkPool->Extract(node->GetAddress(), dataSizeThreshold );

    LOG_DEBUG("Chunks extracted (TotalCount: %d, LocalCount: %d, ExtractedDataSize: %" PRId64 ", DataSizeThreshold: %s)",
        jip->PoolResult->TotalChunkCount,
        jip->PoolResult->LocalChunkCount,
        jip->PoolResult->TotalDataSize,
        ~ToString(dataSizeThreshold));

    NProto::TJobSpec jobSpec;
    BuildJobSpec(jip, &jobSpec);

    *jobSpec.mutable_resource_utilization() = GetRequestedResourcesForJip(jip);

    LOG_DEBUG("Job prepared (Utilization: {%s})",
        ~FormatResources(jobSpec.resource_utilization()));

    jip->Job = Controller->Host->CreateJob(
        EJobType(jobSpec.type()),
        Controller->Operation,
        node);
    jip->Job->Spec().Swap(&jobSpec);

    Controller->RegisterJobInProgress(jip);

    OnJobStarted(jip);

    return jip->Job;
}

bool TOperationControllerBase::TTask::IsPending() const
{
    return ChunkPool->IsPending();
}

bool TOperationControllerBase::TTask::IsCompleted() const
{
    return ChunkPool->IsCompleted();
}

const TProgressCounter& TOperationControllerBase::TTask::DataSizeCounter() const
{
    return ChunkPool->DataSizeCounter();
}

const TProgressCounter& TOperationControllerBase::TTask::ChunkCounter() const
{
    return ChunkPool->ChunkCounter();
}

void TOperationControllerBase::TTask::OnJobStarted(TJobInProgressPtr jip)
{
    UNUSED(jip);
}

void TOperationControllerBase::TTask::OnJobCompleted(TJobInProgressPtr jip)
{
    ChunkPool->OnCompleted(jip->PoolResult);
}

void TOperationControllerBase::TTask::OnJobFailed(TJobInProgressPtr jip)
{
    ChunkPool->OnFailed(jip->PoolResult);

    Controller->ReleaseChunkLists(jip->ChunkListIds);

    FOREACH (const auto& stripe, jip->PoolResult->Stripes) {
        AddInputLocalityHint(stripe);
    }
    AddPendingHint();
}

void TOperationControllerBase::TTask::OnTaskCompleted()
{
    LOG_DEBUG("Task completed (Task: %s)", ~GetId());
}

void TOperationControllerBase::TTask::AddPendingHint()
{
    Controller->AddTaskPendingHint(this);
}

void TOperationControllerBase::TTask::AddInputLocalityHint(TChunkStripePtr stripe)
{
    Controller->AddTaskLocalityHint(this, stripe);
}

i64 TOperationControllerBase::TTask::GetJobDataSizeThresholdGeneric(int pendingJobCount, i64 pendingDataSize)
{
    return static_cast<i64>(std::ceil((double) pendingDataSize / pendingJobCount));
}

void TOperationControllerBase::TTask::AddSequentialInputSpec(
    NScheduler::NProto::TJobSpec* jobSpec,
    TJobInProgressPtr jip)
{
    auto* inputSpec = jobSpec->add_input_specs();
    FOREACH (const auto& stripe, jip->PoolResult->Stripes) {
        AddInputChunks(inputSpec, stripe);
    }
}

void TOperationControllerBase::TTask::AddParallelInputSpec(
    NScheduler::NProto::TJobSpec* jobSpec,
    TJobInProgressPtr jip)
{
    FOREACH (const auto& stripe, jip->PoolResult->Stripes) {
        auto* inputSpec = jobSpec->add_input_specs();
        AddInputChunks(inputSpec, stripe);
    }
}

void TOperationControllerBase::TTask::AddTabularOutputSpec(
    NScheduler::NProto::TJobSpec* jobSpec,
    TJobInProgressPtr jip,
    int tableIndex)
{
    const auto& table = Controller->OutputTables[tableIndex];
    auto* outputSpec = jobSpec->add_output_specs();
    outputSpec->set_channels(table.Channels.Data());
    auto chunkListId = Controller->GetFreshChunkList();
    jip->ChunkListIds.push_back(chunkListId);
    *outputSpec->mutable_chunk_list_id() = chunkListId.ToProto();
}

void TOperationControllerBase::TTask::AddInputChunks(
    NScheduler::NProto::TTableInputSpec* inputSpec,
    TChunkStripePtr stripe)
{
    FOREACH (const auto& weightedChunk, stripe->Chunks) {
        auto* inputChunk = inputSpec->add_chunks();
        *inputChunk = *weightedChunk.InputChunk;
        inputChunk->set_uncompressed_data_size(weightedChunk.DataSizeOverride);
        inputChunk->set_row_count(weightedChunk.RowCountOverride);
    }
}

NProto::TNodeResources TOperationControllerBase::TTask::GetRequestedResourcesForJip(TJobInProgressPtr jip) const
{
    UNUSED(jip);
    return GetMinRequestedResources();
}

bool TOperationControllerBase::TTask::HasEnoughResources(TExecNodePtr node) const
{
    return NScheduler::HasEnoughResources(
        node->ResourceUtilization(),
        GetMinRequestedResources(),
        node->ResourceLimits());
}

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

    ExecNodeCount = static_cast<int>(Host->GetExecNodes().size());

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
        TUserFile file;
        file.Path = path;
        Files.push_back(file);
    }

    try {
        DoInitialize();
    } catch (const std::exception& ex) {
        LOG_INFO("Operation has failed to initialize\n%s", ex.what());
        Active = false;
        throw;
    }

    Active = true;

    LOG_INFO("Operation initialized");
}

void TOperationControllerBase::DoInitialize()
{ }

void TOperationControllerBase::DoGetProgress(IYsonConsumer* consumer)
{
    UNUSED(consumer);
}

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
        OnOperationFailed(TError("Operation has failed to initialize\n%s",
            ex.what()));
        // This promise is never fulfilled.
        return NewPromise<void>();
    }
    return Prepare();
}

TFuture<void> TOperationControllerBase::Commit()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YCHECK(Active);

    LOG_INFO("Committing operation");

    auto this_ = MakeStrong(this);
    return StartAsyncPipeline(Host->GetBackgroundInvoker())
        ->Add(BIND(&TThis::CommitOutputs, MakeStrong(this)))
        ->Add(BIND(&TThis::OnOutputsCommitted, MakeStrong(this)))
        ->Run()
        .Apply(BIND([=] (TValueOrError<void> result) -> TFuture<void> {
            Active = false;
            if (result.IsOK()) {
                LOG_INFO("Operation committed");
                return MakeFuture();
            } else {
                LOG_WARNING("Operation has failed to commit\n%s", ~result.ToString());
                this_->Host->OnOperationFailed(this_->Operation, result);
                return NewPromise<void>();
            }
        }));
}

void TOperationControllerBase::OnJobRunning(TJobPtr job)
{
    UNUSED(job);
}

void TOperationControllerBase::OnJobCompleted(TJobPtr job)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    --RunningJobCount;
    ++CompletedJobCount;

    auto jip = GetJobInProgress(job);
    jip->Task->OnJobCompleted(jip);
    
    RemoveJobInProgress(job);

    LogProgress();

    if (jip->Task->IsCompleted()) {
        jip->Task->OnTaskCompleted();
    }

    if (RunningJobCount == 0 && GetPendingJobCount() == 0) {
        OnOperationCompleted();
    }
}

void TOperationControllerBase::OnJobFailed(TJobPtr job)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    --RunningJobCount;
    ++FailedJobCount;

    auto jip = GetJobInProgress(job);
    jip->Task->OnJobFailed(jip);

    RemoveJobInProgress(job);

    LogProgress();

    if (FailedJobCount >= Config->FailedJobsLimit) {
        OnOperationFailed(TError("Failed jobs limit %d has been reached",
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
    OnOperationFailed(TError("Unable to read input chunk %s", ~chunkId.ToString()));
}

void TOperationControllerBase::OnIntermediateChunkFailed(const TChunkId& chunkId)
{
    OnOperationFailed(TError("Unable to read intermediate chunk %s", ~chunkId.ToString()));
}

void TOperationControllerBase::Abort()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_INFO("Aborting operation");

    Running = false;
    Active = false;

    AbortTransactions();

    LOG_INFO("Operation aborted");
}

TJobPtr TOperationControllerBase::ScheduleJob(TExecNodePtr node)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
 
    if (!Running) {
        LOG_TRACE("Operation is not running, scheduling request ignored");
        return NULL;
    }

    if (GetPendingJobCount() == 0) {
        LOG_TRACE("No pending jobs left, scheduling request ignored");
        return NULL;
    }

    // Make a course check to see if the node has enough resources.
    if (!HasEnoughResources(
            node->ResourceUtilization(),
            GetMinRequestedResources(),
            node->ResourceLimits()))
    {
        return NULL;
    }

    auto job = DoScheduleJob(node);
    if (job) {
        ++RunningJobCount;
        LogProgress();
    }

    return job;
}

void TOperationControllerBase::AddTaskPendingHint(TTaskPtr task)
{
    if (task->GetPendingJobCount() > 0) {
        if (PendingTasks.insert(task).second) {
            LOG_DEBUG("Task pending hint added (Task: %s)",
                ~task->GetId());
        }
    }
}

void TOperationControllerBase::AddTaskLocalityHint(TTaskPtr task, const Stroka& address)
{
    if (AddressToLocalTasks[address].insert(task).second) {
        LOG_TRACE("Task locality hint added (Task: %s, Address: %s)",
            ~task->GetId(),
            ~address);
    }
}

void TOperationControllerBase::AddTaskLocalityHint(TTaskPtr task, TChunkStripePtr stripe)
{
    FOREACH (const auto& chunk, stripe->Chunks) {
        const auto& inputChunk = chunk.InputChunk;
        FOREACH (const auto& address, inputChunk->node_addresses()) {
            AddTaskLocalityHint(task, address);
        }
    }
}

TJobPtr TOperationControllerBase::DoScheduleJob(TExecNodePtr node)
{
    // Examine all (potentially) pending tasks, pick one with the best priority.
    // Perform lazy cleanup of pending hints.

    auto address = node->GetAddress();

    TTaskPtr bestTask;

    typedef std::pair<int, i64> TWeightedLocality;
    TWeightedLocality bestLocality(-1, -1);

    auto now = TInstant::Now();
    auto it = PendingTasks.begin();
    while (it != PendingTasks.end()) {
        auto jt = it++;
        auto candidate = *jt;

        if (candidate->GetPendingJobCount() == 0 ) {
            LOG_DEBUG("Task pending hint removed (Task: %s)", ~candidate->GetId());
            PendingTasks.erase(jt);
            continue;
        }

        if (!candidate->HasEnoughResources(node)) {
            continue;
        }

        TWeightedLocality locality(candidate->GetPriority(), candidate->GetLocality(address));
        if (locality.second < 0) {
            continue;
        }

        if (locality.second == 0) {
            if (!candidate->GetNonLocalRequestTime()) {
                candidate->SetNonLocalRequestTime(now);
            }
            if (candidate->GetNonLocalRequestTime().Get() + candidate->GetLocalityTimeout() > now) {
                continue;
            }
        }

        if (locality > bestLocality) {
            bestTask = candidate;
            bestLocality = locality;
        }
    }

    if (!bestTask) {
        return NULL;
    }

    LOG_DEBUG("Scheduling a %s job (Task: %s, Address: %s, Priority: %d, Locality: %" PRId64 ")",
        bestLocality.second == 0 ? "non-local" : "local",
        ~bestTask->GetId(),
        ~node->GetAddress(),
        bestLocality.first,
        bestLocality.second);

    auto job = bestTask->ScheduleJob(node);
    if (!job) {
        return NULL;
    }

    if (bestLocality.second > 0) {
        bestTask->SetNonLocalRequestTime(Null);
    }

    return job;
}

int TOperationControllerBase::GetPendingJobCount()
{
    // Examine all (potentially) pending tasks.
    // Perform lazy cleanup of pending hints.
    int result = 0;
    auto it = PendingTasks.begin();
    while (it != PendingTasks.end()) {
        auto jt = it++;
        const auto& candidate = *jt;
        int count = candidate->GetPendingJobCount();
        if (count == 0) {
            LOG_DEBUG("Task pending hint removed (Task: %s)", ~candidate->GetId());
            PendingTasks.erase(jt);
        }
        result += count;
    }

    YCHECK(result == 0 || !PendingTasks.empty());

    return result;
}

void TOperationControllerBase::OnOperationCompleted()
{
    VERIFY_THREAD_AFFINITY_ANY();

    YCHECK(Active);
    LOG_INFO("Operation completed");

    Running = false;

    Host->OnOperationCompleted(Operation);
}

void TOperationControllerBase::OnOperationFailed(const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!Active)
        return;

    LOG_INFO("Operation failed\n%s", ~error.ToString());

    Running = false;
    Active = false;

    Host->OnOperationFailed(Operation, error);
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

TObjectServiceProxy::TInvExecuteBatch TOperationControllerBase::CommitOutputs()
{
    VERIFY_THREAD_AFFINITY(BackgroundThread);

    LOG_INFO("Committing outputs");

    auto batchReq = ObjectProxy.ExecuteBatch();

    FOREACH (const auto& table, OutputTables) {
        auto ypath = FromObjectId(table.ObjectId);
        {
            auto req = TChunkListYPathProxy::Attach(FromObjectId(table.OutputChunkListId));
            FOREACH (const auto& pair, table.OutputChunkTreeIds) {
                *req->add_children_ids() = pair.second.ToProto();
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

    {
        auto rsps = batchRsp->GetResponses("set_out_sorted");
        FOREACH (auto rsp, rsps) {
            CheckResponse(rsp, "Error marking output table as sorted");
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
        PrimaryTransaction = Host->GetTransactionManager()->Attach(id, true);
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
        InputTransaction = Host->GetTransactionManager()->Attach(id, true);
    }

    {
        auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_out_tx");
        CheckResponse(rsp, "Error starting output transaction");
        auto id = TTransactionId::FromProto(rsp->object_id());
        LOG_INFO("Output transaction is %s", ~id.ToString());
        OutputTransaction = Host->GetTransactionManager()->Attach(id, true);
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
        // TODO(babenko): should we allow this?
        req->set_allow_nonempty_path_suffix(true);
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
            // NB: Use table.Path, not YPath here, otherwise path suffix is ignored.
            auto req = TTableYPathProxy::Fetch(WithTransaction(table.Path, InputTransaction->GetId()));
            req->set_fetch_node_addresses(true);
            req->set_fetch_all_meta_extensions(true);
            req->set_negate(table.NegateFetch);
            batchReq->AddRequest(req, "fetch_in");
        }
        {
            auto req = TYPathProxy::Get(WithTransaction(ypath, InputTransaction->GetId()) + "/@sorted");
            batchReq->AddRequest(req, "get_in_sorted");
        }
        {
            auto req = TYPathProxy::Get(WithTransaction(ypath, InputTransaction->GetId()) + "/@key_columns");
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
            auto req = TYPathProxy::Get(WithTransaction(ypath, OutputTransaction->GetId()) + "/@row_count");
            batchReq->AddRequest(req, "get_out_row_count");
        }
        {
            auto req = TTableYPathProxy::Clear(WithTransaction(ypath, OutputTransaction->GetId()));
            // Even if |Clear| is False we still add a dummy request
            // to keep "clear_out" requests aligned with output tables.
            batchReq->AddRequest(table.Clear ? req : NULL, "clear_out");
        }
        {
            auto req = TTableYPathProxy::GetChunkListForUpdate(WithTransaction(ypath, OutputTransaction->GetId()));
            batchReq->AddRequest(req, "get_out_chunk_list");
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
        auto getInKeyColumns = batchRsp->GetResponses<TYPathProxy::TRspGet>("get_in_key_columns");
        for (int index = 0; index < static_cast<int>(InputTables.size()); ++index) {
            auto& table = InputTables[index];
            {
                auto rsp = lockInRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error locking input table %s", ~table.Path));
                LOG_INFO("Input table %s was locked successfully",
                    ~table.Path);
            }
            {
                auto rsp = fetchInRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error fetching input input table %s", ~table.Path));
                table.FetchResponse = rsp;
                FOREACH (const auto& chunk, rsp->chunks()) {
                    auto chunkId = TChunkId::FromProto(chunk.slice().chunk_id());
                    if (chunk.node_addresses_size() == 0) {
                        ythrow yexception() << Sprintf("Chunk %s in input table %s is lost",
                            ~chunkId.ToString(),
                            ~table.Path);
                    }
                    InputChunkIds.insert(chunkId);
                }
                LOG_INFO("Input table %s has %d chunks",
                    ~table.Path,
                    rsp->chunks_size());
            }
            {
                auto rsp = getInSortedRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error getting \"sorted\" attribute for input table %s", ~table.Path));
                table.Sorted = ConvertTo<bool>(TYsonString(rsp->value()));
                LOG_INFO("Input table %s is %s",
                    ~table.Path,
                    table.Sorted ? "sorted" : "not sorted");
            }
            if (table.Sorted) {
                auto rsp = getInKeyColumns[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error getting \"key_columns\" attribute for input table %s", ~table.Path));
                table.KeyColumns = ConvertTo< std::vector<Stroka> >(TYsonString(rsp->value()));
                LOG_INFO("Input table %s has key columns %s",
                    ~table.Path,
                    ~ConvertToYsonString(table.KeyColumns, EYsonFormat::Text).Data());
            }
        }
    }

    {
        auto lockOutRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_out");
        auto clearOutRsps = batchRsp->GetResponses<TTableYPathProxy::TRspClear>("clear_out");
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
                LOG_INFO("Output table %s was locked successfully",
                    ~table.Path);
            }
            {
                auto rsp = getOutChannelsRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error getting channels for output table %s", ~table.Path));
                table.Channels = TYsonString(rsp->value());
                LOG_INFO("Output table %s has channels %s",
                    ~table.Path,
                    ~ConvertToYsonString(table.Channels, EYsonFormat::Text).Data());
            }
            {
                auto rsp = getOutRowCountRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error getting \"row_count\" attribute for output table %s", ~table.Path));
                table.InitialRowCount = ConvertTo<i64>(TYsonString(rsp->value()));
            }
            if (table.Clear) {
                auto rsp = clearOutRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error clearing output table %s", ~table.Path));
                LOG_INFO("Output table %s was cleared successfully",
                    ~table.Path);
            }
            {
                auto rsp = getOutChunkListRsps[index];
                CheckResponse(
                    rsp,
                    Sprintf("Error getting output chunk list for table %s", ~table.Path));
                table.OutputChunkListId = TChunkListId::FromProto(rsp->chunk_list_id());
                LOG_INFO("Output table %s has output chunk list %s",
                    ~table.Path,
                    ~table.OutputChunkListId.ToString());
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
                LOG_INFO("File %s consists of chunk %s",
                    ~file.Path,
                    ~TChunkId::FromProto(rsp->chunk_id()).ToString());
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
    ChunkListPool->Allocate(Config->ChunkListPreallocationCount);
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

std::vector<TRefCountedInputChunkPtr> TOperationControllerBase::CollectInputTablesChunks()
{
    // TODO(babenko): set row_attributes
    std::vector<TRefCountedInputChunkPtr> result;
    FOREACH (const auto& table, InputTables) {
        FOREACH (const auto& inputChunk, table.FetchResponse->chunks()) {
            result.push_back(New<TRefCountedInputChunk>(inputChunk));
        }
    }
    return result;
}

std::vector<TChunkStripePtr> TOperationControllerBase::PrepareChunkStripes(
    const std::vector<TRefCountedInputChunkPtr>& inputChunks,
    TNullable<int> jobCount,
    i64 jobSliceDataSize)
{
    using ::ToString;

    i64 sliceDataSize = jobSliceDataSize;
    if (jobCount) {
        i64 totalDataSize = 0;
        FOREACH (auto inputChunk, inputChunks) {
            totalDataSize += inputChunk->uncompressed_data_size();
        }
        sliceDataSize = std::min(sliceDataSize, totalDataSize / jobCount.Get() + 1);
    }

    YCHECK(sliceDataSize > 0);

    LOG_DEBUG("Preparing chunk stripes (ChunkCount: %d, JobCount: %s, JobSliceDataSize: %" PRId64 ", SliceDataSize: %" PRId64 ")",
        static_cast<int>(inputChunks.size()),
        ~ToString(jobCount),
        jobSliceDataSize,
        sliceDataSize);

    std::vector<TChunkStripePtr> result;

    // Ensure that no input chunk has size larger than sliceSize.
    FOREACH (auto inputChunk, inputChunks) {
        auto chunkId = TChunkId::FromProto(inputChunk->slice().chunk_id());
        if (inputChunk->uncompressed_data_size() > sliceDataSize) {
            int sliceCount = (int) std::ceil((double) inputChunk->uncompressed_data_size() / (double) sliceDataSize);
            auto slicedInputChunks = SliceChunkEvenly(*inputChunk, sliceCount);
            FOREACH (auto slicedInputChunk, slicedInputChunks) {
                auto stripe = New<TChunkStripe>(slicedInputChunk);
                result.push_back(stripe);
            }
            LOG_DEBUG("Slicing chunk (ChunkId: %s, SliceCount: %d)",
                ~chunkId.ToString(),
                sliceCount);
        } else {
            auto stripe = New<TChunkStripe>(inputChunk);
            result.push_back(stripe);
            LOG_DEBUG("Taking whole chunk (ChunkId: %s)",
                ~chunkId.ToString());
        }
    }

    LOG_DEBUG("Chunk stripes prepared (StripeCount: %d)",
        static_cast<int>(result.size()));

    return result;
}

std::vector<Stroka> TOperationControllerBase::CheckInputTablesSorted(const TNullable< std::vector<Stroka> >& keyColumns)
{
    YCHECK(!InputTables.empty());

    FOREACH (const auto& table, InputTables) {
        if (!table.Sorted) {
            ythrow yexception() << Sprintf("Input table %s is not sorted", ~table.Path);
        }
    }

    if (keyColumns) {
        FOREACH (const auto& table, InputTables) {
            if (!AreKeysCompatible(table.KeyColumns, keyColumns.Get())) {
                ythrow yexception() << Sprintf("Input table %s has key columns %s that are not compatible with the requested key columns %s",
                    ~table.Path,
                    ~ConvertToYsonString(table.KeyColumns, EYsonFormat::Text).Data(),
                    ~ConvertToYsonString(keyColumns.Get(), EYsonFormat::Text).Data());
            }
        }
        return keyColumns.Get();
    } else {
        const auto& referenceTable = InputTables[0];
        FOREACH (const auto& table, InputTables) {
            if (table.KeyColumns != referenceTable.KeyColumns) {
                ythrow yexception() << Sprintf("Key columns do not match: input table %s is sorted by %s while input table %s is sorted by %s",
                    ~table.Path,
                    ~ConvertToYsonString(table.KeyColumns, EYsonFormat::Text).Data(),
                    ~referenceTable.Path,
                    ~ConvertToYsonString(referenceTable.KeyColumns, EYsonFormat::Text).Data());
            }
        }
        return referenceTable.KeyColumns;
    }
}

bool TOperationControllerBase::AreKeysCompatible(
    const std::vector<Stroka>& fullColumns,
    const std::vector<Stroka>& prefixColumns)
{
    if (fullColumns.size() < prefixColumns.size()) {
        return false;
    }

    for (int index = 0; index < static_cast<int>(prefixColumns.size()); ++index) {
        if (fullColumns[index] != prefixColumns[index]) {
            return false;
        }
    }

    return true;
}

void TOperationControllerBase::CheckOutputTablesEmpty()
{
    FOREACH (const auto& table, OutputTables) {
        if (table.InitialRowCount > 0) {
            ythrow yexception() << Sprintf("Output table %s is not empty", ~table.Path);
        }
    }
}

void TOperationControllerBase::ScheduleClearOutputTables()
{
    FOREACH (auto& table, OutputTables) {
        table.Clear = true;
    }
}

void TOperationControllerBase::ScheduleSetOutputTablesSorted(const std::vector<Stroka>& keyColumns)
{
    FOREACH (auto& table, OutputTables) {
        table.SetSorted = true;
        table.KeyColumns = keyColumns;
    }
}

void TOperationControllerBase::RegisterOutputChunkTree(
    const NChunkServer::TChunkTreeId& chunkTreeId,
    int key,
    int tableIndex)
{
    auto& table = OutputTables[tableIndex];
    table.OutputChunkTreeIds.insert(std::make_pair(key, chunkTreeId));

    LOG_DEBUG("Output chunk tree registered (Table: %d, ChunkTreeId: %s, Key: %d)",
        tableIndex,
        ~chunkTreeId.ToString(),
        key);
}

bool TOperationControllerBase::CheckAvailableChunkLists(int requestedCount)
{
    if (ChunkListPool->GetSize() >= requestedCount + Config->ChunkListWatermarkCount) {
        // Enough chunk lists. Above the watermark even after extraction.
        return true;
    }

    // Additional chunk lists are definitely needed but still could be a success.
    bool success = ChunkListPool->GetSize() >= requestedCount;

    int allocateCount = requestedCount * Config->ChunkListAllocationMultiplier;
    LOG_DEBUG("Insufficient pooled chunk lists left, requesting another %d", allocateCount);
    ChunkListPool->Allocate(allocateCount);

    return success;
}

TChunkListId TOperationControllerBase::GetFreshChunkList()
{
    return ChunkListPool->Extract();
}

void TOperationControllerBase::RegisterJobInProgress(TJobInProgressPtr jip)
{
    YCHECK(JobsInProgress.insert(MakePair(jip->Job, jip)).second);
}

TOperationControllerBase::TJobInProgressPtr TOperationControllerBase::GetJobInProgress(TJobPtr job)
{
    auto it = JobsInProgress.find(job);
    YCHECK(it != JobsInProgress.end());
    return it->second;
}

void TOperationControllerBase::RemoveJobInProgress(TJobPtr job)
{
    YCHECK(JobsInProgress.erase(job) == 1);
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
    i64 totalDataSize,
    i64 dataSizePerJob,
    TNullable<int> configJobCount,
    int chunkCount)
{
    int result = configJobCount
        ? configJobCount.Get()
        : static_cast<int>(std::ceil((double) totalDataSize / dataSizePerJob));
    result = std::min(result, chunkCount);
    YCHECK(result > 0);
    return result;
}

void TOperationControllerBase::InitUserJobSpec(
    NScheduler::NProto::TUserJobSpec* proto,
    TUserJobSpecPtr config,
    const std::vector<TUserFile>& files)
{
    proto->set_shell_command(config->Command);

    {
        // Set input and output format.
        TFormat inputFormat(EFormatType::Yson);
        TFormat outputFormat(EFormatType::Yson);

        if (config->Format) {
            inputFormat = outputFormat = TFormat::FromYson(config->Format);
        }

        if (config->InputFormat) {
            inputFormat = TFormat::FromYson(config->InputFormat);
        }

        if (config->OutputFormat) {
            outputFormat = TFormat::FromYson(config->OutputFormat);
        }

        proto->set_input_format(inputFormat.ToYson().Data());
        proto->set_output_format(outputFormat.ToYson().Data());
    }

    // TODO(babenko): think about per-job files
    FOREACH (const auto& file, files) {
        *proto->add_files() = *file.FetchResponse;
    }
}

TJobIOConfigPtr TOperationControllerBase::BuildJobIOConfig(
    TJobIOConfigPtr schedulerConfig,
    INodePtr specConfigNode)
{
    if (specConfigNode) {
        auto schedulerConfigNode = ConvertTo<INodePtr>(schedulerConfig);
        return ConvertTo<TJobIOConfigPtr>(UpdateNode(schedulerConfigNode, specConfigNode));
    } else {
        return CloneConfigurable(schedulerConfig);
    }
}

void TOperationControllerBase::InitIntermediateOutputConfig(TJobIOConfigPtr config)
{
    // Don't replicate intermediate output.
    config->TableWriter->ReplicationFactor = 1;
    config->TableWriter->UploadReplicationFactor = 1;

    // Cache blocks on nodes.
    config->TableWriter->EnableNodeCaching = true;

    // Don't move intermediate chunks.
    config->TableWriter->ChunksMovable = false;
}

void TOperationControllerBase::InitIntermediateInputConfig(TJobIOConfigPtr config)
{
    // Disable master requests.
    config->TableReader->AllowFetchingSeedsFromMaster = false;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT


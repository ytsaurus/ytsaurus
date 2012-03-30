#include "stdafx.h"
#include "operation_controller_detail.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"

namespace NYT {
namespace NScheduler {

using namespace NCypress;
using namespace NTransactionServer;
using namespace NTableClient::NProto;
using namespace NChunkServer;

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
{ }

TFuture<TVoid>::TPtr TOperationControllerBase::Prepare()
{
    return MakeFuture(TVoid());
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
{ }

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
}

TFuture<TVoid>::TPtr TOperationControllerBase::OnInitComplete(TValueOrError<TVoid> result)
{
    if (result.IsOK()) {
        return MakeFuture(TVoid());
    } else {
        OnOperationFailed(result);
        return New< TFuture<TVoid> >();
    }
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
        FromMethod(&TChunkListPool::OnChunkListsCreated, MakeWeak(this))
        ->Via(ControlInvoker));

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
        FromFunctor([=] () -> TIntrusivePtr< TFuture< TValueOrError<TVoid> > > {
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


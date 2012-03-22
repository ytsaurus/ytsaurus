#include "stdafx.h"
#include "map_controller.h"
#include "operation_controller.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"
#include "private.h"

#include <ytlib/logging/tagged_logger.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/table_server/table_ypath_proxy.h>
#include <ytlib/table_client/table_reader.pb.h>
#include <ytlib/object_server/object_ypath_proxy.h>
//#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/file_server/file_ypath_proxy.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/chunk_server/public.h>
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

static NLog::TLogger& Logger(OperationLogger);

////////////////////////////////////////////////////////////////////

class TOperationControllerBase
    : public IOperationController
{
public:
    TOperationControllerBase(IOperationHost* host, TOperation* operation)
        : Host(host)
        , Operation(operation)
        , CypressProxy(host->GetMasterChannel())
        , Logger(OperationLogger)
    {
        VERIFY_INVOKER_AFFINITY(Host->GetControlInvoker(), ControlThread);
        VERIFY_INVOKER_AFFINITY(Host->GetBackgroundInvoker(), BackgroundThread);
    }

    virtual TError Initialize()
    {
        return TError();
    }

    virtual TAsyncError Prepare()
    {
        return MakeFuture(TError());
    }

    virtual void OnJobRunning(TJobPtr job)
    {
        UNUSED(job);
    }

    virtual void OnJobCompleted(TJobPtr job)
    {
        UNUSED(job);

    }

    virtual void OnJobFailed(TJobPtr job)
    {
        UNUSED(job);
    }

    virtual void OnOperationAborted()
    { }

    virtual void ScheduleJobs(
        TExecNodePtr node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort)
    {
        UNUSED(node);
        UNUSED(jobsToStart);
        UNUSED(jobsToAbort);
    }

protected:
    IOperationHost* Host;
    TOperation* Operation;

    TCypressServiceProxy CypressProxy;
    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(BackgroundThread);

    void Fail(const TError& error)
    {
        Host->OnOperationFailed(Operation, error);
    }

    void Complete()
    {
        Host->OnOperationCompleted(Operation);
    }


    //static i64 GetRowCount(const TFetchedChunk& fetchedChunk)
    //{
    //    TChunkAttributes chunkAttributes;
    //    DeserializeProtobuf(&chunkAttributes, fetchedChunk.attributes());

    //    YASSERT(chunkAttributes.HasExtension(TTableChunkAttributes::table_attributes));
    //    const auto& tableChunkAttributes = chunkAttributes.GetExtension(TTableChunkAttributes::table_attributes);

    //    return tableChunkAttributes.row_count();
    //}

    //static i64 GetRowCount(TTableYPathProxy::TRspFetch::TPtr rsp)
    //{
    //    i64 result = 0;
    //    FOREACH (const auto& chunk, rsp->chunks()) {
    //        result += GetRowCount(chunk);
    //    }
    //    return result;
    //}
};

////////////////////////////////////////////////////////////////////

class TChunkAllocationMap
{
public:
    TChunkAllocationMap(std::vector<TFetchedChunk>& chunks)
    {
        // TODO(babenko): use move ctor when available
        Chunks.swap(chunks);

        for (int index = 0; index < static_cast<int>(Chunks.size()); ++index) {
            PutChunk(index);
        }
    }

    const TFetchedChunk& GetChunk(int index)
    {
        return Chunks[index];
    }

    void AllocateChunks(
        const Stroka& address,
        int maxChunks,
        std::vector<int>* indexes,
        int* localCount,
        int* remoteCount)
    {
        indexes->reserve(maxChunks);

        // Take local chunks first.
        *localCount = 0;
        auto addressIt = AddressToIndexSet.find(address);
        if (addressIt != AddressToIndexSet.end()) {
            auto& localIndexes = addressIt->second;
            auto localIt = localIndexes.begin();
            while (localIt != localIndexes.end()) {
                if (indexes->size() >= maxChunks) {
                    break;
                }

                auto nextLocalIt = localIt;
                ++nextLocalIt;
                int chunkIndex = *localIt;

                indexes->push_back(chunkIndex);
                localIndexes.erase(localIt);
                YVERIFY(UnallocatedIndexes.erase(chunkIndex) == 1);
                ++*localCount;
                
                localIt = nextLocalIt;
            }
        }

        // Proceed with remote chunks next.
        *remoteCount = 0;
        auto remoteIt = UnallocatedIndexes.begin();
        while (remoteIt != UnallocatedIndexes.end()) {
            if (indexes->size() >= maxChunks) {
                break;
            }

            auto nextRemoteIt = remoteIt;
            ++nextRemoteIt;
            int chunkIndex = *remoteIt;

            indexes->push_back(*remoteIt);
            const auto& chunk = Chunks[chunkIndex];
            FOREACH (const auto& address, chunk.holder_addresses()) {
                YVERIFY(AddressToIndexSet[address].erase(chunkIndex) == 1);
            }
            ++*remoteCount;
            
            remoteIt = nextRemoteIt;
        }
    }

    void DeallocateChunks(const std::vector<int>& indexes)
    {
        FOREACH (auto index, indexes) {
            PutChunk(index);
        }
    }

private:
    std::vector<TFetchedChunk> Chunks;
    yhash_map<Stroka, yhash_set<int> > AddressToIndexSet;
    yhash_set<int> UnallocatedIndexes;

    void PutChunk(int index)
    {
        const auto& chunk = Chunks[index];
        FOREACH (const auto& address, chunk.holder_addresses()) {
            YVERIFY(AddressToIndexSet[address].insert(index).second);
        }
        YVERIFY(UnallocatedIndexes.insert(index).second);
    }
};

////////////////////////////////////////////////////////////////////

class TChunkListPool
    : public TRefCounted
{
public:
    TChunkListPool(
        NRpc::IChannel::TPtr masterChannel,
        IInvoker::TPtr controlInvoker,
        const TTransactionId& transactionId)
        : MasterChannel(masterChannel)
        , ControlInvoker(controlInvoker)
        , TransactionId(transactionId)
        , RequestInProgress(false)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker, ControlThread);
    }

    int GetSize() const
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return static_cast<int>(Ids.size());
    }

    TChunkListId Pop()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YASSERT(!Ids.empty());
        auto id = Ids.back();
        Ids.pop_back();
        return id;
    }

    void Allocate(int count)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        if (RequestInProgress)
            return;

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

private:
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    NRpc::IChannel::TPtr MasterChannel;
    IInvoker::TPtr ControlInvoker;
    TTransactionId TransactionId;

    bool RequestInProgress;
    std::vector<TChunkListId> Ids;

    void OnChunkListsCreated(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        RequestInProgress = false;

        if (!batchRsp->IsOK()) {
            LOG_ERROR("Error allocating chunk lists\n%s", ~batchRsp->GetError().ToString());
            return;
        }

        YASSERT(RequestInProgress);
        YASSERT(Ids.empty());

        FOREACH  (auto rsp, batchRsp->GetResponses<TTransactionYPathProxy::TRspCreateObject>()) {
            Ids.push_back(TChunkListId::FromProto(rsp->object_id()));
        }
    }
};

typedef TIntrusivePtr<TChunkListPool> TChunkListPoolPtr;

////////////////////////////////////////////////////////////////////

class TRunningCounter
{
public:
    TRunningCounter()
        : Total_(-1)
        , Running_(-1)
        , Done_(-1)
        , Pending_(-1)
        , Failed_(-1)
    { }

    void Init(i64 total)
    {
        Total_ = total;
        Running_ = 0;
        Done_ = 0;
        Pending_ = total;
        Failed_ = 0;
    }


    i64 GetTotal() const
    {
        return Total_;
    }

    i64 GetRunning() const
    {
        return Running_;
    }

    i64 GetDone() const
    {
        return Done_;
    }

    i64 GetPending() const
    {
        return Pending_;
    }

    i64 GetFailed() const
    {
        return Failed_;
    }


    void Start(i64 count)
    {
        Running_ += count;
        Pending_ -= count;
    }

    void Completed(i64 count)
    {
        Running_ -= count;
        Done_ += count;
    }

    void Failed(i64 count)
    {
        Running_ -= count;
        Pending_ += count;
        Failed_ += count;
    }

private:
    i64 Total_;
    i64 Running_;
    i64 Done_;
    i64 Pending_;
    i64 Failed_;
};

////////////////////////////////////////////////////////////////////

class TMapController
    : public TOperationControllerBase
{
public:
    TMapController(IOperationHost* host, TOperation* operation)
        : TOperationControllerBase(host, operation)
    { }

    virtual TError Initialize()
    {
        Spec = New<TMapOperationSpec>();
        try {
            Spec->Load(~Operation->GetSpec());
        } catch (const std::exception& ex) {
            return TError(Sprintf("Error parsing operation spec\n%s", ex.what()));
        }

        if (Spec->In.empty()) {
            // TODO(babenko): is this an error?
            return TError("No input tables are given");
        }

        return TError();
    }

    virtual TAsyncError Prepare()
    {
        //FromMethod(&TThis::StartPrimaryTransaction, this)->AsyncVia(Host->GetBackgroundInvoker())->Do()
        //->Apply(FromMethod(&TThis::OnPrimaryTransactionStarted)->AsyncVia(Host->GetBackgroundInvoker())
        return MakeFuture(TError());
    }


    virtual void OnJobCompleted(TJobPtr job)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto jobInfo = GetJobInfo(job);

        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            auto chunkListId = jobInfo->OutputChunkListIds[index];
            OutputTables[index].DoneChunkListIds.push_back(chunkListId);
        }

        JobCounter.Completed(1);
        ChunkCounter.Completed(jobInfo->ChunkIndexes.size());

        RemoveJobInfo(job);
    }

    virtual void OnJobFailed(TJobPtr job)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto jobInfo = GetJobInfo(job);

        for (int index = 0; index < static_cast<int>(OutputTables.size()); ++index) {
            auto chunkListId = jobInfo->OutputChunkListIds[index];
            OutputTables[index].DoneChunkListIds.push_back(chunkListId);
        }

        JobCounter.Failed(1);
        ChunkCounter.Failed(jobInfo->ChunkIndexes.size());

        // TODO(babenko): failed jobs threshold

        ReleaseChunkLists(jobInfo->OutputChunkListIds);

        RemoveJobInfo(job);
    }


    virtual void OnOperationAborted()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        AbortTransactions();
    }


    virtual void ScheduleJobs(
        TExecNodePtr node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // Check if we have enough chunk lists in the pool.
        if (ChunkListPool->GetSize() < OutputTables.size()) {
            // TODO(babenko): make configurable
            ChunkListPool->Allocate(OutputTables.size() * 10);
            return;
        }

        // Make a copy of generic spec and customize it.
        auto jobSpec = GenericJobSpec;
        auto* mapJobSpec = jobSpec.MutableExtension(TMapJobSpec::map_job_spec);

        i64 pendingJobs = JobCounter.GetPending();
        YASSERT(pendingJobs > 0);
        i64 pendingChunks = ChunkCounter.GetPending();
        int maxChunksPerJob = (pendingChunks + pendingJobs - 1) / pendingJobs;

        auto jobInfo = New<TJobInfo>();

        auto& chunkIndexes = jobInfo->ChunkIndexes;
        int localCount;
        int remoteCount;
        ChunkAllocationMap->AllocateChunks(
            node->GetAddress(),
            maxChunksPerJob,
            &chunkIndexes,
            &localCount,
            &remoteCount);

        LOG_DEBUG("Allocated %d input chunks for node %s (MaxCount: %d, LocalCount: %d, RemoteCount: %d)",
            static_cast<int>(chunkIndexes.size()),
            ~node->GetAddress(),
            maxChunksPerJob,
            localCount,
            remoteCount);

        if (chunkIndexes.empty())
            return;

        FOREACH (int chunkIndex, chunkIndexes) {
            const auto& chunk = ChunkAllocationMap->GetChunk(chunkIndex);
            *mapJobSpec->mutable_input_spec()->add_chunks() = chunk;
        }

        FOREACH (auto& outputSpec, *mapJobSpec->mutable_output_specs()) {
            auto chunkListId = ChunkListPool->Pop();
            jobInfo->OutputChunkListIds.push_back(chunkListId);
            outputSpec.set_chunk_list_id(chunkListId.ToProto());
        }
        
        auto job = Host->CreateJob(
            Operation,
            node,
            jobSpec);

        PutJobInfo(job, jobInfo);

        JobCounter.Start(1);
        ChunkCounter.Start(chunkIndexes.size());

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
    ITransaction::TPtr PrimaryTransaction;
    ITransaction::TPtr InputTransaction;
    ITransaction::TPtr OutputTransaction;

    struct TInputTable
    {
        TTableYPathProxy::TRspFetch::TPtr FetchResponse;
    };

    TInputTable InputTable;

    struct TOutputTable
    {
        TOutputTable()
            // TODO(babenko): don't need this default
            : Schema("{}")
        { }

        TYson Schema;
        std::vector<TChunkListId> DoneChunkListIds;
    };

    std::vector<TOutputTable> OutputTables;

    struct TFile
    {
        TFileYPathProxy::TRspFetch::TPtr FetchResponse;
    };

    std::vector<TFile> Files;

    TRunningCounter JobCounter;
    TRunningCounter ChunkCounter;

    ::THolder<TChunkAllocationMap> ChunkAllocationMap;
    TChunkListPoolPtr ChunkListPool;

    TJobSpec GenericJobSpec;

    struct TJobInfo
        : public TIntrinsicRefCounted
    {
        std::vector<int> ChunkIndexes;
        std::vector<TChunkListId> OutputChunkListIds;
    };

    typedef TIntrusivePtr<TJobInfo> TJobInfoPtr;

    yhash_map<TJobPtr, TJobInfoPtr> JobInfos;


    TVoid StartTransactions()
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        LOG_INFO("Creating primary transaction");
        PrimaryTransaction = Host->GetTransactionManager()->Start(NULL, Operation->GetTransactionId());
        LOG_INFO("Primary transaction id is %s", ~PrimaryTransaction->GetId().ToString());

        LOG_INFO("Creating input transaction");
        InputTransaction = Host->GetTransactionManager()->Start(NULL, PrimaryTransaction->GetId());
        LOG_INFO("Input transaction id is %s", ~InputTransaction->GetId().ToString());

        LOG_INFO("Creating output transaction");
        OutputTransaction = Host->GetTransactionManager()->Start(NULL, PrimaryTransaction->GetId());
        LOG_INFO("Output transaction id is %s", ~OutputTransaction->GetId().ToString());
    }

    void CommitTransactions()
    {
        try {
            LOG_INFO("Committing input transaction");
            InputTransaction->Commit();
            LOG_INFO("Input transaction committed");

            LOG_INFO("Committing output transaction");
            OutputTransaction->Commit();
            LOG_INFO("Output transaction committed");

            LOG_INFO("Committing primary transaction");
            PrimaryTransaction->Commit();
            LOG_INFO("Primary transaction committed");
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error committing transactions\n%s",
                ex.what());
        }
    }

    void AbortTransactions()
    {
        LOG_INFO("Aborting operation transactions")
        if (PrimaryTransaction) {
            PrimaryTransaction->Abort();
        }

        // No need to abort the others.

        PrimaryTransaction.Reset();
        InputTransaction.Reset();
        OutputTransaction.Reset();
    }


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


    void InitGenericJobSpec()
    {
        TJobSpec jobSpec;
        jobSpec.set_type(EJobType::Map);

        TUserJobSpec userJobSpec;
        userJobSpec.set_shell_comand(Spec->ShellCommand);
        FOREACH (const auto& file, Files) {
            *userJobSpec.add_files() = *file.FetchResponse;
        }
        *jobSpec.MutableExtension(TUserJobSpec::user_job_spec) = userJobSpec;

        TMapJobSpec mapJobSpec;
        *mapJobSpec.mutable_input_spec()->mutable_channel() = InputTable.FetchResponse->channel();
        mapJobSpec.set_input_transaction_id(InputTransaction->GetId().ToProto());
        FOREACH (const auto& table, OutputTables) {
            auto* outputSpec = mapJobSpec.add_output_specs();
            outputSpec->set_schema(table.Schema);
        }
        *jobSpec.MutableExtension(TMapJobSpec::map_job_spec) = mapJobSpec;

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
        batchReq->Invoke();
    }

    /*
    TCypressServiceProxy::TInvExecuteBatch::TPtr AcquireTables()
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        LOG_INFO("Acquiring tables");

        auto batchReq = CypressProxy.ExecuteBatch();

        FOREACH (const auto& path, Spec->In) {
            auto req = TTableYPathProxy::Fetch(WithTransaction(path, PrimaryTransaction->GetId()));
            req->set_fetch_holder_addresses(true);
            req->set_fetch_chunk_attributes(true);
            batchReq->AddRequest(req, "fetch_in");
        }

        FOREACH (const auto& path, Spec->In) {
            auto req = TCypressYPathProxy::Lock(WithTransaction(path, InputTransaction->GetId()));
            req->set_mode(ELockMode::Snapshot);
            batchReq->AddRequest(req, "lock_in");
        }

        FOREACH (const auto& path, Spec->Out) {
            auto req = TCypressYPathProxy::Lock(WithTransaction(path, OutputTransaction->GetId()));
            req->set_mode(ELockMode::Shared);
            batchReq->AddRequest(req, "lock_out");
        }

        // TODO(babenko): request output schemas

        FOREACH (const auto& path, Spec->Out) {
            auto req = TTableYPathProxy::GetChunkListForUpdate(WithTransaction(path, OutputTransaction->GetId()));
            batchReq->AddRequest(req, "get_out_chunk_lists");
        }

        return batchReq->Invoke();
    }

    TVoid OnTablesAcquired(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        if (!batchRsp->IsOK()) {
            Fail(TError(Sprintf("Error acquiring input\n%s",
                ~batchRsp->GetError().ToString())));
            return TVoid();
        }

        {
            InputTables.resize(Spec->In.size());
            TotalRowCount = 0;
            auto fetchInRsps = batchRsp->GetResponses<TTableYPathProxy::TRspFetch>("fetch_in");
            auto lockInRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_in");
            for (int index = 0; index < static_cast<int>(Spec->In.size()); ++index) {
                auto lockInRsp = lockInRsps[index];
                if (!lockInRsp->IsOK()) {
                    Fail(TError(Sprintf("Error locking input table\n%s",
                        ~lockInRsp->GetError().ToString())));
                    return TVoid();
                }

                auto fetchInRsp = fetchInRsps[index];
                if (!lockInRsp->IsOK()) {
                    Fail(TError(Sprintf("Error fetching input table\n%s",
                        ~fetchInRsp->GetError().ToString())));
                    return TVoid();
                }

                auto& table = InputTables[index];
                table.FetchRsp = fetchInRsp;
            }
        }

        {
            OutputTables.resize(Spec->Out.size());
            auto lockOutRsps = batchRsp->GetResponses<TCypressYPathProxy::TRspLock>("lock_out");
            auto getOutChunkListsRsps = batchRsp->GetResponses<TTableYPathProxy::TRspGetChunkListForUpdate>("get_out_chunk_lists");
            for (int index = 0; index < static_cast<int>(Spec->Out.size()); ++index) {
                auto lockOutRsp = lockOutRsps[index];
                if (!lockOutRsp->IsOK()) {
                    Fail(TError(Sprintf("Error locking output table\n%s",
                        ~lockOutRsp->GetError().ToString())));
                    return TVoid();
                }

                auto getOutChunkListRsp = getOutChunkListsRsps[index];
                if (!getOutChunkListRsp->IsOK()) {
                    Fail(TError(Sprintf("Error getting output chunk list\n%s",
                        ~getOutChunkListRsp->GetError().ToString())));
                    return TVoid();
                }

                auto& table = OutputTables[index];
                // TODO(babenko): fill schema
                table.ChunkListId = TChunkListId::FromProto(getOutChunkListRsp->chunk_list_id());
            }
        }

        // TODO(babenko): statistics
        LOG_INFO("Tables acquired");
    }
    */

    //TCypressServiceProxy::TInvExecuteBatch::TPtr StartPrimaryTransaction()
    //{
    //    auto batchReq = CypressProxy.ExecuteBatch();

    //    {
    //        auto req = TTransactionYPathProxy::CreateObject(
    //            FromObjectId(Operation->GetTransactionId()));
    //        req->set_type(EObjectType::Transaction);
    //        batchReq->AddRequest(req, "start_primary_tx");
    //    }

    //    return batchReq->Invoke();
    //}

    //TVoid OnPrimaryTransactionStarted(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
    //{
    //    if (!batchRsp->IsOK()) {
    //        Fail(TError(Sprintf("Error creating primary transaction\n%s",
    //            ~batchRsp->GetError().ToString())));
    //        return TVoid();
    //    }

    //    {
    //        auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_primary_tx");
    //        auto id = TTransactionId::FromProto(rsp->object_id());
    //        PrimaryTransaction = Host->GetTransactionManager()->Attach(id);
    //    }

    //    return TVoid();
    //}

    //TCypressServiceProxy::TInvExecuteBatch::TPtr StartSeconaryTransactions()
    //{
    //    auto batchReq = CypressProxy.ExecuteBatch();

    //    {
    //        auto req = TTransactionYPathProxy::CreateObject(
    //            FromObjectId(PrimaryTransaction->GetId()));
    //        req->set_type(EObjectType::Transaction);
    //        batchReq->AddRequest(req, "start_input_tx");
    //    }

    //    {
    //        auto req = TTransactionYPathProxy::CreateObject(
    //            FromObjectId(PrimaryTransaction->GetId()));
    //        req->set_type(EObjectType::Transaction);
    //        batchReq->AddRequest(req, "start_output_tx");
    //    }

    //    return batchReq->Invoke();
    //}

    //TVoid OnSecondaryTransactionsStarted(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
    //{
    //    if (!batchRsp->IsOK()) {
    //        Fail(TError(Sprintf("Error creating secondary transactions\n%s",
    //            ~batchRsp->GetError().ToString())));
    //        return TVoid();
    //    }

    //    {
    //        auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_input_tx");
    //        auto id = TTransactionId::FromProto(rsp->object_id());
    //        LOG_INFO("Input transaction id is %s", ~id.ToString());
    //        InputTransaction = Host->GetTransactionManager()->Attach(id);
    //    }

    //    {
    //        auto rsp = batchRsp->GetResponse<TTransactionYPathProxy::TRspCreateObject>("start_output_tx");
    //        auto id = TTransactionId::FromProto(rsp->object_id());
    //        LOG_INFO("Output transaction id is %s", ~id.ToString());
    //        OutputTransaction = Host->GetTransactionManager()->Attach(id);
    //    }

    //    return TVoid();
    //}


    void Complete()
    {
        VERIFY_THREAD_AFFINITY(BackgroundThread);

        try {
            CommitTransactions();
        } catch (const std::exception& ex) {
            Fail(TError(ex.what()));
            return;
        }

        TOperationControllerBase::Complete();
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


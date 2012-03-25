#include "stdafx.h"
#include "map_controller.h"
#include "operation_controller.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"
#include "private.h"

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/fluent.h>

#include <ytlib/misc/thread_affinity.h>
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

// TODO(babenko): extract to a proper place
class TOperationControllerBase
    : public IOperationController
{
public:
    TOperationControllerBase(IOperationHost* host, TOperation* operation)
        : Host(host)
        , Operation(operation)
        , CypressProxy(host->GetMasterChannel())
        , Logger(OperationsLogger)
    {
        VERIFY_INVOKER_AFFINITY(Host->GetControlInvoker(), ControlThread);
        VERIFY_INVOKER_AFFINITY(Host->GetBackgroundInvoker(), BackgroundThread);

        Logger.AddTag(Sprintf("OperationId: %s", ~operation->GetOperationId().ToString()));
    }

    virtual void Initialize()
    { }

    virtual TFuture<TError>::TPtr Prepare()
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


    void OnOperationFailed(const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_INFO("Operation failed\n%s", ~error.ToString());

        Host->OnOperationFailed(Operation, error);

        Host->GetControlInvoker()->Invoke(FromMethod(
            &TOperationControllerBase::AbortOperation,
            MakeWeak(this)));
    }

    void OnOperationCompleted()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_INFO("Operation completed");

        Host->OnOperationCompleted(Operation);
    }

    virtual void AbortOperation()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
    }
};

////////////////////////////////////////////////////////////////////

// TODO(babenko): extract to a proper place
class TChunkPool
{
public:
    TChunkPool(TOperationPtr operation)
        : Logger(OperationsLogger)
    {
        Logger.AddTag(Sprintf("OperationId: %s", ~operation->GetOperationId().ToString()));
    }

    int PutChunk(const TInputChunk& chunk, i64 weight)
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

    const TInputChunk& GetChunk(int index)
    {
        return ChunkInfos[index].Chunk;
    }

    void AllocateChunks(
        const Stroka& address,
        i64 maxWeight,
        std::vector<int>* indexes,
        i64* allocatedWeight,
        int* localCount,
        int* remoteCount)
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

    void DeallocateChunks(const std::vector<int>& indexes)
    {
        FOREACH (auto index, indexes) {
            RegisterChunk(index);
        }

        LOG_DEBUG("Chunks [%s] are back in the pool", ~JoinToString(indexes));
    }

private:
    NLog::TTaggedLogger Logger;

    struct TChunkInfo
    {
        TInputChunk Chunk;
        i64 Weight;
    };

    std::vector<TChunkInfo> ChunkInfos;
    yhash_map<Stroka, yhash_set<int> > AddressToIndexSet;
    yhash_set<int> UnallocatedIndexes;

    void RegisterChunk(int index)
    {
        const auto& info = ChunkInfos[index];
        FOREACH (const auto& address, info.Chunk.holder_addresses()) {
            YVERIFY(AddressToIndexSet[address].insert(index).second);
        }
        YVERIFY(UnallocatedIndexes.insert(index).second);
    }

    void UnregisterChunk(int index)
    {
        const auto& info = ChunkInfos[index];
        FOREACH (const auto& address, info.Chunk.holder_addresses()) {
            YVERIFY(AddressToIndexSet[address].erase(index) == 1);
        }
        YVERIFY(UnallocatedIndexes.erase(index) == 1);
    }
};

////////////////////////////////////////////////////////////////////

// TODO(babenko): extract to a proper place
class TChunkListPool
    : public TRefCounted
{
public:
    TChunkListPool(
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

    int GetSize() const
    {
        return static_cast<int>(Ids.size());
    }

    TChunkListId Extract()
    {
        YASSERT(!Ids.empty());
        auto id = Ids.back();
        Ids.pop_back();

        LOG_DEBUG("Extracted chunk list %s from the pool, %d remaining",
            ~id.ToString(),
            static_cast<int>(Ids.size()));

        return id;
    }

    void Allocate(int count)
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

private:
    NRpc::IChannel::TPtr MasterChannel;
    IInvoker::TPtr ControlInvoker;
    TOperationPtr Operation;
    TTransactionId TransactionId;

    NLog::TTaggedLogger Logger;
    bool RequestInProgress;
    std::vector<TChunkListId> Ids;

    void OnChunkListsCreated(TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp)
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
};

typedef TIntrusivePtr<TChunkListPool> TChunkListPoolPtr;

////////////////////////////////////////////////////////////////////

// TODO(babenko): extract to a proper place
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
        YASSERT(Pending_ >= count);
        Running_ += count;
        Pending_ -= count;
    }

    void Completed(i64 count)
    {
        YASSERT(Running_ >= count);
        Running_ -= count;
        Done_ += count;
    }

    void Failed(i64 count)
    {
        YASSERT(Running_ >= count);
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

// TODO(babenko): move to a proper place

template <class T>
class TAsyncPipeline
    : public TRefCounted
{
public:
    TAsyncPipeline(
        IInvoker::TPtr invoker,
        TIntrusivePtr< IFunc< TIntrusivePtr < TFuture< TValueOrError<T> > > > > head)
        : Invoker(invoker)
        , Lazy(head)
    { }

    TIntrusivePtr< TFuture< TValueOrError<T> > > Run()
    {
        return Lazy->Do();
    }

    typedef T T1;

    template <class T2>
    TIntrusivePtr< TAsyncPipeline<T2> > Add(TIntrusivePtr< IParamFunc<T1, T2> > func)
    {
        auto wrappedFunc = FromFunctor([=] (TValueOrError<T1> x) -> TIntrusivePtr< TFuture< TValueOrError<T2> > > {
            if (!x.IsOK()) {
                return MakeFuture(TValueOrError<T2>(TError(x)));
            }
            try {
                auto y = func->Do(x.Value());
                return MakeFuture(TValueOrError<T2>(y));
            } catch (const std::exception& ex) {
                return MakeFuture(TValueOrError<T2>(TError(ex.what())));
            }
        });

        if (Invoker) {
            wrappedFunc = wrappedFunc->AsyncVia(Invoker);
        }

        auto lazy = Lazy;
        auto newLazy = FromFunctor([=] () {
            return lazy->Do()->Apply(wrappedFunc);
        });

        return New< TAsyncPipeline<T2> >(Invoker, newLazy);
    }

    template <class T2>
    TIntrusivePtr< TAsyncPipeline<T2> > Add(TIntrusivePtr< IParamFunc<T1, TIntrusivePtr< TFuture<T2> > > > func)
    {
        auto toValueOrError = FromFunctor([] (T2 x) {
            return TValueOrError<T2>(x);
        });

        auto wrappedFunc = FromFunctor([=] (TValueOrError<T1> x) -> TIntrusivePtr< TFuture< TValueOrError<T2> > > {
            if (!x.IsOK()) {
                return MakeFuture(TValueOrError<T2>(TError(x)));
            }
            try {
                auto y = func->Do(x.Value());
                return y->Apply(toValueOrError);
            } catch (const std::exception& ex) {
                return MakeFuture(TValueOrError<T2>(TError(ex.what())));
            }
        });

        if (Invoker) {
            wrappedFunc = wrappedFunc->AsyncVia(Invoker);
        }

        auto lazy = Lazy;
        auto newLazy = FromFunctor([=] () {
            return lazy->Do()->Apply(wrappedFunc);
        });

        return New< TAsyncPipeline<T2> >(Invoker, newLazy);
    }


private:
    IInvoker::TPtr Invoker;
    TIntrusivePtr< IFunc< TIntrusivePtr < TFuture< TValueOrError<T> > > > > Lazy;

};

TIntrusivePtr< TAsyncPipeline<TVoid> > StartAsyncPipeline(IInvoker::TPtr invoker = NULL)
{
    return New< TAsyncPipeline<TVoid> >(
        invoker,
        FromFunctor([=] () -> TIntrusivePtr< TFuture< TValueOrError<TVoid> > > {
            return MakeFuture(TValueOrError<TVoid>(TVoid()));
        }));
}


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

        ExecNodeCount = Host->GetExecNodeCount();
        if (ExecNodeCount == 0) {
            ythrow yexception() << "No online exec nodes";
        }
    }

    virtual TFuture<TError>::TPtr Prepare()
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
            // TODO(babenko): get rid of this
            ->Apply(FromFunctor([] (TValueOrError<TVoid> x) -> TError {
                return x.IsOK() ? TError() : TError(x);
            }));
    }


    virtual void OnJobCompleted(TJobPtr job)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        LOG_INFO("Job %s completed", ~job->GetId().ToString());

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
            OnOperationFailed(TError("%d jobs failed, aborting the operation",
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
        userJobSpec.set_shell_comand(Spec->ShellCommand);
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
            FOREACH (const auto& chunkInfo, fetchRsp->chunks()) {
                TInputChunk inputChunk;
                *inputChunk.mutable_slice() = chunkInfo.slice();
                inputChunk.mutable_holder_addresses()->MergeFrom(chunkInfo.holder_addresses());
                *inputChunk.mutable_channel() = fetchRsp->channel();
                if (rowAttributes) {
                    inputChunk.set_row_attributes(rowAttributes.Get());
                }

                TChunkAttributes chunkAttributes;
                YVERIFY(DeserializeProtobuf(&chunkAttributes, chunkInfo.attributes()));

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
        LOG_INFO("Aborting operation transactions")

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


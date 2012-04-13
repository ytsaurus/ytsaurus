#pragma once

#include "public.h"
#include "operation_controller.h"
#include "progress_counter.h"

#include <ytlib/logging/tagged_logger.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_server/table_ypath_proxy.h>
#include <ytlib/file_server/file_ypath_proxy.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): extract to a proper place
class TChunkListPool
    : public TRefCounted
{
public:
    TChunkListPool(
        NRpc::IChannel::TPtr masterChannel,
        IInvoker::TPtr controlInvoker,
        TOperationPtr operation,
        const TTransactionId& transactionId);

    int GetSize() const;

    NChunkServer::TChunkListId Extract();

    void Allocate(int count);

private:
    NRpc::IChannel::TPtr MasterChannel;
    IInvoker::TPtr ControlInvoker;
    TOperationPtr Operation;
    TTransactionId TransactionId;

    NLog::TTaggedLogger Logger;
    bool RequestInProgress;
    std::vector<NChunkServer::TChunkListId> Ids;

    void OnChunkListsCreated(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);
};

typedef TIntrusivePtr<TChunkListPool> TChunkListPoolPtr;

////////////////////////////////////////////////////////////////////

// TODO(babenko): extract to a proper place
class TOperationControllerBase
    : public IOperationController
{
public:
    TOperationControllerBase(
        TSchedulerConfigPtr config,
        IOperationHost* host,
        TOperation* operation);

    virtual void Initialize();
    virtual TFuture<TVoid>::TPtr Prepare();
    virtual TFuture<TVoid>::TPtr Revive();

    virtual void OnJobRunning(TJobPtr job);
    virtual void OnJobCompleted(TJobPtr job);
    virtual void OnJobFailed(TJobPtr job);

    virtual void OnOperationAborted();

    virtual TJobPtr ScheduleJob(TExecNodePtr node);
    virtual i64 GetPendingJobCount();

    virtual void BuildProgressYson(NYTree::IYsonConsumer* consumer);
    virtual void BuildResultYson(NYTree::IYsonConsumer* consumer);

private:
    typedef TOperationControllerBase TThis;

protected:
    TSchedulerConfigPtr Config;
    IOperationHost* Host;
    TOperation* Operation;

    NCypress::TCypressServiceProxy CypressProxy;
    NLog::TTaggedLogger Logger;

    // Remains True as long as the operation is not failed, completed, or aborted.
    bool Active;
    // Remains True as long as the operation can schedule new jobs.
    bool Running;

    // Fixed during init time, used to compute job count.
    int ExecNodeCount;

    // Running counters.
    TProgressCounter JobCounter;

    TChunkListPoolPtr ChunkListPool;

    // The primary transaction for the whole operation (nested inside operation's transaction).
    NTransactionClient::ITransaction::TPtr PrimaryTransaction;
    // The transaction for reading input tables (nested inside the primary one).
    // These tables are locked with Snapshot mode.
    NTransactionClient::ITransaction::TPtr InputTransaction;
    // The transaction for writing output tables (nested inside the primary one).
    // These tables are locked with Shared mode.
    NTransactionClient::ITransaction::TPtr OutputTransaction;

    // Input tables.
    struct TInputTable
    {
        NYTree::TYPath Path;
        NTableServer::TTableYPathProxy::TRspFetch::TPtr FetchResponse;
    };

    std::vector<TInputTable> InputTables;

    // Output tables.
    struct TOutputTable
    {
        NYTree::TYPath Path;
        NYTree::TYson Schema;
        // Chunk list for appending the output.
        NChunkServer::TChunkListId OutputChunkListId;
        // Chunk trees comprising the output (the order matters!)
        std::vector<NChunkServer::TChunkTreeId> PartitionTreeIds;
    };

    std::vector<TOutputTable> OutputTables;

    // Files.
    struct TFile
    {
        NYTree::TYPath Path;
        NFileServer::TFileYPathProxy::TRspFetch::TPtr FetchResponse;
    };

    std::vector<TFile> Files;

    struct TJobHandlers
        : public TIntrinsicRefCounted
    {
        TClosure OnCompleted;
        TClosure OnFailed;
    };

    typedef TIntrusivePtr<TJobHandlers> TJobHandlersPtr;

    yhash_map<TJobPtr, TJobHandlersPtr> JobHandlers;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(BackgroundThread);

    virtual void DoInitialize() = 0;
    virtual bool HasPendingJobs() = 0;
    virtual void LogProgress() = 0;
    virtual void DoGetProgress(NYTree::IYsonConsumer* consumer) = 0;

    // Jobs handlers management.
    TJobPtr CreateJob(
        TOperationPtr operation,
        TExecNodePtr node, 
        const NProto::TJobSpec& spec,
        TClosure onCompleted,
        TClosure onFailed);
    TJobHandlersPtr GetJobHandlers(TJobPtr job);
    void RemoveJobHandlers(TJobPtr job);

    //! Performs the actual scheduling.
    virtual TJobPtr DoScheduleJob(TExecNodePtr node) = 0;


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

    NCypress::TCypressServiceProxy::TInvExecuteBatch::TPtr StartPrimaryTransaction(TVoid);

    TVoid OnPrimaryTransactionStarted(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    // Round 2:
    // - Start input transaction.
    // - Start output transaction.

    NCypress::TCypressServiceProxy::TInvExecuteBatch::TPtr StartSeconaryTransactions(TVoid);

    TVoid OnSecondaryTransactionsStarted(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    // Round 3:
    // - Fetch input tables.
    // - Lock input tables.
    // - Lock output tables.
    // - Fetch files.
    // - Get output tables schemata.
    // - Get output chunk lists.

    NCypress::TCypressServiceProxy::TInvExecuteBatch::TPtr RequestInputs(TVoid);

    TVoid OnInputsReceived(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    virtual std::vector<NYTree::TYPath> GetInputTablePaths() = 0;
    virtual std::vector<NYTree::TYPath> GetOutputTablePaths() = 0;
    virtual std::vector<NYTree::TYPath> GetFilePaths() = 0;


    // Round 4.
    // - (Custom)

    TVoid CompletePreparation(TVoid);

    virtual void DoCompletePreparation() = 0;


    // Here comes the completion pipeline.

    void FinalizeOperation();

    // Round 1.
    // - Attach chunk trees.
    // - Commit input transaction.
    // - Commit output transaction.
    // - Commit primary transaction.

    NCypress::TCypressServiceProxy::TInvExecuteBatch::TPtr CommitOutputs(TVoid);

    TVoid OnOutputsCommitted(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);


    // Abort is not a pipeline really :)

    virtual void AbortOperation();

    void AbortTransactions();


    void FailOperation(const TError& error);


    // Unsorted helpers.
    bool CheckChunkListsPoolSize(int count);
    void ReleaseChunkList(const NChunkServer::TChunkListId& id);
    void ReleaseChunkLists(const std::vector<NChunkServer::TChunkListId>& ids);
    void OnChunkListsReleased(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);
    static i64 GetJobWeightThreshold(i64 pendingJobs, i64 pendingWeight);

};

////////////////////////////////////////////////////////////////////

// TODO(babenko): move to a proper place

template <class T>
class TAsyncPipeline
    : public TRefCounted
{
public:
    TAsyncPipeline(
        IInvoker::TPtr invoker,
        TCallback< TIntrusivePtr < TFuture< TValueOrError<T> > >() > head)
        : Invoker(invoker)
        , Lazy(head)
    { }

    TIntrusivePtr< TFuture< TValueOrError<T> > > Run()
    {
        return Lazy.Run();
    }

    typedef T T1;

    template <class T2>
    TIntrusivePtr< TAsyncPipeline<T2> > Add(TCallback<T2(T1)> func)
    {
        auto wrappedFunc = BIND([=] (TValueOrError<T1> x) -> TIntrusivePtr< TFuture< TValueOrError<T2> > > {
            if (!x.IsOK()) {
                return MakeFuture(TValueOrError<T2>(TError(x)));
            }
            try {
                auto y = func.Run(x.Value());
                return MakeFuture(TValueOrError<T2>(y));
            } catch (const std::exception& ex) {
                return MakeFuture(TValueOrError<T2>(TError(ex.what())));
            }
        });

        if (Invoker) {
            wrappedFunc = wrappedFunc.AsyncVia(Invoker);
        }

        auto lazy = Lazy;
        auto newLazy = BIND([=] () {
            return lazy.Run()->Apply(wrappedFunc);
        });

        return New< TAsyncPipeline<T2> >(Invoker, newLazy);
    }

    template <class T2>
    TIntrusivePtr< TAsyncPipeline<T2> > Add(TCallback< TIntrusivePtr< TFuture<T2> >(T1) > func)
    {
        auto toValueOrError = BIND([] (T2 x) {
            return TValueOrError<T2>(x);
        });

        auto wrappedFunc = BIND([=] (TValueOrError<T1> x) -> TIntrusivePtr< TFuture< TValueOrError<T2> > > {
            if (!x.IsOK()) {
                return MakeFuture(TValueOrError<T2>(TError(x)));
            }
            try {
                auto y = func.Run(x.Value());
                return y->Apply(toValueOrError);
            } catch (const std::exception& ex) {
                return MakeFuture(TValueOrError<T2>(TError(ex.what())));
            }
        });

        if (Invoker) {
            wrappedFunc = wrappedFunc.AsyncVia(Invoker);
        }

        auto lazy = Lazy;
        auto newLazy = BIND([=] () {
            return lazy.Run()->Apply(wrappedFunc);
        });

        return New< TAsyncPipeline<T2> >(Invoker, newLazy);
    }


private:
    IInvoker::TPtr Invoker;
    TCallback< TIntrusivePtr < TFuture< TValueOrError<T> > >() > Lazy;

};

TIntrusivePtr< TAsyncPipeline<TVoid> > StartAsyncPipeline(IInvoker::TPtr invoker = NULL);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

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

class TOperationControllerBase
    : public IOperationController
{
public:
    TOperationControllerBase(
        TSchedulerConfigPtr config,
        IOperationHost* host,
        TOperation* operation);

    virtual void Initialize();
    virtual TFuture<void> Prepare();
    virtual TFuture<void> Revive();

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
    static void CheckResponse(TResponse response, const Stroka& failureMessage) 
    {
        if (response->IsOK())
            return;

        ythrow yexception() << failureMessage + "\n" + response->GetError().ToString();
    }


    // Here comes the preparation pipeline.

    // Round 1:
    // - Start primary transaction.

    NCypress::TCypressServiceProxy::TInvExecuteBatch StartPrimaryTransaction();

    void OnPrimaryTransactionStarted(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    // Round 2:
    // - Start input transaction.
    // - Start output transaction.

    NCypress::TCypressServiceProxy::TInvExecuteBatch StartSeconaryTransactions();

    void OnSecondaryTransactionsStarted(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    // Round 3:
    // - Fetch input tables.
    // - Lock input tables.
    // - Lock output tables.
    // - Fetch files.
    // - Get output tables schemata.
    // - Get output chunk lists.
    // - (Custom)

    NCypress::TCypressServiceProxy::TInvExecuteBatch RequestInputs();
    void OnInputsReceived(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    //! Extensibility point for requesting additional info from master.
    virtual void RequestCustomInputs(NCypress::TCypressServiceProxy::TReqExecuteBatch::TPtr batchReq);
    //! Extensibility point for handling additional info from master.
    virtual void OnCustomInputsRecieved(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    virtual std::vector<NYTree::TYPath> GetInputTablePaths() = 0;
    virtual std::vector<NYTree::TYPath> GetOutputTablePaths() = 0;
    virtual std::vector<NYTree::TYPath> GetFilePaths() = 0;


    // Round 4.
    // - (Custom)

    void CompletePreparation();

    virtual void DoCompletePreparation() = 0;


    // Here comes the completion pipeline.

    void FinalizeOperation();

    // Round 1.
    // - Attach chunk trees.
    // - (Custom)
    // - Commit input transaction.
    // - Commit output transaction.
    // - Commit primary transaction.

    NCypress::TCypressServiceProxy::TInvExecuteBatch CommitOutputs();
    void OnOutputsCommitted(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    //! Extensibility point for additional finalization logic.
    virtual void CommitCustomOutputs(NCypress::TCypressServiceProxy::TReqExecuteBatch::TPtr batchReq);
    //! Extensibility point for handling additional finalization outcome.
    virtual void OnCustomOutputsCommitted(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);

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

template <class Signature>
struct TAsyncPipelineSignatureCracker
{ };

template <class T1, class T2>
struct TAsyncPipelineSignatureCracker<T1(T2)>
{
    typedef T1 ReturnType;
    typedef T2 ArgType;
};

template <class T1>
struct TAsyncPipelineSignatureCracker<T1()>
{
    typedef T1 ReturnType;
    typedef void ArgType;
};

template <class ArgType, class ReturnType>
struct TAsyncPipelineHelpers
{
    static TFuture< TValueOrError<ReturnType> > Wrapper(TCallback<ReturnType(ArgType)> func, TValueOrError<ArgType> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TValueOrError<ReturnType>(TError(x)));
        }

        try {
            auto&& y = func.Run(x.Value());
            return MakeFuture(TValueOrError<ReturnType>(ForwardRV<ReturnType>(y)));
        } catch (const std::exception& ex) {
            return MakeFuture(TValueOrError<ReturnType>(TError(ex.what())));
        }
    }
};

template <class ArgType, class ReturnType>
struct TAsyncPipelineHelpers< ArgType, TFuture<ReturnType> >
{
    static TFuture< TValueOrError<ReturnType> > Wrapper(TCallback<TFuture<ReturnType>(ArgType)> func, TValueOrError<ArgType> x)
    {
        auto toValueOrError = BIND([] (ReturnType x) {
            return TValueOrError<ReturnType>(x);
        });

        if (!x.IsOK()) {
            return MakeFuture(TValueOrError<ReturnType>(TError(x)));
        }

        try {
            auto&& y = func.Run(x.Value());
            return y.Apply(toValueOrError);
        } catch (const std::exception& ex) {
            return MakeFuture(TValueOrError<ReturnType>(TError(ex.what())));
        }
    }
};

template <class ArgType>
struct TAsyncPipelineHelpers<ArgType, void>
{
    static TFuture< TValueOrError<void> > Wrapper(TCallback<void(ArgType)> func, TValueOrError<ArgType> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TValueOrError<void>(TError(x)));
        }

        try {
            func.Run(x.Value());
            return MakeFuture(TValueOrError<void>());
        } catch (const std::exception& ex) {
            return MakeFuture(TValueOrError<void>(TError(ex.what())));
        }
    }
};

template <>
struct TAsyncPipelineHelpers<void, void>
{
    static TFuture< TValueOrError<void> > Wrapper(TCallback<void(void)> func, TValueOrError<void> x)
    {
        if (!x.IsOK()) {
            return MakeFuture(TValueOrError<void>(TError(x)));
        }

        try {
            func.Run();
            return MakeFuture(TValueOrError<void>());
        } catch (const std::exception& ex) {
            return MakeFuture(TValueOrError<void>(TError(ex.what())));
        }
    }
};

template <class ReturnType>
struct TAsyncPipelineHelpers< void, TFuture<ReturnType> >
{
    static TFuture< TValueOrError<ReturnType> > Wrapper(TCallback<TFuture<ReturnType>()> func, TValueOrError<void> x)
    {
        auto toValueOrError = BIND([] (ReturnType x) {
            return TValueOrError<ReturnType>(x);
        });

        if (!x.IsOK()) {
            return MakeFuture(TValueOrError<ReturnType>(TError(x)));
        }

        try {
            auto&& y = func.Run();
            return y.Apply(toValueOrError);
        } catch (const std::exception& ex) {
            return MakeFuture(TValueOrError<ReturnType>(TError(ex.what())));
        }
    }
};

template <class T>
class TAsyncPipeline
    : public TRefCounted
{
public:
    TAsyncPipeline(
        IInvoker::TPtr invoker,
        TCallback< TFuture< TValueOrError<T> >() > head)
        : Invoker(invoker)
        , Lazy(head)
    { }

    TFuture< TValueOrError<T> > Run()
    {
        return Lazy.Run();
    }

    template <class Signature>
    TIntrusivePtr< TAsyncPipeline< typename NYT::NDetail::TFutureHelper< typename TAsyncPipelineSignatureCracker<Signature>::ReturnType >::TValueType > >
    Add(TCallback<Signature> func)
    {
        typedef typename TAsyncPipelineSignatureCracker<Signature>::ReturnType ReturnType;
        typedef typename TAsyncPipelineSignatureCracker<Signature>::ArgType ArgType;

        auto wrappedFunc = BIND(&TAsyncPipelineHelpers<ReturnType, ArgType>::Wrapper, func);

        if (Invoker) {
            wrappedFunc = wrappedFunc.AsyncVia(Invoker);
        }

        auto lazy = Lazy;
        auto newLazy = BIND([=] () {
            return lazy.Run().Apply(wrappedFunc);
        });

        return New< TAsyncPipeline<typename NYT::NDetail::TFutureHelper<ReturnType>::TValueType> >(Invoker, newLazy);
    }

private:
    IInvoker::TPtr Invoker;
    TCallback< TFuture< TValueOrError<T> >() > Lazy;

};

TIntrusivePtr< TAsyncPipeline<void> > StartAsyncPipeline(IInvoker::TPtr invoker = NULL);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

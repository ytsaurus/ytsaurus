#pragma once

#include "public.h"
#include "operation_controller.h"
#include "progress_counter.h"
#include "private.h"

#include <ytlib/logging/tagged_logger.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_server/table_ypath_proxy.h>
#include <ytlib/file_server/file_ypath_proxy.h>

namespace NYT {
namespace NScheduler {

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
        TInputTable()
            : NegateFetch(false)
            , Sorted(false)
        { }

        NYTree::TYPath Path;
        NTableServer::TTableYPathProxy::TRspFetch::TPtr FetchResponse;
        bool NegateFetch;
        bool Sorted;
    };

    std::vector<TInputTable> InputTables;

    // Output tables.
    struct TOutputTable
    {
        TOutputTable()
            : InitialRowCount(0)
            , SetSorted(false)
        { }

        i64 InitialRowCount;
        bool SetSorted;
        NYTree::TYPath Path;
        NYTree::TYson Schema;
        // Chunk list for appending the output.
        NChunkServer::TChunkListId OutputChunkListId;
        // Chunk trees comprising the output (the order matters).
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

    // Job handlers.
    struct TJobHandlers
        : public TIntrinsicRefCounted
    {
        TClosure OnCompleted;
        TClosure OnFailed;
    };

    typedef TIntrusivePtr<TJobHandlers> TJobHandlersPtr;

    yhash_map<TJobPtr, TJobHandlersPtr> JobHandlers;

    // The set of all input chunks. Used in #OnChunkFailed.
    yhash_set<NChunkServer::TChunkId> InputChunkIds;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(BackgroundThread);

    virtual void CustomInitialize();
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
    virtual void CustomRequestInputs(NCypress::TCypressServiceProxy::TReqExecuteBatch::TPtr batchReq);

    //! Extensibility point for handling additional info from master.
    virtual void OnCustomInputsRecieved(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    virtual std::vector<NYTree::TYPath> GetInputTablePaths() = 0;
    virtual std::vector<NYTree::TYPath> GetOutputTablePaths() = 0;
    virtual std::vector<NYTree::TYPath> GetFilePaths() = 0;


    // Round 4.
    // - (Custom)

    void CompletePreparation();

    virtual void CustomCompletePreparation() = 0;


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

    //! Called when a job is unable to read a chunk.
    void OnChunkFailed(const NChunkServer::TChunkId& chunkId);

    //! Called when a job is unable to read a chunk that is not a part of the input.
    /*!
     *  The default implementation calls |YUNREACHABLE|.
     */
    virtual void OnIntermediateChunkFailed(const NChunkServer::TChunkId& chunkId);

    //! Called when a job is unable to read an input chunk.
    /*!
     *  The operation fails immediately.
     */
    void OnInputChunkFailed(const NChunkServer::TChunkId& chunkId);

    // Abort is not a pipeline really :)

    virtual void AbortOperation();

    void AbortTransactions();


    void FailOperation(const TError& error);


    // Unsorted helpers.
    void CheckInputTablesSorted();
    void CheckOutputTablesEmpty();
    void SetOutputTablesSorted();
    bool CheckChunkListsPoolSize(int minSize);
    void ReleaseChunkList(const NChunkServer::TChunkListId& id);
    void ReleaseChunkLists(const std::vector<NChunkServer::TChunkListId>& ids);
    void OnChunkListsReleased(NCypress::TCypressServiceProxy::TRspExecuteBatch::TPtr batchRsp);
    static i64 GetJobWeightThreshold(i64 pendingJobs, i64 pendingWeight);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

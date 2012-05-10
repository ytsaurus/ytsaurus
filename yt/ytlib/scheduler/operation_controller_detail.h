#pragma once

#include "public.h"
#include "operation_controller.h"
#include "private.h"

#include <ytlib/logging/tagged_logger.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/async_pipeline.h>
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

    virtual void BuildProgressYson(NYTree::IYsonConsumer* consumer);
    virtual void BuildResultYson(NYTree::IYsonConsumer* consumer);

private:
    typedef TOperationControllerBase TThis;

protected:
    TSchedulerConfigPtr Config;
    IOperationHost* Host;
    TOperation* Operation;

    NObjectServer::TObjectServiceProxy ObjectProxy;
    NLog::TTaggedLogger Logger;

    // Remains True as long as the operation is not failed, completed, or aborted.
    bool Active;

    // Remains True as long as the operation can schedule new jobs.
    bool Running;

    // Fixed during init time, used to compute job count.
    int ExecNodeCount;

    // Job counters.
    int RunningJobCount;
    int CompletedJobCount;
    int FailedJobCount;

    TChunkListPoolPtr ChunkListPool;

    // The primary transaction for the whole operation (nested inside operation's transaction).
    NTransactionClient::ITransaction::TPtr PrimaryTransaction;
    // The transaction for reading input tables (nested inside the primary one).
    // These tables are locked with Snapshot mode.
    NTransactionClient::ITransaction::TPtr InputTransaction;
    // The transaction for writing output tables (nested inside the primary one).
    // These tables are locked with Shared mode.
    NTransactionClient::ITransaction::TPtr OutputTransaction;

    struct TTableBase
    {
        NYTree::TYPath Path;
        NObjectServer::TObjectId ObjectId;
    };

    // Input tables.
    struct TInputTable
        : TTableBase
    {
        TInputTable()
            : NegateFetch(false)
            , Sorted(false)
        { }

        NTableServer::TTableYPathProxy::TRspFetch::TPtr FetchResponse;
        bool NegateFetch;
        bool Sorted;
        std::vector<Stroka> KeyColumns;
    };

    std::vector<TInputTable> InputTables;

    // Output tables.
    struct TOutputTable
        : TTableBase
    {
        TOutputTable()
            : InitialRowCount(0)
            , SetSorted(false)
        { }

        i64 InitialRowCount;
        bool SetSorted;
        std::vector<Stroka> KeyColumns;
        NYTree::TYson Channels;
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

    // Jobs in progress.
    struct TJobInProgress
        : public TIntrinsicRefCounted
    {
        TJobPtr Job;
        TClosure OnCompleted;
        TClosure OnFailed;
    };

    typedef TIntrusivePtr<TJobInProgress> TJobInProgressPtr;

    yhash_map<TJobPtr, TJobInProgressPtr> JobsInProgress;

    // The set of all input chunks. Used in #OnChunkFailed.
    yhash_set<NChunkServer::TChunkId> InputChunkIds;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(BackgroundThread);

    // Jobs in progress management.
    template <class TConcreteJip>
    TJobPtr CreateJob(
        TIntrusivePtr<TConcreteJip> jip,
        TExecNodePtr node, 
        const NProto::TJobSpec& spec,
        TCallback<void(TConcreteJip*)> onCompleted,
        TCallback<void(TConcreteJip*)> onFailed)
    {
        jip->Job = Host->CreateJob(Operation, node, spec);
        // Pass jip to handlers via raw pointer to avoid cyclic references.
        jip->OnCompleted = BIND(onCompleted, Unretained(~jip));
        jip->OnFailed = BIND(onFailed, Unretained(~jip));
        YVERIFY(JobsInProgress.insert(MakePair(jip->Job, jip)).second);
        return jip->Job;
    }

    TJobInProgressPtr GetJobInProgress(TJobPtr job);
    void RemoveJobInProgress(TJobPtr job);

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

    NObjectServer::TObjectServiceProxy::TInvExecuteBatch StartPrimaryTransaction();

    void OnPrimaryTransactionStarted(NObjectServer::TObjectServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    // Round 2:
    // - Start input transaction.
    // - Start output transaction.

    NObjectServer::TObjectServiceProxy::TInvExecuteBatch StartSeconaryTransactions();

    void OnSecondaryTransactionsStarted(NObjectServer::TObjectServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    // Round 3:
    // - Get input table ids
    // - Get output table ids
    NObjectServer::TObjectServiceProxy::TInvExecuteBatch GetObjectIds();

    void OnObjectIdsReceived(NObjectServer::TObjectServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    // Round 4:
    // - Fetch input tables.
    // - Lock input tables.
    // - Lock output tables.
    // - Fetch files.
    // - Get output tables channels.
    // - Get output chunk lists.
    // - (Custom)

    NObjectServer::TObjectServiceProxy::TInvExecuteBatch RequestInputs();
    void OnInputsReceived(NObjectServer::TObjectServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    //! Extensibility point for requesting additional info from master.
    virtual void CustomRequestInputs(NObjectServer::TObjectServiceProxy::TReqExecuteBatch::TPtr batchReq);

    //! Extensibility point for handling additional info from master.
    virtual void OnCustomInputsRecieved(NObjectServer::TObjectServiceProxy::TRspExecuteBatch::TPtr batchRsp);


    // Round 4.
    // - (Custom)
    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline);

    // Round 5.
    // - Init chunk list pool.
    void CompletePreparation();
    void OnPreparationCompleted();

    // Here comes the completion pipeline.

    void FinalizeOperation();

    // Round 1.
    // - Attach chunk trees.
    // - (Custom)
    // - Commit input transaction.
    // - Commit output transaction.
    // - Commit primary transaction.

    NObjectServer::TObjectServiceProxy::TInvExecuteBatch CommitOutputs();
    void OnOutputsCommitted(NObjectServer::TObjectServiceProxy::TRspExecuteBatch::TPtr batchRsp);

    //! Extensibility point for additional finalization logic.
    virtual void CommitCustomOutputs(NObjectServer::TObjectServiceProxy::TReqExecuteBatch::TPtr batchReq);

    //! Extensibility point for handling additional finalization outcome.
    virtual void OnCustomOutputsCommitted(NObjectServer::TObjectServiceProxy::TRspExecuteBatch::TPtr batchRsp);


    virtual void CustomInitialize();
    virtual void LogProgress() = 0;
    virtual void DoGetProgress(NYTree::IYsonConsumer* consumer) = 0;

    //! Called to extract input table paths from the spec.
    virtual std::vector<NYTree::TYPath> GetInputTablePaths() = 0;
    //! Called to extract output table paths from the spec.
    virtual std::vector<NYTree::TYPath> GetOutputTablePaths() = 0;
    //! Called to extract file paths from the spec.
    virtual std::vector<NYTree::TYPath> GetFilePaths();

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
    std::vector<Stroka> GetInputKeyColumns();
    void SetOutputTablesSorted(const std::vector<Stroka>& keyColumns);
    bool CheckChunkListsPoolSize(int minSize);
    void ReleaseChunkList(const NChunkServer::TChunkListId& id);
    void ReleaseChunkLists(const std::vector<NChunkServer::TChunkListId>& ids);
    void OnChunkListsReleased(NObjectServer::TObjectServiceProxy::TRspExecuteBatch::TPtr batchRsp);
    static i64 GetJobWeightThreshold(int pendingJobCount, i64 pendingWeight);
    static int GetJobCount(
        i64 totalWeight,
        i64 weightPerJob,
        TNullable<int> configJobCount,
        int chunkCount);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

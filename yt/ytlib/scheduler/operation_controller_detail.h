#pragma once

#include "public.h"
#include "operation_controller.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "private.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/nullable.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/actions/async_pipeline.h>

#include <ytlib/chunk_server/public.h>

#include <ytlib/table_server/table_ypath_proxy.h>

#include <ytlib/file_server/file_ypath_proxy.h>

#include <ytlib/scheduler/scheduler_service.pb.h>

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
    virtual TFuture<void> Commit();

    virtual void OnJobRunning(TJobPtr job);
    virtual void OnJobCompleted(TJobPtr job);
    virtual void OnJobFailed(TJobPtr job);

    virtual void Abort();

    virtual TJobPtr ScheduleJob(TExecNodePtr node);

    virtual int GetPendingJobCount();

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
    NTransactionClient::ITransactionPtr PrimaryTransaction;
    // The transaction for reading input tables (nested inside the primary one).
    // These tables are locked with Snapshot mode.
    NTransactionClient::ITransactionPtr InputTransaction;
    // The transaction for writing output tables (nested inside the primary one).
    // These tables are locked with Shared mode.
    NTransactionClient::ITransactionPtr OutputTransaction;

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

        NTableServer::TTableYPathProxy::TRspFetchPtr FetchResponse;
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
            , Clear(false)
        { }

        i64 InitialRowCount;
        bool SetSorted;
        bool Clear;
        std::vector<Stroka> KeyColumns;
        NYTree::TYsonString Channels;

        // Chunk list for appending the output.
        NChunkServer::TChunkListId OutputChunkListId;

        //! Chunk trees comprising the output (the order matters).
        //! Keys are used when the output is sorted (e.g. in sort operations).
        //! Trees are sorted w.r.t. key and appended to #OutputChunkListId.
        std::multimap<int, NChunkServer::TChunkTreeId> OutputChunkTreeIds;
    };

    std::vector<TOutputTable> OutputTables;

    // Files.
    struct TFile
    {
        NYTree::TYPath Path;
        NFileServer::TFileYPathProxy::TRspFetchPtr FetchResponse;
    };

    std::vector<TFile> Files;

    // Forward declarations.

    class TTask;
    typedef TIntrusivePtr<TTask> TTaskPtr;

    struct TJobInProgress;
    typedef TIntrusivePtr<TJobInProgress> TJobInProgressPtr;

    // Jobs in progress.
    struct TJobInProgress
        : public TIntrinsicRefCounted
    {
        explicit TJobInProgress(TTaskPtr task)
            : Task(task)
        { }

        TTaskPtr Task;
        TJobPtr Job;
        TPoolExtractionResultPtr PoolResult;
        std::vector<NChunkServer::TChunkListId> ChunkListIds;
    };

    yhash_map<TJobPtr, TJobInProgressPtr> JobsInProgress;

    // The set of all input chunks. Used in #OnChunkFailed.
    yhash_set<NChunkServer::TChunkId> InputChunkIds;

    // Tasks management.

    class TTask
        : public TRefCounted
    {
    public:
        explicit TTask(TOperationControllerBase* controller);

        virtual Stroka GetId() const = 0;
        virtual int GetPriority() const;
        virtual int GetPendingJobCount() const = 0;
        virtual int GetChunkListCountPerJob() const = 0;
        virtual TDuration GetLocalityTimeout() const = 0;
        virtual i64 GetLocality(const Stroka& address) const;

        virtual NProto::TNodeResources GetMinRequestedResources() const = 0;
        virtual NProto::TNodeResources GetRequestedResourcesForJip(TJobInProgressPtr jip) const;
        bool HasEnoughResources(TExecNodePtr node) const;

        DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, NonLocalRequestTime);

        virtual void AddStripe(TChunkStripePtr stripe);
        void AddStripes(const std::vector<TChunkStripePtr>& stripes);

        TJobPtr ScheduleJob(TExecNodePtr node);

        virtual void OnJobCompleted(TJobInProgressPtr jip);
        virtual void OnJobFailed(TJobInProgressPtr jip);

        virtual void OnTaskCompleted();

        bool IsPending() const;
        bool IsCompleted() const;

        const TProgressCounter& WeightCounter() const;
        const TProgressCounter& ChunkCounter() const;

    private:
        TOperationControllerBase* Controller;

    protected:
        NLog::TTaggedLogger& Logger;
        TAutoPtr<IChunkPool> ChunkPool;

        virtual TNullable<i64> GetJobWeightThreshold() const = 0;

        virtual void BuildJobSpec(
            TJobInProgressPtr jip,
            NProto::TJobSpec* jobSpec) = 0;

        virtual void OnJobStarted(TJobInProgressPtr jip);

        void AddPendingHint();
        virtual void AddInputLocalityHint(TChunkStripePtr stripe);

        static i64 GetJobWeightThresholdGeneric(int pendingJobCount, i64 pendingWeight);

        void AddSequentialInputSpec(NScheduler::NProto::TJobSpec* jobSpec, TJobInProgressPtr jip);
        void AddParallelInputSpec(NScheduler::NProto::TJobSpec* jobSpec, TJobInProgressPtr jip);
        void AddTabularOutputSpec(NScheduler::NProto::TJobSpec* jobSpec, TJobInProgressPtr jip, int tableIndex);

    private:
        void AddInputChunks(NScheduler::NProto::TTableInputSpec* inputSpec, TChunkStripePtr stripe);
    };

    yhash_set<TTaskPtr> PendingTasks;
    yhash_map<Stroka, yhash_set<TTaskPtr>> AddressToLocalTasks;

    void AddTaskLocalityHint(TTaskPtr task, const Stroka& address);
    void AddTaskLocalityHint(TTaskPtr task, TChunkStripePtr stripe);
    void AddTaskPendingHint(TTaskPtr task);

    TJobPtr DoScheduleJob(TExecNodePtr node);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(BackgroundThread);

    // Jobs in progress management.
    void RegisterJobInProgress(TJobInProgressPtr jip);
    TJobInProgressPtr GetJobInProgress(TJobPtr job);
    void RemoveJobInProgress(TJobPtr job);


    // TODO(babenko): YPath and RPC responses currently share no base class.
    template <class TResponse>
    static void CheckResponse(TResponse response, const Stroka& failureMessage) 
    {
        if (!response->IsOK()) {
            ythrow yexception() << failureMessage + "\n" + response->GetError().ToString();
        }
    }


    // Here comes the preparation pipeline.

    // Round 1:
    // - Start primary transaction.

    NObjectServer::TObjectServiceProxy::TInvExecuteBatch StartPrimaryTransaction();

    void OnPrimaryTransactionStarted(NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    // Round 2:
    // - Start input transaction.
    // - Start output transaction.

    NObjectServer::TObjectServiceProxy::TInvExecuteBatch StartSeconaryTransactions();

    void OnSecondaryTransactionsStarted(NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    // Round 3:
    // - Get input table ids
    // - Get output table ids
    NObjectServer::TObjectServiceProxy::TInvExecuteBatch GetObjectIds();

    void OnObjectIdsReceived(NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    // Round 4:
    // - Fetch input tables.
    // - Lock input tables.
    // - Lock output tables.
    // - Fetch files.
    // - Get output tables channels.
    // - Get output chunk lists.
    // - (Custom)

    NObjectServer::TObjectServiceProxy::TInvExecuteBatch RequestInputs();
    void OnInputsReceived(NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    //! Extensibility point for requesting additional info from master.
    virtual void CustomRequestInputs(NObjectServer::TObjectServiceProxy::TReqExecuteBatchPtr batchReq);

    //! Extensibility point for handling additional info from master.
    virtual void OnCustomInputsRecieved(NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);


    // Round 4.
    // - (Custom)
    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline);

    // Round 5.
    // - Init chunk list pool.
    void CompletePreparation();
    void OnPreparationCompleted();

    // Here comes the completion pipeline.

    // Round 1.
    // - Attach chunk trees.
    // - (Custom)
    // - Commit input transaction.
    // - Commit output transaction.
    // - Commit primary transaction.

    NObjectServer::TObjectServiceProxy::TInvExecuteBatch CommitOutputs();
    void OnOutputsCommitted(NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    //! Extensibility point for additional finalization logic.
    virtual void CommitCustomOutputs(NObjectServer::TObjectServiceProxy::TReqExecuteBatchPtr batchReq);

    //! Extensibility point for handling additional finalization outcome.
    virtual void OnCustomOutputsCommitted(NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);


    virtual void DoInitialize();
    virtual void LogProgress() = 0;
    virtual void DoGetProgress(NYTree::IYsonConsumer* consumer);

    //! Called to extract input table paths from the spec.
    virtual std::vector<NYTree::TYPath> GetInputTablePaths() = 0;
    
    //! Called to extract output table paths from the spec.
    virtual std::vector<NYTree::TYPath> GetOutputTablePaths() = 0;
    
    //! Called to extract file paths from the spec.
    virtual std::vector<NYTree::TYPath> GetFilePaths();


    //! Minimum resources that are needed to start any task.
    virtual NProto::TNodeResources GetMinRequestedResources() const = 0;


    //! Called when a job is unable to read a chunk.
    void OnChunkFailed(const NChunkServer::TChunkId& chunkId);

    //! Called when a job is unable to read an intermediate chunk
    //! (i.e. that is not a part of the input).
    /*!
     *  The default implementation fails the operation immediately.
     *  Those operations providing some fault tolerance for intermediate chunks
     *  must override this method.
     */
    virtual void OnIntermediateChunkFailed(const NChunkServer::TChunkId& chunkId);

    //! Called when a job is unable to read an input chunk.
    /*!
     *  The operation fails immediately.
     */
    void OnInputChunkFailed(const NChunkServer::TChunkId& chunkId);

    
    // Abort is not a pipeline really :)

    void AbortTransactions();


    virtual void OnOperationCompleted();
    virtual void OnOperationFailed(const TError& error);


    // Unsorted helpers.

    std::vector<Stroka> CheckInputTablesSorted(
        const TNullable< std::vector<Stroka> >& keyColumns);
    void CheckOutputTablesEmpty();
    void ScheduleClearOutputTables();
    void ScheduleSetOutputTablesSorted(const std::vector<Stroka>& keyColumns);
    void RegisterOutputChunkTree(
        const NChunkServer::TChunkTreeId& chunkTreeId,
        int key,
        int tableIndex);

    bool HasEnoughChunkLists(int minSize);

    void ReleaseChunkList(const NChunkServer::TChunkListId& id);
    void ReleaseChunkLists(const std::vector<NChunkServer::TChunkListId>& ids);

    //! Returns the list of all input chunks collected from all input tables.
    std::vector<NTableClient::TRefCountedInputChunkPtr> CollectInputTablesChunks();

    //! Converts a list of input chunks into a list of chunk stripes for further
    //! processing. Each stripe receives exactly one chunk (as suitable for most
    //! jobs except merge). Tries to slice chunks into smaller parts if
    //! sees necessary based on #desiredJobCount and #maxWeightPerJob.
    std::vector<TChunkStripePtr> PrepareChunkStripes(
        const std::vector<NTableClient::TRefCountedInputChunkPtr>& inputChunks,
        TNullable<int> jobCount,
        i64 jobSliceWeight);

    static int GetJobCount(
        i64 totalWeight,
        i64 weightPerJob,
        TNullable<int> configJobCount,
        int chunkCount);

    static void InitUserJobSpec(
        NScheduler::NProto::TUserJobSpec* proto,
        TUserJobSpecPtr config,
        const std::vector<TFile>& files);

    static NJobProxy::TJobIOConfigPtr BuildJobIOConfig(
        NJobProxy::TJobIOConfigPtr schedulerConfig,
        NYTree::INodePtr specConfigNode);

    static void InitIntermediateOutputConfig(NJobProxy::TJobIOConfigPtr config);

    static void InitIntermediateInputConfig(NJobProxy::TJobIOConfigPtr config);

private:
    static bool AreKeysCompatible(
        const std::vector<Stroka>& fullColumns,
        const std::vector<Stroka>& prefixColumns);

    void OnChunkListsReleased(NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

};

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(TOperation* operation)
{
    auto spec = New<TSpec>();
    try {
        spec->Load(operation->GetSpec());
    } catch (const std::exception& ex) {
        ythrow yexception() << Sprintf("Error parsing operation spec\n%s", ex.what());
    }
    return spec;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

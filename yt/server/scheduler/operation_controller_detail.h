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
#include <ytlib/actions/cancelable_context.h>

#include <ytlib/table_client/table_ypath_proxy.h>

#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/cypress_client/public.h>
#include <ytlib/ytree/ypath_client.h>

#include <server/chunk_server/public.h>

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

    virtual void Initialize() override;
    virtual TFuture<void> Prepare() override;
    virtual TFuture<void> Revive() override;
    virtual TFuture<void> Commit() override;

    virtual void OnJobRunning(TJobPtr job, const NProto::TJobStatus& status) override;
    virtual void OnJobCompleted(TJobPtr job) override;
    virtual void OnJobFailed(TJobPtr job) override;
    virtual void OnJobAborted(TJobPtr job) override;

    virtual void Abort() override;

    virtual TJobPtr ScheduleJob(ISchedulingContext* context) override;

    virtual TCancelableContextPtr GetCancelableContext() override;
    virtual IInvokerPtr GetCancelableControlInvoker() override;
    virtual IInvokerPtr GetCancelableBackgroundInvoker() override;

    virtual int GetPendingJobCount() override;
    virtual NProto::TNodeResources GetUsedResources() override;
    virtual NProto::TNodeResources GetNeededResources() override;

    virtual void BuildProgressYson(NYTree::IYsonConsumer* consumer) override;
    virtual void BuildResultYson(NYTree::IYsonConsumer* consumer) override;

private:
    typedef TOperationControllerBase TThis;

protected:
    TSchedulerConfigPtr Config;
    IOperationHost* Host;
    TOperation* Operation;

    NObjectClient::TObjectServiceProxy ObjectProxy;
    mutable NLog::TTaggedLogger Logger;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableControlInvoker;
    IInvokerPtr CancelableBackgroundInvoker;

    // Remains True as long as the operation is not finished.
    bool Active;

    // Remains True as long as the operation can schedule new jobs.
    bool Running;

    // Job counters.
    int RunningJobCount;
    int CompletedJobCount;
    int FailedJobCount;
    int AbortedJobCount;

    // Total resources used by all running jobs.
    NProto::TNodeResources UsedResources;

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
        NYPath::TRichYPath Path;
        NObjectClient::TObjectId ObjectId;
    };

    // Input tables.
    struct TInputTable
        : public TTableBase
    {
        TInputTable()
            : NegateFetch(false)
        { }

        NTableClient::TTableYPathProxy::TRspFetchPtr FetchResponse;
        bool NegateFetch;
        TNullable< std::vector<Stroka> > KeyColumns;
    };

    std::vector<TInputTable> InputTables;

    // Output tables.
    struct TOutputTable
        : public TTableBase
    {
        TOutputTable()
            : Clear(false)
            , Overwrite(false)
            , LockMode(NCypressClient::ELockMode::Shared)
        { }

        bool Clear;
        bool Overwrite;
        NCypressClient::ELockMode LockMode;
        TNullable< std::vector<Stroka> > KeyColumns;
        NYTree::TYsonString Channels;

        // Chunk list for appending the output.
        NChunkClient::TChunkListId OutputChunkListId;

        //! Chunk trees comprising the output (the order matters).
        //! Keys are used when the output is sorted (e.g. in sort operations).
        //! Trees are sorted w.r.t. key and appended to #OutputChunkListId.
        std::multimap<int, NChunkServer::TChunkTreeId> OutputChunkTreeIds;
    };

    std::vector<TOutputTable> OutputTables;

    // Files.
    struct TUserFile
    {
        NYPath::TRichYPath Path;
        NFileClient::TFileYPathProxy::TRspFetchFilePtr FetchResponse;
    };

    std::vector<TUserFile> Files;

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
        std::vector<NChunkClient::TChunkListId> ChunkListIds;
    };

    yhash_map<TJobPtr, TJobInProgressPtr> JobsInProgress;

    // The set of all input chunks. Used in #OnChunkFailed.
    yhash_set<NChunkClient::TChunkId> InputChunkIds;

    // Tasks management.

    class TTask
        : public TRefCounted
    {
    public:
        explicit TTask(TOperationControllerBase* controller);

        virtual Stroka GetId() const = 0;
        virtual int GetPriority() const;

        virtual int GetPendingJobCount() const = 0;
        int GetPendingJobCountDelta();

        virtual NProto::TNodeResources GetTotalNeededResources() const;
        NProto::TNodeResources GetTotalNeededResourcesDelta();
        
        virtual int GetChunkListCountPerJob() const = 0;
        
        virtual TDuration GetLocalityTimeout() const = 0;
        virtual i64 GetLocality(const Stroka& address) const;
        virtual bool IsStrictlyLocal() const;

        virtual NProto::TNodeResources GetMinNeededResources() const = 0;
        virtual NProto::TNodeResources GetAvgNeededResources() const;
        virtual NProto::TNodeResources GetNeededResources(TJobInProgressPtr jip) const;
        bool HasEnoughResources(TExecNodePtr node) const;

        DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, DelayedTime);

        virtual void AddStripe(TChunkStripePtr stripe);
        void AddStripes(const std::vector<TChunkStripePtr>& stripes);

        TJobPtr ScheduleJob(ISchedulingContext* context);

        virtual void OnJobCompleted(TJobInProgressPtr jip);
        virtual void OnJobFailed(TJobInProgressPtr jip);
        virtual void OnJobAborted(TJobInProgressPtr jip);

        virtual void OnTaskCompleted();

        bool IsPending() const;
        bool IsCompleted() const;

        const TProgressCounter& DataSizeCounter() const;
        const TProgressCounter& ChunkCounter() const;

    private:
        TOperationControllerBase* Controller;
        int CachedPendingJobCount;
        NProto::TNodeResources CachedTotalNeededResources;

    protected:
        NLog::TTaggedLogger& Logger;
        TAutoPtr<IChunkPool> ChunkPool;

        virtual TNullable<i64> GetJobDataSizeThreshold() const = 0;

        virtual void BuildJobSpec(
            TJobInProgressPtr jip,
            NProto::TJobSpec* jobSpec) = 0;

        virtual void OnJobStarted(TJobInProgressPtr jip);

        void AddPendingHint();
        virtual void AddInputLocalityHint(TChunkStripePtr stripe);

        static i64 GetJobDataSizeThresholdGeneric(int pendingJobCount, i64 pendingWeight);

        void AddSequentialInputSpec(NScheduler::NProto::TJobSpec* jobSpec, TJobInProgressPtr jip);
        void AddParallelInputSpec(NScheduler::NProto::TJobSpec* jobSpec, TJobInProgressPtr jip);
        void AddOutputSpecs(NScheduler::NProto::TJobSpec* jobSpec, TJobInProgressPtr jip);
        void AddIntermediateOutputSpec(NScheduler::NProto::TJobSpec* jobSpec, TJobInProgressPtr jip);

    private:
        void ReleaseFailedJob(TJobInProgressPtr jip);
        void AddInputChunks(NScheduler::NProto::TTableInputSpec* inputSpec, TChunkStripePtr stripe);
    };

    struct TPendingTaskInfo
    {
        yhash_set<TTaskPtr> GlobalTasks;
        yhash_map<Stroka, yhash_set<TTaskPtr>> AddressToLocalTasks;
    };

    static const int MaxTaskPriority = 2;
    std::vector<TPendingTaskInfo> PendingTaskInfos;

    int CachedPendingJobCount;
    NProto::TNodeResources CachedNeededResources;

    void OnTaskUpdated(TTaskPtr task);

    void DoAddTaskLocalityHint(TTaskPtr task, const Stroka& address);
    void AddTaskLocalityHint(TTaskPtr task, const Stroka& address);
    void AddTaskLocalityHint(TTaskPtr task, TChunkStripePtr stripe);
    void AddTaskPendingHint(TTaskPtr task);
    TPendingTaskInfo* GetPendingTaskInfo(TTaskPtr task);

    TJobPtr DoScheduleJob(ISchedulingContext* context);
    void OnJobStarted(TJobPtr job);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(BackgroundThread);

    // Jobs in progress management.
    void RegisterJobInProgress(TJobInProgressPtr jip);
    TJobInProgressPtr GetJobInProgress(TJobPtr job);
    void RemoveJobInProgress(TJobPtr job);

    // Here comes the preparation pipeline.

    // Round 1:
    // - Start primary transaction.

    NObjectClient::TObjectServiceProxy::TInvExecuteBatch StartPrimaryTransaction();

    void OnPrimaryTransactionStarted(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    // Round 2:
    // - Start input transaction.
    // - Start output transaction.

    NObjectClient::TObjectServiceProxy::TInvExecuteBatch StartSeconaryTransactions();

    void OnSecondaryTransactionsStarted(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    // Round 3:
    // - Get input table ids
    // - Get output table ids
    NObjectClient::TObjectServiceProxy::TInvExecuteBatch GetObjectIds();

    void OnObjectIdsReceived(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    // Round 4:
    // - Fetch input tables.
    // - Lock input tables.
    // - Lock output tables.
    // - Fetch files.
    // - Get output tables channels.
    // - Get output chunk lists.
    // - (Custom)

    NObjectClient::TObjectServiceProxy::TInvExecuteBatch RequestInputs();
    void OnInputsReceived(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    //! Extensibility point for requesting additional info from master.
    virtual void RequestCustomInputs(NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr batchReq);

    //! Extensibility point for handling additional info from master.
    virtual void OnCustomInputsRecieved(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);


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
    // - Commit input transaction.
    // - Commit output transaction.
    // - Commit primary transaction.

    NObjectClient::TObjectServiceProxy::TInvExecuteBatch CommitOutputs();
    void OnOutputsCommitted(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    virtual void DoInitialize();
    virtual void LogProgress() = 0;

    //! Called to extract input table paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetInputTablePaths() const = 0;
    
    //! Called to extract output table paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetOutputTablePaths() const = 0;
    
    //! Called to extract file paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetFilePaths() const;


    //! Called when a job is unable to read a chunk.
    void OnChunkFailed(const NChunkClient::TChunkId& chunkId);

    //! Called when a job is unable to read an intermediate chunk
    //! (i.e. that is not a part of the input).
    /*!
     *  The default implementation fails the operation immediately.
     *  Those operations providing some fault tolerance for intermediate chunks
     *  must override this method.
     */
    virtual void OnIntermediateChunkFailed(const NChunkClient::TChunkId& chunkId);

    //! Called when a job is unable to read an input chunk.
    /*!
     *  The operation fails immediately.
     */
    void OnInputChunkFailed(const NChunkClient::TChunkId& chunkId);

    
    // Abort is not a pipeline really :)

    void AbortTransactions();


    virtual void OnOperationCompleted();
    virtual void OnOperationFailed(const TError& error);


    // Unsorted helpers.

    std::vector<Stroka> CheckInputTablesSorted(
        const TNullable< std::vector<Stroka> >& keyColumns);
    static bool CheckKeyColumnsCompatible(
        const std::vector<Stroka>& fullColumns,
        const std::vector<Stroka>& prefixColumns);
    void RegisterOutputChunkTree(
        const NChunkServer::TChunkTreeId& chunkTreeId,
        int key,
        int tableIndex);
    void RegisterOutputChunkTrees(
        TJobInProgressPtr jip,
        int key);

    bool HasEnoughChunkLists(int requestedCount);
    NChunkClient::TChunkListId ExtractChunkList();

    void ReleaseChunkList(const NChunkClient::TChunkListId& id);
    void ReleaseChunkLists(const std::vector<NChunkClient::TChunkListId>& ids);

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

    int SuggestJobCount(
        i64 totalDataSize,
        i64 minDataSizePerJob,
        i64 maxDataSizePerJob,
        TNullable<int> configJobCount,
        int chunkCount);

    static void InitUserJobSpec(
        NScheduler::NProto::TUserJobSpec* proto,
        TUserJobSpecPtr config,
        const std::vector<TUserFile>& files);

    static void InitIntermediateOutputConfig(TJobIOConfigPtr config);

    static void InitIntermediateInputConfig(TJobIOConfigPtr config);

private:
    TChunkListPoolPtr ChunkListPool;

    void OnChunkListsReleased(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

};

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(TOperation* operation, NYTree::INodePtr defaultSpec)
{
    auto ysonSpec = NYTree::UpdateNode(defaultSpec, operation->GetSpec());
    auto spec = New<TSpec>();
    try {
        spec->Load(ysonSpec);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing operation spec") << ex;
    }
    return spec;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

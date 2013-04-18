#pragma once

#include "public.h"
#include "operation_controller.h"
#include "chunk_pool.h"
#include "chunk_list_pool.h"
#include "job_resources.h"
#include "private.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/misc/id_generator.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/actions/async_pipeline.h>
#include <ytlib/actions/cancelable_context.h>

#include <ytlib/table_client/table_ypath_proxy.h>
#include <ytlib/table_client/config.h>

#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/cypress_client/public.h>

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/yson_string.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/node_tracker_client/public.h>

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
        TOperationSpecBasePtr spec,
        IOperationHost* host,
        TOperation* operation);

    virtual void Initialize() override;
    virtual TFuture<TError> Prepare() override;
    virtual void SaveSnapshot(TOutputStream* stream) override;
    virtual TFuture<TError> Revive(TInputStream* steam) override;
    virtual TFuture<TError> Commit() override;

    virtual void OnJobRunning(TJobPtr job, const NProto::TJobStatus& status) override;
    virtual void OnJobCompleted(TJobPtr job) override;
    virtual void OnJobFailed(TJobPtr job) override;
    virtual void OnJobAborted(TJobPtr job) override;

    virtual void OnNodeOnline(TExecNodePtr node) override;
    virtual void OnNodeOffline(TExecNodePtr node) override;

    virtual void Abort() override;

    virtual TJobPtr ScheduleJob(ISchedulingContext* context, const NProto::TNodeResources& jobLimits) override;

    virtual TCancelableContextPtr GetCancelableContext() override;
    virtual IInvokerPtr GetCancelableControlInvoker() override;
    virtual IInvokerPtr GetCancelableBackgroundInvoker() override;

    virtual int GetPendingJobCount() override;
    virtual NProto::TNodeResources GetNeededResources() override;

    virtual void BuildProgressYson(NYson::IYsonConsumer* consumer) override;
    virtual void BuildResultYson(NYson::IYsonConsumer* consumer) override;

private:
    typedef TOperationControllerBase TThis;

protected:
    // Forward declarations.
    struct TTaskGroup;

    class TTask;
    typedef TIntrusivePtr<TTask> TTaskPtr;

    struct TJoblet;
    typedef TIntrusivePtr<TJoblet> TJobletPtr;

    struct TCompletedJob;
    typedef TIntrusivePtr<TCompletedJob> TCompleteJobPtr;


    TSchedulerConfigPtr Config;
    IOperationHost* Host;
    TOperation* Operation;

    NObjectClient::TObjectServiceProxy ObjectProxy;
    mutable NLog::TTaggedLogger Logger;

    TCancelableContextPtr CancelableContext;
    IInvokerPtr CancelableControlInvoker;
    IInvokerPtr CancelableBackgroundInvoker;

    // Remains True as long as the operation can schedule new jobs.
    bool Running;

    // Totals.
    int TotalInputChunkCount;
    i64 TotalInputDataSize;
    i64 TotalInputRowCount;
    i64 TotalInputValueCount;

    // Job counters.
    TProgressCounter JobCounter;

    // Maps node ids seen in fetch responses to node descriptors.
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;

    struct TTableBase
    {
        NYPath::TRichYPath Path;
        NObjectClient::TObjectId ObjectId;
    };

    struct TInputTable
        : public TTableBase
    {
        TInputTable()
            : ComplementFetch(false)
        { }

        NTableClient::TTableYPathProxy::TRspFetchPtr FetchResponse;
        bool ComplementFetch;
        TNullable< std::vector<Stroka> > KeyColumns;
    };

    std::vector<TInputTable> InputTables;


    struct TOutputTable
        : public TTableBase
    {
        TOutputTable()
            : Clear(false)
            , Overwrite(false)
            , LockMode(NCypressClient::ELockMode::Shared)
            , Options(New<NTableClient::TTableWriterOptions>())
        { }

        bool Clear;
        bool Overwrite;
        NCypressClient::ELockMode LockMode;
        NTableClient::TTableWriterOptionsPtr Options;

        // Chunk list for appending the output.
        NChunkClient::TChunkListId OutputChunkListId;

        //! Chunk trees comprising the output (the order matters).
        //! Keys are used when the output is sorted (e.g. in sort operations).
        //! Trees are sorted w.r.t. key and appended to #OutputChunkListId.
        std::multimap<int, NChunkServer::TChunkTreeId> OutputChunkTreeIds;


        struct TEndpoint
        {
            NChunkClient::NProto::TKey Key;
            bool Left;
            int ChunkTreeKey;
        };

        std::vector<TEndpoint> Endpoints;
    };

    std::vector<TOutputTable> OutputTables;


    //! Describes which part of the operation needs a particular file.
    DECLARE_ENUM(EOperationStage,
        (Map)
        (Reduce)
    );

    struct TUserFile
    {
        NYPath::TRichYPath Path;
        EOperationStage Stage;
    };


    struct TRegularUserFile
        : public TUserFile
    {
        NFileClient::TFileYPathProxy::TRspFetchFilePtr FetchResponse;
    };

    std::vector<TRegularUserFile> RegularFiles;


    struct TUserTableFile
        : public TUserFile
    {
        NTableClient::TTableYPathProxy::TRspFetchPtr FetchResponse;
        Stroka FileName;
        NYTree::TYsonString Format;
    };

    std::vector<TUserTableFile> TableFiles;


    struct TJoblet
        : public TIntrinsicRefCounted
    {
        explicit TJoblet(TTaskPtr task, int jobIndex)
            : Task(task)
            , JobIndex(jobIndex)
            , StartRowIndex(-1)
            , OutputCookie(IChunkPoolOutput::NullCookie)
        { }

        TTaskPtr Task;
        int JobIndex;
        i64 StartRowIndex;

        TJobPtr Job;
        TChunkStripeListPtr InputStripeList;
        IChunkPoolOutput::TCookie OutputCookie;

        //! All chunk lists allocated for this job.
        /*!
         *  For jobs with intermediate output this list typically contains one element.
         *  For jobs with final output this list typically contains one element per each output table.
         */
        std::vector<NChunkClient::TChunkListId> ChunkListIds;

        //! Chunk stripe constructed from job result.
        TChunkStripePtr OutputStripe;
    };

    struct TCompletedJob
        : public TIntrinsicRefCounted
    {
        TCompletedJob(
            const TJobId& jobId,
            TTaskPtr sourceTask,
            IChunkPoolOutput::TCookie outputCookie,
            IChunkPoolInput* destinationPool,
            IChunkPoolInput::TCookie inputCookie,
            TExecNodePtr execNode)
            : IsLost(false)
            , JobId(jobId)
            , SourceTask(std::move(sourceTask))
            , OutputCookie(outputCookie)
            , DestinationPool(destinationPool)
            , InputCookie(inputCookie)
            , ExecNode(std::move(execNode))
        { }

        bool IsLost;

        TJobId JobId;

        TTaskPtr SourceTask;
        IChunkPoolOutput::TCookie OutputCookie;

        IChunkPoolInput* DestinationPool;
        IChunkPoolInput::TCookie InputCookie;

        TExecNodePtr ExecNode;
    };


    class TTask
        : public TRefCounted
    {
    public:
        explicit TTask(TOperationControllerBase* controller);

        virtual Stroka GetId() const = 0;
        virtual TTaskGroup* GetGroup() const = 0;

        virtual int GetPendingJobCount() const;
        int GetPendingJobCountDelta();

        virtual NProto::TNodeResources GetTotalNeededResources() const;
        NProto::TNodeResources GetTotalNeededResourcesDelta();

        virtual int GetChunkListCountPerJob() const = 0;

        virtual TDuration GetLocalityTimeout() const = 0;
        virtual i64 GetLocality(const Stroka& address) const;
        virtual bool HasInputLocality();

        const NProto::TNodeResources& GetMinNeededResources() const;
        virtual NProto::TNodeResources GetNeededResources(TJobletPtr joblet) const;

        DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, DelayedTime);

        IChunkPoolInput::TCookie AddInput(TChunkStripePtr stripe);
        void AddInput(const std::vector<TChunkStripePtr>& stripes);
        void ResumeInput(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe);
        void FinishInput();

        void CheckCompleted();

        TJobPtr ScheduleJob(ISchedulingContext* context, const NProto::TNodeResources& jobLimits);

        virtual void OnJobCompleted(TJobletPtr joblet);
        virtual void OnJobFailed(TJobletPtr joblet);
        virtual void OnJobAborted(TJobletPtr joblet);
        virtual void OnJobLost(TCompleteJobPtr completedJob);

        // First checks against given node, then againts all nodes if needed.
        void CheckResourceDemandSanity(
            TExecNodePtr node,
            const NProto::TNodeResources& neededResources);

        // Checks against all available nodes.
        void CheckResourceDemandSanity(
            const NProto::TNodeResources& neededResources);

        void DoCheckResourceDemandSanity(const NProto::TNodeResources& neededResources);

        bool IsPending() const;
        bool IsCompleted() const;

        i64 GetTotalDataSize() const;
        i64 GetCompletedDataSize() const;
        i64 GetPendingDataSize() const;

        virtual IChunkPoolInput* GetChunkPoolInput() const = 0;
        virtual IChunkPoolOutput* GetChunkPoolOutput() const = 0;

    private:
        TOperationControllerBase* Controller;

        int CachedPendingJobCount;
        NProto::TNodeResources CachedTotalNeededResources;
        mutable TNullable<NProto::TNodeResources> CachedMinNeededResources;

        TInstant LastDemandSanityCheckTime;
        bool CompletedFired;

        //! For each lost job currently being replayed, maps output cookie to corresponding input cookie.
        yhash_map<IChunkPoolOutput::TCookie, IChunkPoolInput::TCookie> LostJobCookieMap;

    protected:
        NLog::TTaggedLogger& Logger;

        virtual NProto::TNodeResources GetMinNeededResourcesHeavy() const = 0;

        virtual void OnTaskCompleted();

        virtual EJobType GetJobType() const = 0;
        virtual void PrepareJoblet(TJobletPtr joblet);
        virtual void BuildJobSpec(TJobletPtr joblet, NProto::TJobSpec* jobSpec) = 0;

        virtual void OnJobStarted(TJobletPtr joblet);

        void AddPendingHint();
        void AddLocalityHint(const Stroka& address);

        DECLARE_ENUM(EJobReinstallReason,
            (Failed)
            (Aborted)
        );

        void ReinstallJob(TJobletPtr joblet, EJobReinstallReason reason);

        void AddSequentialInputSpec(
            NScheduler::NProto::TJobSpec* jobSpec,
            TJobletPtr joblet,
            bool enableTableIndex = false);
        void AddParallelInputSpec(
            NScheduler::NProto::TJobSpec* jobSpec,
            TJobletPtr joblet,
            bool enableTableIndex = false);
        static void AddChunksToInputSpec(
            NNodeTrackerClient::TNodeDirectoryBuilder* directoryBuilder,
            NScheduler::NProto::TTableInputSpec* inputSpec,
            TChunkStripePtr stripe,
            TNullable<int> partitionTag,
            bool enableTableIndex);

        void AddFinalOutputSpecs(NScheduler::NProto::TJobSpec* jobSpec, TJobletPtr joblet);
        void AddIntermediateOutputSpec(NScheduler::NProto::TJobSpec* jobSpec, TJobletPtr joblet);

        static void UpdateInputSpecTotals(
            NScheduler::NProto::TJobSpec* jobSpec,
            TJobletPtr joblet);

        void RegisterIntermediateChunks(
            TJobletPtr joblet,
            IChunkPoolInput* destinationPool);

        static TChunkStripePtr BuildIntermediateChunkStripe(
            google::protobuf::RepeatedPtrField<NChunkClient::NProto::TInputChunk>* inputChunks);

        void RegisterOutput(TJobletPtr joblet, int key);

    };

    virtual void CustomizeJoblet(TJobletPtr joblet);
    virtual void CustomizeJobSpec(TJobletPtr joblet, NProto::TJobSpec* jobSpec);


    //! Groups serve two purposes:
    //! * Provide means to prioritize tasks
    //! * Quickly skip a vast number of tasks whose resource requirements cannot be met
    struct TTaskGroup
    {
        TTaskGroup()
            : MinNeededResources(ZeroNodeResources())
        { }

        //! No task from this group is considered for scheduling unless this
        //! requirement is met.
        NProto::TNodeResources MinNeededResources;

        //! All non-local tasks.
        yhash_set<TTaskPtr> NonLocalTasks;

        //! Non-local tasks that may possibly be ready (but a delayed check is still needed)
        //! keyed by min memory demand (as reported by TTask::GetMinNeededResources).
        std::multimap<i64, TTaskPtr> CandidateTasks;

        //! Non-local tasks keyed by deadline.
        std::multimap<TInstant, TTaskPtr> DelayedTasks;

        //! Local tasks keyed by address.
        yhash_map<Stroka, yhash_set<TTaskPtr>> LocalTasks;
    };

    void RegisterTaskGroup(TTaskGroup* group);


    void OnTaskUpdated(TTaskPtr task);

    void DoAddTaskLocalityHint(TTaskPtr task, const Stroka& address);
    void AddTaskLocalityHint(TTaskPtr task, const Stroka& address);
    void AddTaskLocalityHint(TTaskPtr task, TChunkStripePtr stripe);
    void AddTaskPendingHint(TTaskPtr task);
    void ResetTaskLocalityDelays();

    void MoveTaskToCandidates(TTaskPtr task, std::multimap<i64, TTaskPtr>& candidateTasks);

    bool CheckJobLimits(TExecNodePtr node, TTaskPtr task, const NProto::TNodeResources& jobLimits);

    TJobPtr DoScheduleJob(ISchedulingContext* context, const NProto::TNodeResources& jobLimits);
    TJobPtr DoScheduleLocalJob(ISchedulingContext* context, const NProto::TNodeResources& jobLimits);
    TJobPtr DoScheduleNonLocalJob(ISchedulingContext* context, const NProto::TNodeResources& jobLimits);

    void OnJobStarted(TJobPtr job);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(BackgroundThread);

    // Jobs in progress management.
    void RegisterJoblet(TJobletPtr joblet);
    TJobletPtr GetJoblet(TJobPtr job);
    void RemoveJoblet(TJobPtr job);

    // Here comes the preparation pipeline.

    // Round 1:
    // - Get input table ids
    // - Get output table ids
    NObjectClient::TObjectServiceProxy::TInvExecuteBatch GetObjectIds();

    void OnObjectIdsReceived(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    // Round 2:
    // - Request file types
    // - Check that input and output are tables
    NObjectClient::TObjectServiceProxy::TInvExecuteBatch GetInputTypes();

    void OnInputTypesReceived(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    // Round 3:
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

    // Round 5.
    // - (Custom)
    virtual TAsyncPipeline<void>::TPtr CustomizePreparationPipeline(TAsyncPipeline<void>::TPtr pipeline);

    // Round 6.
    // - Collect totals.
    // - Check for empty inputs.
    // - Init chunk list pool.
    TFuture<void> CompletePreparation();


    // Here comes the completion pipeline.

    // Round 1.
    // - Sort parts of output, if needed.
    // - Attach chunk trees.

    NObjectClient::TObjectServiceProxy::TInvExecuteBatch CommitResults();
    void OnResultsCommitted(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);


    virtual void DoInitialize();

    //! Called to extract input table paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetInputTablePaths() const = 0;

    //! Called to extract output table paths from the spec.
    virtual std::vector<NYPath::TRichYPath> GetOutputTablePaths() const = 0;

    typedef std::pair<NYPath::TRichYPath, EOperationStage> TPathWithStage;

    //! Called to extract file paths from the spec.
    virtual std::vector<TPathWithStage> GetFilePaths() const;

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

    void OnOperationCompleted();
    virtual void DoOperationCompleted();

    void OnOperationFailed(const TError& error);
    virtual void DoOperationFailed(const TError& error);


    // Unsorted helpers.

    // Enables sorted output from user jobs.
    virtual bool IsSortedOutputSupported() const;

    std::vector<Stroka> CheckInputTablesSorted(
        const TNullable< std::vector<Stroka> >& keyColumns);
    static bool CheckKeyColumnsCompatible(
        const std::vector<Stroka>& fullColumns,
        const std::vector<Stroka>& prefixColumns);

    void RegisterOutput(
        const NChunkServer::TChunkTreeId& chunkTreeId,
        int key,
        int tableIndex);
    void RegisterOutput(
        const NChunkServer::TChunkTreeId& chunkTreeId,
        int key,
        int tableIndex,
        TOutputTable& table);
    void RegisterOutput(
        TJobletPtr joblet,
        int key);

    bool HasEnoughChunkLists(int requestedCount);
    NChunkClient::TChunkListId ExtractChunkList();

    //! Returns the list of all input chunks collected from all input tables.
    std::vector<NChunkClient::TRefCountedInputChunkPtr> CollectInputChunks();

    //! Converts a list of input chunks into a list of chunk stripes for further
    //! processing. Each stripe receives exactly one chunk (as suitable for most
    //! jobs except merge). The resulting stripes are of approximately equal
    //! size. The size per stripe is either |maxSliceDataSize| or
    //! |TotalInputDataSize / jobCount|, whichever is smaller. If the resulting
    //! list contains less than |jobCount| stripes then |jobCount| is decreased
    //! appropriately.
    std::vector<TChunkStripePtr> SliceInputChunks(i64 maxSliceDataSize, int* jobCount);

    int SuggestJobCount(
        i64 totalDataSize,
        i64 dataSizePerJob,
        TNullable<int> configJobCount) const;

    void InitUserJobSpec(
        NScheduler::NProto::TUserJobSpec* proto,
        TUserJobSpecPtr config,
        const std::vector<TRegularUserFile>& regularFiles,
        const std::vector<TUserTableFile>& tableFiles);

    static void AddUserJobEnvironment(
        NScheduler::NProto::TUserJobSpec* proto,
        TJobletPtr joblet);

    static void InitIntermediateInputConfig(TJobIOConfigPtr config);

    static void InitIntermediateOutputConfig(TJobIOConfigPtr config);
    void InitFinalOutputConfig(TJobIOConfigPtr config);

private:
    TOperationSpecBasePtr Spec;
    TChunkListPoolPtr ChunkListPool;

    int CachedPendingJobCount;
    NProto::TNodeResources CachedNeededResources;

    //! All tasks declared by calling #RegisterTaskGroup, in the order of decreasing priority.
    std::vector<TTaskGroup*> TaskGroups;

    //! Maps intermediate chunk id to its originating completed job.
    yhash_map<NChunkServer::TChunkId, TCompleteJobPtr> ChunkOriginMap;

    //! Maps scheduler's jobs to controller's joblets.
    yhash_map<TJobPtr, TJobletPtr> JobletMap;

    //! Used to distinguish between input and intermediate chunks on failure.
    yhash_set<NChunkClient::TChunkId> InputChunkIds;

    //! Increments each time a new job is scheduled.
    TIdGenerator JobIndexGenerator;


    static const NProto::TUserJobResult* FindUserJobResult(TJobletPtr joblet);

};

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(TOperation* operation, NYTree::INodePtr specTemplateNode)
{
    auto specNode = specTemplateNode
        ? NYTree::UpdateNode(specTemplateNode, operation->GetSpec())
        : operation->GetSpec();
    auto spec = New<TSpec>();
    try {
        spec->Load(specNode);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing operation spec") << ex;
    }
    return spec;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

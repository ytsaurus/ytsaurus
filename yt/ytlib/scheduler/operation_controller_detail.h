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
        NFileServer::TFileYPathProxy::TRspFetchPtr FetchResponse;
    };

    std::vector<TFile> Files;

    // Jobs in progress.
    struct TJobInProgress
        : public TIntrinsicRefCounted
    {
        TJobPtr Job;
        TClosure OnCompleted;
        TClosure OnFailed;
        TPoolExtractionResultPtr PoolResult;
        std::vector<NChunkServer::TChunkListId> ChunkListIds;
    };

    typedef TIntrusivePtr<TJobInProgress> TJobInProgressPtr;

    yhash_map<TJobPtr, TJobInProgressPtr> JobsInProgress;

    // The set of all input chunks. Used in #OnChunkFailed.
    yhash_set<NChunkServer::TChunkId> InputChunkIds;

    // Tasks management.

    class TTask
        : public TRefCounted
    {
    public:
        explicit TTask(TOperationControllerBase* controller)
            : Controller(controller)
            , Logger(Controller->Logger)
        { }

        virtual Stroka GetId() const = 0;
        virtual int GetPendingJobCount() const = 0;
        virtual int GetChunkListCountPerJob() const = 0;
        virtual TDuration GetMaxLocalityDelay() const = 0;

        virtual i64 GetLocality(const Stroka& address) const
        {
            return ChunkPool->GetLocality(address);
        }


        DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, NonLocalRequestTime);


        void AddStripe(TChunkStripePtr stripe)
        {
            ChunkPool->Add(stripe);
            AddInputLocalityHint(stripe);
            AddPendingHint();
        }

        TJobPtr ScheduleJob(TExecNodePtr node)
        {
            using ::ToString;

            if (!Controller->CheckChunkListsPoolSize(GetChunkListCountPerJob())) {
                return NULL;
            }

            auto jip = New<TJobInProgress>();
            auto weightThreshold = GetJobWeightThreshold();
            jip->PoolResult = ChunkPool->Extract(node->GetAddress(), weightThreshold);

            LOG_DEBUG("Chunks extracted (Address: %s, TotalCount: %d, LocalCount: %d, ExtractedWeight: %" PRId64 ", WeightThreshold: %s)",
                ~node->GetAddress(),
                jip->PoolResult->TotalChunkCount,
                jip->PoolResult->LocalChunkCount,
                jip->PoolResult->TotalChunkWeight,
                ~ToString(weightThreshold));

            auto jobSpec = GetJobSpec(~jip);

            // Pass jip to handlers via raw pointer to avoid cyclic references.
            jip->OnCompleted = BIND(&TTask::OnJobCompleted, MakeStrong(this), Unretained(~jip));
            jip->OnFailed = BIND(&TTask::OnJobFailed, MakeStrong(this), Unretained(~jip));

            jip->Job = Controller->Host->CreateJob(Controller->Operation, node, jobSpec);
            Controller->RegisterJobInProgress(jip);

            OnJobStarted(~jip);

            return jip->Job;
        }


        bool IsPending() const
        {
            return ChunkPool->IsPending();
        }

        bool IsCompleted() const
        {
            return ChunkPool->IsCompleted();
        }

        const TProgressCounter& WeightCounter() const
        {
            return ChunkPool->WeightCounter();
        }

        const TProgressCounter& ChunkCounter() const
        {
            return ChunkPool->ChunkCounter();
        }

    private:
        TOperationControllerBase* Controller;

    protected:
        NLog::TTaggedLogger& Logger;
        TAutoPtr<IChunkPool> ChunkPool;

        virtual TNullable<i64> GetJobWeightThreshold() const = 0;

        virtual NScheduler::NProto::TJobSpec GetJobSpec(TJobInProgress* jip) = 0;

        virtual void OnJobStarted(TJobInProgress* jip)
        {
            UNUSED(jip);
        }

        virtual void OnJobCompleted(TJobInProgress* jip)
        {
            ChunkPool->OnCompleted(jip->PoolResult);

            if (IsCompleted()) {
                OnTaskCompleted();
            }
        }

        virtual void OnJobFailed(TJobInProgress* jip)
        {
            ChunkPool->OnFailed(jip->PoolResult);
            
            Controller->ReleaseChunkLists(jip->ChunkListIds);

            FOREACH (const auto& stripe, jip->PoolResult->Stripes) {
                AddInputLocalityHint(stripe);
            }
            AddPendingHint();
        }

        virtual void OnTaskCompleted()
        {
            LOG_DEBUG("Task completed (Task: %s)", ~GetId());
        }


        void AddPendingHint()
        {
            Controller->AddTaskPendingHint(this);
        }

        virtual void AddInputLocalityHint(TChunkStripePtr stripe)
        {
            Controller->AddTaskLocalityHint(this, stripe);
        }


        static i64 GetJobWeightThresholdGeneric(int pendingJobCount, i64 pendingWeight)
        {
            YASSERT(pendingJobCount > 0);
            YASSERT(pendingWeight > 0);
            return static_cast<i64>(std::ceil((double) pendingWeight / pendingJobCount));
        }


        void AddSequentialInputSpec(NScheduler::NProto::TJobSpec* jobSpec, TJobInProgressPtr jip)
        {
            auto* inputSpec = jobSpec->add_input_specs();
            FOREACH (const auto& stripe, jip->PoolResult->Stripes) {
                AddInputChunks(inputSpec, stripe);
            }
        }

        void AddParallelInputSpec(NScheduler::NProto::TJobSpec* jobSpec, TJobInProgressPtr jip)
        {
            FOREACH (const auto& stripe, jip->PoolResult->Stripes) {
                auto* inputSpec = jobSpec->add_input_specs();
                AddInputChunks(inputSpec, stripe);
            }
        }

        void AddTabularOutputSpec(NScheduler::NProto::TJobSpec* jobSpec, TJobInProgressPtr jip, const TOutputTable& table)
        {
            auto* outputSpec = jobSpec->add_output_specs();
            outputSpec->set_channels(table.Channels);
            auto chunkListId = Controller->ChunkListPool->Extract();
            jip->ChunkListIds.push_back(chunkListId);
            *outputSpec->mutable_chunk_list_id() = chunkListId.ToProto();
        }

    private:
        void AddInputChunks(NScheduler::NProto::TTableInputSpec* inputSpec, TChunkStripePtr stripe)
        {
            FOREACH (const auto& weightedChunk, stripe->Chunks) {
                auto* inputChunk = inputSpec->add_chunks();

                // Copy everything.
                *inputChunk = weightedChunk.InputChunk;

                // Apply overrides.
                auto miscExt = GetProtoExtension<NChunkHolder::NProto::TMiscExt>(inputChunk->extensions());
                miscExt.set_data_weight(weightedChunk.DataWeightOverride);
                miscExt.set_row_count(weightedChunk.RowCountOverride);
                UpdateProtoExtension(inputChunk->mutable_extensions(), miscExt);
            }
        }

    };

    typedef TIntrusivePtr<TTask> TTaskPtr;

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
    virtual void DoGetProgress(NYTree::IYsonConsumer* consumer) = 0;

    //! Called to extract input table paths from the spec.
    virtual std::vector<NYTree::TYPath> GetInputTablePaths() = 0;
    
    //! Called to extract output table paths from the spec.
    virtual std::vector<NYTree::TYPath> GetOutputTablePaths() = 0;
    
    //! Called to extract file paths from the spec.
    virtual std::vector<NYTree::TYPath> GetFilePaths();

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
    std::vector<Stroka> CheckInputTablesSorted(const TNullable< yvector<Stroka> >& keyColumns);
    static bool AreKeysCompatible(const std::vector<Stroka>& fullColumns, const std::vector<Stroka>& prefixColumns);
    void CheckOutputTablesEmpty();
    void SetOutputTablesSorted(const std::vector<Stroka>& keyColumns);
    bool CheckChunkListsPoolSize(int minSize);
    void ReleaseChunkList(const NChunkServer::TChunkListId& id);
    void ReleaseChunkLists(const std::vector<NChunkServer::TChunkListId>& ids);
    void OnChunkListsReleased(NObjectServer::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);
    static int GetJobCount(
        i64 totalWeight,
        i64 weightPerJob,
        TNullable<int> configJobCount,
        int chunkCount);
    static void InitUserJobSpec(
        NScheduler::NProto::TUserJobSpec* proto,
        TUserJobSpecPtr config,
        const std::vector<TFile>& files);

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

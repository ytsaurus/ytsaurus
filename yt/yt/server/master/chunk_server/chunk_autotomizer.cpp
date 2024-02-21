#include "chunk_autotomizer.h"

#include "chunk.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "config.h"
#include "domestic_medium.h"
#include "helpers.h"
#include "job.h"
#include "job_registry.h"
#include "private.h"

#include <yt/yt/server/lib/hydra/mutation_context.h>

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/proto/chunk_autotomizer.pb.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>

#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>
#include <yt/yt/server/master/transaction_server/transaction_rotator.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_service.pb.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NChunkServer::NProto;
using namespace NConcurrency;
using namespace NLogging;
using namespace NHydra;
using namespace NHiveClient;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectServer;
using namespace NProfiling;
using namespace NTransactionServer;
using namespace NYTree;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////test

static const TLogger Logger("ChunkAutotomizer");

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EChunkNonAutotomicityReason,
    ((None)                      (0))
    ((AutotomizerIsDisabled)     (1))
    ((ChunkIsDead)               (2))
    ((ChunkIsSealed)             (3))
);

////////////////////////////////////////////////////////////////////////////////

//! Stores set of chunks allocated for some autotomy.
class TChunkPool
{
public:
    void AddChunk(TChunkId chunkId)
    {
        Chunks_.push_back(chunkId);
    }

    TChunkId ExtractChunk()
    {
        YT_VERIFY(!IsEmpty());
        auto chunkId = Chunks_.back();
        Chunks_.pop_back();

        return chunkId;
    }

    bool IsEmpty() const
    {
        return Chunks_.empty();
    }

private:
    std::vector<TChunkId> Chunks_;
};

////////////////////////////////////////////////////////////////////////////////

class TAutotomyJob
    : public TJob
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, BodyChunkId);
    DEFINE_BYVAL_RO_PROPERTY(TChunkId, TailChunkId);

    DEFINE_BYVAL_RO_PROPERTY(bool, Speculative);
    DEFINE_BYVAL_RO_PROPERTY(bool, Urgent);

public:
    TAutotomyJob(
        NChunkServer::TJobId jobId,
        TJobEpoch jobEpoch,
        TChunkId bodyChunkId,
        const TChunkSealInfo& bodySealInfo,
        TChunkId tailChunkId,
        bool speculative,
        bool urgent)
        : TJob(
            jobId,
            EJobType::AutotomizeChunk,
            jobEpoch,
            /*node*/ nullptr,
            TAutotomyJob::GetResourceUsage(),
            TChunkIdWithIndexes(bodyChunkId, GenericChunkReplicaIndex, GenericMediumIndex))
        , BodyChunkId_(bodyChunkId)
        , TailChunkId_(tailChunkId)
        , Speculative_(speculative)
        , Urgent_(urgent)
        , BodySealInfo_(bodySealInfo)
    { }

    bool FillJobSpec(TBootstrap* bootstrap, TJobSpec* jobSpec) const override
    {
        const auto& chunkManager = bootstrap->GetChunkManager();

        jobSpec->set_urgent(Urgent_);

        auto* jobSpecExt = jobSpec->MutableExtension(TAutotomizeChunkJobSpecExt::autotomize_chunk_job_spec_ext);

        NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());

        auto* bodyChunk = chunkManager->FindChunk(BodyChunkId_);
        YT_VERIFY(IsObjectAlive(bodyChunk));
        const auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
        const auto& aggregatedReplication = bodyChunk->GetAggregatedReplication(requisitionRegistry);
        YT_VERIFY(aggregatedReplication.GetSize() == 1);
        const auto& replication = *aggregatedReplication.begin();

        ToProto(jobSpecExt->mutable_body_chunk_id(), BodyChunkId_);
        YT_VERIFY(bodyChunk->GetOverlayed());
        jobSpecExt->set_body_chunk_first_overlayed_row_index(BodySealInfo_.first_overlayed_row_index());
        jobSpecExt->set_body_chunk_replica_lag_limit(bodyChunk->GetReplicaLagLimit());

        // This chunk better not have Sequoia replicas.
        const auto& bodyChunkReplicas = bodyChunk->StoredReplicas();
        ToProto(jobSpecExt->mutable_body_chunk_replicas(), bodyChunkReplicas);
        builder.Add(bodyChunkReplicas);

        ToProto(jobSpecExt->mutable_tail_chunk_id(), TailChunkId_);

        jobSpecExt->set_read_quorum(bodyChunk->GetReadQuorum());
        jobSpecExt->set_write_quorum(bodyChunk->GetWriteQuorum());
        jobSpecExt->set_medium_index(replication.GetMediumIndex());
        jobSpecExt->set_erasure_codec(ToProto<int>(bodyChunk->GetErasureCodec()));
        jobSpecExt->set_replication_factor(replication.Policy().GetReplicationFactor());
        jobSpecExt->set_overlayed(bodyChunk->GetOverlayed());

        return true;
    }

    void SetNode(TNode* node)
    {
        NodeAddress_ = node->GetDefaultAddress();
    }

private:
    const TChunkSealInfo BodySealInfo_;

    static TNodeResources GetResourceUsage()
    {
        TNodeResources resourceUsage;
        resourceUsage.set_autotomy_slots(1);

        return resourceUsage;
    }
};

DEFINE_REFCOUNTED_TYPE(TAutotomyJob)

////////////////////////////////////////////////////////////////////////////////

//! Describes state of some autotomy.
struct TChunkAutotomyState
{
    // Persistent fields.

    //! Outcome of Chunk Sealer.
    TChunkSealInfo ChunkSealInfo;

    // Transient fields.

    //! Pool of allocated chunks.
    TChunkPool ChunkPool;

    //! Set of alive jobs.
    THashMap<NChunkServer::TJobId, TAutotomyJobPtr> Jobs;

    //! Last time we scheduled some job.
    TInstant LastJobScheduleTime = TInstant::Zero();

    void Persist(const TPersistenceContext& context)
    {
        using NYT::Persist;

        Persist(context, ChunkSealInfo);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkAutotomizer
    : public IChunkAutotomizer
    , public TMasterAutomatonPart
{
public:
    explicit TChunkAutotomizer(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::ChunkAutotomizer)
        , TransactionRotator_(bootstrap, "Chunk autotomizer transaction")
        , SuccessfulAutotomyCounter_(ChunkServerProfiler.Counter("/chunk_autotomizer/successful_autotomies"))
        , UnsuccessfulAutotomyCounter_(ChunkServerProfiler.Counter("/chunk_autotomizer/unsuccessful_autotomies"))
        , SpeculativeJobWinCounter_(ChunkServerProfiler.Counter("/chunk_autotomizer/speculative_job_wins"))
        , SpeculativeJobLossCounter_(ChunkServerProfiler.Counter("/chunk_autotomizer/speculative_job_losses"))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Default),
            AutomatonThread);

        RegisterLoader(
            "ChunkAutotomizer",
            BIND(&TChunkAutotomizer::Load, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Values,
            "ChunkAutotomizer",
            BIND(&TChunkAutotomizer::Save, Unretained(this)));

        RegisterMethod(BIND(&TChunkAutotomizer::HydraAutotomizeChunk, Unretained(this)));
        RegisterMethod(BIND(&TChunkAutotomizer::HydraUpdateChunkAutotomizerTransactions, Unretained(this)));
        RegisterMethod(BIND(&TChunkAutotomizer::HydraUpdateAutotomizerState, Unretained(this)));
        RegisterMethod(BIND(&TChunkAutotomizer::HydraOnChunkAutotomyCompleted, Unretained(this)));
    }

    // IJobController implementation.

    void ScheduleJobs(EJobType jobType, IJobSchedulingContext* context) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsMasterJobType(jobType));

        if (!IsLeader()) {
            return;
        }

        if (jobType != EJobType::AutotomizeChunk) {
            return;
        }

        const auto& resourceUsage = context->GetNodeResourceUsage();
        const auto& resourceLimits = context->GetNodeResourceLimits();
        auto* node = context->GetNode();

        auto hasSpareAutotomySlots = [&] {
            return resourceUsage.autotomy_slots() < resourceLimits.autotomy_slots();
        };

        while (!PendingJobs_.empty() && hasSpareAutotomySlots()) {
            auto jobId = PendingJobs_.front();
            PendingJobs_.pop();

            auto job = FindJob(jobId);
            if (!job) {
                continue;
            }

            auto bodyChunkId = job->GetBodyChunkId();
            auto tailChunkId = job->GetTailChunkId();
            if (!IsChunkRegistered(bodyChunkId)) {
                continue;
            }

            if (!IsChunkAutotomizable(bodyChunkId)) {
                continue;
            }

            job->SetNode(node);
            context->ScheduleJob(job);

            YT_LOG_DEBUG("Autotomy job scheduled (JobId: %v, Address: %v, BodyChunkId: %v, TailChunkId: %v)",
                job->GetJobId(),
                node->GetDefaultAddress(),
                bodyChunkId,
                tailChunkId);
        }
    }

    void OnJobWaiting(const TAutotomyJobPtr& job, IJobControllerCallbacks* callbacks) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        // In Chunk Autotomizer we do not distinguish between waiting and running jobs.
        OnJobRunning(job, callbacks);
    }

    void OnJobRunning(const TAutotomyJobPtr& job, IJobControllerCallbacks* callbacks) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        auto jobTimeout = GetDynamicConfig()->JobTimeout;
        auto jobDuration = TInstant::Now() - job->GetStartTime();
        if (jobDuration > jobTimeout) {
            YT_LOG_WARNING("Job timed out, aborting "
                "(JobId: %v, BodyChunkId: %v, Address: %v, Duration: %v, Timeout: %v)",
                job->GetJobId(),
                job->GetBodyChunkId(),
                job->NodeAddress(),
                jobDuration,
                jobTimeout);

            callbacks->AbortJob(job);
        }
    }

    void OnJobCompleted(const TAutotomyJobPtr& job) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (!IsChunkRegistered(job->GetBodyChunkId())) {
            YT_LOG_DEBUG("Autotomy job completed for unregistered chunk, ignored (BodyChunkId: %v)",
                job->GetBodyChunkId());
            return;
        }

        YT_LOG_DEBUG("Autotomy job is completed "
            "(JobId: %v, BodyChunkId: %v, TailChunkId: %v, Speculative: %v)",
            job->GetJobId(),
            job->GetBodyChunkId(),
            job->GetTailChunkId(),
            job->GetSpeculative());

        const auto& jobResult = job->Result();
        const auto& jobResultExt = jobResult.GetExtension(NChunkClient::NProto::TAutotomizeChunkJobResultExt::autotomize_chunk_job_result_ext);

        TReqOnChunkAutotomyCompleted request;
        *request.mutable_body_chunk_id() = jobResultExt.body_chunk_id();
        *request.mutable_body_chunk_seal_info() = jobResultExt.body_chunk_seal_info();
        *request.mutable_tail_chunk_id() = jobResultExt.tail_chunk_id();
        *request.mutable_tail_chunk_seal_info() = jobResultExt.tail_chunk_seal_info();

        request.set_speculative_job_won(job->GetSpeculative());

        UnregisterJob(job->GetJobId());

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger));
    }

    void OnJobAborted(const TAutotomyJobPtr& job) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        auto bodyChunkId = job->GetBodyChunkId();
        auto jobId = job->GetJobId();

        YT_LOG_DEBUG("Job aborted (BodyChunkId: %v, JobId: %v, Error: %v)",
            bodyChunkId,
            jobId,
            job->Error());

        UnregisterJob(jobId);
        TouchChunk(bodyChunkId);
    }

    void OnJobFailed(const TAutotomyJobPtr& job) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        auto bodyChunkId = job->GetBodyChunkId();
        auto jobId = job->GetJobId();

        YT_LOG_DEBUG("Job failed (BodyChunkId: %v, JobId: %v, Error: %v)",
            bodyChunkId,
            jobId,
            job->Error());

        UnregisterJob(jobId);
        TouchChunk(bodyChunkId);
    }

    // IChunkAutotomizer implementation.

    void Initialize() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionAborted(BIND_NO_PROPAGATE(&TChunkAutotomizer::OnTransactionFinished, MakeWeak(this)));
        transactionManager->SubscribeTransactionCommitted(BIND_NO_PROPAGATE(&TChunkAutotomizer::OnTransactionFinished, MakeWeak(this)));
    }

    void OnProfiling(TSensorBuffer* buffer) const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        buffer->AddGauge("/chunk_autotomizer/registered_chunks", RegisteredChunks_.size());
        buffer->AddGauge("/chunk_autotomizer/pending_jobs", PendingJobs_.size());
    }

    bool IsChunkRegistered(TChunkId bodyChunkId) const override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return RegisteredChunks_.contains(bodyChunkId);
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    // Persistent fields.

    //! Set of all registered chunks.
    THashMap<TChunkId, TChunkAutotomyState> RegisteredChunks_;

    //! Transactions used for tail chunk allocation.
    TTransactionRotator TransactionRotator_;

    // Transient fields.

    //! Executor responsible for autotomizer transactions rotation.
    NConcurrency::TPeriodicExecutorPtr UpdateTransactionsExecutor_;
    //! Executor responsible for autotomizable chunks refresh.
    NConcurrency::TPeriodicExecutorPtr RefreshExecutor_;
    //! Executor responsible for unnecessary chunks unstage.
    NConcurrency::TPeriodicExecutorPtr ChunkUnstageExecutor_;

    //! Set of allocated chunks that are not longer needed and thus can be unstaged.
    std::queue<TChunkId> ChunksToUnstage_;

    //! Queue of all registered chunks used for periodic chunks touch.
    std::queue<TChunkId> ChunkRefreshQueue_;

    //! Set of all alive autotomy jobs.
    THashMap<NChunkServer::TJobId, TChunkId> JobIdToBodyChunkId_;

    //! Queue of created but not scheduled jobs.
    std::queue<NChunkServer::TJobId> PendingJobs_;

    //! Current job epoch.
    TJobEpoch JobEpoch_ = InvalidJobEpoch;

    // Autotomy is successful if it ends up with tail chunk seal
    // and unsuccessful otherwise.
    const TCounter SuccessfulAutotomyCounter_;
    const TCounter UnsuccessfulAutotomyCounter_;

    // Consider a moment of autotomy completion.
    // Completed speculative job wins, other running speculative jobs lose.
    const TCounter SpeculativeJobWinCounter_;
    const TCounter SpeculativeJobLossCounter_;

    void HydraAutotomizeChunk(TReqAutotomizeChunk* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto bodyChunkId = ::NYT::FromProto<TChunkId>(request->chunk_id());
        const auto& chunkSealInfo = request->chunk_seal_info();

        // Persistent actions.
        if (!RegisterChunk(bodyChunkId, chunkSealInfo)) {
            return;
        }

        // Preallocate tail chunks for first autotomy jobs.
        AllocateTailChunks(bodyChunkId);

        // Transient actions.
        if (IsLeader()) {
            ChunkRefreshQueue_.push(bodyChunkId);
            TouchChunk(bodyChunkId);
        }
    }

    void HydraUpdateChunkAutotomizerTransactions(TReqUpdateChunkAutotomizerTransactions* /*request*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        YT_LOG_INFO(
            "Updating chunk autotomizer transactions "
            "(TransactionId: %v, PreviousTransactionId: %v)",
            TransactionRotator_.GetTransactionId(),
            TransactionRotator_.GetPreviousTransactionId());

        TransactionRotator_.Rotate();

        YT_LOG_INFO(
            "Chunk autotomizer transactions updated "
            "(TransactionId: %v, PreviousTransactionId: %v)",
            TransactionRotator_.GetTransactionId(),
            TransactionRotator_.GetPreviousTransactionId());
    }

    void HydraUpdateAutotomizerState(TReqUpdateAutotomizerState* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto chunksToUnregister = ::NYT::FromProto<std::vector<TChunkId>>(request->chunks_to_unregister());
        for (auto bodyChunkId : chunksToUnregister) {
            // Persistent actions.
            UnregisterChunk(bodyChunkId);
        }

        auto chunksToAllocateTail = ::NYT::FromProto<std::vector<TChunkId>>(request->chunks_to_allocate_tail());
        for (auto bodyChunkId : chunksToAllocateTail) {
            if (!IsChunkRegistered(bodyChunkId)) {
                YT_LOG_DEBUG(
                    "Tail chunk allocation is requested for unregistered chunk, ignored (BodyChunkId: %v)",
                    bodyChunkId);
                continue;
            }

            if (!IsChunkAutotomizable(bodyChunkId)) {
                YT_LOG_DEBUG(
                    "Tail chunk allocation is requested for non-autotomizable chunk, ignored "
                    "(BodyChunkId: %v, NonAutotomicityReason: %v)",
                    bodyChunkId,
                    GetChunkNonAutotomicityReason(bodyChunkId));
                continue;
            }

            // Persistent actions.
            AllocateTailChunks(bodyChunkId);

            // Transient actions.
            if (IsLeader()) {
                TouchChunk(bodyChunkId);
            }
        }
    }

    void HydraOnChunkAutotomyCompleted(TReqOnChunkAutotomyCompleted* request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto bodyChunkId = ::NYT::FromProto<TChunkId>(request->body_chunk_id());
        const auto& bodyChunkSealInfo = request->body_chunk_seal_info();
        auto tailChunkId = ::NYT::FromProto<TChunkId>(request->tail_chunk_id());
        const auto& tailChunkSealInfo = request->tail_chunk_seal_info();

        YT_LOG_DEBUG(
            "Chunk autotomy completed "
            "(BodyChunkId: %v, BodyLogicalRowCount: %v, TailChunkId: %v, TailLogicalRowCount: %v)",
            bodyChunkId,
            bodyChunkSealInfo.row_count(),
            tailChunkId,
            tailChunkSealInfo.row_count());

        if (!IsChunkRegistered(bodyChunkId)) {
            YT_LOG_DEBUG(
                "Chunk is not registered, ignoring autotomy result (BodyChunkId: %v)",
                bodyChunkId);
            return;
        }

        if (!IsChunkAutotomizable(bodyChunkId)) {
            YT_LOG_DEBUG(
                "Chunk is no longer autotomizable, ignoring autotomy result "
                "(BodyChunkId: %v, NonAutotomicityReason: %v)",
                bodyChunkId,
                GetChunkNonAutotomicityReason(bodyChunkId));
            UnsuccessfulAutotomyCounter_.Increment();
            UnregisterChunk(bodyChunkId);
            return;
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* bodyChunk = chunkManager->FindChunk(bodyChunkId);
        auto* tailChunk = chunkManager->FindChunk(tailChunkId);

        // Body chunk is still autotomizable, so it must be alive.
        YT_VERIFY(IsObjectAlive(bodyChunk));

        // NB: Tail might be dead.
        if (!IsObjectAlive(tailChunk)) {
            YT_LOG_DEBUG(
                "Tail chunk is dead, restarting autotomy (BodyChunkId: %v, TailChunkId: %v)",
                bodyChunkId,
                tailChunkId);
            if (IsLeader()) {
                TouchChunk(bodyChunkId);
            }
            return;
        }

        // Body chunk should be attached to journal chunk list,
        // tail chunk should have no parents.
        YT_VERIFY(bodyChunk->GetParentCount() == 1);
        YT_VERIFY(tailChunk->GetParentCount() == 0);

        auto* journalChunkList = bodyChunk->Parents().begin()->first->AsChunkList();
        auto& journalChunks = journalChunkList->Children();

        // Insert tail chunk just after the body chunk.
        auto bodyChunkIndex = GetChildIndex(journalChunkList, bodyChunk);
        journalChunks.insert(journalChunks.begin() + bodyChunkIndex + 1, tailChunk);

        tailChunk->AddParent(journalChunkList);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(tailChunk);

        // Unsealed chunks have zero statistics. Also they form a suffix of a journal chunk list,
        // so adding an unsealed chunk in the middle of chunk list is equivavent to adding empty
        // statistics entry to its end.
        journalChunkList->CumulativeStatistics().PushBack(TCumulativeStatisticsEntry{});

        RecomputeChildToIndexMapping(journalChunkList);

        auto sealChunk = [&] (TChunk* chunk, const TChunkSealInfo& chunkSealInfo) {
            try {
                chunkManager->SealChunk(chunk, chunkSealInfo);
            } catch (const TErrorException& ex) {
                // This situation is not that bad since both body and tail chunks will stay alive
                // and still can be (unreliably) sealed to prevent data loss.
                YT_LOG_ALERT(
                    ex,
                    "Failed to seal chunk (ChunkId: %v)",
                    chunk->GetId());
            }
        };
        sealChunk(bodyChunk, bodyChunkSealInfo);
        sealChunk(tailChunk, tailChunkSealInfo);

        if (IsLeader()) {
            SuccessfulAutotomyCounter_.Increment();

            if (request->speculative_job_won()) {
                SpeculativeJobWinCounter_.Increment();
            }

            auto* autotomyState = GetChunkAutotomyState(bodyChunkId);
            for (const auto& [jobId, chunkJob] : autotomyState->Jobs) {
                // Two jobs are equals iff their tail chunks coincide.
                if (chunkJob->GetSpeculative() && chunkJob->GetTailChunkId() != tailChunkId) {
                    SpeculativeJobLossCounter_.Increment();
                }
            }
        }

        YT_VERIFY(UnregisterChunk(bodyChunkId));
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        using NYT::Load;

        Load(context, RegisteredChunks_);
        Load(context, TransactionRotator_);
    }

    void Save(NCellMaster::TSaveContext& context) const
    {
        using ::NYT::Save;

        Save(context, RegisteredChunks_);
        Save(context, TransactionRotator_);
    }

    void Clear() override
    {
        TMasterAutomatonPart::Clear();

        RegisteredChunks_.clear();

        TransactionRotator_.Clear();
    }

    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        const auto& config = GetDynamicConfig();

        UpdateTransactionsExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkAutotomizer),
            BIND(&TChunkAutotomizer::UpdateTransactions, MakeWeak(this)),
            config->TransactionUpdatePeriod);
        UpdateTransactionsExecutor_->Start();

        RefreshExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkAutotomizer),
            BIND(&TChunkAutotomizer::RefreshChunks, MakeWeak(this)),
            config->RefreshPeriod);
        RefreshExecutor_->Start();

        ChunkUnstageExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkAutotomizer),
            BIND(&TChunkAutotomizer::UnstageChunks, MakeWeak(this)),
            config->ChunkUnstagePeriod);
        ChunkUnstageExecutor_->Start();

        for (const auto& [bodyChunkId, autotomyState] : RegisteredChunks_) {
            ChunkRefreshQueue_.push(bodyChunkId);
        }

        const auto& jobRegistry = Bootstrap_->GetChunkManager()->GetJobRegistry();
        YT_VERIFY(JobEpoch_ == InvalidJobEpoch);
        JobEpoch_ = jobRegistry->StartEpoch();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        if (UpdateTransactionsExecutor_) {
            YT_UNUSED_FUTURE(UpdateTransactionsExecutor_->Stop());
            UpdateTransactionsExecutor_.Reset();
        }

        if (RefreshExecutor_) {
            YT_UNUSED_FUTURE(RefreshExecutor_->Stop());
            RefreshExecutor_.Reset();
        }

        if (ChunkUnstageExecutor_) {
            YT_UNUSED_FUTURE(ChunkUnstageExecutor_->Stop());
            ChunkUnstageExecutor_.Reset();
        }

        PendingJobs_ = {};

        if (JobEpoch_ != InvalidJobEpoch) {
            const auto& jobRegistry = Bootstrap_->GetChunkManager()->GetJobRegistry();
            jobRegistry->OnEpochFinished(JobEpoch_);
            JobEpoch_ = InvalidJobEpoch;
        }
    }

    const TDynamicChunkAutotomizerConfigPtr& GetDynamicConfig() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->ChunkManager->ChunkAutotomizer;
    }

    bool IsEnabled() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->ChunkManager->EnableChunkAutotomizer;
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        const auto& config = GetDynamicConfig();
        if (UpdateTransactionsExecutor_) {
            UpdateTransactionsExecutor_->SetPeriod(config->TransactionUpdatePeriod);
        }
        if (RefreshExecutor_) {
            RefreshExecutor_->SetPeriod(config->RefreshPeriod);
        }
        if (ChunkUnstageExecutor_) {
            ChunkUnstageExecutor_->SetPeriod(config->ChunkUnstagePeriod);
        }
    }

    bool RegisterChunk(TChunkId bodyChunkId, const TChunkSealInfo& chunkSealInfo)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        // Persistent actions.
        YT_VERIFY(IsJournalChunkId(bodyChunkId));

        if (!IsChunkAutotomizable(bodyChunkId)) {
            YT_LOG_DEBUG(
                "Attempted to register non-autotomizable chunk, ignored (BodyChunkId: %v, NonAutotomicityReason: %v)",
                bodyChunkId,
                GetChunkNonAutotomicityReason(bodyChunkId));
            return false;
        }

        if (RegisteredChunks_.contains(bodyChunkId)) {
            YT_LOG_DEBUG(
                "Chunk is already registered (BodyChunkId: %v)",
                bodyChunkId);
            return false;
        }

        TChunkAutotomyState autotomyState{
            .ChunkSealInfo = chunkSealInfo
        };
        YT_VERIFY(RegisteredChunks_.emplace(bodyChunkId, autotomyState).second);

        YT_LOG_DEBUG(
            "Registered chunk (BodyChunkId: %v, RowCount: %v, FirstOverlayedRowIndex: %v)",
            bodyChunkId,
            chunkSealInfo.row_count(),
            chunkSealInfo.has_first_overlayed_row_index() ? std::make_optional(chunkSealInfo.first_overlayed_row_index()) : std::nullopt);

        return true;
    }

    bool UnregisterChunk(TChunkId bodyChunkId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        if (!IsChunkRegistered(bodyChunkId)) {
            return false;
        }

        auto* autotomyState = GetChunkAutotomyState(bodyChunkId);

        // Transient actions.
        if (IsLeader()) {
            // Unstage chunks from chunk pool.
            auto& chunkPool = autotomyState->ChunkPool;
            while (!chunkPool.IsEmpty()) {
                auto chunkId = chunkPool.ExtractChunk();
                UnstageChunk(chunkId);
            }

            // Unregister chunk jobs.
            auto jobMap = autotomyState->Jobs;
            for (const auto& [jobId, job] : jobMap) {
                UnregisterJob(jobId);
            }
        }

        // Persistent actions.
        YT_VERIFY(RegisteredChunks_.erase(bodyChunkId) > 0);

        YT_LOG_DEBUG(
            "Unregistered chunk (BodyChunkId: %v)",
            bodyChunkId);

        return true;
    }

    void UpdateTransactions()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        YT_LOG_DEBUG("Requesting to update chunk autotomizer transactions");

        NProto::TReqUpdateChunkAutotomizerTransactions request;

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger));
    }

    void OnTransactionFinished(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());
        YT_VERIFY(IsObjectAlive(transaction));

        // Persistent actions.
        auto transactionId = TransactionRotator_.GetTransactionId();
        auto previousTransactionId = TransactionRotator_.GetPreviousTransactionId();

        if (!TransactionRotator_.OnTransactionFinished(transaction)) {
            return;
        }

        YT_LOG_DEBUG(
            "Chunk autotomizer transaction finished "
            "(FinishedTransactionId: %v, ChunkAutotomizerTransactionId: %v, ChunkAutotomizerPreviousTransactionId: %v)",
            transaction->GetId(),
            transactionId,
            previousTransactionId);

        if (IsLeader() && transaction->GetId() == transactionId) {
            UpdateTransactions();
        }
    }

    TChunkId AllocateTailChunk(TChunkId bodyChunkId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* bodyChunk = chunkManager->FindChunk(bodyChunkId);
        YT_VERIFY(IsObjectAlive(bodyChunk));

        auto transaction = TransactionRotator_.GetTransaction();
        if (!IsObjectAlive(transaction)) {
            return NullChunkId;
        }

        const auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
        const auto& replication = bodyChunk->GetAggregatedReplication(requisitionRegistry);
        YT_VERIFY(replication.GetSize() == 1);
        const auto& replicationEntry = *replication.begin();
        auto replicationFactor = replicationEntry.Policy().GetReplicationFactor();
        auto mediumIndex = replicationEntry.GetMediumIndex();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        YT_VERIFY(IsObjectAlive(medium));

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* rootAccount = securityManager->GetRootAccount();

        auto* tailChunk = chunkManager->CreateChunk(
            transaction,
            /*chunkList*/ nullptr,
            bodyChunk->GetType(),
            rootAccount,
            replicationFactor,
            bodyChunk->GetErasureCodec(),
            medium,
            bodyChunk->GetReadQuorum(),
            bodyChunk->GetWriteQuorum(),
            /*movable*/ true,
            /*vital*/ true,
            bodyChunk->GetOverlayed(),
            NullConsistentReplicaPlacementHash,
            bodyChunk->GetReplicaLagLimit());
        YT_VERIFY(IsObjectAlive(tailChunk));

        YT_LOG_DEBUG(
            "Tail chunk allocated for chunk autotomy (BodyChunkId: %v, TailChunkId: %v)",
            bodyChunkId,
            tailChunk->GetId());

        return tailChunk->GetId();
    }

    void AllocateTailChunks(TChunkId bodyChunkId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        auto* autotomyState = GetChunkAutotomyState(bodyChunkId);

        auto tailChunkCount = GetDynamicConfig()->TailChunksPerAllocation;

        YT_LOG_DEBUG(
            "Allocating tail chunks (BodyChunkId: %v, TailChunkCount: %v)",
            bodyChunkId,
            tailChunkCount);

        for (int chunkIndex = 0; chunkIndex < tailChunkCount; ++chunkIndex) {
            // Persistent actions.
            auto tailChunkId = AllocateTailChunk(bodyChunkId);
            if (tailChunkId == NullChunkId) {
                continue;
            }
            // Transient actions.
            if (IsLeader()) {
                autotomyState->ChunkPool.AddChunk(tailChunkId);
            }
        }
    }

    TChunkAutotomyState* GetChunkAutotomyState(TChunkId bodyChunkId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return &GetOrCrash(RegisteredChunks_, bodyChunkId);
    }

    EChunkNonAutotomicityReason GetChunkNonAutotomicityReason(TChunkId bodyChunkId) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!IsEnabled()) {
            return EChunkNonAutotomicityReason::AutotomizerIsDisabled;
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* chunk = chunkManager->FindChunk(bodyChunkId);
        if (!IsObjectAlive(chunk)) {
            return EChunkNonAutotomicityReason::ChunkIsDead;
        }

        if (chunk->IsSealed()) {
            return EChunkNonAutotomicityReason::ChunkIsSealed;
        }

        return EChunkNonAutotomicityReason::None;
    }

    bool IsChunkAutotomizable(TChunkId bodyChunkId) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return GetChunkNonAutotomicityReason(bodyChunkId) == EChunkNonAutotomicityReason::None;
    }

    void UnstageChunk(TChunkId chunkId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        YT_LOG_DEBUG("Unstaging chunk (ChunkId: %v)", chunkId);

        ChunksToUnstage_.push(chunkId);
    }

    void UnstageChunks()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        TReqExecuteBatch request;

        int unstagedChunkCount = 0;
        while (!ChunksToUnstage_.empty() && unstagedChunkCount < GetDynamicConfig()->MaxChunksPerUnstage) {
            auto chunkId = ChunksToUnstage_.front();
            ChunksToUnstage_.pop();

            auto* unstageChunkSubrequest = request.add_unstage_chunk_tree_subrequests();
            ToProto(unstageChunkSubrequest->mutable_chunk_tree_id(), chunkId);
            ++unstagedChunkCount;
        }

        YT_LOG_DEBUG("Unstaging chunks (ChunkCount: %v)", unstagedChunkCount);
        if (unstagedChunkCount > 0) {
            const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
            YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
                ->CommitAndLog(Logger));
        }
    }

    TAutotomyJobPtr FindJob(NChunkServer::TJobId jobId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (auto chunkIt = JobIdToBodyChunkId_.find(jobId); chunkIt == JobIdToBodyChunkId_.end()) {
            return nullptr;
        } else {
            auto bodyChunkId = chunkIt->second;
            auto* autotomyState = GetChunkAutotomyState(bodyChunkId);
            if (auto jobIt = autotomyState->Jobs.find(jobId); jobIt == autotomyState->Jobs.end()) {
                return nullptr;
            } else {
                return jobIt->second;
            }
        }
    }

    TAutotomyJobPtr GetJob(NChunkServer::TJobId jobId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        auto job = FindJob(jobId);
        YT_VERIFY(job);

        return job;
    }

    void RegisterJob(const TAutotomyJobPtr& job)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        auto jobId = job->GetJobId();
        auto bodyChunkId = job->GetBodyChunkId();

        YT_VERIFY(JobIdToBodyChunkId_.emplace(jobId, bodyChunkId).second);

        auto* autotomyState = GetChunkAutotomyState(bodyChunkId);
        YT_VERIFY(autotomyState->Jobs.emplace(jobId, job).second);

        YT_LOG_DEBUG("Job registered (JobId: %v)", jobId);
    }

    bool UnregisterJob(NChunkServer::TJobId jobId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        auto job = FindJob(jobId);
        if (!job) {
            return false;
        }

        auto bodyChunkId = job->GetBodyChunkId();

        YT_VERIFY(JobIdToBodyChunkId_.erase(jobId) > 0);

        auto* autotomyState = GetChunkAutotomyState(bodyChunkId);
        YT_VERIFY(autotomyState->Jobs.erase(jobId) > 0);

        YT_LOG_DEBUG("Job unregistered (JobId: %v)", jobId);

        return true;
    }

    void RefreshChunks()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        auto chunksToTouch = std::min<int>(ChunkRefreshQueue_.size(), GetDynamicConfig()->MaxChunksPerRefresh);
        YT_LOG_DEBUG("Refreshing chunks (ChunkCount: %v)", chunksToTouch);

        // Fast path.
        if (chunksToTouch == 0) {
            return;
        }

        TReqUpdateAutotomizerState request;

        auto changedChunkCount = 0;
        auto maxChangedChunkCount = GetDynamicConfig()->MaxChangedChunksPerRefresh;

        for (int chunkIndex = 0; chunkIndex < chunksToTouch; ++chunkIndex) {
            auto bodyChunkId = ChunkRefreshQueue_.front();
            ChunkRefreshQueue_.pop();

            // Unregistered chunks are removed from refresh queue.
            if (!IsChunkRegistered(bodyChunkId)) {
                continue;
            }

            // Chunk is no longer autotomizable, unregister it and remove from queue.
            if (!IsChunkAutotomizable(bodyChunkId)) {
                UnsuccessfulAutotomyCounter_.Increment();
                ToProto(request.add_chunks_to_unregister(), bodyChunkId);
                continue;
            }

            ChunkRefreshQueue_.push(bodyChunkId);

            if (TouchChunk(bodyChunkId, &request)) {
                ++changedChunkCount;
                if (changedChunkCount == maxChangedChunkCount) {
                    break;
                }
            }
        }

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger));
    }

    // This function can be called from two contexts: chunks refresh and a single chunk state change.
    // In second case we don't want to execute a mutation, so we deny persistent actions like chunk allocation.
    bool TouchChunk(TChunkId bodyChunkId, TReqUpdateAutotomizerState* request = nullptr)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        const auto& config = GetDynamicConfig();

        // Unknown chunk, do nothing.
        if (!IsChunkRegistered(bodyChunkId)) {
            return false;
        }

        // Chunk is no longer autotomizable, do nothing.
        if (!IsChunkAutotomizable(bodyChunkId)) {
            return false;
        }

        auto* autotomyState = GetChunkAutotomyState(bodyChunkId);

        auto canCreateJob = false;
        auto speculative = false;

        if (autotomyState->Jobs.empty()) {
            // There are no jobs running for this chunk, job can be scheduled
            // despite of last job schedule time.
            canCreateJob = true;
        } else if (std::ssize(autotomyState->Jobs) < config->MaxConcurrentJobsPerChunk &&
            autotomyState->LastJobScheduleTime + config->JobSpeculationTimeout < TInstant::Now())
        {
            // Job speculation timeout expired and one more speculative job can be launched.
            canCreateJob = true;
            speculative = true;
        }

        // No need to create a new job, do nothing.
        if (!canCreateJob) {
            return false;
        }

        if (autotomyState->ChunkPool.IsEmpty()) {
            // No preallocated chunks, schedule chunk creation (if possible).
            if (request) {
                ToProto(request->add_chunks_to_allocate_tail(), bodyChunkId);
                return true;
            }

            return false;
        } else {
            auto tailChunkId = autotomyState->ChunkPool.ExtractChunk();

            const auto& chunkManager = Bootstrap_->GetChunkManager();
            auto jobId = chunkManager->GenerateJobId();

            auto urgent = GetDynamicConfig()->ScheduleUrgentJobs;

            auto job = New<TAutotomyJob>(
                jobId,
                JobEpoch_,
                bodyChunkId,
                autotomyState->ChunkSealInfo,
                tailChunkId,
                speculative,
                urgent);

            YT_LOG_DEBUG("Autotomy job created "
                "(JobId: %v, JobEpoch: %v, BodyChunkId: %v, TailChunkId: %v, Speculative: %v, Urgent: %v)",
                jobId,
                JobEpoch_,
                bodyChunkId,
                tailChunkId,
                speculative,
                urgent);

            RegisterJob(job);

            PendingJobs_.push(jobId);

            return true;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkAutotomizerPtr CreateChunkAutotomizer(TBootstrap* bootstrap)
{
    return New<TChunkAutotomizer>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

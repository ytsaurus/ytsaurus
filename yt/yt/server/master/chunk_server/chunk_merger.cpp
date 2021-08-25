#include "chunk_merger.h"
#include "chunk_owner_base.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_tree_traverser.h"
#include "config.h"
#include "job_registry.h"
#include "job.h"
#include "medium.h"

#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/server/master/table_server/table_node.h>
#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/ytlib/cypress_client/cypress_service_proxy.h>

#include <yt/yt/library/erasure/public.h>
#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <stack>

namespace NYT::NChunkServer {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCellMaster;
using namespace NChunkClient::NProto;
using namespace NYTree;
using namespace NTransactionServer;
using namespace NProto;
using namespace NSecurityServer;
using namespace NObjectClient;
using namespace NHydra;
using namespace NYson;
using namespace NProfiling;
using namespace NTableClient;
using namespace NTableServer;
using namespace NObjectServer;
using namespace NCypressServer;
using namespace NChunkClient;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkReplacerCallbacks
    : public IChunkReplacerCallbacks
{
public:
    explicit TChunkReplacerCallbacks(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children) override
    {
        Bootstrap_->GetChunkManager()->AttachToChunkList(chunkList, children);
    }

    virtual void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* child) override
    {
        Bootstrap_->GetChunkManager()->AttachToChunkList(chunkList, child);
    }

    virtual void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd) override
    {
        Bootstrap_->GetChunkManager()->AttachToChunkList(chunkList, childrenBegin, childrenEnd);
    }

    virtual bool IsMutationLoggingEnabled() override
    {
        return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsMutationLoggingEnabled();
    }

private:
    TBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

class TMergeChunkVisitor
    : public IChunkVisitor
{
public:
    TMergeChunkVisitor(
        TBootstrap* bootstrap,
        TChunkOwnerBase* node,
        TWeakPtr<IMergeChunkVisitorHost> chunkVisitorHost)
        : Bootstrap_(bootstrap)
        , Node_(node)
        , RootChunkList_(Node_->GetChunkList())
        , Account_(Node_->GetAccount())
        , ChunkVisitorHost_(std::move(chunkVisitorHost))
    { }

    void Run()
    {
        YT_VERIFY(IsObjectAlive(Node_));
        YT_VERIFY(IsObjectAlive(Account_));

        auto callbacks = CreateAsyncChunkTraverserContext(
            Bootstrap_,
            EAutomatonThreadQueue::ChunkMerger);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->EphemeralRefObject(Account_);
        objectManager->EphemeralRefObject(Node_);
        objectManager->EphemeralRefObject(RootChunkList_);

        TraverseChunkTree(std::move(callbacks), this, RootChunkList_);
    }

private:
    TBootstrap* const Bootstrap_;
    TChunkOwnerBase* const Node_;
    TChunkList* const RootChunkList_;
    TAccount* const Account_;
    const TWeakPtr<IMergeChunkVisitorHost> ChunkVisitorHost_;

    std::vector<TChunkId> ChunkIds_;

    i64 CurrentRowCount_ = 0;
    i64 CurrentDataWeight_ = 0;
    i64 CurrentUncompressedDataSize_ = 0;


    virtual bool OnChunk(
        TChunk* chunk,
        std::optional<i64> /*rowIndex*/,
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TReadLimit& /*lowerLimit*/,
        const NChunkClient::TReadLimit& /*upperLimit*/,
        TTransactionId /*timestampTransactionId*/) override
    {
        if (MaybeAddChunk(chunk)) {
            return true;
        }

        MaybePlanJob();

        ChunkIds_.clear();
        CurrentRowCount_ = 0;
        CurrentDataWeight_ = 0;
        CurrentUncompressedDataSize_ = 0;

        MaybeAddChunk(chunk);
        return true;
    }

    virtual bool OnChunkView(TChunkView*) override
    {
        return false;
    }

    virtual bool OnDynamicStore(
        TDynamicStore*,
        std::optional<int>,
        const NChunkClient::TReadLimit&,
        const NChunkClient::TReadLimit&) override
    {
        return false;
    }

    virtual void OnFinish(const TError& error) override
    {
        auto chunkVisitorHost = ChunkVisitorHost_.Lock();
        if (!chunkVisitorHost) {
            return;
        }

        MaybePlanJob();
        if (IsObjectAlive(Account_)) {
            Account_->IncrementMergeJobRate(-1);
        }

        auto nodeId = Node_->GetId();
        auto result = !error.IsOK() || Node_->GetChunkList() != RootChunkList_
            ? EMergeSessionResult::TransientFailure
            : EMergeSessionResult::OK;
        chunkVisitorHost->OnTraversalFinished(nodeId, result);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->EphemeralUnrefObject(Account_);
        objectManager->EphemeralUnrefObject(Node_);
        objectManager->EphemeralUnrefObject(RootChunkList_);
    }

    bool MaybeAddChunk(TChunk* chunk)
    {
        const auto& config = GetDynamicConfig();
        if (CurrentRowCount_ + chunk->GetRowCount() < config->MaxRowCount &&
            CurrentDataWeight_ + chunk->GetDataWeight() < config->MaxDataWeight &&
            CurrentUncompressedDataSize_ + chunk->GetUncompressedDataSize() < config->MaxUncompressedDataSize &&
            std::ssize(ChunkIds_) < config->MaxChunkCount &&
            chunk->GetDataWeight() < config->MaxInputChunkDataWeight)
        {
            CurrentRowCount_ += chunk->GetRowCount();
            CurrentDataWeight_ += chunk->GetDataWeight();
            CurrentUncompressedDataSize_ += chunk->GetUncompressedDataSize();
            ChunkIds_.push_back(chunk->GetId());
            return true;
        }
        return false;
    };

    void MaybePlanJob()
    {
        auto chunkVisitorHost = ChunkVisitorHost_.Lock();
        if (!chunkVisitorHost) {
            return;
        }

        const auto& config = GetDynamicConfig();
        if (std::ssize(ChunkIds_) < config->MinChunkCount) {
            return;
        }

        auto nodeId = Node_->GetId();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto jobId = chunkManager->GenerateJobId();
        chunkVisitorHost->RegisterJobAwaitingChunkCreation(jobId, nodeId, RootChunkList_->GetId(), std::move(ChunkIds_));
    }

    const TDynamicChunkMergerConfigPtr& GetDynamicConfig() const
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->ChunkManager->ChunkMerger;
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkMerger::TChunkMerger(TBootstrap* bootstrap)
    : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::ChunkMerger)
    , Bootstrap_(bootstrap)
    , ChunkReplacer_(New<TChunkReplacerCallbacks>(Bootstrap_), Logger)
{
    YT_VERIFY(Bootstrap_);

    RegisterLoader(
        "ChunkMerger",
        BIND(&TChunkMerger::Load, Unretained(this)));

    RegisterSaver(
        ESyncSerializationPriority::Values,
        "ChunkMerger",
        BIND(&TChunkMerger::Save, Unretained(this)));

    RegisterMethod(BIND(&TChunkMerger::HydraStartMergeTransaction, Unretained(this)));
    RegisterMethod(BIND(&TChunkMerger::HydraCreateChunks, Unretained(this)));
    RegisterMethod(BIND(&TChunkMerger::HydraReplaceChunks, Unretained(this)));
    RegisterMethod(BIND(&TChunkMerger::HydraFinalizeChunkMergeSessions, Unretained(this)));
}

void TChunkMerger::Initialize()
{
    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    transactionManager->SubscribeTransactionAborted(BIND(&TChunkMerger::OnTransactionAborted, MakeWeak(this)));

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(BIND(&TChunkMerger::OnDynamicConfigChanged, MakeWeak(this)));
}

void TChunkMerger::ScheduleMerge(TObjectId nodeId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    if (auto* trunkNode = FindChunkOwner(nodeId)) {
        ScheduleMerge(trunkNode);
    }
}

void TChunkMerger::ScheduleMerge(TChunkOwnerBase* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    YT_VERIFY(trunkNode->IsTrunk());

    if (!Enabled_) {
        YT_LOG_DEBUG("Cannot schedule merge: chunk merger is disabled");
        return;
    }

    if (trunkNode->GetType() != EObjectType::Table) {
        YT_LOG_DEBUG("Chunk merging is supported only for table types (NodeId: %v)",
            trunkNode->GetId());
        return;
    }

    auto* table = trunkNode->As<TTableNode>();
    if (table->IsDynamic()) {
        YT_LOG_DEBUG("Chunk merging is not supported for dynamic tables (NodeId: %v)",
            trunkNode->GetId());
        return;
    }

    DoScheduleMerge(trunkNode);
}

void TChunkMerger::DoScheduleMerge(TChunkOwnerBase* chunkOwner)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    if (!RegisterSession(chunkOwner)) {
        return;
    };

    auto* account = chunkOwner->GetAccount();
    YT_LOG_DEBUG_IF(
        IsMutationLoggingEnabled(),
        "Scheduling merge (NodeId: %v, Account: %v)",
        chunkOwner->GetId(),
        account->GetName());

    if (IsLeader()) {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto [it, inserted] = AccountToNodeQueue_.emplace(account, TNodeQueue());
        if (inserted) {
            objectManager->EphemeralRefObject(account);
        }

        auto& queue = it->second;
        queue.push(chunkOwner->GetId());
    }
}

bool TChunkMerger::IsNodeBeingMerged(TObjectId nodeId) const
{
    return NodesBeingMerged_.contains(nodeId);
}

void TChunkMerger::ScheduleJobs(IJobSchedulingContext* context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& resourceUsage = context->GetNodeResourceUsage();
    const auto& resourceLimits = context->GetNodeResourceLimits();

    auto hasSpareMergeResources = [&] {
        return resourceUsage.merge_slots() < resourceLimits.merge_slots() &&
            (resourceUsage.merge_slots() == 0 || resourceUsage.merge_data_size() < resourceLimits.merge_data_size());
    };

    while (!JobsAwaitingNodeHeartbeat_.empty() && hasSpareMergeResources()) {
        auto jobInfo = std::move(JobsAwaitingNodeHeartbeat_.front());
        JobsAwaitingNodeHeartbeat_.pop();

        if (!TryScheduleMergeJob(context, jobInfo)) {
            auto* trunkNode = FindChunkOwner(jobInfo.NodeId);
            FinalizeJob(
                jobInfo.NodeId,
                jobInfo.JobId,
                CanScheduleMerge(trunkNode) ? EMergeSessionResult::TransientFailure : EMergeSessionResult::PermanentFailure);
            continue;
        }
    }
}

void TChunkMerger::OnProfiling(TSensorBuffer* buffer) const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    for (const auto& [account, queue] : AccountToNodeQueue_) {
        if (!IsObjectAlive(account)) {
            continue;
        }
        buffer->PushTag({"account", account->GetName()});
        buffer->AddGauge("/chunk_merger_account_queue_size", queue.size());
        buffer->PopTag();
    }

    buffer->AddGauge("/chunk_merger_jobs_awaiting_chunk_creation", JobsAwaitingChunkCreation_.size());
    buffer->AddGauge("/chunk_merger_jobs_undergoing_chunk_creation", JobsUndergoingChunkCreation_.size());
    buffer->AddGauge("/chunk_merger_jobs_awaiting_node_heartbeat", JobsAwaitingNodeHeartbeat_.size());
    buffer->AddGauge("/chunk_merger_running_jobs", RunningJobs_.size());

    buffer->AddCounter("/chunk_merger_chunk_replacement_succeeded", ChunkReplacementSucceded_);
    buffer->AddCounter("/chunk_merger_chunk_replacement_failed", ChunkReplacementFailed_);
    buffer->AddCounter("/chunk_merger_chunk_count_saving", ChunkCountSaving_);

    buffer->AddCounter("/chunk_merger_sessions_awaiting_finalization", SessionsAwaitingFinalizaton_.size());
}

void TChunkMerger::OnJobWaiting(const TJobPtr& job, IJobControllerCallbacks* callbacks)
{
    // In Chunk Merger we don't distinguish between waiting and running jobs.
    OnJobRunning(job, callbacks);
}

void TChunkMerger::OnJobRunning(const TJobPtr& job, IJobControllerCallbacks* callbacks)
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    auto jobTimeout = configManager->GetConfig()->ChunkManager->JobTimeout;
    if (TInstant::Now() - job->GetStartTime() > jobTimeout) {
        YT_LOG_WARNING("Job timed out, aborting (JobId: %v, JobType: %v, Address: %v, Duration: %v, ChunkId: %v)",
            job->GetJobId(),
            job->GetType(),
            job->GetNode()->GetDefaultAddress(),
            TInstant::Now() - job->GetStartTime(),
            job->GetChunkIdWithIndexes());

        callbacks->AbortJob(job);
    }
}

void TChunkMerger::OnJobCompleted(const TJobPtr& job)
{
    OnJobFinished(job);
}

void TChunkMerger::OnJobAborted(const TJobPtr& job)
{
    OnJobFinished(job);
}

void TChunkMerger::OnJobFailed(const TJobPtr& job)
{
    OnJobFinished(job);
}

void TChunkMerger::OnLeaderActive()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::OnLeaderActive();

    ResetTransientState();

    StartMergeTransaction();

    const auto& config = GetDynamicConfig();

    ScheduleExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMerger),
        BIND(&TChunkMerger::ProcessTouchedNodes, MakeWeak(this)),
        config->SchedulePeriod);
    ScheduleExecutor_->Start();

    ChunkCreatorExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMerger),
        BIND(&TChunkMerger::CreateChunks, MakeWeak(this)),
        config->CreateChunksPeriod);
    ChunkCreatorExecutor_->Start();

    StartTransactionExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMerger),
        BIND(&TChunkMerger::StartMergeTransaction, MakeWeak(this)),
        config->TransactionUpdatePeriod);
    StartTransactionExecutor_->Start();

    FinalizeSessionExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMerger),
        BIND(&TChunkMerger::FinalizeSessions, MakeWeak(this)),
        config->SessionFinalizationPeriod);
    FinalizeSessionExecutor_->Start();
}

void TChunkMerger::OnStopLeading()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::OnStopLeading();

    ResetTransientState();

    if (ScheduleExecutor_) {
        ScheduleExecutor_->Stop();
        ScheduleExecutor_.Reset();
    }

    if (ChunkCreatorExecutor_) {
        ChunkCreatorExecutor_->Stop();
        ChunkCreatorExecutor_.Reset();
    }

    if (StartTransactionExecutor_) {
        StartTransactionExecutor_->Stop();
        StartTransactionExecutor_.Reset();
    }

    if (FinalizeSessionExecutor_) {
        FinalizeSessionExecutor_->Stop();
        FinalizeSessionExecutor_.Reset();
    }
}

void TChunkMerger::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::Clear();

    TransactionId_ = {};
    PreviousTransactionId_ = {};
    NodesBeingMerged_.clear();
}

void TChunkMerger::ResetTransientState()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    AccountToNodeQueue_ = {};
    JobsAwaitingChunkCreation_ = {};
    JobsUndergoingChunkCreation_ = {};
    JobsAwaitingNodeHeartbeat_ = {};
    RunningJobs_ = {};
    RunningSessions_ = {};
    SessionsAwaitingFinalizaton_ = {};
}

bool TChunkMerger::IsMergeTransactionAlive() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!TransactionId_) {
        return false;
    }

    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    auto* transaction = transactionManager->FindTransaction(TransactionId_);
    return IsObjectAlive(transaction);
}

bool TChunkMerger::CanScheduleMerge(TChunkOwnerBase* chunkOwner) const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return Enabled_ && IsObjectAlive(chunkOwner);
}

void TChunkMerger::StartMergeTransaction()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(IsLeader());

    NProto::TReqStartMergeTransaction request;
    CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
        ->CommitAndLog(Logger);
}

void TChunkMerger::OnTransactionAborted(TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    auto transactionId = transaction->GetId();

    if (transactionId == PreviousTransactionId_) {
        PreviousTransactionId_ = {};
    }

    if (transactionId == TransactionId_) {
        TransactionId_ = {};
        if (IsLeader()) {
            StartMergeTransaction();
        }
    }
}

bool TChunkMerger::RegisterSession(TChunkOwnerBase* chunkOwner)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    if (NodesBeingMerged_.contains(chunkOwner->GetId())) {
        chunkOwner->SetUpdatedSinceLastMerge(true);
        return false;
    }

    if (chunkOwner->GetUpdatedSinceLastMerge()) {
        YT_LOG_ALERT_IF(
            IsMutationLoggingEnabled(),
            "Node is marked as updated, but has no running merge sessions (NodeId: %v)",
            chunkOwner->GetId());
        chunkOwner->SetUpdatedSinceLastMerge(false);
    }

    YT_VERIFY(NodesBeingMerged_.insert(chunkOwner->GetId()).second);

    if (IsLeader()) {
        YT_LOG_DEBUG("Starting new merge job session (NodeId: %v)", chunkOwner->GetId());
        YT_VERIFY(RunningSessions_.emplace(chunkOwner->GetId(), TChunkMergerSession()).second);
    }
    return true;
}

void TChunkMerger::FinalizeJob(TObjectId nodeId, TJobId jobId, EMergeSessionResult result)
{
    if (!IsLeader()) {
        return;
    }

    YT_LOG_DEBUG("Finalizing merge job (NodeId: %v, JobId: %v)", nodeId, jobId);
    auto it = RunningSessions_.find(nodeId);
    YT_VERIFY(it != RunningSessions_.end());
    auto& session = it->second;
    YT_VERIFY(session.Jobs.erase(jobId) > 0);
    session.Result = std::max(session.Result, result);
    if (session.Jobs.empty()) {
        ScheduleSessionFinalization(nodeId, EMergeSessionResult::None);
    }
}

void TChunkMerger::RegisterJobAwaitingChunkCreation(
    TJobId jobId,
    TObjectId nodeId,
    TChunkListId rootChunkListId,
    std::vector<TChunkId> inputChunkIds)
{
    JobsAwaitingChunkCreation_.push({
        .JobId = jobId,
        .NodeId = nodeId,
        .RootChunkListId = rootChunkListId,
        .InputChunkIds = std::move(inputChunkIds)
    });

    YT_LOG_DEBUG("Planning merge job (JobId: %v, NodeId: %v, RootChunkListId: %v)",
        jobId,
        nodeId,
        rootChunkListId);

    auto it = RunningSessions_.find(nodeId);
    YT_VERIFY(it != RunningSessions_.end());
    YT_VERIFY(it->second.Jobs.insert(jobId).second);
}

void TChunkMerger::OnTraversalFinished(TObjectId nodeId, EMergeSessionResult result)
{
    auto it = RunningSessions_.find(nodeId);
    YT_VERIFY(it != RunningSessions_.end());
    const auto& session = it->second;
    if (session.Jobs.empty()) {
        ScheduleSessionFinalization(nodeId, result);
    }
}

void TChunkMerger::ScheduleSessionFinalization(TObjectId nodeId, EMergeSessionResult result)
{
    if (!IsLeader()) {
        return;
    }

    auto it = RunningSessions_.find(nodeId);
    YT_VERIFY(it != RunningSessions_.end());
    auto& session = it->second;
    session.Result = std::max(session.Result, result);

    YT_VERIFY(session.Jobs.empty());
    YT_LOG_DEBUG_IF(
        IsMutationLoggingEnabled(),
        "Finalizing merge session (NodeId: %v, Result: %v)",
        nodeId,
        session.Result);
    SessionsAwaitingFinalizaton_.push({
        .NodeId = nodeId,
        .Result = session.Result
    });
    RunningSessions_.erase(it);
}

void TChunkMerger::FinalizeSessions()
{
    if (SessionsAwaitingFinalizaton_.empty()) {
        return;
    }

    const auto& config = GetDynamicConfig();
    TReqFinalizeChunkMergeSessions request;
    for (auto index = 0; index < config->SessionFinalizationBatchSize && !SessionsAwaitingFinalizaton_.empty(); ++index) {
        auto* req = request.add_subrequests();
        const auto& sessionResult = SessionsAwaitingFinalizaton_.front();
        ToProto(req->mutable_node_id(), sessionResult.NodeId);
        req->set_result(ToProto<int>(sessionResult.Result));
        SessionsAwaitingFinalizaton_.pop();
    }

    CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
        ->CommitAndLog(Logger);
}

void TChunkMerger::ProcessTouchedNodes()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(IsLeader());

    const auto& config = GetDynamicConfig();

    std::vector<TAccount*> accountsToRemove;
    const auto& objectManager = Bootstrap_->GetObjectManager();

    for (auto& [account, queue] : AccountToNodeQueue_) {
        if (!IsObjectAlive(account)) {
            accountsToRemove.push_back(account);
            continue;
        }

        auto maxRate = account->GetMergeJobRateLimit();
        while (!queue.empty() &&
            account->GetMergeJobRate() < maxRate &&
            std::ssize(JobsAwaitingNodeHeartbeat_) < config->QueueSizeLimit)
        {
            auto nodeId = queue.front();
            queue.pop();

            YT_VERIFY(RunningSessions_.contains(nodeId));
            auto* node = FindChunkOwner(nodeId);

            if (CanScheduleMerge(node)) {
                account->IncrementMergeJobRate(1);
                New<TMergeChunkVisitor>(
                    Bootstrap_,
                    node,
                    MakeWeak(this))
                    ->Run();
            } else {
                ScheduleSessionFinalization(nodeId, EMergeSessionResult::PermanentFailure);
            }
        }
    }

    for (auto* account : accountsToRemove) {
        auto it = AccountToNodeQueue_.find(account);
        YT_VERIFY(it != AccountToNodeQueue_.end());

        auto& queue = it->second;
        while (!queue.empty()) {
            auto nodeId = queue.front();
            ScheduleSessionFinalization(nodeId, EMergeSessionResult::OK);
            queue.pop();
        }

        objectManager->EphemeralUnrefObject(account);
        AccountToNodeQueue_.erase(it);
    }
}

void TChunkMerger::CreateChunks()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(IsLeader());

    if (JobsAwaitingChunkCreation_.empty()) {
        return;
    }

    if (!IsMergeTransactionAlive()) {
        return;
    }

    TReqCreateChunks createChunksReq;
    ToProto(createChunksReq.mutable_transaction_id(), TransactionId_);

    const auto& config = GetDynamicConfig();
    for (auto index = 0; index < config->CreateChunksBatchSize && !JobsAwaitingChunkCreation_.empty(); ++index) {
        const auto& jobInfo = JobsAwaitingChunkCreation_.front();

        auto* node = FindChunkOwner(jobInfo.NodeId);
        if (!node) {
            FinalizeJob(jobInfo.NodeId, jobInfo.JobId, EMergeSessionResult::PermanentFailure);
            JobsAwaitingChunkCreation_.pop();
            continue;
        }

        YT_LOG_DEBUG("Creating chunks for merge (JobId: %v, NodeId: %v)",
            jobInfo.JobId,
            jobInfo.NodeId);

        auto* req = createChunksReq.add_subrequests();

        auto mediumIndex = node->GetPrimaryMediumIndex();
        req->set_medium_index(mediumIndex);

        auto erasureCodec = node->GetErasureCodec();
        req->set_erasure_codec(ToProto<int>(erasureCodec));

        if (erasureCodec == NErasure::ECodec::None) {
            req->set_type(ToProto<int>(EObjectType::Chunk));
            const auto& policy = node->Replication().Get(mediumIndex);
            req->set_replication_factor(policy.GetReplicationFactor());
        } else {
            req->set_type(ToProto<int>(EObjectType::ErasureChunk));
            req->set_replication_factor(1);
        }

        req->set_account(node->GetAccount()->GetName());

        req->set_vital(node->Replication().GetVital());
        ToProto(req->mutable_job_id(), jobInfo.JobId);

        YT_VERIFY(JobsUndergoingChunkCreation_.emplace(jobInfo.JobId, std::move(jobInfo)).second);
        JobsAwaitingChunkCreation_.pop();
    }

    CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), createChunksReq)
        ->CommitAndLog(Logger);
}

bool TChunkMerger::TryScheduleMergeJob(IJobSchedulingContext* context, const TMergeJobInfo& jobInfo)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* chunkOwner = FindChunkOwner(jobInfo.NodeId);
    if (!CanScheduleMerge(chunkOwner)) {
        return false;
    }
    if (!chunkOwner->GetChunkList() || chunkOwner->GetChunkList()->GetId() != jobInfo.RootChunkListId) {
        return false;
    }

    TChunkMergerWriterOptions chunkMergerWriterOptions;
    if (chunkOwner->GetType() == EObjectType::Table) {
        const auto* table = chunkOwner->As<TTableNode>();
        ToProto(chunkMergerWriterOptions.mutable_schema(), *table->GetSchema()->AsTableSchema());
        chunkMergerWriterOptions.set_optimize_for(ToProto<int>(table->GetOptimizeFor()));
    }
    chunkMergerWriterOptions.set_compression_codec(ToProto<int>(chunkOwner->GetCompressionCodec()));
    chunkMergerWriterOptions.set_erasure_codec(ToProto<int>(chunkOwner->GetErasureCodec()));
    chunkMergerWriterOptions.set_enable_skynet_sharing(chunkOwner->GetEnableSkynetSharing());

    TMergeJob::TChunkVector inputChunks;
    inputChunks.reserve(jobInfo.InputChunkIds.size());

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    for (auto chunkId : jobInfo.InputChunkIds) {
        auto* chunk = chunkManager->FindChunk(chunkId);
        if (!IsObjectAlive(chunk)) {
            return false;
        }
        inputChunks.push_back(chunk);
    }

    auto* chunkRequstitionRegistry = chunkManager->GetChunkRequisitionRegistry();
    auto* outputChunk = chunkManager->FindChunk(jobInfo.OutputChunkId);
    if (!IsObjectAlive(outputChunk)) {
        return false;
    }

    const auto& requisition = outputChunk->GetAggregatedRequisition(chunkRequstitionRegistry);
    TChunkIdWithIndexes chunkIdWithIndexes(
        jobInfo.OutputChunkId,
        GenericChunkReplicaIndex,
        requisition.begin()->MediumIndex);
    auto erasureCodec = outputChunk->GetErasureCodec();
    int targetCount = erasureCodec == NErasure::ECodec::None
        ? outputChunk->GetAggregatedReplicationFactor(
            chunkIdWithIndexes.MediumIndex,
            chunkRequstitionRegistry)
        : NErasure::GetCodec(erasureCodec)->GetTotalPartCount();

    auto* dataCenter = context->GetNode()->GetDataCenter();
    const auto& feasibleDataCenters = context->GetJobRegistry()->GetUnsaturatedInterDCEdgesStartingFrom(dataCenter);
    auto targetNodes = chunkManager->AllocateWriteTargets(
        chunkManager->GetMediumByIndexOrThrow(chunkIdWithIndexes.MediumIndex),
        outputChunk,
        targetCount,
        targetCount,
        /*replicationFactorOverride*/ std::nullopt,
        feasibleDataCenters);
    if (targetNodes.empty()) {
        return false;
    }

    TNodePtrWithIndexesList targetReplicas;
    int targetIndex = 0;
    for (auto* node : targetNodes) {
        targetReplicas.emplace_back(
            node,
            erasureCodec == NErasure::ECodec::None ? GenericChunkReplicaIndex : targetIndex++,
            chunkIdWithIndexes.MediumIndex);
    }

    auto job = New<TMergeJob>(
        jobInfo.JobId,
        context->GetNode(),
        chunkIdWithIndexes,
        std::move(inputChunks),
        std::move(chunkMergerWriterOptions),
        std::move(targetReplicas));
    context->ScheduleJob(job);

    YT_LOG_DEBUG("Merge job scheduled (JobId: %v, Address: %v, NodeId: %v, InputChunkIds: %v, OutputChunkId: %v)",
        job->GetJobId(),
        context->GetNode()->GetDefaultAddress(),
        jobInfo.NodeId,
        jobInfo.InputChunkIds,
        jobInfo.OutputChunkId);

    YT_VERIFY(RunningJobs_.emplace(job->GetJobId(), std::move(jobInfo)).second);

    return true;
}

void TChunkMerger::ScheduleReplaceChunks(const TMergeJobInfo& jobInfo)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_LOG_DEBUG("Scheduling chunk replace after merge (JobId: %v, NodeId: %v)",
        jobInfo.JobId,
        jobInfo.NodeId);

    TReqReplaceChunks request;
    ToProto(request.mutable_new_chunk_id(), jobInfo.OutputChunkId);
    ToProto(request.mutable_node_id(), jobInfo.NodeId);
    ToProto(request.mutable_old_chunk_ids(), jobInfo.InputChunkIds);
    ToProto(request.mutable_job_id(), jobInfo.JobId);

    CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
        ->CommitAndLog(Logger);
}

void TChunkMerger::OnJobFinished(const TJobPtr& job)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(job->GetType() == EJobType::MergeChunks);

    auto jobId = job->GetJobId();
    auto it = RunningJobs_.find(jobId);
    if (it == RunningJobs_.end()) {
        YT_LOG_ALERT("Unknown job finished in chunk merger (JobId: %v)",
            jobId);
        return;
    }

    const auto& jobInfo = it->second;
    switch (job->GetState()) {
        case EJobState::Completed:
            ScheduleReplaceChunks(jobInfo);
            break;

        case EJobState::Failed:
        case EJobState::Aborted:
            FinalizeJob(jobInfo.NodeId, jobInfo.JobId, EMergeSessionResult::TransientFailure);
            break;

        default:
            YT_ABORT();
    }

    RunningJobs_.erase(it);
}

const TDynamicChunkMergerConfigPtr& TChunkMerger::GetDynamicConfig() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->ChunkManager->ChunkMerger;
}

void TChunkMerger::OnDynamicConfigChanged(TDynamicClusterConfigPtr)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& config = GetDynamicConfig();

    if (ScheduleExecutor_) {
        ScheduleExecutor_->SetPeriod(config->SchedulePeriod);
    }
    if (ChunkCreatorExecutor_) {
        ChunkCreatorExecutor_->SetPeriod(config->CreateChunksPeriod);
    }
    if (StartTransactionExecutor_) {
        StartTransactionExecutor_->SetPeriod(config->TransactionUpdatePeriod);
    }
    if (FinalizeSessionExecutor_) {
        FinalizeSessionExecutor_->SetPeriod(config->SessionFinalizationPeriod);
    }

    auto enable = config->Enable;

    if (Enabled_ && !enable) {
        YT_LOG_INFO("Chunk merger is disabled, see //sys/@config");
        Enabled_ = false;
    }

    if (!Enabled_ && enable) {
        YT_LOG_INFO("Chunk merger is enabled");
        Enabled_ = true;
    }
}

TChunkOwnerBase* TChunkMerger::FindChunkOwner(TObjectId nodeId)
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto* node = cypressManager->FindNode(TVersionedNodeId(nodeId));
    if (!IsObjectAlive(node)) {
        return nullptr;
    }
    if (!IsChunkOwnerType(node->GetType())) {
        return nullptr;
    }
    return node->As<TChunkOwnerBase>();
}

void TChunkMerger::HydraCreateChunks(NProto::TReqCreateChunks* request)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    const auto& transactionManager = Bootstrap_->GetTransactionManager();

    auto transactionId = FromProto<TTransactionId>(request->transaction_id());
    auto* transaction = transactionManager->FindTransaction(transactionId);

    for (const auto& subrequest : request->subrequests()) {
        auto jobId = FromProto<TJobId>(subrequest.job_id());

        auto eraseFromQueue = [&] () {
            if (!IsLeader()) {
                return;
            }

            auto it = JobsUndergoingChunkCreation_.find(jobId);
            if (it == JobsUndergoingChunkCreation_.end()) {
                return;
            }

            FinalizeJob(it->second.NodeId, jobId, EMergeSessionResult::TransientFailure);
            JobsUndergoingChunkCreation_.erase(it);
        };

        if (!IsObjectAlive(transaction)) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk merger cannot create chunks: no such transaction (JobId: %v, TransactionId: %v)",
                jobId,
                transactionId);
            eraseFromQueue();
            continue;
        }

        auto chunkType = FromProto<EObjectType>(subrequest.type());

        auto mediumIndex = subrequest.medium_index();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!IsObjectAlive(medium)) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk merger cannot create chunks: no such medium (JobId: %v, MediumIndex: %v)",
                jobId,
                mediumIndex);
            eraseFromQueue();
            continue;
        }

        const auto& accountName = subrequest.account();
        auto* account = securityManager->FindAccountByName(accountName, /*activeLifeStageOnly*/ true);
        if (!IsObjectAlive(account)) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk merger cannot create chunks: no such account (JobId: %v, Account: %v)",
                jobId,
                accountName);
            eraseFromQueue();
            continue;
        }

        auto* chunk = chunkManager->CreateChunk(
            transaction,
            nullptr,
            chunkType,
            account,
            subrequest.replication_factor(),
            FromProto<NErasure::ECodec>(subrequest.erasure_codec()),
            medium,
            /*readQuorum*/ 0,
            /*writeQuorum*/ 0,
            /*movable*/ true,
            subrequest.vital());

        if (IsLeader()) {
            // NB: JobsUndergoingChunkCreation_ is transient, do not make any persistent decisions based on it.
            auto it = JobsUndergoingChunkCreation_.find(jobId);
            if (it == JobsUndergoingChunkCreation_.end()) {
                YT_LOG_DEBUG("Merge job is not registered, chunk will not be used (JobId: %v, OutputChunkId: %v)",
                    jobId,
                    chunk->GetId());
            } else {
                YT_LOG_DEBUG("Output chunk created for merge job (JobId: %v, OutputChunkId: %v)",
                    jobId,
                    chunk->GetId());
                auto& jobInfo = it->second;
                jobInfo.OutputChunkId = chunk->GetId();
                JobsAwaitingNodeHeartbeat_.push(std::move(jobInfo));
                JobsUndergoingChunkCreation_.erase(it);
            }
        }
    }
}

void TChunkMerger::HydraReplaceChunks(NProto::TReqReplaceChunks* request)
{
    auto nodeId = FromProto<TObjectId>(request->node_id());
    auto jobId = FromProto<TObjectId>(request->job_id());
    auto* chunkOwner = FindChunkOwner(nodeId);
    if (!chunkOwner) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: no such chunk owner node (NodeId: %v)",
            nodeId);
        ++ChunkReplacementFailed_;
        FinalizeJob(nodeId, jobId, EMergeSessionResult::PermanentFailure);
        return;
    }

    if (chunkOwner->GetType() == EObjectType::Table) {
        auto* table = chunkOwner->As<TTableNode>();
        if (table->IsDynamic()) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: table is dynamic (NodeId: %v)",
                chunkOwner->GetId());
            ++ChunkReplacementFailed_;
            FinalizeJob(nodeId, jobId, EMergeSessionResult::PermanentFailure);
            return;
        }
    }
    auto* oldChunkList = chunkOwner->GetChunkList();

    auto newChunkId = FromProto<TChunkId>(request->new_chunk_id());

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto* newChunk = chunkManager->FindChunk(newChunkId);
    if (!IsObjectAlive(newChunk)) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: no such chunk (NodeId: %v, ChunkId: %v)",
            nodeId,
            newChunkId);
        ++ChunkReplacementFailed_;
        FinalizeJob(nodeId, jobId, EMergeSessionResult::TransientFailure);
        return;
    }

    auto oldChunkIds = FromProto<std::vector<TChunkId>>(request->old_chunk_ids());

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Replacing chunks after merge (NodeId: %v, OldChunkIds: %v, NewChunkId: %v)",
        nodeId,
        oldChunkIds,
        newChunkId);

    auto* newChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);

    if (!ChunkReplacer_.Replace(oldChunkList, newChunkList, newChunk, oldChunkIds)) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: root has changed (NodeId: %v)",
            nodeId);
        ++ChunkReplacementFailed_;
        FinalizeJob(nodeId, jobId, EMergeSessionResult::TransientFailure);
        return;
    }

    // Change chunk list.
    const auto& objectManager = Bootstrap_->GetObjectManager();

    newChunkList->AddOwningNode(chunkOwner);
    oldChunkList->RemoveOwningNode(chunkOwner);

    chunkOwner->SetChunkList(newChunkList);
    chunkOwner->SnapshotStatistics() = newChunkList->Statistics().ToDataStatistics();

    objectManager->RefObject(newChunkList);

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk list replaced after merge (NodeId: %v, OldChunkListId: %v, NewChunkListId: %v)",
        nodeId,
        oldChunkList->GetId(),
        newChunkList->GetId());
    ++ChunkReplacementSucceded_;
    ChunkCountSaving_ += oldChunkIds.size() - 1;

    objectManager->UnrefObject(oldChunkList);

    chunkManager->ScheduleChunkRequisitionUpdate(newChunk);

    FinalizeJob(nodeId, jobId, EMergeSessionResult::OK);

    if (chunkOwner->IsForeign()) {
        const auto& tableManager = Bootstrap_->GetTableManager();
        tableManager->ScheduleStatisticsUpdate(
            chunkOwner,
            /*updateDataStatistics*/ true,
            /*updateTabletStatistics*/ false,
            /*useNativeContentRevisionCas*/ true);
    }
}

void TChunkMerger::HydraFinalizeChunkMergeSessions(NProto::TReqFinalizeChunkMergeSessions* request)
{
    for (const auto& subrequest : request->subrequests()) {
        auto nodeId = FromProto<TObjectId>(subrequest.node_id());
        auto result = FromProto<EMergeSessionResult>(subrequest.result());
        YT_VERIFY(result != EMergeSessionResult::None);

        YT_VERIFY(NodesBeingMerged_.erase(nodeId) > 0);
        auto* chunkOwner = FindChunkOwner(nodeId);
        if (!chunkOwner) {
            continue;
        }

        auto nodeTouched = chunkOwner->GetUpdatedSinceLastMerge();
        chunkOwner->SetUpdatedSinceLastMerge(false);
        YT_LOG_DEBUG_IF(
            IsMutationLoggingEnabled(),
            "Finalizing merge session (NodeId: %v, NodeTouched: %v, Result: %v)",
            nodeId,
            nodeTouched,
            result);
        if (result == EMergeSessionResult::TransientFailure || (result == EMergeSessionResult::OK && nodeTouched)) {
            ScheduleMerge(nodeId);
        }
    }
}

void TChunkMerger::HydraStartMergeTransaction(NProto::TReqStartMergeTransaction* /*request*/)
{
    const auto& transactionManager = Bootstrap_->GetTransactionManager();

    if (PreviousTransactionId_) {
        auto* previousTransaction = transactionManager->FindTransaction(PreviousTransactionId_);
        if (IsObjectAlive(previousTransaction)) {
            // It does not matter whether we commit or abort this transaction.
            // (Except it does because of OnTransactionAborted.)
            transactionManager->CommitTransaction(previousTransaction, NullTimestamp);
        }
    }

    PreviousTransactionId_ = TransactionId_;

    auto* transaction = transactionManager->StartTransaction(
        /*parent*/ nullptr,
        /*prerequisiteTransactions*/ {},
        {},
        /*timeout*/ std::nullopt,
        /*deadline*/ std::nullopt,
        "Chunk merger transaction",
        EmptyAttributes());

    TransactionId_ = transaction->GetId();
    YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Merge transaction updated (NewTransactionId: %v, PreviousTransactionId: %v)",
        TransactionId_,
        PreviousTransactionId_);
}

void TChunkMerger::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    // Persist transactions so that we can commit them.
    Save(context, TransactionId_);
    Save(context, PreviousTransactionId_);
    Save(context, NodesBeingMerged_);
}

void TChunkMerger::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, TransactionId_);
    Load(context, PreviousTransactionId_);
    if (context.GetVersion() >= EMasterReign::PersistNodesBeingMerged) {
        Load(context, NodesBeingMerged_);
    }
}

void TChunkMerger::OnAfterSnapshotLoaded()
{
    TMasterAutomatonPart::OnAfterSnapshotLoaded();

    auto nodesBeingMerged = std::move(NodesBeingMerged_);
    NodesBeingMerged_.clear();

    for (auto nodeId : nodesBeingMerged) {
        auto* node = FindChunkOwner(nodeId);
        if (!IsObjectAlive(node)) {
            continue;
        }

        node->SetUpdatedSinceLastMerge(false);
        ScheduleMerge(node);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

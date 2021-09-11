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

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/cypress_client/cypress_service_proxy.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/library/erasure/public.h>
#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <google/protobuf/util/message_differencer.h>

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
using namespace NNodeTrackerClient::NProto;
using namespace NTableClient::NProto;

using NYT::ToProto;
using NYT::FromProto;
using NChunkClient::NProto::TMiscExt;

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

    void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children) override
    {
        Bootstrap_->GetChunkManager()->AttachToChunkList(chunkList, children);
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* child) override
    {
        Bootstrap_->GetChunkManager()->AttachToChunkList(chunkList, child);
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd) override
    {
        Bootstrap_->GetChunkManager()->AttachToChunkList(chunkList, childrenBegin, childrenEnd);
    }

    TChunkList* CreateChunkList(EChunkListKind kind) override
    {
        return Bootstrap_->GetChunkManager()->CreateChunkList(kind);
    }

    bool IsMutationLoggingEnabled() override
    {
        return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsMutationLoggingEnabled();
    }

private:
    TBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

namespace {

bool ChunkMetaEqual(const TChunk* lhs, const TChunk* rhs)
{
    const auto& lhsMeta = lhs->ChunkMeta();
    const auto& rhsMeta = rhs->ChunkMeta();

    if (lhsMeta->GetType() != rhsMeta->GetType() || lhsMeta->GetFormat() != rhsMeta->GetFormat()) {
        return false;
    }

    auto lhsMiscExt = lhsMeta->FindExtension<TMiscExt>();
    auto rhsMiscExt = rhsMeta->FindExtension<TMiscExt>();

    if (!lhsMiscExt || !rhsMiscExt) {
        return false;
    }

    if (lhsMiscExt->compression_codec() != rhsMiscExt->compression_codec()) {
        return false;
    }

    auto lhsNameTableExt = lhsMeta->FindExtension<TNameTableExt>();
    auto rhsNameTableExt = rhsMeta->FindExtension<TNameTableExt>();
    if (lhsNameTableExt.has_value() != rhsNameTableExt.has_value()) {
        return false;
    }

    if (lhsNameTableExt && rhsNameTableExt) {
        return google::protobuf::util::MessageDifferencer::Equals(*lhsNameTableExt, *rhsNameTableExt);
    }

    return true;
}

} // namespace

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
        , Mode_(Node_->GetChunkMergerMode())
        , Account_(Node_->GetAccount())
        , ChunkVisitorHost_(std::move(chunkVisitorHost))
        , CurrentJobMode_(Mode_)
    {
        YT_VERIFY(Mode_ != EChunkMergerMode::None);
    }

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

        YT_LOG_DEBUG("Traversal started (NodeId: %v, RootChunkListId: %v)",
            Node_->GetId(),
            Node_->GetChunkList()->GetId());
        TraverseChunkTree(std::move(callbacks), this, Node_->GetChunkList());
    }

private:
    TBootstrap* const Bootstrap_;
    TChunkOwnerBase* const Node_;
    const EChunkMergerMode Mode_;
    TAccount* const Account_;
    const TWeakPtr<IMergeChunkVisitorHost> ChunkVisitorHost_;

    // Used to order jobs results correctly for chunk replace.
    int JobIndex_ = 0;

    std::vector<TChunkId> ChunkIds_;
    EChunkMergerMode CurrentJobMode_;

    TChunkListId ParentChunkListId_;
    i64 CurrentRowCount_ = 0;
    i64 CurrentDataWeight_ = 0;
    i64 CurrentUncompressedDataSize_ = 0;

    TChunkListId LastChunkListId_;
    int JobsForLastChunkList_ = 0;

    bool OnChunk(
        TChunk* chunk,
        TChunkList* parent,
        std::optional<i64> /*rowIndex*/,
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TReadLimit& /*lowerLimit*/,
        const NChunkClient::TReadLimit& /*upperLimit*/,
        TTransactionId /*timestampTransactionId*/) override
    {
        if (MaybeAddChunk(chunk, parent)) {
            return true;
        }

        MaybePlanJob();

        ResetStatistics();

        MaybeAddChunk(chunk, parent);
        return true;
    }

    bool OnChunkView(TChunkView* /*chunkView*/) override
    {
        return false;
    }

    bool OnDynamicStore(
        TDynamicStore* /*dynamicStore*/,
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TReadLimit& /*startLimit*/,
        const NChunkClient::TReadLimit& /*endLimit*/) override
    {
        return false;
    }

    void OnFinish(const TError& error) override
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
        auto result = error.IsOK() ? EMergeSessionResult::OK : EMergeSessionResult::TransientFailure;
        chunkVisitorHost->OnTraversalFinished(nodeId, result);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->EphemeralUnrefObject(Account_);
        objectManager->EphemeralUnrefObject(Node_);
    }


    void ResetStatistics()
    {
        ChunkIds_.clear();
        CurrentRowCount_ = 0;
        CurrentDataWeight_ = 0;
        CurrentUncompressedDataSize_ = 0;
        CurrentJobMode_ = Mode_;
        ParentChunkListId_ = NullObjectId;
    }

    bool SatisfiesShallowMergeCriteria(TChunk* chunk)
    {
        if (ChunkIds_.empty()) {
            return true;
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* firstChunk = chunkManager->FindChunk(ChunkIds_.front());
        if (!IsObjectAlive(firstChunk)) {
            return false;
        }
        if (!ChunkMetaEqual(firstChunk, chunk)) {
            return false;
        }

        return true;
    }

    bool MaybeAddChunk(TChunk* chunk, TChunkList* parent)
    {
        const auto& config = GetDynamicConfig();

        if (parent->GetId() == LastChunkListId_ && JobsForLastChunkList_ >= config->MaxJobsPerChunkList) {
            return false;
        }

        if (CurrentJobMode_ == EChunkMergerMode::Shallow && !SatisfiesShallowMergeCriteria(chunk)) {
            return false;
        }

        if (CurrentJobMode_ == EChunkMergerMode::Auto && !SatisfiesShallowMergeCriteria(chunk)) {
            if (ssize(ChunkIds_) >= config->MinShallowMergeChunkCount) {
                return false;
            }
            CurrentJobMode_ = EChunkMergerMode::Deep;
        }

        if (CurrentRowCount_ + chunk->GetRowCount() < config->MaxRowCount &&
            CurrentDataWeight_ + chunk->GetDataWeight() < config->MaxDataWeight &&
            CurrentUncompressedDataSize_ + chunk->GetUncompressedDataSize() < config->MaxUncompressedDataSize &&
            std::ssize(ChunkIds_) < config->MaxChunkCount &&
            chunk->GetDataWeight() < config->MaxInputChunkDataWeight &&
            (ParentChunkListId_ == NullObjectId || ParentChunkListId_ == parent->GetId()))
        {
            CurrentRowCount_ += chunk->GetRowCount();
            CurrentDataWeight_ += chunk->GetDataWeight();
            CurrentUncompressedDataSize_ += chunk->GetUncompressedDataSize();
            ParentChunkListId_ = parent->GetId();
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

        if (LastChunkListId_ == ParentChunkListId_) {
            ++JobsForLastChunkList_;
        } else {
            LastChunkListId_ = ParentChunkListId_;
            JobsForLastChunkList_ = 1;
        }

        auto nodeId = Node_->GetId();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto jobId = chunkManager->GenerateJobId();
        chunkVisitorHost->RegisterJobAwaitingChunkCreation(
            jobId,
            CurrentJobMode_,
            JobIndex_++,
            nodeId,
            ParentChunkListId_,
            std::move(ChunkIds_));
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
    , ChunkReplacerCallbacks_(New<TChunkReplacerCallbacks>(Bootstrap_))
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

    const auto& config = GetDynamicConfig();
    if (!config->Enable) {
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

    RegisterSession(trunkNode);
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
                jobInfo.ParentChunkListId,
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

    for (auto nodeId : NodesBeingMerged_) {
        auto* node = FindChunkOwner(nodeId);
        if (!CanScheduleMerge(node)) {
            SessionsAwaitingFinalizaton_.push({
                .NodeId = nodeId,
                .Result = EMergeSessionResult::PermanentFailure
            });
            continue;
        }

        RegisterSessionTransient(node);
    }
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

    const auto& config = GetDynamicConfig();
    return
        config->Enable &&
        IsObjectAlive(chunkOwner) &&
        chunkOwner->GetChunkMergerMode() != EChunkMergerMode::None;
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

void TChunkMerger::RegisterSession(TChunkOwnerBase* chunkOwner)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    if (NodesBeingMerged_.contains(chunkOwner->GetId())) {
        chunkOwner->SetUpdatedSinceLastMerge(true);
        return;
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
        RegisterSessionTransient(chunkOwner);
    }
}

void TChunkMerger::RegisterSessionTransient(TChunkOwnerBase* chunkOwner)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(IsLeader());

    auto nodeId = chunkOwner->GetId();
    auto* account = chunkOwner->GetAccount();

    YT_LOG_DEBUG("Starting new merge job session (NodeId: %v, Account: %v)",
        nodeId,
        account->GetName());
    YT_VERIFY(RunningSessions_.emplace(nodeId, TChunkMergerSession()).second);

    const auto& objectManager = Bootstrap_->GetObjectManager();

    auto [it, inserted] = AccountToNodeQueue_.emplace(account, TNodeQueue());
    if (inserted) {
        objectManager->EphemeralRefObject(account);
    }

    auto& queue = it->second;
    queue.push(chunkOwner->GetId());
}

void TChunkMerger::FinalizeJob(
    TObjectId nodeId,
    TChunkListId parentChunkListId,
    TJobId jobId,
    EMergeSessionResult result)
{
    if (!IsLeader()) {
        return;
    }

    YT_LOG_DEBUG("Finalizing merge job (NodeId: %v, JobId: %v)",
        nodeId,
        jobId);

    auto& session = GetOrCrash(RunningSessions_, nodeId);
    session.Result = std::max(session.Result, result);

    auto& runningJobs = GetOrCrash(session.ChunkListIdToRunningJobs, parentChunkListId);
    EraseOrCrash(runningJobs, jobId);

    if (result == EMergeSessionResult::OK) {
        session.ChunkListIdToCompletedJobs[parentChunkListId].insert(jobId);
    }

    if (runningJobs.empty()) {
        EraseOrCrash(session.ChunkListIdToRunningJobs, parentChunkListId);
        auto completedJobsIt = session.ChunkListIdToCompletedJobs.find(parentChunkListId);
        if (completedJobsIt != session.ChunkListIdToCompletedJobs.end()) {
            ScheduleReplaceChunks(nodeId, parentChunkListId, completedJobsIt->second);
        }
    }

    if (session.ChunkListIdToRunningJobs.empty() && session.ChunkListIdToCompletedJobs.empty()) {
        ScheduleSessionFinalization(nodeId, EMergeSessionResult::None);
    }
}

void TChunkMerger::FinalizeReplacement(
    TObjectId nodeId,
    TChunkListId chunkListId,
    EMergeSessionResult result)
{
    if (!IsLeader()) {
        return;
    }

    YT_LOG_DEBUG("Finalizing chunk merge replacement (NodeId: %v, ChunkListId: %v, Result: %v)",
        nodeId,
        chunkListId,
        result);

    auto& session = GetOrCrash(RunningSessions_, nodeId);
    session.Result = std::max(session.Result, result);
    EraseOrCrash(session.ChunkListIdToCompletedJobs,chunkListId);

    if (session.ChunkListIdToRunningJobs.empty() && session.ChunkListIdToCompletedJobs.empty()) {
        ScheduleSessionFinalization(nodeId, EMergeSessionResult::None);
    }
}

void TChunkMerger::ScheduleReplaceChunks(
    TObjectId nodeId,
    TChunkListId parentChunkListId,
    const THashSet<TJobId>& jobIds)
{
    YT_LOG_DEBUG("Scheduling chunk replace after merge (NodeId: %v, ParentChunkListId: %v)",
        nodeId,
        parentChunkListId);

    TReqReplaceChunks request;
    std::vector<const TMergeJobInfo*> jobs;
    jobs.reserve(jobIds.size());
    for (auto jobId : jobIds) {
        const auto& jobInfo = GetOrCrash(RunningJobs_, jobId);
        jobs.push_back(&jobInfo);
    }

    std::sort(jobs.begin(), jobs.end(), [] (const TMergeJobInfo* lhs, const TMergeJobInfo* rhs) {
        return lhs->JobIndex < rhs->JobIndex;
    });

    for (const auto& job : jobs) {
        auto* replacement = request.add_replacements();
        ToProto(replacement->mutable_new_chunk_id(), job->OutputChunkId);
        ToProto(replacement->mutable_old_chunk_ids(), job->InputChunkIds);
    }

    ToProto(request.mutable_node_id(), nodeId);
    ToProto(request.mutable_chunk_list_id(), parentChunkListId);

    CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
        ->CommitAndLog(Logger);
}

void TChunkMerger::RegisterJobAwaitingChunkCreation(
    TJobId jobId,
    EChunkMergerMode mode,
    int jobIndex,
    TObjectId nodeId,
    TChunkListId parentChunkListId,
    std::vector<TChunkId> inputChunkIds)
{
    JobsAwaitingChunkCreation_.push({
        .JobId = jobId,
        .JobIndex = jobIndex,
        .NodeId = nodeId,
        .MergeMode = mode,
        .ParentChunkListId = parentChunkListId,
        .InputChunkIds = std::move(inputChunkIds),
    });

    YT_LOG_DEBUG("Planning merge job (JobId: %v, NodeId: %v, ParentChunkListId: %v)",
        jobId,
        nodeId,
        parentChunkListId);

    auto& session = GetOrCrash(RunningSessions_, nodeId);
    InsertOrCrash(session.ChunkListIdToRunningJobs[parentChunkListId], jobId);
}

void TChunkMerger::OnTraversalFinished(TObjectId nodeId, EMergeSessionResult result)
{
    auto& session = GetOrCrash(RunningSessions_, nodeId);
    if (session.ChunkListIdToRunningJobs.empty() && session.ChunkListIdToCompletedJobs.empty()) {
        ScheduleSessionFinalization(nodeId, result);
    }
}

void TChunkMerger::ScheduleSessionFinalization(TObjectId nodeId, EMergeSessionResult result)
{
    if (!IsLeader()) {
        return;
    }

    auto& session = GetOrCrash(RunningSessions_, nodeId);
    session.Result = std::max(session.Result, result);

    YT_VERIFY(session.ChunkListIdToRunningJobs.empty() && session.ChunkListIdToCompletedJobs.empty());
    YT_LOG_DEBUG_IF(
        IsMutationLoggingEnabled(),
        "Finalizing chunk merge session (NodeId: %v, Result: %v)",
        nodeId,
        session.Result);
    SessionsAwaitingFinalizaton_.push({
        .NodeId = nodeId,
        .Result = session.Result
    });
    EraseOrCrash(RunningSessions_, nodeId);
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
            FinalizeJob(
                jobInfo.NodeId,
                jobInfo.ParentChunkListId,
                jobInfo.JobId,
                EMergeSessionResult::PermanentFailure);
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

    TChunkMergerWriterOptions chunkMergerWriterOptions;
    if (chunkOwner->GetType() == EObjectType::Table) {
        const auto* table = chunkOwner->As<TTableNode>();
        ToProto(chunkMergerWriterOptions.mutable_schema(), *table->GetSchema()->AsTableSchema());
        chunkMergerWriterOptions.set_optimize_for(ToProto<int>(table->GetOptimizeFor()));
    }
    chunkMergerWriterOptions.set_compression_codec(ToProto<int>(chunkOwner->GetCompressionCodec()));
    chunkMergerWriterOptions.set_erasure_codec(ToProto<int>(chunkOwner->GetErasureCodec()));
    chunkMergerWriterOptions.set_enable_skynet_sharing(chunkOwner->GetEnableSkynetSharing());
    chunkMergerWriterOptions.set_merge_mode(ToProto<int>(jobInfo.MergeMode));
    chunkMergerWriterOptions.set_max_heavy_columns(Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->MaxHeavyColumns);
    chunkMergerWriterOptions.set_max_block_count(GetDynamicConfig()->MaxBlockCount);

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
    auto state = job->GetState();

    auto result = job->GetState() == EJobState::Completed
        ? EMergeSessionResult::OK
        : EMergeSessionResult::TransientFailure;

    if (state == EJobState::Failed && job->Error().FindMatching(NChunkClient::EErrorCode::IncompatibleChunkMetas)) {
        YT_LOG_DEBUG(
            job->Error(),
            "Chunks do not satisfy shallow merge criteria, will not try merging them again (JobId: %v)",
            jobId);
        result = EMergeSessionResult::OK;
    }

    FinalizeJob(
        jobInfo.NodeId,
        jobInfo.ParentChunkListId,
        jobInfo.JobId,
        result);

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

            FinalizeJob(
                it->second.NodeId,
                it->second.ParentChunkListId,
                jobId,
                EMergeSessionResult::TransientFailure);
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
    auto chunkListId = FromProto<TChunkListId>(request->chunk_list_id());

    auto replacementCount = request->replacements_size();
    auto* chunkOwner = FindChunkOwner(nodeId);
    if (!chunkOwner) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: no such chunk owner node (NodeId: %v)",
            nodeId);
        ChunkReplacementFailed_ += replacementCount;
        FinalizeReplacement(nodeId, chunkListId, EMergeSessionResult::PermanentFailure);
        return;
    }

    if (chunkOwner->GetType() == EObjectType::Table) {
        auto* table = chunkOwner->As<TTableNode>();
        if (table->IsDynamic()) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: table is dynamic (NodeId: %v)",
                chunkOwner->GetId());
            ChunkReplacementFailed_ += replacementCount;
            FinalizeReplacement(nodeId, chunkListId, EMergeSessionResult::PermanentFailure);
            return;
        }
    }

    auto* rootChunkList = chunkOwner->GetChunkList();
    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Replacing chunks after merge (NodeId: %v, ChunkListId: %v)",
        nodeId,
        chunkListId);

    TChunkReplacer chunkReplacer(ChunkReplacerCallbacks_, Logger);
    if (!chunkReplacer.FindChunkList(rootChunkList, chunkListId)) {
        YT_LOG_DEBUG_IF(
            IsMutationLoggingEnabled(),
            "Cannot replace chunks after merge: parent chunk list is no longer there (NodeId: %v, ParentChunkListId: %v)",
            nodeId,
            chunkListId);
        ChunkReplacementFailed_ += replacementCount;
        FinalizeReplacement(nodeId, chunkListId, EMergeSessionResult::OK);
        return;
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    auto chunkReplacementSucceded = 0;
    for (int index = 0; index < replacementCount; ++index) {
        const auto& replacement = request->replacements()[index];
        auto newChunkId = FromProto<TChunkId>(replacement.new_chunk_id());
        auto* newChunk = chunkManager->FindChunk(newChunkId);
        if (!IsObjectAlive(newChunk)) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: no such chunk (NodeId: %v, ChunkId: %v)",
                nodeId,
                newChunkId);
            ++ChunkReplacementFailed_;
            continue;
        }

        auto chunkIds = FromProto<std::vector<TChunkId>>(replacement.old_chunk_ids());
        if (chunkReplacer.ReplaceChunkSequence(newChunk, chunkIds)) {
            YT_LOG_DEBUG_IF(
                IsMutationLoggingEnabled(),
                "Replaced chunks after merge (NodeId: %v, InputChunkIds: %v, ChunkId: %v)",
                nodeId,
                chunkIds,
                newChunkId);
            ++ChunkReplacementSucceded_;
            ++chunkReplacementSucceded;
            ChunkCountSaving_ += std::ssize(chunkIds) - 1;
            chunkManager->ScheduleChunkRequisitionUpdate(newChunk);
        } else {
            YT_LOG_DEBUG_IF(
                IsMutationLoggingEnabled(),
                "Cannot replace chunks after merge: input chunk sequence is no longer there (NodeId: %v, InputChunkIds: %v, ChunkId: %v)",
                nodeId,
                chunkIds,
                newChunkId);
            ChunkReplacementFailed_ += replacementCount - index;
            break;
        }
    }

    auto result = chunkReplacementSucceded == replacementCount
        ? EMergeSessionResult::OK
        : EMergeSessionResult::TransientFailure;
    FinalizeReplacement(nodeId, chunkListId, result);

    if (chunkReplacementSucceded == 0) {
        return;
    }

    auto* newRootChunkList = chunkReplacer.Finish();
    YT_VERIFY(newRootChunkList);

    // Change chunk list.
    const auto& objectManager = Bootstrap_->GetObjectManager();

    newRootChunkList->AddOwningNode(chunkOwner);
    rootChunkList->RemoveOwningNode(chunkOwner);

    chunkOwner->SetChunkList(newRootChunkList);
    chunkOwner->SnapshotStatistics() = newRootChunkList->Statistics().ToDataStatistics();

    objectManager->RefObject(newRootChunkList);

    YT_LOG_DEBUG_IF(
        IsMutationLoggingEnabled(),
        "Chunk list replaced after merge (NodeId: %v, OldChunkListId: %v, NewChunkListId: %v)",
        nodeId,
        rootChunkList->GetId(),
        newRootChunkList->GetId());

    objectManager->UnrefObject(rootChunkList);

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

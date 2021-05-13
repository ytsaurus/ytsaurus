#include "chunk_merger.h"
#include "chunk_owner_base.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_tree_traverser.h"
#include "config.h"
#include "job_tracker.h"
#include "job.h"
#include "medium.h"

#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/server/master/table_server/table_node.h>
#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/ytlib/cypress_client/cypress_service_proxy.h>

#include <yt/yt/library/erasure/public.h>

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
using namespace NNodeTrackerClient::NProto;

using NYT::ToProto;

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

namespace {

void IncrementMergeJobCounter(TBootstrap* bootstrap, TChunkOwnerBase* node)
{
    if (!IsObjectAlive(node)) {
        return;
    }

    const auto& objectManager = bootstrap->GetObjectManager();
    auto epoch = objectManager->GetCurrentEpoch();
    node->IncrementMergeJobCounter(epoch, +1);
    YT_LOG_DEBUG("Incrementing merge job counter (NodeId: %v, MergeJobCounter: %v)",
        node->GetId(),
        node->GetMergeJobCounter(epoch));
}

void DecrementMergeJobCounter(TBootstrap* bootstrap, TChunkOwnerBase* node)
{
    if (!IsObjectAlive(node)) {
        return;
    }

    const auto& objectManager = bootstrap->GetObjectManager();
    auto epoch = objectManager->GetCurrentEpoch();
    node->IncrementMergeJobCounter(epoch, -1);
    YT_LOG_DEBUG("Decrementing merge job counter (NodeId: %v, MergeJobCounter: %v)",
        node->GetId(),
        node->GetMergeJobCounter(epoch));
}

void LockNode(TBootstrap* bootstrap, TChunkOwnerBase* node)
{
    const auto& objectManager = bootstrap->GetObjectManager();
    objectManager->EphemeralRefObject(node);
    IncrementMergeJobCounter(bootstrap, node);
}

void UnlockNode(TBootstrap* bootstrap, TChunkOwnerBase* node)
{
    const auto& objectManager = bootstrap->GetObjectManager();
    DecrementMergeJobCounter(bootstrap, node);
    objectManager->EphemeralUnrefObject(node);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TMergeChunkVisitor
    : public IChunkVisitor
{
public:
    TMergeChunkVisitor(
        TBootstrap* bootstrap,
        TJobTrackerPtr jobTracker,
        TChunkOwnerBase* node,
        std::queue<TMergeJobInfo>* jobsAwaitingChunkCreation)
        : Bootstrap_(bootstrap)
        , JobTracker_(std::move(jobTracker))
        , Node_(node)
        , RootChunkList_(Node_->GetChunkList())
        , Account_(Node_->GetAccount())
        , JobsAwaitingChunkCreation_(jobsAwaitingChunkCreation)
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
        objectManager->EphemeralRefObject(RootChunkList_);

        LockNode(Bootstrap_, Node_);

        TraverseChunkTree(std::move(callbacks), this, RootChunkList_);
    }

private:
    TBootstrap* const Bootstrap_;
    const TJobTrackerPtr JobTracker_;
    TChunkOwnerBase* const Node_;
    TChunkList* const RootChunkList_;
    TAccount* const Account_;
    std::queue<TMergeJobInfo>* const JobsAwaitingChunkCreation_;

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
        if (chunk->IsErasure()) {
            YT_LOG_DEBUG("Chunk merging is not supported for erasure chunks (NodeId: %v, ChunkId: %v)",
                Node_->GetId(),
                chunk->GetId());
            return false;
        }

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
        MaybePlanJob();
        if (IsObjectAlive(Account_)) {
            Account_->IncrementMergeJobRate(-1);
        }

        if (!error.IsOK() || Node_->GetChunkList() != RootChunkList_) {
            Bootstrap_->GetChunkManager()->ScheduleChunkMerge(Node_);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->EphemeralUnrefObject(Account_);
        objectManager->EphemeralUnrefObject(RootChunkList_);

        UnlockNode(Bootstrap_, Node_);
    }

    bool MaybeAddChunk(TChunk* chunk)
    {
        const auto& config = GetDynamicConfig();
        if (CurrentRowCount_ < config->MaxRowCount &&
            CurrentDataWeight_ < config->MaxDataWeight &&
            CurrentUncompressedDataSize_ < config->MaxUncompressedDataSize &&
            std::ssize(ChunkIds_) < config->MaxChunkCount &&
            (ChunkIds_.empty() || CurrentUncompressedDataSize_ / std::ssize(ChunkIds_) < config->MaxAverageChunkSize))
        {
            CurrentRowCount_ += chunk->MiscExt().row_count();
            CurrentDataWeight_ += chunk->MiscExt().data_weight();
            CurrentUncompressedDataSize_ += chunk->MiscExt().uncompressed_data_size();
            ChunkIds_.push_back(chunk->GetId());
            return true;
        }
        return false;
    };

    void MaybePlanJob()
    {
        const auto& config = GetDynamicConfig();
        if (std::ssize(ChunkIds_) < config->MinChunkCount) {
            return;
        }

        IncrementMergeJobCounter(Bootstrap_, Node_);

        auto jobId = JobTracker_->GenerateJobId();
        JobsAwaitingChunkCreation_->push({
            .JobId = jobId,
            .NodeId = Node_->GetId(),
            .RootChunkListId = RootChunkList_->GetId(),
            .InputChunkIds = std::move(ChunkIds_)
        });

        YT_LOG_DEBUG("Planning merge job (JobId: %v, NodeId: %v, RootChunkListId: %v)",
            jobId,
            Node_->GetId(),
            RootChunkList_->GetId());
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
}

void TChunkMerger::ScheduleMerge(NCypressServer::TNodeId nodeId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (auto* trunkNode = FindChunkOwner(nodeId)) {
        ScheduleMerge(trunkNode);
    }
}

void TChunkMerger::ScheduleMerge(TChunkOwnerBase* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(IsLeader());
    YT_VERIFY(trunkNode->IsTrunk());

    if (trunkNode->GetType() != EObjectType::Table) {
        YT_LOG_DEBUG("Chunk merging is supported only for table types (NodeId: %v)",
            trunkNode->GetId());
        return;
    }

    if (trunkNode->GetErasureCodec() != NErasure::ECodec::None) {
        YT_LOG_DEBUG("Chunk merging is not supported for erasure chunks (NodeId: %v)",
            trunkNode->GetId());
        return;
    }

    auto* table = trunkNode->As<TTableNode>();
    if (table->IsDynamic()) {
        YT_LOG_DEBUG("Chunk merging is not supported for dynamic tables (NodeId: %v)",
            trunkNode->GetId());
        return;
    }

    const auto& objectManager = Bootstrap_->GetObjectManager();
    auto epoch = objectManager->GetCurrentEpoch();
    if (trunkNode->GetMergeJobCounter(epoch) == 0) {
        DoScheduleMerge(trunkNode);
    }
}

void TChunkMerger::ScheduleJobs(
    TNode* node,
    NNodeTrackerClient::NProto::TNodeResources* resourceUsage,
    const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
    std::vector<TJobPtr>* jobsToStart)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto hasSpareMergeResources = [&] () {
        return resourceUsage->merge_slots() < resourceLimits.merge_slots();
    };

    while (!JobsAwaitingNodeHeartbeat_.empty() && hasSpareMergeResources()) {
        auto jobInfo = std::move(JobsAwaitingNodeHeartbeat_.front());
        JobsAwaitingNodeHeartbeat_.pop();

        TJobPtr job;
        if (!CreateMergeJob(node, jobInfo, &job)) {
            if (auto* trunkNode = FindChunkOwner(jobInfo.NodeId)) {
                DecrementMergeJobCounter(Bootstrap_, trunkNode);
            }
            ScheduleMerge(jobInfo.NodeId);
            continue;
        }

        JobTracker_->RegisterJob(std::move(job), jobsToStart, resourceUsage);

        YT_LOG_DEBUG("Merge job scheduled (JobId: %v, Address: %v, NodeId: %v, InputChunkIds: %v, OutputChunkId: %v)",
            job->GetJobId(),
            node->GetDefaultAddress(),
            jobInfo.NodeId,
            jobInfo.InputChunkIds,
            jobInfo.OutputChunkId);

        YT_VERIFY(RunningJobs_.emplace(job->GetJobId(), std::move(jobInfo)).second);
    }
}

void TChunkMerger::ProcessJobs(const std::vector<TJobPtr>& jobs)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    for (const auto& job : jobs) {
        auto jobType = job->GetType();
        if (jobType != EJobType::MergeChunks) {
            continue;
        }

        auto jobId = job->GetJobId();
        auto it = RunningJobs_.find(jobId);
        if (it == RunningJobs_.end()) {
            YT_LOG_DEBUG("Chunk merger skipped processing an unknown job (JobId: %v)",
                jobId);
            continue;
        }

        const auto& jobInfo = it->second;
        if (auto* trunkNode = FindChunkOwner(jobInfo.NodeId)) {
            DecrementMergeJobCounter(Bootstrap_, trunkNode);
        }

        switch (job->GetState()) {
            case EJobState::Completed:
                ScheduleReplaceChunks(jobInfo);
                break;

            case EJobState::Failed:
            case EJobState::Aborted:
                ScheduleMerge(jobInfo.NodeId);
                break;

            default:
                break;
        }

        RunningJobs_.erase(it);
    }
}

void TChunkMerger::SetJobTracker(TJobTrackerPtr jobTracker)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    JobTracker_ = std::move(jobTracker);
}

void TChunkMerger::OnProfiling(TSensorBuffer* buffer) const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    for (const auto& [account, queue] : AccountToNodeQueue_) {
        if (!IsObjectAlive(account)) {
            continue;
        }
        buffer->PushTag({"account", account->GetName()});
        buffer->AddGauge("/account_queue_size", queue.size());
        buffer->PopTag();
    }

    buffer->AddGauge("/merge_jobs_awaiting_chunk_creation", JobsAwaitingChunkCreation_.size());
    buffer->AddGauge("/merge_jobs_undergoing_chunk_creation", JobsUndergoingChunkCreation_.size());
    buffer->AddGauge("/merge_jobs_awaiting_node_heartbeat", JobsAwaitingNodeHeartbeat_.size());
    buffer->AddGauge("/running_merge_jobs", RunningJobs_.size());
}

void TChunkMerger::OnRecoveryComplete()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::OnRecoveryComplete();

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(
        BIND(&TChunkMerger::OnDynamicConfigChanged, MakeWeak(this)));
    OnDynamicConfigChanged();
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

    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    transactionManager->SubscribeTransactionAborted(TransactionAbortedCallback_);
}

void TChunkMerger::OnStopLeading()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::OnStopLeading();

    ResetTransientState();

    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    transactionManager->UnsubscribeTransactionAborted(TransactionAbortedCallback_);

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
}

void TChunkMerger::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::Clear();

    TransactionId_ = {};
    PreviousTransactionId_ = {};
}

void TChunkMerger::ResetTransientState()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    // TODO(aleksandra-zh): persist some information (probably a flag) about nodes we are currently merging,
    // so that we can initiate merge for them after recovery.
    AccountToNodeQueue_ = {};
    JobsAwaitingChunkCreation_ = {};
    JobsUndergoingChunkCreation_ = {};
    JobsAwaitingNodeHeartbeat_ = {};
    RunningJobs_ = {};
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

    return
        Enabled_ &&
        IsMergeTransactionAlive() &&
        IsObjectAlive(chunkOwner) &&
        IsObjectAlive(chunkOwner->GetChunkList());
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

void TChunkMerger::DoScheduleMerge(TChunkOwnerBase* chunkOwner)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(IsLeader());

    if (!Enabled_) {
        YT_LOG_DEBUG("Cannot schedule merge: chunk merger is disabled");
        return;
    }

    auto* account = chunkOwner->GetAccount();

    YT_LOG_DEBUG("Scheduling merge (NodeId: %v, ChunkListId: %v, Account: %v)",
        chunkOwner->GetId(),
        chunkOwner->GetChunkList()->GetId(),
        account->GetName());

    const auto& objectManager = Bootstrap_->GetObjectManager();

    auto [it, inserted] = AccountToNodeQueue_.emplace(account, TNodeQueue());
    if (inserted) {
        objectManager->EphemeralRefObject(account);
    }

    auto& queue = it->second;
    queue.push(chunkOwner);

    LockNode(Bootstrap_, chunkOwner);
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
            auto* node = queue.front();
            queue.pop();

            if (CanScheduleMerge(node)) {
                account->IncrementMergeJobRate(1);
                New<TMergeChunkVisitor>(
                    Bootstrap_,
                    JobTracker_,
                    node,
                    &JobsAwaitingChunkCreation_)
                    ->Run();
            }

            UnlockNode(Bootstrap_, node);
        }
    }

    for (auto* account : accountsToRemove) {
        auto it = AccountToNodeQueue_.find(account);
        YT_VERIFY(it != AccountToNodeQueue_.end());

        auto& queue = it->second;
        while (!queue.empty()) {
            auto* node = queue.front();
            UnlockNode(Bootstrap_, node);
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
            JobsAwaitingChunkCreation_.pop();
            continue;
        }

        YT_LOG_DEBUG("Creating chunks for merge (JobId: %v, NodeId: %v)",
            jobInfo.JobId,
            jobInfo.NodeId);

        auto* req = createChunksReq.add_subrequests();

        auto mediumIndex = node->GetPrimaryMediumIndex();
        req->set_medium_index(mediumIndex);

        const auto& policy = node->Replication().Get(mediumIndex);
        req->set_replication_factor(policy.GetReplicationFactor());

        auto erasureCodec = node->GetErasureCodec();
        req->set_erasure_codec(ToProto<int>(erasureCodec));
        req->set_account(node->GetAccount()->GetName());

        auto chunkType = erasureCodec == NErasure::ECodec::None
            ? EObjectType::Chunk
            : EObjectType::ErasureChunk;
        req->set_type(ToProto<int>(chunkType));

        req->set_vital(node->Replication().GetVital());
        ToProto(req->mutable_job_id(), jobInfo.JobId);

        YT_VERIFY(JobsUndergoingChunkCreation_.emplace(jobInfo.JobId, std::move(jobInfo)).second);
        JobsAwaitingChunkCreation_.pop();
    }

    CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), createChunksReq)
        ->CommitAndLog(Logger);
}

bool TChunkMerger::CreateMergeJob(TNode* node, const TMergeJobInfo& jobInfo, TJobPtr* job)
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
        const auto& schema = table->SharedTableSchema();
        if (schema) {
            ToProto(chunkMergerWriterOptions.mutable_schema(), schema->GetTableSchema());
        }
        chunkMergerWriterOptions.set_optimize_for(ToProto<int>(table->GetOptimizeFor()));
    }
    chunkMergerWriterOptions.set_compression_codec(ToProto<int>(chunkOwner->GetCompressionCodec()));
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

    *job = New<TMergeJob>(
        jobInfo.JobId,
        node,
        TChunkIdWithIndexes(jobInfo.OutputChunkId, GenericChunkReplicaIndex, chunkOwner->GetPrimaryMediumIndex()),
        std::move(inputChunks),
        std::move(chunkMergerWriterOptions));

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

    CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
        ->CommitAndLog(Logger);
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

TChunkOwnerBase* TChunkMerger::FindChunkOwner(NCypressServer::TNodeId nodeId)
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
            if (IsLeader()) {
                // NB: Job could be missing.
                JobsUndergoingChunkCreation_.erase(jobId);
            }
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
    auto* chunkOwner = FindChunkOwner(nodeId);
    if (!chunkOwner) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: no such chunk owner node (NodeId: %v)",
            nodeId);
        return;
    }

    auto* oldChunkList = chunkOwner->GetChunkList();

    auto newChunkId = FromProto<TChunkId>(request->new_chunk_id());

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto* newChunk = chunkManager->FindChunk(newChunkId);
    if (!IsObjectAlive(newChunk)) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: no such chunk (NodeId: %v, ChunkId: %v)",
            nodeId,
            newChunkId);
        if (IsLeader()) {
            ScheduleMerge(chunkOwner);
        }
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
        if (IsLeader()) {
            ScheduleMerge(chunkOwner);
        }
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

    objectManager->UnrefObject(oldChunkList);

    chunkManager->ScheduleChunkRequisitionUpdate(newChunk);

    if (IsLeader()) {
        ScheduleMerge(chunkOwner);
    }

    if (chunkOwner->IsForeign()) {
        const auto& tableManager = Bootstrap_->GetTableManager();
        tableManager->ScheduleStatisticsUpdate(
            chunkOwner,
            /*updateDataStatistics*/ true,
            /*updateTabletStatistics*/ false,
            /*useNativeContentRevisionCas*/ true);
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
}

void TChunkMerger::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, TransactionId_);
    Load(context, PreviousTransactionId_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

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

void LockNode(
    TBootstrap* bootstrap,
    TChunkOwnerBase* node,
    TChunkList* rootChunkList)
{
    const auto& objectManager = bootstrap->GetObjectManager();

    objectManager->EphemeralRefObject(node);
    if (rootChunkList) {
        objectManager->EphemeralRefObject(rootChunkList);
    }

    if (IsObjectAlive(node)) {
        auto epoch = objectManager->GetCurrentEpoch();
        node->IncrementMergeJobCounter(epoch, +1);
        YT_LOG_DEBUG("Incrementing merge job counter (NodeId: %v, MergeJobCounter: %v)",
            node->GetId(),
            node->GetMergeJobCounter(epoch));
    }
}

void UnlockNode(
    TBootstrap* bootstrap,
    TChunkOwnerBase* node,
    TChunkList* rootChunkList)
{
    const auto& objectManager = bootstrap->GetObjectManager();

    if (IsObjectAlive(node)) {
        auto epoch = objectManager->GetCurrentEpoch();
        node->IncrementMergeJobCounter(epoch, -1);
        YT_LOG_DEBUG("Decrementing merge job counter (NodeId: %v, MergeJobCounter: %v)",
            node->GetId(),
            node->GetMergeJobCounter(epoch));
    }

    objectManager->EphemeralUnrefObject(node);
    if (rootChunkList) {
        objectManager->EphemeralUnrefObject(rootChunkList);
    }
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
        std::queue<TMergeJobInfo>* jobsAwaitingChunkCreation,
        i64* createdChunkCounter)
        : Bootstrap_(bootstrap)
        , Node_(node)
        , RootChunkList_(Node_->GetChunkList())
        , Account_(Node_->GetAccount())
        , JobsAwaitingChunkCreation_(jobsAwaitingChunkCreation)
        , CreatedChunkCounter_(createdChunkCounter)
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

        LockNode(Bootstrap_, Node_, RootChunkList_);

        TraverseChunkTree(std::move(callbacks), this, RootChunkList_);
    }

private:
    TBootstrap* const Bootstrap_;
    TChunkOwnerBase* const Node_;
    TChunkList* const RootChunkList_;
    TAccount* const Account_;

    std::queue<TMergeJobInfo>* const JobsAwaitingChunkCreation_;
    i64* const CreatedChunkCounter_;

    std::vector<TChunk*> Chunks_;

    i64 CurrentRowCount_ = 0;
    i64 CurrentDataWeight_ = 0;
    i64 CurrentUncompressedDataSize_ = 0;


    virtual bool OnChunk(
        TChunk* chunk,
        std::optional<i64> rowIndex,
        std::optional<int> tabletIndex,
        const NChunkClient::TReadLimit& lowerLimit,
        const NChunkClient::TReadLimit& upperLimit,
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

        Chunks_.clear();
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

        UnlockNode(Bootstrap_, Node_, RootChunkList_);
    }

    bool MaybeAddChunk(TChunk* chunk)
    {
        const auto& config = GetDynamicConfig();
        if (CurrentRowCount_ < config->MaxRowCount &&
            CurrentDataWeight_ < config->MaxDataWeight &&
            CurrentUncompressedDataSize_ < config->MaxUncompressedDataSize &&
            Chunks_.size() < config->MaxChunkCount &&
            (Chunks_.empty() || CurrentUncompressedDataSize_ / Chunks_.size() < config->MaxAverageChunkSize))
        {
            CurrentRowCount_ += chunk->MiscExt().row_count();
            CurrentDataWeight_ += chunk->MiscExt().data_weight();
            CurrentUncompressedDataSize_ += chunk->MiscExt().uncompressed_data_size();
            Chunks_.push_back(chunk);
            return true;
        }
        return false;
    };

    void MaybePlanJob()
    {
        const auto& config = GetDynamicConfig();

        if (Chunks_.size() >= config->MinChunkCount) {
            LockNode(Bootstrap_, Node_, RootChunkList_);

            JobsAwaitingChunkCreation_->emplace(std::move(Chunks_), Node_, RootChunkList_, *CreatedChunkCounter_);
            ++(*CreatedChunkCounter_);

            YT_LOG_DEBUG("Planning merge job (NodeId: %v, RootChunkListId: %v)",
                Node_->GetId(),
                RootChunkList_->GetId());
        }
    }

    const TDynamicChunkMergerConfigPtr& GetDynamicConfig() const
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->ChunkManager->ChunkMerger;
    }
};

////////////////////////////////////////////////////////////////////////////////

TMergeJobInfo::TMergeJobInfo(
    std::vector<TChunk*> inputChunks,
    TChunkOwnerBase* node,
    TChunkList* rootChunkList,
    i64 outputChunkCounter)
    : InputChunks(std::move(inputChunks))
    , Node(node)
    , RootChunkList(rootChunkList)
    , OutputChunkCounter(outputChunkCounter)
{ }

////////////////////////////////////////////////////////////////////////////////

TChunkMerger::TChunkMerger(TBootstrap* bootstrap)
    : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::ChunkMerger)
    , Bootstrap_(bootstrap)
    , ChunkReplacer_(New<TChunkReplacerCallbacks>(Bootstrap_), Logger)
{
    YT_VERIFY(Bootstrap_);

    RegisterMethod(BIND(&TChunkMerger::HydraStartMergeTransaction, Unretained(this)));
    RegisterMethod(BIND(&TChunkMerger::HydraCreateChunks, Unretained(this)));
    RegisterMethod(BIND(&TChunkMerger::HydraReplaceChunks, Unretained(this)));
}

void TChunkMerger::ScheduleMerge(TChunkOwnerBase* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(IsLeader());
    YT_VERIFY(trunkNode->IsTrunk());

    if (!Enabled_) {
        YT_LOG_DEBUG("Cannot schedule merge: chunk merger is disabled");
        return;
    }

    if (trunkNode->GetType() != EObjectType::Table) {
        YT_LOG_DEBUG("Chunk merging is supported only for table types (NodeId: %v)", trunkNode->GetId());
        return;
    }

    if (trunkNode->GetErasureCodec() != NErasure::ECodec::None) {
        YT_LOG_DEBUG("Chunk merging is not supported for erasure chunks (NodeId: %v)", trunkNode->GetId());
        return;
    }

    auto* table = trunkNode->As<TTableNode>();
    if (table->IsDynamic()) {
        YT_LOG_DEBUG("Chunk merging is not supported for dynamic tables (NodeId: %v)", trunkNode->GetId());
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
            if (IsObjectAlive(jobInfo.Node)) {
                DoScheduleMerge(jobInfo.Node);
            }
            UnlockNode(Bootstrap_, jobInfo.Node, jobInfo.RootChunkList);
            continue;
        }

        JobTracker_->RegisterJob(std::move(job), jobsToStart, resourceUsage);

        YT_LOG_DEBUG("Merge job scheduled (JobId: %v, Address: %v, NodeId: %v, ChunkId: %v)",
            job->GetJobId(),
            node->GetDefaultAddress(),
            jobInfo.Node->GetId(),
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
            YT_LOG_DEBUG("Chunk merger skipped processing an unknown job (JobId: %v)", jobId);
            continue;
        }

        auto&& jobInfo = it->second;
        switch (job->GetState()) {
            case EJobState::Completed:
                ReplaceChunks(jobInfo);
                break;

            case EJobState::Failed:
            case EJobState::Aborted:
                ScheduleMerge(jobInfo.Node);
                break;

            default:
                break;
        }

        UnlockNode(Bootstrap_, jobInfo.Node, jobInfo.RootChunkList);
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

    for (const auto& [account, queue] : TouchedNodes_) {
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
    TMasterAutomatonPart::OnRecoveryComplete();

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(
        BIND(&TChunkMerger::OnDynamicConfigChanged, MakeWeak(this)));
    OnDynamicConfigChanged();
}

void TChunkMerger::OnLeaderActive()
{
    TMasterAutomatonPart::OnLeaderActive();

    // TODO(aleksandra-zh): persist some information (probably, a flag) about nodes we are currently merging,
    // so that we can initiate merge for them after recovery.
    TouchedNodes_ = {};
    JobsAwaitingChunkCreation_ = {};
    JobsUndergoingChunkCreation_ = {};
    JobsAwaitingNodeHeartbeat_ = {};
    RunningJobs_ = {};

    StartMergeTransaction();
    // We want to make sure both transactions from previous run are aborted
    // to avoid clash on CreatedChunkCounter_ in JobsUndergoingChunkCreation_.
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
    TMasterAutomatonPart::OnStopLeading();

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

bool TChunkMerger::CanScheduleMerge(TChunkOwnerBase* node, TChunkList* rootChunkList) const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!IsMergeTransactionAlive()) {
        return false;
    }

    return IsObjectAlive(node) && IsObjectAlive(rootChunkList);
}

void TChunkMerger::StartMergeTransaction()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

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

void TChunkMerger::DoScheduleMerge(TChunkOwnerBase* node)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(IsLeader());

    auto* account = node->GetAccount();

    YT_LOG_DEBUG("Scheduling merge (NodeId: %v, ChunkListId: %v, Account: %v)",
        node->GetId(),
        node->GetChunkList()->GetId(),
        account->GetName());

    const auto& objectManager = Bootstrap_->GetObjectManager();
    if (!TouchedNodes_.contains(account)) {
        objectManager->EphemeralRefObject(account);
    }

    TouchedNodes_[account].push(node);

    LockNode(Bootstrap_, node, nullptr);
}

void TChunkMerger::ProcessTouchedNodes()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    SmallVector<TAccount*, 8> accountsToRemove;
    const auto& objectManager = Bootstrap_->GetObjectManager();

    for (auto& [account, queue] : TouchedNodes_) {
        if (!IsObjectAlive(account)) {
            accountsToRemove.push_back(account);
            continue;
        }

        auto maxRate = account->GetMergeJobRateLimit();
        while (!queue.empty() && account->GetMergeJobRate() < maxRate) {
            auto* node = queue.front();
            queue.pop();

            if (CanScheduleMerge(node, node->GetChunkList())) {
                account->IncrementMergeJobRate(1);
                New<TMergeChunkVisitor>(Bootstrap_, node, &JobsAwaitingChunkCreation_, &CreatedChunkCounter_)->Run();
            }

            UnlockNode(Bootstrap_, node, nullptr);
        }
    }

    for (auto* account : accountsToRemove) {
        auto it = TouchedNodes_.find(account);
        YT_VERIFY(it != TouchedNodes_.end());

        auto& queue = it->second;
        while (!queue.empty()) {
            auto* node = queue.front();
            UnlockNode(Bootstrap_, node, nullptr);
            queue.pop();
        }

        objectManager->EphemeralUnrefObject(account);
        TouchedNodes_.erase(it);
    }
}

void TChunkMerger::CreateChunks()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (JobsAwaitingChunkCreation_.empty()) {
        return;
    }

    if (!IsMergeTransactionAlive()) {
        return;
    }

    const auto& config = GetDynamicConfig();

    TReqCreateChunks createChunksReq;
    ToProto(createChunksReq.mutable_transaction_id(), TransactionId_);

    for (auto i = 0; i < config->CreateChunksBatchSize && !JobsAwaitingChunkCreation_.empty(); ++i) {
        auto& entry = JobsAwaitingChunkCreation_.front();
        auto* node = entry.Node;
        if (!IsObjectAlive(node)) {
            UnlockNode(Bootstrap_, node, entry.RootChunkList);
            JobsAwaitingChunkCreation_.pop();
            continue;
        }

        YT_LOG_DEBUG("Creating chunks for merge (NodeId: %v)", node->GetId());

        auto* req = createChunksReq.add_subrequests();

        auto mediumIndex = node->GetPrimaryMediumIndex();
        req->set_medium_index(mediumIndex);

        const auto& policy = node->Replication().Get(mediumIndex);
        req->set_replication_factor(policy.GetReplicationFactor());

        auto erasureCodec = node->GetErasureCodec();
        req->set_erasure_codec(NYT::ToProto<int>(erasureCodec));
        req->set_account(node->GetAccount()->GetName());

        auto chunkType = erasureCodec == NErasure::ECodec::None
            ? EObjectType::Chunk
            : EObjectType::ErasureChunk;
        req->set_type(NYT::ToProto<int>(chunkType));

        req->set_vital(node->Replication().GetVital());
        req->set_chunk_counter(entry.OutputChunkCounter);

        JobsUndergoingChunkCreation_.emplace(entry.OutputChunkCounter, std::move(entry));
        JobsAwaitingChunkCreation_.pop();
    }

    CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), createChunksReq)
        ->CommitAndLog(Logger);
}

bool TChunkMerger::CreateMergeJob(TNode* node, const TMergeJobInfo& jobInfo, TJobPtr* job)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (!CanScheduleMerge(jobInfo.Node, jobInfo.RootChunkList)) {
        return false;
    }

    for (auto* chunk: jobInfo.InputChunks) {
        if (!IsObjectAlive(chunk)) {
            return false;
        }
    }

    TChunkMergerWriterOptions chunkMergerWriterOptions;
    if (jobInfo.Node->GetType() == EObjectType::Table) {
        const auto* table = jobInfo.Node->As<TTableNode>();
        const auto& schema = table->SharedTableSchema();
        if (schema) {
            ToProto(chunkMergerWriterOptions.mutable_schema(), schema->GetTableSchema());
        }
        chunkMergerWriterOptions.set_optimize_for(NYT::ToProto<int>(table->GetOptimizeFor()));
    }
    chunkMergerWriterOptions.set_compression_codec(NYT::ToProto<int>(jobInfo.Node->GetCompressionCodec()));

    // TODO(aleksandra-zh): make enable_skynet_sharing builtin to avoid this.
    try {
        auto enableSkynetSharing = jobInfo.Node->FindAttribute("enable_skynet_sharing");
        if (enableSkynetSharing) {
            chunkMergerWriterOptions.set_enable_skynet_sharing(ConvertTo<bool>(*enableSkynetSharing));
        }
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Attribute 'enable_skynet_sharing' contails invalid value (NodeId: %v)", jobInfo.Node->GetId());
    }

    *job = TJob::CreateMerge(
        JobTracker_->GenerateJobId(),
        jobInfo.OutputChunkId,
        jobInfo.Node->GetPrimaryMediumIndex(),
        jobInfo.InputChunks,
        node,
        std::move(chunkMergerWriterOptions));

    return true;
}

void TChunkMerger::ReplaceChunks(const TMergeJobInfo& jobInfo)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TReqReplaceChunks request;
    ToProto(request.mutable_new_chunk_id(), jobInfo.OutputChunkId);
    ToProto(request.mutable_node_id(), jobInfo.Node->GetId());
    for (auto* chunk : jobInfo.InputChunks) {
        ToProto(request.add_old_chunk_ids(), chunk->GetId());
    }

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

void TChunkMerger::HydraCreateChunks(NProto::TReqCreateChunks* request)
{
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& securityManager = Bootstrap_->GetSecurityManager();
    const auto& transactionManager = Bootstrap_->GetTransactionManager();

    auto transactionId = FromProto<TTransactionId>(request->transaction_id());
    auto* transaction = transactionManager->FindTransaction(transactionId);

    for (const auto& chunkInfo : request->subrequests()) {
        // JobsUndergoingChunkCreation_ is transient, do not make any decisions based on it!
        auto it = JobsUndergoingChunkCreation_.find(chunkInfo.chunk_counter());

        auto eraseFromQueue = [&] () {
            if (it != JobsUndergoingChunkCreation_.end()) {
                const auto& entry = it->second;
                UnlockNode(Bootstrap_, entry.Node, entry.RootChunkList);
                JobsUndergoingChunkCreation_.erase(it);
            }
        };

        if (!IsObjectAlive(transaction)) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk merger cannot create chunks: no such transaction (TransactionId: %v)",
                transactionId);
            eraseFromQueue();
            continue;
        }

        auto chunkType = CheckedEnumCast<EObjectType>(chunkInfo.type());

        const auto& mediumIndex = chunkInfo.medium_index();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!IsObjectAlive(medium)) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk merger cannot create chunks: no such medium (MediumIndex: %v)", mediumIndex);
            eraseFromQueue();
            continue;
        }

        auto* account = securityManager->FindAccountByName(chunkInfo.account(), true /*activeLifeStageOnly*/);
        if (!IsObjectAlive(account)) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk merger cannot create chunks: no such account (AccountName: %v)", chunkInfo.account());
            eraseFromQueue();
            continue;
        }

        auto* chunk = chunkManager->CreateChunk(
            transaction,
            nullptr,
            chunkType,
            account,
            chunkInfo.replication_factor(),
            CheckedEnumCast<NErasure::ECodec>(chunkInfo.erasure_codec()),
            medium,
            /*readQuorum*/ 0,
            /*writeQuorum*/ 0,
            /*movable*/ true,
            chunkInfo.vital());

        if (it != JobsUndergoingChunkCreation_.end()) {
            auto& entry = it->second;
            entry.OutputChunkId = chunk->GetId();
            JobsAwaitingNodeHeartbeat_.emplace(std::move(entry));
            JobsUndergoingChunkCreation_.erase(it);
        }
    }
}

void TChunkMerger::HydraReplaceChunks(NProto::TReqReplaceChunks* request)
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();

    auto nodeId = FromProto<TObjectId>(request->node_id());
    auto* node = cypressManager->FindNode(TVersionedNodeId(nodeId));
    if (!IsObjectAlive(node)) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: no such node (NodeId: %v)", nodeId);
        return;
    }

    if (!IsChunkOwnerType(node->GetType())) {
        YT_LOG_ALERT_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: unexpected node type (NodeId: %v, Type: %v)",
            nodeId,
            node->GetType());
        return;
    }

    auto* chunkOwner = node->As<TChunkOwnerBase>();
    auto* oldChunkList = chunkOwner->GetChunkList();

    const auto& chunkManager = Bootstrap_->GetChunkManager();

    auto newChunkId = FromProto<TChunkId>(request->new_chunk_id());
    auto* newChunk = chunkManager->FindChunk(newChunkId);
    if (!IsObjectAlive(newChunk)) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: no such chunk (ChunkId: %v)", newChunkId);
        if (IsLeader()) {
            DoScheduleMerge(chunkOwner);
        }
        return;
    }

    auto oldChunkIds = FromProto<std::vector<TChunkId>>(request->old_chunk_ids());

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Replacing chunks after merge (NewChunkId: %v, NodeId: %v)",
        newChunk->GetId(),
        chunkOwner->GetId());

    auto* newChunkList = chunkManager->CreateChunkList(EChunkListKind::Static);

    if (!ChunkReplacer_.Replace(oldChunkList, newChunkList, newChunk, oldChunkIds)) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: root has changed (NodeId: %v)",
            nodeId);
        if (IsLeader()) {
            DoScheduleMerge(chunkOwner);
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

    YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Chunk list replaced after merge (OldChunkListId: %v, NewChunkListId: %v, NodeId: %v)",
        oldChunkList->GetId(),
        newChunkList->GetId(),
        nodeId);

    objectManager->UnrefObject(oldChunkList);

    chunkManager->ScheduleChunkRequisitionUpdate(newChunk);

    if (IsLeader()) {
        ScheduleMerge(chunkOwner);
    }

    if (node->IsForeign()) {
        const auto& tableManager = Bootstrap_->GetTableManager();
        tableManager->ScheduleStatisticsUpdate(
            chunkOwner,
            /*updateDataStatistics*/ true,
            /*updateTabletStatistics*/ false,
            /*useNativeContentRevisionCas*/ true);
    }
}

void TChunkMerger::HydraStartMergeTransaction(NProto::TReqStartMergeTransaction* request)
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

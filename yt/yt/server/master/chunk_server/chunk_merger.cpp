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
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>

#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/server/master/table_server/table_node.h>
#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

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
using namespace NJobTrackerClient::NProto;
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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TTraversalInfo* protoTraversalInfo, const TChunkMergerTraversalInfo& traversalInfo)
{
    protoTraversalInfo->set_chunk_count(traversalInfo.ChunkCount);
    protoTraversalInfo->set_config_version(traversalInfo.ConfigVersion);
}

void FromProto(TChunkMergerTraversalInfo* traversalInfo, const NProto::TTraversalInfo& protoTraversalInfo)
{
    traversalInfo->ChunkCount = protoTraversalInfo.chunk_count();
    traversalInfo->ConfigVersion = protoTraversalInfo.config_version();
}

////////////////////////////////////////////////////////////////////////////////

bool TChunkMergerSession::IsReadyForFinalization() const
{
    return TraversalFinished && ChunkListIdToRunningJobs.empty() && ChunkListIdToCompletedJobs.empty();
}

////////////////////////////////////////////////////////////////////////////////

TMergeJob::TMergeJob(
    TJobId jobId,
    TJobEpoch jobEpoch,
    TMergeJobInfo jobInfo,
    NNodeTrackerServer::TNode* node,
    TChunkIdWithIndexes chunkIdWithIndexes,
    TChunkVector inputChunks,
    TChunkMergerWriterOptions chunkMergerWriterOptions,
    TNodePtrWithReplicaAndMediumIndexList targetReplicas,
    bool validateShallowMerge)
    : TJob(
        jobId,
        EJobType::MergeChunks,
        jobEpoch,
        node,
        TMergeJob::GetResourceUsage(inputChunks),
        chunkIdWithIndexes)
    , TargetReplicas_(targetReplicas)
    , JobInfo_(std::move(jobInfo))
    , InputChunks_(std::move(inputChunks))
    , ChunkMergerWriterOptions_(std::move(chunkMergerWriterOptions))
    , ValidateShallowMerge_(validateShallowMerge)
{ }

bool TMergeJob::FillJobSpec(TBootstrap* bootstrap, TJobSpec* jobSpec) const
{
    if (!AllOf(InputChunks_, [] (const TEphemeralObjectPtr<TChunk>& chunk) {
            return IsObjectAlive(chunk);
        }))
    {
        return false;
    }

    auto* jobSpecExt = jobSpec->MutableExtension(TMergeChunksJobSpecExt::merge_chunks_job_spec_ext);

    jobSpecExt->set_cell_tag(bootstrap->GetCellTag());

    ToProto(jobSpecExt->mutable_output_chunk_id(), ChunkIdWithIndexes_.Id);
    jobSpecExt->set_medium_index(ChunkIdWithIndexes_.MediumIndex);
    *jobSpecExt->mutable_chunk_merger_writer_options() = ChunkMergerWriterOptions_;

    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());

    for (const auto& chunk : InputChunks_) {
        auto* protoChunk = jobSpecExt->add_input_chunks();
        ToProto(protoChunk->mutable_id(), chunk->GetId());

        const auto& replicas = chunk->StoredReplicas();
        ToProto(protoChunk->mutable_source_replicas(), replicas);
        builder.Add(replicas);

        protoChunk->set_erasure_codec(ToProto<int>(chunk->GetErasureCodec()));
        protoChunk->set_row_count(chunk->GetRowCount());
    }

    builder.Add(TargetReplicas_);
    for (auto replica : TargetReplicas_) {
        jobSpecExt->add_target_replicas(ToProto<ui64>(replica));
    }

    jobSpecExt->set_validate_shallow_merge(ValidateShallowMerge_);

    return true;
}

TNodeResources TMergeJob::GetResourceUsage(const TChunkVector& inputChunks)
{
    i64 dataSize = 0;
    for (auto chunk : inputChunks) {
        dataSize += chunk->GetPartDiskSpace();
    }

    TNodeResources resourceUsage;
    resourceUsage.set_merge_slots(1);
    resourceUsage.set_merge_data_size(dataSize);

    return resourceUsage;
}

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

    void RefObject(TObject* object) override
    {
        Bootstrap_->GetObjectManager()->RefObject(object);
    }

    void UnrefObject(TObject* object) override
    {
        Bootstrap_->GetObjectManager()->UnrefObject(object);
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

    return lhs->GetCompressionCodec() == rhs->GetCompressionCodec();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TMergeChunkVisitor
    : public IChunkVisitor
{
public:
    TMergeChunkVisitor(
        TBootstrap* bootstrap,
        TEphemeralObjectPtr<TChunkOwnerBase> node,
        i64 configVersion,
        TWeakPtr<IMergeChunkVisitorHost> chunkVisitorHost)
        : Bootstrap_(bootstrap)
        , Node_(std::move(node))
        , NodeId_(Node_->GetId())
        , ConfigVersion_(configVersion)
        , Mode_(Node_->GetChunkMergerMode())
        , Account_(Node_->Account().Get())
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

        const auto& config = GetDynamicConfig();
        const auto& info = Node_->ChunkMergerTraversalInfo();
        TraversalInfo_.ConfigVersion = ConfigVersion_;
        TraversalInfo_.ChunkCount = TraversalInfo_.ConfigVersion == info.ConfigVersion ? info.ChunkCount : 0;
        TraversalInfo_.ChunkCount -= config->MaxChunkCount;
        TraversalInfo_.ChunkCount = std::max(TraversalInfo_.ChunkCount, 0);

        NChunkClient::TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(TraversalInfo_.ChunkCount);
        YT_LOG_DEBUG("Traversal started (NodeId: %v, RootChunkListId: %v, LowerLimit: %v)",
            NodeId_,
            Node_->GetChunkList()->GetId(),
            lowerLimit);

        if (!IsNodeMergeable()) {
            OnFinish(TError());
            return;
        }

        auto* table = Node_->As<TTableNode>();
        const auto& schema = table->GetSchema()->AsTableSchema();
        YT_VERIFY(schema);

        TTraverserTestingOptions testingOptions;
        if (config->MaxChunksPerIteration) {
            testingOptions.MaxChunksPerIteration = config->MaxChunksPerIteration;
        }
        if (config->DelayBetweenIterations) {
            testingOptions.DelayBetweenIterations = config->DelayBetweenIterations;
        }

        TraverseChunkTree(
            std::move(callbacks),
            this,
            Node_->GetChunkList(),
            lowerLimit,
            {},
            schema->ToComparator(),
            testingOptions);
    }

private:
    TBootstrap* const Bootstrap_;
    const TEphemeralObjectPtr<TChunkOwnerBase> Node_;
    const TObjectId NodeId_;
    const i64 ConfigVersion_;
    const EChunkMergerMode Mode_;
    const TAccountChunkMergerNodeTraversalsPtr<TAccount> Account_;
    const TWeakPtr<IMergeChunkVisitorHost> ChunkVisitorHost_;

    // Used to order jobs results correctly for chunk replace.
    int JobIndex_ = 0;

    std::vector<TChunkId> ChunkIds_;
    EChunkMergerMode CurrentJobMode_;

    TChunkMergerTraversalInfo TraversalInfo_;

    TChunkListId ParentChunkListId_;
    i64 CurrentRowCount_ = 0;
    i64 CurrentDataWeight_ = 0;
    i64 CurrentUncompressedDataSize_ = 0;
    i64 CurrentCompressedDataSize_ = 0;

    TChunkListId LastChunkListId_;
    int JobsForLastChunkList_ = 0;

    bool OnChunk(
        TChunk* chunk,
        TChunkList* parent,
        std::optional<i64> /*rowIndex*/,
        std::optional<int> /*tabletIndex*/,
        const NChunkClient::TReadLimit& /*lowerLimit*/,
        const NChunkClient::TReadLimit& /*upperLimit*/,
        const TChunkViewModifier* /*modifier*/) override
    {
        if (JobIndex_ == 0) {
            ++TraversalInfo_.ChunkCount;
        }

        if (!IsNodeMergeable()) {
            return false;
        }

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

        if (!IsNodeMergeable()) {
            chunkVisitorHost->OnTraversalFinished(
                NodeId_,
                EMergeSessionResult::PermanentFailure,
                TraversalInfo_);
            return;
        }

        MaybePlanJob();

        auto result = error.IsOK() ? EMergeSessionResult::OK : EMergeSessionResult::TransientFailure;
        chunkVisitorHost->OnTraversalFinished(
            NodeId_,
            result,
            TraversalInfo_);
    }


    void ResetStatistics()
    {
        ChunkIds_.clear();
        CurrentRowCount_ = 0;
        CurrentDataWeight_ = 0;
        CurrentUncompressedDataSize_ = 0;
        CurrentCompressedDataSize_ = 0;
        CurrentJobMode_ = Mode_;
        ParentChunkListId_ = NullObjectId;
    }

    bool SatisfiesShallowMergeCriteria(TChunk* chunk) const
    {
        if (ChunkIds_.empty()) {
            return true;
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* firstChunk = chunkManager->FindChunk(ChunkIds_.front());
        if (!IsObjectAlive(firstChunk)) {
            YT_LOG_DEBUG("Shallow merge criteria violated: chunk is dead (NodeId: %v, ChunkId: %v)",
                NodeId_,
                ChunkIds_.front());
            return false;
        }
        if (!ChunkMetaEqual(firstChunk, chunk)) {
            YT_LOG_DEBUG("Shallow merge criteria violated: chunk metas differ (NodeId: %v, ChunkId: %v, ChunkId: %v)",
                NodeId_,
                ChunkIds_.front(),
                chunk->GetId());
            return false;
        }

        return true;
    }

    bool IsNodeMergeable() const
    {
        if (!IsObjectAlive(Node_)) {
            return false;
        }

        if (Node_->GetType() != EObjectType::Table) {
            return false;
        }

        auto* table = Node_->As<TTableNode>();
        return !table->IsDynamic();
    }

    bool MaybeAddChunk(TChunk* chunk, TChunkList* parent)
    {
        const auto& config = GetDynamicConfig();

        if (parent->GetId() == LastChunkListId_ && JobsForLastChunkList_ >= config->MaxJobsPerChunkList) {
            YT_LOG_DEBUG("Cannot add chunk to merge job due to job limit (NodeId: %v, "
                "ChunkListId: %v, JobsForLastChunkList: %v, MaxJobsPerChunkList: %v)",
                NodeId_,
                LastChunkListId_,
                JobsForLastChunkList_,
                config->MaxJobsPerChunkList);
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

        if (chunk->GetSystemBlockCount() != 0) {
            YT_LOG_DEBUG("Cannot add chunk to merge job due to nonzero system block count (NodeId: %v, ChunkId: %v, SystemBlockCount: %v)",
                NodeId_,
                chunk->GetId(),
                chunk->GetSystemBlockCount());
            return false;
        }

        auto accountCriteria = Account_->ChunkMergerCriteria();
        auto mergerCriteria = TChunkMergerCriteria{
            config->MaxChunkCount,
            config->MaxRowCount,
            config->MaxDataWeight,
            config->MaxUncompressedDataSize,
            config->MaxCompressedDataSize,
            config->MaxInputChunkDataWeight
        };
        mergerCriteria.AssignNotNull(accountCriteria);

        if (CurrentRowCount_ + chunk->GetRowCount() < mergerCriteria.MaxRowCount &&
            CurrentDataWeight_ + chunk->GetDataWeight() < mergerCriteria.MaxDataWeight &&
            CurrentCompressedDataSize_ + chunk->GetCompressedDataSize() < mergerCriteria.MaxCompressedDataSize &&
            CurrentUncompressedDataSize_ + chunk->GetUncompressedDataSize() < mergerCriteria.MaxUncompressedDataSize &&
            std::ssize(ChunkIds_) < mergerCriteria.MaxChunkCount &&
            chunk->GetDataWeight() < mergerCriteria.MaxInputChunkDataWeight &&
            (ParentChunkListId_ == NullObjectId || ParentChunkListId_ == parent->GetId()))
        {
            CurrentRowCount_ += chunk->GetRowCount();
            CurrentDataWeight_ += chunk->GetDataWeight();
            CurrentCompressedDataSize_ += chunk->GetCompressedDataSize();
            CurrentUncompressedDataSize_ += chunk->GetUncompressedDataSize();
            ParentChunkListId_ = parent->GetId();
            ChunkIds_.push_back(chunk->GetId());
            return true;
        } else {
            YT_LOG_DEBUG("Cannot add chunk to merje job due to limits violation (NodeId: %v, "
                "CurrentRowCount: %v, ChunkRowCount: %v, MaxRowCount: %v, "
                "CurrentDataWeight: %v, ChunkDataWeight: %v, MaxDataWeight: %v, MaxInputChunkDataWeight: %v, "
                "CurrentCompressedDataSize: %v, ChunkCompressedDataSize: %v, MaxCompressedDataSize: %v, "
                "CurrentUncompressedDataSize: %v, ChunkUncompressedDataSize: %v, MaxUncompressedDataSize: %v, "
                "CurrentChunkCount: %v, MaxChunkCount: %v, "
                "DesiredParentChunkListId: %v, ParentChunkListId: %v)",
                NodeId_,
                CurrentRowCount_,
                chunk->GetRowCount(),
                *mergerCriteria.MaxRowCount,
                CurrentDataWeight_,
                chunk->GetDataWeight(),
                *mergerCriteria.MaxDataWeight,
                *mergerCriteria.MaxInputChunkDataWeight,
                CurrentCompressedDataSize_,
                chunk->GetCompressedDataSize(),
                *mergerCriteria.MaxCompressedDataSize,
                CurrentUncompressedDataSize_,
                chunk->GetUncompressedDataSize(),
                *mergerCriteria.MaxUncompressedDataSize,
                std::ssize(ChunkIds_),
                *mergerCriteria.MaxChunkCount,
                ParentChunkListId_,
                parent->GetId());
        }
        return false;
    }

    void MaybePlanJob()
    {
        if (!IsObjectAlive(Account_)) {
            return;
        }

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

        const auto& mergeJobThrottler = Account_->MergeJobThrottler();
        // May overdraft a lot.
        mergeJobThrottler->Acquire(1);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto jobId = chunkManager->GenerateJobId();

        if (JobIndex_ == 0) {
            YT_VERIFY(TraversalInfo_.ChunkCount > 0);
            --TraversalInfo_.ChunkCount;
        }

        chunkVisitorHost->RegisterJobAwaitingChunkCreation(
            jobId,
            CurrentJobMode_,
            JobIndex_++,
            NodeId_,
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
    , ChunkReplacerCallbacks_(New<TChunkReplacerCallbacks>(Bootstrap_))
    , TransactionRotator_(bootstrap, "Chunk merger transaction")
{
    YT_VERIFY(Bootstrap_);

    RegisterLoader(
        "ChunkMerger",
        BIND(&TChunkMerger::Load, Unretained(this)));
    RegisterSaver(
        ESyncSerializationPriority::Values,
        "ChunkMerger",
        BIND(&TChunkMerger::Save, Unretained(this)));

    RegisterMethod(BIND_NO_PROPAGATE(&TChunkMerger::HydraStartMergeTransaction, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TChunkMerger::HydraCreateChunks, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TChunkMerger::HydraReplaceChunks, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TChunkMerger::HydraFinalizeChunkMergeSessions, Unretained(this)));
}

void TChunkMerger::Initialize()
{
    const auto& transactionManager = Bootstrap_->GetTransactionManager();
    transactionManager->SubscribeTransactionAborted(BIND_NO_PROPAGATE(&TChunkMerger::OnTransactionFinished, MakeWeak(this)));
    transactionManager->SubscribeTransactionCommitted(BIND_NO_PROPAGATE(&TChunkMerger::OnTransactionFinished, MakeWeak(this)));

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TChunkMerger::OnDynamicConfigChanged, MakeWeak(this)));
}

void TChunkMerger::ScheduleMerge(TObjectId chunkOwnerId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    if (auto* trunkNode = FindChunkOwner(chunkOwnerId)) {
        ScheduleMerge(trunkNode);
    }
}

void TChunkMerger::ScheduleMerge(TChunkOwnerBase* trunkChunkOwner)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    YT_VERIFY(trunkChunkOwner->IsTrunk());

    const auto& config = GetDynamicConfig();
    if (!config->Enable && !config->EnableQueueSizeLimitChanges) {
        YT_LOG_DEBUG("Cannot schedule merge: chunk merger is disabled");
        return;
    }

    if (!IsObjectAlive(trunkChunkOwner)) {
        return;
    }

    if (trunkChunkOwner->GetType() != EObjectType::Table) {
        YT_LOG_DEBUG("Chunk merging is supported only for table types (ChunkOwnerId: %v)",
            trunkChunkOwner->GetId());
        return;
    }

    auto* table = trunkChunkOwner->As<TTableNode>();
    if (table->IsDynamic()) {
        YT_LOG_DEBUG("Chunk merging is not supported for dynamic tables (ChunkOwnerId: %v)",
            trunkChunkOwner->GetId());
        return;
    }

    auto* account = trunkChunkOwner->Account().Get();
    if (config->RespectAccountSpecificToggle && !account->GetAllowUsingChunkMerger()) {
        YT_LOG_DEBUG("Skipping node as its account is banned from using chunk merger (NodeId: %v, Account: %v)",
            trunkChunkOwner->GetId(),
            account->GetName());
        return;
    }

    RegisterSession(trunkChunkOwner);
}

bool TChunkMerger::IsNodeBeingMerged(TObjectId nodeId) const
{
    return NodesBeingMerged_.contains(nodeId);
}

void TChunkMerger::ScheduleJobs(EJobType jobType, IJobSchedulingContext* context)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(IsMasterJobType(jobType));

    if (!IsLeader()) {
        return;
    }

    if (jobType != EJobType::MergeChunks) {
        return;
    }

    const auto& resourceUsage = context->GetNodeResourceUsage();
    const auto& resourceLimits = context->GetNodeResourceLimits();

    const auto& config = GetDynamicConfig();
    if (!config->Enable) {
        return;
    }

    auto hasSpareMergeResources = [&] {
        return resourceUsage.merge_slots() < resourceLimits.merge_slots() &&
            (resourceUsage.merge_slots() == 0 || resourceUsage.merge_data_size() < resourceLimits.merge_data_size()) &&
            context->GetJobRegistry()->GetJobCount(EJobType::MergeChunks) < config->MaxRunningJobCount;
    };

    while (!JobsAwaitingNodeHeartbeat_.empty() && hasSpareMergeResources()) {
        auto jobInfo = std::move(JobsAwaitingNodeHeartbeat_.front());
        JobsAwaitingNodeHeartbeat_.pop();

        if (!TryScheduleMergeJob(context, jobInfo)) {
            auto* trunkNode = FindChunkOwner(jobInfo.NodeId);
            FinalizeJob(
                std::move(jobInfo),
                CanScheduleMerge(trunkNode) ? EMergeSessionResult::TransientFailure : EMergeSessionResult::PermanentFailure);
            continue;
        }
    }
}

void TChunkMerger::OnProfiling(TSensorBuffer* buffer)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    for (const auto& [account, queue] : AccountToNodeQueue_) {
        if (!IsObjectAlive(account)) {
            continue;
        }
        TWithTagGuard tagGuard(buffer, "account", account->GetName());
        buffer->AddGauge("/chunk_merger_account_queue_size", queue.size());
    }

    buffer->AddGauge("/chunk_merger_nodes_being_merged", NodesBeingMerged_.size());
    buffer->AddGauge("/chunk_merger_jobs_awaiting_chunk_creation", JobsAwaitingChunkCreation_.size());
    buffer->AddGauge("/chunk_merger_jobs_undergoing_chunk_creation", JobsUndergoingChunkCreation_.size());
    buffer->AddGauge("/chunk_merger_jobs_awaiting_node_heartbeat", JobsAwaitingNodeHeartbeat_.size());

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    for (const auto& [accountId, statistics] : AccountToChunkReplacementStatistics_) {
        auto* account = securityManager->FindAccount(accountId);
        if (!IsObjectAlive(account)) {
            continue;
        }
        TWithTagGuard tagGuard(buffer, "account", account->GetName());
        buffer->AddGauge("/chunk_merger_chunk_replacements_succeeded", statistics.ChunkReplacementsSucceeded);
        buffer->AddGauge("/chunk_merger_chunk_replacements_failed", statistics.ChunkReplacementsFailed);
        buffer->AddGauge("/chunk_merger_chunk_count_saving", statistics.ChunkCountSaving);
    }
    AccountToChunkReplacementStatistics_.clear();

    buffer->AddCounter("/chunk_merger_sessions_awaiting_finalization", SessionsAwaitingFinalization_.size());

    for (auto mergerMode : TEnumTraits<NChunkClient::EChunkMergerMode>::GetDomainValues()) {
        if (mergerMode == NChunkClient::EChunkMergerMode::None) {
            continue;
        }
        TWithTagGuard tagGuard(buffer, "merger_mode", FormatEnum(mergerMode));
        buffer->AddCounter("/chunk_merger_completed_job_count", CompletedJobCountPerMode_[mergerMode]);
    }
    buffer->AddCounter("/chunk_merger_auto_merge_fallback_count", AutoMergeFallbackJobCount_);
}

void TChunkMerger::OnJobWaiting(const TMergeJobPtr& job, IJobControllerCallbacks* callbacks)
{
    // In Chunk Merger we don't distinguish between waiting and running jobs.
    OnJobRunning(job, callbacks);
}

void TChunkMerger::OnJobRunning(const TMergeJobPtr& job, IJobControllerCallbacks* callbacks)
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    auto jobTimeout = configManager->GetConfig()->ChunkManager->JobTimeout;
    if (TInstant::Now() - job->GetStartTime() > jobTimeout) {
        YT_LOG_WARNING("Job timed out, aborting (JobId: %v, JobType: %v, Address: %v, Duration: %v, ChunkId: %v)",
            job->GetJobId(),
            job->GetType(),
            job->NodeAddress(),
            TInstant::Now() - job->GetStartTime(),
            job->GetChunkIdWithIndexes());

        callbacks->AbortJob(job);
    }
}

void TChunkMerger::OnJobCompleted(const TMergeJobPtr& job)
{
    const auto& jobResult = job->Result();
    const auto& jobResultExt = jobResult.GetExtension(NChunkClient::NProto::TMergeChunksJobResultExt::merge_chunks_job_result_ext);

    auto mergeMode = job->JobInfo().MergeMode;
    ++CompletedJobCountPerMode_[mergeMode];
    if (jobResultExt.deep_merge_fallback_occurred()) {
        ++AutoMergeFallbackJobCount_;
    }

    OnJobFinished(job);
}

void TChunkMerger::OnJobAborted(const TMergeJobPtr& job)
{
    OnJobFinished(job);
}

void TChunkMerger::OnJobFailed(const TMergeJobPtr& job)
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
            SessionsAwaitingFinalization_.push({
                .NodeId = nodeId,
                .Result = EMergeSessionResult::PermanentFailure
            });
            continue;
        }

        RegisterSessionTransient(node);
    }

    const auto& jobRegistry = Bootstrap_->GetChunkManager()->GetJobRegistry();
    YT_VERIFY(JobEpoch_ == InvalidJobEpoch);
    JobEpoch_ = jobRegistry->StartEpoch();
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

    if (JobEpoch_ != InvalidJobEpoch) {
        const auto& jobRegistry = Bootstrap_->GetChunkManager()->GetJobRegistry();
        jobRegistry->OnEpochFinished(JobEpoch_);
        JobEpoch_ = InvalidJobEpoch;
    }
}

void TChunkMerger::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    TMasterAutomatonPart::Clear();

    TransactionRotator_.Clear();
    NodesBeingMerged_.clear();
    ConfigVersion_ = 0;
}

void TChunkMerger::ResetTransientState()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    AccountToNodeQueue_ = {};
    JobsAwaitingChunkCreation_ = {};
    JobsUndergoingChunkCreation_ = {};
    JobsAwaitingNodeHeartbeat_ = {};
    RunningSessions_ = {};
    SessionsAwaitingFinalization_ = {};
}

bool TChunkMerger::IsMergeTransactionAlive() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return IsObjectAlive(TransactionRotator_.GetTransaction());
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

void TChunkMerger::OnTransactionFinished(TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    auto currentTransactionId = TransactionRotator_.GetTransactionId();
    if (!TransactionRotator_.OnTransactionFinished(transaction)) {
        return;
    }

    if (IsLeader() && transaction->GetId() == currentTransactionId) {
        StartMergeTransaction();
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
        YT_LOG_ALERT("Node is marked as updated, but has no running merge sessions (NodeId: %v)",
            chunkOwner->GetId());
        chunkOwner->SetUpdatedSinceLastMerge(false);
    }

    const auto& config = GetDynamicConfig();
    if (config->EnableQueueSizeLimitChanges && ssize(NodesBeingMerged_) >= config->MaxNodesBeingMerged) {
        YT_LOG_DEBUG("Skipping merge due to nodes being merged count exceeding the limit (NodeId: %v)",
            chunkOwner->GetId());
        return;
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
    auto* account = chunkOwner->Account().Get();

    YT_LOG_DEBUG("Starting new merge job session (NodeId: %v, Account: %v)",
        nodeId,
        account->GetName());
    YT_VERIFY(RunningSessions_.emplace(nodeId, TChunkMergerSession({.AccountId = account->GetId()})).second);

    auto [it, inserted] = AccountToNodeQueue_.emplace(account, TNodeQueue());

    auto& queue = it->second;
    queue.push(chunkOwner->GetId());
}

void TChunkMerger::FinalizeJob(
    TMergeJobInfo jobInfo,
    EMergeSessionResult result)
{
    if (!IsLeader()) {
        return;
    }

    auto jobId = jobInfo.JobId;
    auto nodeId = jobInfo.NodeId;
    auto parentChunkListId = jobInfo.ParentChunkListId;

    YT_LOG_DEBUG("Finalizing merge job (NodeId: %v, JobId: %v)",
        nodeId,
        jobId);

    auto& session = GetOrCrash(RunningSessions_, jobInfo.NodeId);
    session.Result = std::max(session.Result, result);

    auto& runningJobs = GetOrCrash(session.ChunkListIdToRunningJobs, parentChunkListId);
    EraseOrCrash(runningJobs, jobId);

    if (result == EMergeSessionResult::OK) {
        session.ChunkListIdToCompletedJobs[parentChunkListId].push_back(std::move(jobInfo));
    }

    if (runningJobs.empty()) {
        EraseOrCrash(session.ChunkListIdToRunningJobs, parentChunkListId);
        auto completedJobsIt = session.ChunkListIdToCompletedJobs.find(parentChunkListId);
        if (completedJobsIt != session.ChunkListIdToCompletedJobs.end()) {
            ScheduleReplaceChunks(
                nodeId,
                parentChunkListId,
                session.AccountId,
                &completedJobsIt->second);
        }
    }

    if (session.IsReadyForFinalization()) {
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
    EraseOrCrash(session.ChunkListIdToCompletedJobs, chunkListId);

    if (session.IsReadyForFinalization()) {
        ScheduleSessionFinalization(nodeId, EMergeSessionResult::None);
    }
}

void TChunkMerger::ScheduleReplaceChunks(
    TObjectId nodeId,
    TChunkListId parentChunkListId,
    TObjectId accountId,
    std::vector<TMergeJobInfo>* jobInfos)
{
    YT_LOG_DEBUG("Scheduling chunk replacement (NodeId: %v, ParentChunkListId: %v, AccountId: %v, JobIds: %v)",
        nodeId,
        parentChunkListId,
        accountId,
        MakeFormattableView(*jobInfos, [] (auto* builder, const auto& jobInfo) {
            builder->AppendFormat("%v", jobInfo.JobId);
        }));

    TReqReplaceChunks request;

    SortBy(jobInfos->begin(), jobInfos->end(), [] (const TMergeJobInfo& info) {
        return info.JobIndex;
    });

    for (const auto& job : *jobInfos) {
        auto* replacement = request.add_replacements();
        ToProto(replacement->mutable_new_chunk_id(), job.OutputChunkId);
        ToProto(replacement->mutable_old_chunk_ids(), job.InputChunkIds);
    }

    ToProto(request.mutable_node_id(), nodeId);
    ToProto(request.mutable_account_id(), accountId);
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
        .ParentChunkListId = parentChunkListId,
        .InputChunkIds = std::move(inputChunkIds),
        .MergeMode = mode,
    });

    YT_LOG_DEBUG("Planning merge job (JobId: %v, NodeId: %v, ParentChunkListId: %v)",
        jobId,
        nodeId,
        parentChunkListId);

    auto& session = GetOrCrash(RunningSessions_, nodeId);
    InsertOrCrash(session.ChunkListIdToRunningJobs[parentChunkListId], jobId);
    ++session.JobCount;
}

void TChunkMerger::OnTraversalFinished(TObjectId nodeId, EMergeSessionResult result, TChunkMergerTraversalInfo traversalInfo)
{
    auto& session = GetOrCrash(RunningSessions_, nodeId);
    session.TraversalInfo = traversalInfo;
    session.TraversalFinished = true;
    if (session.IsReadyForFinalization()) {
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
        "Finalizing chunk merge session (NodeId: %v, Result: %v, TraversalInfo: %v)",
        nodeId,
        session.Result,
        session.TraversalInfo);

    SessionsAwaitingFinalization_.push({
        .NodeId = nodeId,
        .Result = session.Result,
        .TraversalInfo = session.TraversalInfo,
        .JobCount = session.JobCount
    });
    EraseOrCrash(RunningSessions_, nodeId);
}

void TChunkMerger::FinalizeSessions()
{
    if (SessionsAwaitingFinalization_.empty()) {
        return;
    }

    const auto& config = GetDynamicConfig();
    TReqFinalizeChunkMergeSessions request;
    for (auto index = 0; index < config->SessionFinalizationBatchSize && !SessionsAwaitingFinalization_.empty(); ++index) {
        auto* req = request.add_subrequests();
        const auto& sessionResult = SessionsAwaitingFinalization_.front();
        ToProto(req->mutable_node_id(), sessionResult.NodeId);
        req->set_result(ToProto<int>(sessionResult.Result));

        if (sessionResult.Result != EMergeSessionResult::TransientFailure) {
            ToProto(req->mutable_traversal_info(), sessionResult.TraversalInfo);
        }
        req->set_job_count(sessionResult.JobCount);
        SessionsAwaitingFinalization_.pop();
    }

    CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
        ->CommitAndLog(Logger);
}

void TChunkMerger::ProcessTouchedNodes()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(IsLeader());

    const auto& config = GetDynamicConfig();
    if (!config->Enable) {
        return;
    }

    // Do not create new merge jobs if too many jobs are already running.
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& jobRegistry = chunkManager->GetJobRegistry();
    if (jobRegistry->GetJobCount(EJobType::MergeChunks) > config->MaxRunningJobCount) {
        return;
    }

    std::vector<TAccount*> accountsToRemove;
    for (auto& [account, queue] : AccountToNodeQueue_) {
        if (!IsObjectAlive(account)) {
            accountsToRemove.push_back(account.Get());
            continue;
        }

        auto maxNodeCount = account->GetChunkMergerNodeTraversalConcurrency();
        const auto& mergeJobThrottler = account->MergeJobThrottler();
        while (!queue.empty() &&
            !mergeJobThrottler->IsOverdraft() &&
            account->GetChunkMergerNodeTraversals() < maxNodeCount &&
            std::ssize(JobsAwaitingNodeHeartbeat_) < config->QueueSizeLimit)
        {
            auto nodeId = queue.front();
            queue.pop();

            YT_VERIFY(RunningSessions_.contains(nodeId));
            auto* node = FindChunkOwner(nodeId);

            if (CanScheduleMerge(node)) {
                New<TMergeChunkVisitor>(
                    Bootstrap_,
                    TEphemeralObjectPtr<TChunkOwnerBase>(node),
                    ConfigVersion_,
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
    ToProto(createChunksReq.mutable_transaction_id(), TransactionRotator_.GetTransactionId());

    const auto& config = GetDynamicConfig();
    for (auto index = 0; index < config->CreateChunksBatchSize && !JobsAwaitingChunkCreation_.empty(); ++index) {
        const auto& jobInfo = JobsAwaitingChunkCreation_.front();

        auto* chunkOwner = FindChunkOwner(jobInfo.NodeId);
        if (!chunkOwner) {
            FinalizeJob(
                std::move(jobInfo),
                EMergeSessionResult::PermanentFailure);
            JobsAwaitingChunkCreation_.pop();
            continue;
        }

        YT_LOG_DEBUG("Creating chunks for merge (JobId: %v, ChunkOwnerId: %v)",
            jobInfo.JobId,
            jobInfo.NodeId);

        auto* req = createChunksReq.add_subrequests();

        auto mediumIndex = chunkOwner->GetPrimaryMediumIndex();
        req->set_medium_index(mediumIndex);

        auto erasureCodec = chunkOwner->GetErasureCodec();
        req->set_erasure_codec(ToProto<int>(erasureCodec));

        if (erasureCodec == NErasure::ECodec::None) {
            req->set_type(ToProto<int>(EObjectType::Chunk));
            const auto& policy = chunkOwner->Replication().Get(mediumIndex);
            req->set_replication_factor(policy.GetReplicationFactor());
        } else {
            req->set_type(ToProto<int>(EObjectType::ErasureChunk));
            req->set_replication_factor(1);
        }

        req->set_account(chunkOwner->Account()->GetName());

        req->set_vital(chunkOwner->Replication().GetVital());
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
        inputChunks.emplace_back(chunk);
    }

    auto* chunkRequisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
    auto* outputChunk = chunkManager->FindChunk(jobInfo.OutputChunkId);
    if (!IsObjectAlive(outputChunk)) {
        return false;
    }

    const auto& requisition = outputChunk->GetAggregatedRequisition(chunkRequisitionRegistry);
    TChunkIdWithIndexes chunkIdWithIndexes(
        jobInfo.OutputChunkId,
        GenericChunkReplicaIndex,
        requisition.begin()->MediumIndex);
    auto erasureCodec = outputChunk->GetErasureCodec();
    int targetCount = erasureCodec == NErasure::ECodec::None
        ? outputChunk->GetAggregatedReplicationFactor(
            chunkIdWithIndexes.MediumIndex,
            chunkRequisitionRegistry)
        : NErasure::GetCodec(erasureCodec)->GetTotalPartCount();

    auto targetNodes = chunkManager->AllocateWriteTargets(
        chunkManager->GetMediumByIndexOrThrow(chunkIdWithIndexes.MediumIndex),
        outputChunk,
        GenericChunkReplicaIndex,
        targetCount,
        targetCount,
        /*replicationFactorOverride*/ std::nullopt);
    if (targetNodes.empty()) {
        return false;
    }

    TNodePtrWithReplicaAndMediumIndexList targetReplicas;
    int targetIndex = 0;
    for (auto* node : targetNodes) {
        targetReplicas.emplace_back(
            node,
            erasureCodec == NErasure::ECodec::None ? GenericChunkReplicaIndex : targetIndex++,
            chunkIdWithIndexes.MediumIndex);
    }

    const auto& config = GetDynamicConfig();
    auto validateShallowMerge = static_cast<int>(RandomNumber<ui32>() % 100) < config->ShallowMergeValidationProbability;
    auto job = New<TMergeJob>(
        jobInfo.JobId,
        JobEpoch_,
        jobInfo,
        context->GetNode(),
        chunkIdWithIndexes,
        std::move(inputChunks),
        std::move(chunkMergerWriterOptions),
        std::move(targetReplicas),
        validateShallowMerge);
    context->ScheduleJob(job);

    YT_LOG_DEBUG("Merge job scheduled "
        "(JobId: %v, JobEpoch: %v, Address: %v, NodeId: %v, InputChunkIds: %v, OutputChunkId: %v, ValidateShallowMerge: %v)",
        job->GetJobId(),
        job->GetJobEpoch(),
        context->GetNode()->GetDefaultAddress(),
        jobInfo.NodeId,
        jobInfo.InputChunkIds,
        jobInfo.OutputChunkId,
        validateShallowMerge);

    return true;
}

void TChunkMerger::OnJobFinished(const TMergeJobPtr& job)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(job->GetType() == EJobType::MergeChunks);

    auto state = job->GetState();

    auto result = state == EJobState::Completed
        ? EMergeSessionResult::OK
        : EMergeSessionResult::TransientFailure;

    if (state == EJobState::Failed && job->Error().FindMatching(NChunkClient::EErrorCode::IncompatibleChunkMetas)) {
        YT_LOG_DEBUG(
            job->Error(),
            "Chunks do not satisfy shallow merge criteria, will not try merging them again (JobId: %v)",
            job->GetJobId());
        result = EMergeSessionResult::PermanentFailure;
    }

    {
        const auto& jobResult = job->Result();
        const auto& jobResultExt = jobResult.GetExtension(NChunkClient::NProto::TMergeChunksJobResultExt::merge_chunks_job_result_ext);

        TError error;
        FromProto(&error, jobResultExt.shallow_merge_validation_error());
        if (!error.IsOK()) {
            YT_VERIFY(job->GetState() == EJobState::Failed);
            YT_LOG_ALERT(error, "Shallow merge validation failed; disabling chunk merger (JobId: %v)",
                job->GetJobId());
            NRpc::TDispatcher::Get()->GetHeavyInvoker()->Invoke(
                BIND(&TChunkMerger::DisableChunkMerger, MakeStrong(this)));
        }
    }

    FinalizeJob(
        job->JobInfo(),
        result);
}

const TDynamicChunkMergerConfigPtr& TChunkMerger::GetDynamicConfig() const
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->ChunkManager->ChunkMerger;
}

void TChunkMerger::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& config = GetDynamicConfig();

    // Ignore leader switches for config versioning to uphold determinism.
    if (HasMutationContext()) {
        ++ConfigVersion_;
    }

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
        YT_LOG_INFO("Chunk merger is disabled");
        Enabled_ = false;
    }

    if (!Enabled_ && enable) {
        YT_LOG_INFO("Chunk merger is enabled");
        Enabled_ = true;
    }
}

TChunkOwnerBase* TChunkMerger::FindChunkOwner(NCypressServer::TNodeId chunkOwnerId)
{
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto* node = cypressManager->FindNode(TVersionedNodeId(chunkOwnerId));
    if (!IsObjectAlive(node)) {
        return nullptr;
    }
    if (!IsChunkOwnerType(node->GetType())) {
        return nullptr;
    }
    return node->As<TChunkOwnerBase>();
}

void TChunkMerger::DisableChunkMerger()
{
    VERIFY_THREAD_AFFINITY_ANY();

    try {
        GuardedDisableChunkMerger();
    } catch (const std::exception& ex) {
        YT_LOG_ALERT(ex, "Failed to disable chunk merger");
    }
}

void TChunkMerger::GuardedDisableChunkMerger()
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetRootClient());
    auto batchReq = proxy.ExecuteBatch();
    auto req = TYPathProxy::Set("//sys/@config/chunk_manager/chunk_merger/enable");
    req->set_value("\%false");
    batchReq->AddRequest(req);
    WaitFor(batchReq->Invoke())
        .ThrowOnError();
}

void TChunkMerger::ValidateStatistics(
    TObjectId nodeId,
    const NChunkClient::NProto::TDataStatistics& oldStatistics,
    const NChunkClient::NProto::TDataStatistics& newStatistics)
{
    YT_LOG_ALERT_IF(oldStatistics.row_count() != newStatistics.row_count(),
        "Row count in new statistics is different (NodeId: %v, OldRowCount: %v, NewRowCount: %v)",
        nodeId,
        oldStatistics.row_count(),
        newStatistics.row_count());

    YT_LOG_ALERT_IF(oldStatistics.data_weight() != newStatistics.data_weight(),
        "Data weight in new statistics is different (NodeId: %v, OldDataWeight: %v, NewDataWeight: %v)",
        nodeId,
        oldStatistics.data_weight(),
        newStatistics.data_weight());
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
                std::move(it->second),
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
    auto accountId = FromProto<TObjectId>(request->account_id());

    auto& statistics = AccountToChunkReplacementStatistics_[accountId];
    auto updateFailedReplacements = [&] (int replacementCount) {
        if (!IsLeader()) {
            return;
        }
        statistics.ChunkReplacementsFailed += replacementCount;
    };

    auto replacementCount = request->replacements_size();
    auto* chunkOwner = FindChunkOwner(nodeId);
    if (!chunkOwner) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: no such chunk owner node (NodeId: %v)",
            nodeId);
        updateFailedReplacements(replacementCount);
        FinalizeReplacement(nodeId, chunkListId, EMergeSessionResult::PermanentFailure);
        return;
    }

    if (chunkOwner->GetType() == EObjectType::Table) {
        auto* table = chunkOwner->As<TTableNode>();
        if (table->IsDynamic()) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: table is dynamic (NodeId: %v)",
                chunkOwner->GetId());
            updateFailedReplacements(replacementCount);
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
        updateFailedReplacements(replacementCount);
        FinalizeReplacement(nodeId, chunkListId, EMergeSessionResult::TransientFailure);
        return;
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto chunkReplacementsSucceeded = 0;
    for (int index = 0; index < replacementCount; ++index) {
        const auto& replacement = request->replacements()[index];
        auto newChunkId = FromProto<TChunkId>(replacement.new_chunk_id());
        auto* newChunk = chunkManager->FindChunk(newChunkId);
        if (!IsObjectAlive(newChunk)) {
            YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Cannot replace chunks after merge: no such chunk (NodeId: %v, ChunkId: %v)",
                nodeId,
                newChunkId);
            updateFailedReplacements(1);
            continue;
        }

        if (!newChunk->IsConfirmed()) {
            YT_LOG_ALERT("Cannot replace chunks after merge: new chunk is not confirmed (NodeId: %v, ChunkId: %v)",
                nodeId,
                newChunkId);
            updateFailedReplacements(1);
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

            ++chunkReplacementsSucceeded;
            if (IsLeader()) {
                auto chunkCountDiff = std::ssize(chunkIds) - 1;

                ++statistics.ChunkReplacementsSucceeded;
                statistics.ChunkCountSaving += chunkCountDiff;

                auto& session = GetOrCrash(RunningSessions_, nodeId);
                if (session.TraversalInfo.ChunkCount < 0) {
                    YT_LOG_ALERT("Node traversed chunk count is negative (NodeId: %v, TraversalInfo: %v)",
                        chunkOwner->GetId(),
                        session.TraversalInfo);
                    session.TraversalInfo.ChunkCount = 0;
                }
            }
        } else {
            YT_LOG_DEBUG_IF(
                IsMutationLoggingEnabled(),
                "Cannot replace chunks after merge: input chunk sequence is no longer there (NodeId: %v, InputChunkIds: %v, ChunkId: %v)",
                nodeId,
                chunkIds,
                newChunkId);
            updateFailedReplacements(1);
            break;
        }
    }

    auto result = chunkReplacementsSucceeded == replacementCount
        ? EMergeSessionResult::OK
        : EMergeSessionResult::TransientFailure;
    FinalizeReplacement(nodeId, chunkListId, result);

    if (chunkReplacementsSucceeded == 0) {
        return;
    }

    auto* newRootChunkList = chunkReplacer.Finish();
    YT_VERIFY(newRootChunkList->GetObjectRefCounter(/*flushUnrefs*/ true) == 1);

    // Change chunk list.
    newRootChunkList->AddOwningNode(chunkOwner);
    rootChunkList->RemoveOwningNode(chunkOwner);

    chunkManager->ScheduleChunkRequisitionUpdate(rootChunkList);
    chunkManager->ScheduleChunkRequisitionUpdate(newRootChunkList);

    YT_LOG_DEBUG_IF(
        IsMutationLoggingEnabled(),
        "Chunk list replaced after merge (NodeId: %v, OldChunkListId: %v, NewChunkListId: %v)",
        nodeId,
        rootChunkList->GetId(),
        newRootChunkList->GetId());

    // NB: this may destroy old chunk list, so be sure to schedule requisition
    // update beforehand.
    chunkOwner->SetChunkList(newRootChunkList);

    if (GetDynamicConfig()->EnableNodeStatisticsFix) {
        chunkOwner->FixStatisticsAndAlert();
    }

    auto newStatistics = newRootChunkList->Statistics().ToDataStatistics();
    ValidateStatistics(nodeId, chunkOwner->SnapshotStatistics(), newStatistics);
    chunkOwner->SnapshotStatistics() = std::move(newStatistics);

    // TODO(aleksandra-zh): Move to HydraFinalizeChunkMergeSessions?
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

        YT_VERIFY(chunkOwner->GetType() == EObjectType::Table);
        auto* table = chunkOwner->As<TTableNode>();
        if (table->IsDynamic()) {
            YT_LOG_DEBUG_IF(
                IsMutationLoggingEnabled(),
                "Table became dynamic between chunk replacement and "
                "merge session finalization, ignored (TableId: %v)",
                table->GetId());
            continue;
        }

        if (subrequest.has_traversal_info()) {
            FromProto(&chunkOwner->ChunkMergerTraversalInfo(), subrequest.traversal_info());
        }
        auto jobCount = subrequest.job_count();

        YT_LOG_DEBUG_IF(
            IsMutationLoggingEnabled(),
            "Finalizing merge session (NodeId: %v, NodeTouched: %v, Result: %v, JobCount: %v)",
            nodeId,
            nodeTouched,
            result,
            jobCount);

        const auto& config = GetDynamicConfig();
        if (config->RescheduleMergeOnSuccess && result == EMergeSessionResult::OK && jobCount > 0) {
            ScheduleMerge(nodeId);
            continue;
        }

        if (result == EMergeSessionResult::TransientFailure || (result == EMergeSessionResult::OK && nodeTouched)) {
            // TODO(shakurov): "RescheduleMerge"?
            ScheduleMerge(nodeId);
            continue;
        } else if (result == EMergeSessionResult::PermanentFailure) {
            continue;
        }

        YT_VERIFY(result == EMergeSessionResult::OK);

        auto* oldRootChunkList = chunkOwner->GetChunkList();
        if (oldRootChunkList->GetKind() != EChunkListKind::Static) {
            YT_LOG_ALERT("Merge session finalized with chunk list of unexpected kind, ignored "
                "(NodeId: %v, ChunkListId: %v, ChunkListKind: %v)",
                nodeId,
                oldRootChunkList->GetId(),
                oldRootChunkList->GetKind());
            continue;
        }

        const auto& children = oldRootChunkList->Children();
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* newRootChunkList = chunkManager->CreateChunkList(oldRootChunkList->GetKind());
        chunkManager->AttachToChunkList(
            newRootChunkList,
            children);

        chunkManager->RebalanceChunkTree(newRootChunkList, EChunkTreeBalancerMode::Strict);

        chunkOwner->SetChunkList(newRootChunkList);
        newRootChunkList->AddOwningNode(chunkOwner);
        oldRootChunkList->RemoveOwningNode(chunkOwner);

        if (GetDynamicConfig()->EnableNodeStatisticsFix) {
            chunkOwner->FixStatisticsAndAlert();
        }

        auto newStatistics = newRootChunkList->Statistics().ToDataStatistics();
        ValidateStatistics(nodeId, chunkOwner->SnapshotStatistics(), newStatistics);
        chunkOwner->SnapshotStatistics() = std::move(newStatistics);

        if (chunkOwner->IsForeign()) {
            const auto& tableManager = Bootstrap_->GetTableManager();
            tableManager->ScheduleStatisticsUpdate(
                chunkOwner,
                /*updateDataStatistics*/ true,
                /*updateTabletStatistics*/ false,
                /*useNativeContentRevisionCas*/ true);
        }
    }
}

void TChunkMerger::HydraStartMergeTransaction(NProto::TReqStartMergeTransaction* /*request*/)
{
    TransactionRotator_.Rotate();

    YT_LOG_INFO_IF(IsMutationLoggingEnabled(), "Merge transaction updated (NewTransactionId: %v, PreviousTransactionId: %v)",
        TransactionRotator_.GetTransactionId(),
        TransactionRotator_.GetPreviousTransactionId());
}

void TChunkMerger::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    // Persist transactions so that we can commit them.
    Save(context, TransactionRotator_);
    Save(context, NodesBeingMerged_);
    Save(context, ConfigVersion_);
}

void TChunkMerger::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, TransactionRotator_);
    Load(context, NodesBeingMerged_);
    Load(context, ConfigVersion_);
}

void TChunkMerger::OnAfterSnapshotLoaded()
{
    TransactionRotator_.OnAfterSnapshotLoaded();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

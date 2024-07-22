#include "chunk_merger.h"

#include "chunk_owner_base.h"
#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_replica_fetcher.h"
#include "chunk_tree_traverser.h"
#include "config.h"
#include "job_registry.h"
#include "job.h"

#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>

#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/server/master/table_server/table_node.h>
#include <yt/yt/server/master/table_server/table_manager.h>

#include <yt/yt/server/master/cypress_server/helpers.h>

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

namespace NYT::NChunkServer {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCellMaster;
using namespace NChunkClient::NProto;
using namespace NYTree;
using namespace NTransactionServer;
using namespace NChunkServer::NProto;
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
using namespace NLogging;
using namespace NNodeTrackerClient::NProto;
using namespace NTableClient::NProto;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ChunkServerLogger;

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

    jobSpecExt->set_cell_tag(ToProto<int>(bootstrap->GetCellTag()));

    ToProto(jobSpecExt->mutable_output_chunk_id(), ChunkIdWithIndexes_.Id);
    jobSpecExt->set_medium_index(ChunkIdWithIndexes_.MediumIndex);
    *jobSpecExt->mutable_chunk_merger_writer_options() = ChunkMergerWriterOptions_;

    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());

    const auto& chunkManager = bootstrap->GetChunkManager();
    const auto& chunkReplicaFetcher = chunkManager->GetChunkReplicaFetcher();
    for (const auto& chunk : InputChunks_) {
        auto* protoChunk = jobSpecExt->add_input_chunks();
        ToProto(protoChunk->mutable_id(), chunk->GetId());

        // This is context switch, but chunk is ephemeral pointer.
        auto replicasOrError = chunkReplicaFetcher->GetChunkReplicas(chunk);
        if (!replicasOrError.IsOK()) {
            return false;
        }

        const auto& replicas = replicasOrError.Value();
        ToProto(protoChunk->mutable_legacy_source_replicas(), replicas);
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
    for (const auto& chunk : inputChunks) {
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
        TRange<TChunkTree*> children) override
    {
        Bootstrap_->GetChunkManager()->AttachToChunkList(chunkList, children);
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
        : Logger(ChunkServerLogger().WithTag("NodeId: %v, AccountId: %v", node->GetId(), node->Account()->GetId()))
        , Bootstrap_(bootstrap)
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
        TraversalStatistics_.ConfigVersion = ConfigVersion_;
        TraversalStatistics_.ChunkCount = TraversalStatistics_.ConfigVersion == info.ConfigVersion ? info.ChunkCount : 0;
        TraversalStatistics_.ChunkCount -= config->MaxChunkCount;
        TraversalStatistics_.ChunkCount = std::max(TraversalStatistics_.ChunkCount, 0);

        NChunkClient::TReadLimit lowerLimit;
        lowerLimit.SetChunkIndex(TraversalStatistics_.ChunkCount);
        YT_LOG_DEBUG("Traversal started (RootChunkListId: %v, LowerLimit: %v)",
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
    const TLogger Logger;
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

    TChunkMergerTraversalStatistics TraversalStatistics_;

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
            ++TraversalStatistics_.ChunkCount;
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
                TraversalStatistics_);
            return;
        }

        MaybePlanJob();

        auto result = error.IsOK() ? EMergeSessionResult::OK : EMergeSessionResult::TransientFailure;
        chunkVisitorHost->OnTraversalFinished(
            NodeId_,
            result,
            TraversalStatistics_);
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
            YT_LOG_DEBUG("Shallow merge criteria violated: chunk is dead (ChunkId: %v)",
                ChunkIds_.front());
            return false;
        }
        if (!ChunkMetaEqual(firstChunk, chunk)) {
            YT_LOG_DEBUG("Shallow merge criteria violated: chunk metas differ (ChunkId: %v, ChunkId: %v)",
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
        auto schema = table->GetSchema()->AsTableSchema();
        return !table->IsDynamic() && !schema->HasHunkColumns();
    }

    bool MaybeAddChunk(TChunk* chunk, TChunkList* parent)
    {
        const auto& config = GetDynamicConfig();

        if (parent->GetId() == LastChunkListId_ && JobsForLastChunkList_ >= config->MaxJobsPerChunkList) {
            YT_LOG_DEBUG("Cannot add chunk to merge job due to job limit "
                "(ChunkListId: %v, JobsForLastChunkList: %v, MaxJobsPerChunkList: %v)",
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
            YT_LOG_DEBUG("Cannot add chunk to merge job due to nonzero system block count (ChunkId: %v, SystemBlockCount: %v)",
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
            config->MaxInputChunkDataWeight,
            config->MaxChunkMeta
        };
        mergerCriteria.AssignNotNull(accountCriteria);

        auto satisfiedCriteriaCount = 0;
        const auto targetSatisfiedCriteriaCount = 8;

        auto& statistics = TraversalStatistics_.ViolatedCriteriaStatistics;
        auto incrementSatisfiedOrViolatedCriteriaCount = [&] (bool condition, auto& violatedCriteriaCount, auto criterionName, auto formattedArguments) {
            if (condition) {
                ++satisfiedCriteriaCount;
            } else {
                ++violatedCriteriaCount;
                YT_LOG_DEBUG(
                    "Cannot add chunk to merge job due to limit violation "
                    "(ViolatedCriterion: %v, %v, DesiredParentChunkListId: %v, ParentChunkListId: %v)",
                    criterionName,
                    formattedArguments,
                    ParentChunkListId_,
                    parent->GetId());
            }
        };

        incrementSatisfiedOrViolatedCriteriaCount(
            chunk->ChunkMeta()->GetExtensionsByteSize() < mergerCriteria.MaxMetaChunkSize,
            statistics.MaxChunkMetaViolatedCriteria,
            "ChunkMeta",
            Format("ChunkMetaSize: %v, MaxMetaChunkSize: %v",
                chunk->ChunkMeta()->GetExtensionsByteSize(),
                config->MaxChunkMeta));

        incrementSatisfiedOrViolatedCriteriaCount(
            CurrentRowCount_ + chunk->GetRowCount() < mergerCriteria.MaxRowCount,
            statistics.MaxRowCountViolatedCriteria,
            "RowCount",
            Format("CurrentRowCount: %v, ChunkRowCount: %v, MaxRowCount: %v",
                CurrentRowCount_,
                chunk->GetRowCount(),
                mergerCriteria.MaxRowCount));

        incrementSatisfiedOrViolatedCriteriaCount(
            CurrentDataWeight_ + chunk->GetDataWeight() < mergerCriteria.MaxDataWeight,
            statistics.MaxDataWeightViolatedCriteria,
            "DataWeight",
            Format("CurrentDataWeight: %v, ChunkDataWeight: %v, MaxDataWeight: %v",
                CurrentDataWeight_,
                chunk->GetDataWeight(),
                mergerCriteria.MaxDataWeight));

        incrementSatisfiedOrViolatedCriteriaCount(
            CurrentCompressedDataSize_ + chunk->GetCompressedDataSize() < mergerCriteria.MaxCompressedDataSize,
            statistics.MaxCompressedDataSizeViolatedCriteria,
            "CompressedDataSize",
            Format("CurrentCompressedDataSize: %v, ChunkCompressedDataSize: %v, MaxCompressedDataSize: %v",
                CurrentCompressedDataSize_,
                chunk->GetCompressedDataSize(),
                mergerCriteria.MaxCompressedDataSize));

        incrementSatisfiedOrViolatedCriteriaCount(
            CurrentUncompressedDataSize_ + chunk->GetUncompressedDataSize() < mergerCriteria.MaxUncompressedDataSize,
            statistics.MaxUncompressedDataSizeViolatedCriteria,
            "UncompressedDataSize",
            Format("CurrentUncompressedDataSize: %v, ChunkUncompressedDataSize: %v, MaxUncompressedDataSize: %v",
                CurrentUncompressedDataSize_,
                chunk->GetUncompressedDataSize(),
                mergerCriteria.MaxUncompressedDataSize));

        incrementSatisfiedOrViolatedCriteriaCount(
            std::ssize(ChunkIds_) < mergerCriteria.MaxChunkCount,
            statistics.MaxChunkCountViolatedCriteria,
            "ChunkCount",
            Format("CurrentChunkCount: %v, MaxChunkCount: %v",
                std::ssize(ChunkIds_),
                mergerCriteria.MaxChunkCount));

        incrementSatisfiedOrViolatedCriteriaCount(
            chunk->GetDataWeight() < mergerCriteria.MaxInputChunkDataWeight,
            statistics.MaxInputChunkDataWeightViolatedCriteria,
            "InputChunkDataWeight",
            Format("ChunkDataWeight: %v, MaxInputChunkDataWeight: %v",
                chunk->GetDataWeight(),
                mergerCriteria.MaxInputChunkDataWeight));

        if (ParentChunkListId_ == NullObjectId || ParentChunkListId_ == parent->GetId()) {
            ++satisfiedCriteriaCount;
        }

        if (satisfiedCriteriaCount == targetSatisfiedCriteriaCount) {
            CurrentRowCount_ += chunk->GetRowCount();
            CurrentDataWeight_ += chunk->GetDataWeight();
            CurrentCompressedDataSize_ += chunk->GetCompressedDataSize();
            CurrentUncompressedDataSize_ += chunk->GetUncompressedDataSize();
            ParentChunkListId_ = parent->GetId();
            ChunkIds_.push_back(chunk->GetId());
            return true;
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
            YT_VERIFY(TraversalStatistics_.ChunkCount > 0);
            --TraversalStatistics_.ChunkCount;
        }

        chunkVisitorHost->RegisterJobAwaitingChunkCreation(
            jobId,
            CurrentJobMode_,
            JobIndex_++,
            NodeId_,
            ParentChunkListId_,
            std::move(ChunkIds_),
            Account_->GetId());
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
        BIND_NO_PROPAGATE(&TChunkMerger::Load, Unretained(this)));
    RegisterSaver(
        ESyncSerializationPriority::Values,
        "ChunkMerger",
        BIND_NO_PROPAGATE(&TChunkMerger::Save, Unretained(this)));

    RegisterMethod(BIND_NO_PROPAGATE(&TChunkMerger::HydraStartMergeTransaction, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TChunkMerger::HydraCreateChunks, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TChunkMerger::HydraReplaceChunks, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TChunkMerger::HydraFinalizeChunkMergeSessions, Unretained(this)));
    RegisterMethod(BIND_NO_PROPAGATE(&TChunkMerger::HydraRescheduleMerge, Unretained(this)));
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

bool TChunkMerger::CanRegisterMergeSession(TChunkOwnerBase* trunkChunkOwner)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    YT_VERIFY(trunkChunkOwner->IsTrunk());

    const auto& config = GetDynamicConfig();
    if (!config->Enable && !config->EnableQueueSizeLimitChanges) {
        YT_LOG_DEBUG("Cannot schedule merge: chunk merger is disabled");
        return false;
    }

    if (!IsObjectAlive(trunkChunkOwner)) {
        return false;
    }

    if (trunkChunkOwner->GetType() != EObjectType::Table) {
        YT_LOG_DEBUG("Chunk merging is supported only for table types (ChunkOwnerId: %v)",
            trunkChunkOwner->GetId());
        return false;
    }

    auto* table = trunkChunkOwner->As<TTableNode>();
    if (table->IsDynamic()) {
        YT_LOG_DEBUG("Chunk merging is not supported for dynamic tables (ChunkOwnerId: %v)",
            trunkChunkOwner->GetId());
        return false;
    }

    auto schema = table->GetSchema()->AsTableSchema();
    if (schema->HasHunkColumns()) {
        YT_LOG_DEBUG("Chunk merging is not supported for tables with hunk columns (ChunkOwnerId: %v)",
            trunkChunkOwner->GetId());
        return false;
    }

    auto* account = trunkChunkOwner->Account().Get();
    if (config->RespectAccountSpecificToggle && !account->GetAllowUsingChunkMerger()) {
        YT_LOG_DEBUG("Skipping node as its account is banned from using chunk merger (NodeId: %v, Account: %v)",
            trunkChunkOwner->GetId(),
            account->GetName());
        return false;
    }

    return true;
}

void TChunkMerger::ScheduleMerge(TChunkOwnerBase* trunkChunkOwner)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    YT_VERIFY(trunkChunkOwner->IsTrunk());

    if (!CanRegisterMergeSession(trunkChunkOwner)) {
        return;
    }

    RegisterSession(trunkChunkOwner);
}

EChunkMergerStatus TChunkMerger::GetNodeChunkMergerStatus(NCypressServer::TNodeId nodeId) const
{
    YT_VERIFY(!HasMutationContext());

    auto it = NodeToChunkMergerStatus_.find(nodeId);
    return it == NodeToChunkMergerStatus_.end() ? EChunkMergerStatus::NotInMergePipeline : it->second;
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

        DecrementTracker(
            &TAccountQueuesUsage::JobsAwaitingNodeHeartbeat,
            jobInfo.AccountId);
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

    const auto& securityManager = Bootstrap_->GetSecurityManager();
    auto getAccountTag = [&] (TAccountId accountId) {
        auto* account = securityManager->FindAccount(accountId);
        return IsObjectAlive(account) ? account->GetName() : Format("<%v>", accountId);
    };

    for (const auto& [accountId, nodesBeingMerged] : NodesBeingMergedPerAccount_) {
        TWithTagGuard tagGuard(buffer, "account", getAccountTag(accountId));
        buffer->AddGauge("/chunk_merger_nodes_being_merged", nodesBeingMerged);
    }

    for (const auto& [accountId, usages] : QueuesUsage_) {
        TWithTagGuard tagGuard(buffer, "account", getAccountTag(accountId));
        buffer->AddGauge("/chunk_merger_jobs_awaiting_chunk_creation", usages.JobsAwaitingChunkCreation);
        buffer->AddGauge("/chunk_merger_jobs_undergoing_chunk_creation", usages.JobsUndergoingChunkCreation);
        buffer->AddGauge("/chunk_merger_jobs_awaiting_node_heartbeat", usages.JobsAwaitingNodeHeartbeat);
    }

    for (const auto& [accountId, statistics] : AccountToChunkMergerStatistics_) {
        auto* account = securityManager->FindAccount(accountId);
        if (!IsObjectAlive(account)) {
            continue;
        }
        TWithTagGuard tagGuard(buffer, "account", account->GetName());
        buffer->AddGauge("/chunk_merger_chunk_replacements_succeeded", statistics.ChunkReplacementsSucceeded);
        buffer->AddGauge("/chunk_merger_chunk_replacements_failed", statistics.ChunkReplacementsFailed);
        buffer->AddGauge("/chunk_merger_chunk_count_saving", statistics.ChunkCountSaving);

        const auto& violatedCriteria = statistics.ViolatedCriteria;
        buffer->AddGauge("/chunk_merger_max_chunk_count_violated_criteria", violatedCriteria.MaxChunkCountViolatedCriteria);
        buffer->AddGauge("/chunk_merger_max_row_count_violated_criteria", violatedCriteria.MaxRowCountViolatedCriteria);
        buffer->AddGauge("/chunk_merger_max_data_weight_violated_criteria", violatedCriteria.MaxDataWeightViolatedCriteria);
        buffer->AddGauge("/chunk_merger_max_uncompressed_data_violated_criteria", violatedCriteria.MaxUncompressedDataSizeViolatedCriteria);
        buffer->AddGauge("/chunk_merger_max_compressed_data_violated_criteria", violatedCriteria.MaxCompressedDataSizeViolatedCriteria);
        buffer->AddGauge("/chunk_merger_max_input_chunk_data_weight_violated_criteria", violatedCriteria.MaxInputChunkDataWeightViolatedCriteria);
    }
    AccountToChunkMergerStatistics_.clear();

    buffer->AddCounter("/chunk_merger_sessions_awaiting_finalization", SessionsAwaitingFinalization_.size());

    for (auto mergerMode : TEnumTraits<NChunkClient::EChunkMergerMode>::GetDomainValues()) {
        if (mergerMode == NChunkClient::EChunkMergerMode::None) {
            continue;
        }
        TWithTagGuard tagGuard(buffer, "merger_mode", FormatEnum(mergerMode));
        buffer->AddCounter("/chunk_merger_completed_job_count", CompletedJobCountPerMode_[mergerMode]);
    }
    buffer->AddCounter("/chunk_merger_auto_merge_fallback_count", AutoMergeFallbackJobCount_);

    for (const auto& [accountId, sessionDurations] : AccountIdToNodeMergeDurations_) {
        TWithTagGuard tagGuard(buffer, "account", getAccountTag(accountId));
        for (auto sessionDuration : sessionDurations) {
            buffer->AddGauge("/chunk_merger_average_merge_duration", sessionDuration.GetValue());
        }
    }
    AccountIdToNodeMergeDurations_.clear();

    for (const auto& [accountId, stuckNodes] : AccountIdToStuckNodes_) {
        TWithTagGuard tagGuard(buffer, "account", getAccountTag(accountId));
        buffer->AddGauge("/chunk_merger_stuck_nodes_count", stuckNodes.size());
    }
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
    const auto& epochAutomatonInvoker = Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMerger);

    ScheduleExecutor_ = New<TPeriodicExecutor>(
        epochAutomatonInvoker,
        BIND(&TChunkMerger::ProcessTouchedNodes, MakeWeak(this)),
        config->SchedulePeriod);
    ScheduleExecutor_->Start();

    ChunkCreatorExecutor_ = New<TPeriodicExecutor>(
        epochAutomatonInvoker,
        BIND(&TChunkMerger::CreateChunks, MakeWeak(this)),
        config->CreateChunksPeriod);
    ChunkCreatorExecutor_->Start();

    StartTransactionExecutor_ = New<TPeriodicExecutor>(
        epochAutomatonInvoker,
        BIND(&TChunkMerger::StartMergeTransaction, MakeWeak(this)),
        config->TransactionUpdatePeriod);
    StartTransactionExecutor_->Start();

    FinalizeSessionExecutor_ = New<TPeriodicExecutor>(
        epochAutomatonInvoker,
        BIND(&TChunkMerger::FinalizeSessions, MakeWeak(this)),
        config->SessionFinalizationPeriod);
    FinalizeSessionExecutor_->Start();

    for (auto [nodeId, accountId] : NodesBeingMerged_) {
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
        YT_UNUSED_FUTURE(ScheduleExecutor_->Stop());
        ScheduleExecutor_.Reset();
    }

    if (ChunkCreatorExecutor_) {
        YT_UNUSED_FUTURE(ChunkCreatorExecutor_->Stop());
        ChunkCreatorExecutor_.Reset();
    }

    if (StartTransactionExecutor_) {
        YT_UNUSED_FUTURE(StartTransactionExecutor_->Stop());
        StartTransactionExecutor_.Reset();
    }

    if (FinalizeSessionExecutor_) {
        YT_UNUSED_FUTURE(FinalizeSessionExecutor_->Stop());
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
    NodesBeingMergedPerAccount_.clear();
    AccountIdToNodeMergeDurations_.clear();
    NodeToChunkMergerStatus_.clear();
    ConfigVersion_ = 0;
    NodeToRescheduleCountAfterMaxBackoffDelay_.clear();
    NodeToBackoffPeriod_.clear();
    AccountIdToStuckNodes_.clear();

    OldNodesBeingMerged_.reset();
    NeedRestorePersistentStatistics_ = false;
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
    QueuesUsage_ = {};
    AccountIdToNodeMergeDurations_ = {};
    NodeToChunkMergerStatus_ = {};
    NodeToRescheduleCountAfterMaxBackoffDelay_ = {};
    NodeToBackoffPeriod_ = {};
    AccountIdToStuckNodes_ = {};
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
    YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
        ->CommitAndLog(Logger()));
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

void TChunkMerger::RescheduleMerge(TObjectId nodeId, TAccountId accountId)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    YT_LOG_DEBUG(
        "Initiating backoff reschedule merge (NodeId: %v, AccountId: %v)",
        nodeId,
        accountId);

    auto [it, _] = NodeToBackoffPeriod_.emplace(nodeId, GetDynamicConfig()->MinBackoffPeriod);
    auto maxBackoffPeriod = GetDynamicConfig()->MaxBackoffPeriod;
    auto backoff = it->second;
    EmplaceOrCrash(NodesBeingMerged_, nodeId, accountId);

    TDelayedExecutor::Submit(
        BIND([this, this_ = MakeStrong(this), nodeId, accountId] {
            if (IsLeader()) {
                const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
                TReqRescheduleMerge request;
                ToProto(request.mutable_node_id(), nodeId);
                ToProto(request.mutable_account_id(), accountId);
                YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)->Commit());
            }
        }),
        backoff,
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ChunkMerger));

    if (backoff == maxBackoffPeriod) {
        if (NodeToRescheduleCountAfterMaxBackoffDelay_[nodeId] + 1 > GetDynamicConfig()->MaxAllowedBackoffReschedulingsPerSession) {
            YT_LOG_ALERT("Node is suspected in being stuck in merge pipeline (NodeId: %v, RescheduleIteration: %v, AccountId: %v)",
                nodeId,
                NodeToRescheduleCountAfterMaxBackoffDelay_[nodeId],
                accountId);
            AccountIdToStuckNodes_[accountId].insert(nodeId);
        }
        ++NodeToRescheduleCountAfterMaxBackoffDelay_[nodeId];
    }
    NodeToBackoffPeriod_[nodeId] = std::min(2 * backoff, maxBackoffPeriod);
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

    auto accountId = chunkOwner->Account()->GetId();
    EmplaceOrCrash(NodesBeingMerged_, chunkOwner->GetId(), accountId);
    DoRegisterSession(chunkOwner);
}

void TChunkMerger::DoRegisterSession(TChunkOwnerBase* chunkOwner)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());
    YT_VERIFY(NodesBeingMerged_.contains(chunkOwner->GetId()));

    auto accountId = chunkOwner->Account()->GetId();
    EmplaceOrCrash(NodeToChunkMergerStatus_, chunkOwner->GetId(), EChunkMergerStatus::AwaitingMerge);
    IncrementPersistentTracker(accountId);

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
    YT_VERIFY(RunningSessions_.emplace(nodeId, TChunkMergerSession({.AccountId = account->GetId(), .SessionCreationTime = TInstant::Now()})).second);

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

    auto& session = GetOrCrash(RunningSessions_, jobInfo.NodeId);
    session.Result = std::max(session.Result, result);

    YT_LOG_DEBUG("Finalizing merge job (NodeId: %v, JobId: %v, AccountId: %v, JobResult: %v)",
        nodeId,
        jobId,
        jobInfo.AccountId,
        result);

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

    auto& session = GetOrCrash(RunningSessions_, nodeId);
    session.Result = std::max(session.Result, result);

    YT_LOG_DEBUG("Finalizing chunk merge replacement (NodeId: %v, ChunkListId: %v, Result: %v, AccountId: %v)",
        nodeId,
        chunkListId,
        result,
        session.AccountId);
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

    YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
        ->CommitAndLog(Logger()));
}

void TChunkMerger::RegisterJobAwaitingChunkCreation(
    TJobId jobId,
    EChunkMergerMode mode,
    int jobIndex,
    TObjectId nodeId,
    TChunkListId parentChunkListId,
    std::vector<TChunkId> inputChunkIds,
    TAccountId accountId)
{
    JobsAwaitingChunkCreation_.push({
        .JobId = jobId,
        .JobIndex = jobIndex,
        .NodeId = nodeId,
        .ParentChunkListId = parentChunkListId,
        .InputChunkIds = std::move(inputChunkIds),
        .MergeMode = mode,
        .AccountId = accountId,
    });

    IncrementTracker(
        &TAccountQueuesUsage::JobsAwaitingChunkCreation,
        accountId);

    YT_LOG_DEBUG("Planning merge job (JobId: %v, NodeId: %v, ParentChunkListId: %v, AccountId: %v)",
        jobId,
        nodeId,
        parentChunkListId,
        accountId);

    auto& session = GetOrCrash(RunningSessions_, nodeId);
    InsertOrCrash(session.ChunkListIdToRunningJobs[parentChunkListId], jobId);
    ++session.JobCount;
}

void TChunkMerger::OnTraversalFinished(TObjectId nodeId, EMergeSessionResult result, TChunkMergerTraversalStatistics traversalStatistics)
{
    auto& session = GetOrCrash(RunningSessions_, nodeId);
    session.TraversalStatistics = traversalStatistics;
    session.TraversalFinished = true;

    auto& statistics = AccountToChunkMergerStatistics_[session.AccountId];
    statistics.ViolatedCriteria += traversalStatistics.ViolatedCriteriaStatistics;

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
    YT_LOG_DEBUG(
        "Finalizing chunk merge session (NodeId: %v, Result: %v, TraversalStatistics: %v, AccountId: %v)",
        nodeId,
        session.Result,
        session.TraversalStatistics,
        session.AccountId);

    SessionsAwaitingFinalization_.push({
        .NodeId = nodeId,
        .Result = session.Result,
        .TraversalStatistics = session.TraversalStatistics,
        .JobCount = session.JobCount,
        .AccountId = session.AccountId,
        .SessionCreationTime = session.SessionCreationTime,
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
            ToProto(req->mutable_traversal_info(), sessionResult.TraversalStatistics);
        }
        req->set_job_count(sessionResult.JobCount);
        auto sessionLifetimeDuration = TInstant::Now() - sessionResult.SessionCreationTime;
        YT_LOG_DEBUG("Finalized merge session (NodeId: %v, MergeSessionDuration: %v, AccountId: %v)",
            sessionResult.NodeId,
            sessionLifetimeDuration,
            sessionResult.AccountId);

        AccountIdToNodeMergeDurations_[sessionResult.AccountId].push_back(sessionLifetimeDuration);
        SessionsAwaitingFinalization_.pop();
    }

    YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
        ->CommitAndLog(Logger()));
}

bool TChunkMerger::CanAdvanceNodeInMergePipeline()
{
    return ssize(JobsAwaitingChunkCreation_) + ssize(JobsUndergoingChunkCreation_)
        + ssize(JobsAwaitingNodeHeartbeat_) + ssize(SessionsAwaitingFinalization_) < GetDynamicConfig()->MaxNodesBeingMerged;
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
            if (!CanAdvanceNodeInMergePipeline()) {
                YT_LOG_DEBUG("Too many nodes are being merged, cannot advance node in merge pipeline (Account: %v, NodeId: %v)",
                    account->GetName(),
                    nodeId);
                break;
            }
            queue.pop();

            YT_VERIFY(RunningSessions_.contains(nodeId));
            auto* node = FindChunkOwner(nodeId);

            if (CanScheduleMerge(node)) {
                YT_VERIFY(NodesBeingMerged_.contains(nodeId));
                NodeToChunkMergerStatus_[nodeId] = EChunkMergerStatus::InMergePipeline;

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

        DecrementTracker(
            &TAccountQueuesUsage::JobsAwaitingChunkCreation,
            jobInfo.AccountId);

        if (!chunkOwner) {
            FinalizeJob(
                std::move(jobInfo),
                EMergeSessionResult::PermanentFailure);
            JobsAwaitingChunkCreation_.pop();
            continue;
        }

        YT_LOG_DEBUG("Creating chunks for merge (JobId: %v, ChunkOwnerId: %v, AccountId: %v)",
            jobInfo.JobId,
            jobInfo.NodeId,
            jobInfo.AccountId);

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

        IncrementTracker(
            &TAccountQueuesUsage::JobsUndergoingChunkCreation,
            jobInfo.AccountId);

        YT_VERIFY(JobsUndergoingChunkCreation_.emplace(jobInfo.JobId, std::move(jobInfo)).second);
        JobsAwaitingChunkCreation_.pop();
    }

    YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), createChunksReq)
        ->CommitAndLog(Logger()));
}

bool TChunkMerger::TryScheduleMergeJob(IJobSchedulingContext* context, const TMergeJobInfo& jobInfo)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto* chunkOwner = FindChunkOwner(jobInfo.NodeId);
    if (!CanScheduleMerge(chunkOwner)) {
        return false;
    }

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto* chunkRequisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
    auto* outputChunk = chunkManager->FindChunk(jobInfo.OutputChunkId);
    if (!IsObjectAlive(outputChunk)) {
        return false;
    }
    auto erasureCodec = outputChunk->GetErasureCodec();

    TChunkMergerWriterOptions chunkMergerWriterOptions;
    if (chunkOwner->GetType() == EObjectType::Table) {
        const auto* table = chunkOwner->As<TTableNode>();
        const auto* schema = table->GetSchema();
        ToProto(chunkMergerWriterOptions.mutable_schema(), *schema->AsTableSchema());
        ToProto(chunkMergerWriterOptions.mutable_schema_id(), schema->GetId());
        chunkMergerWriterOptions.set_optimize_for(ToProto<int>(table->GetOptimizeFor()));
    }
    chunkMergerWriterOptions.set_compression_codec(ToProto<int>(chunkOwner->GetCompressionCodec()));
    chunkMergerWriterOptions.set_erasure_codec(ToProto<int>(erasureCodec));
    chunkMergerWriterOptions.set_enable_skynet_sharing(chunkOwner->GetEnableSkynetSharing());
    chunkMergerWriterOptions.set_merge_mode(ToProto<int>(jobInfo.MergeMode));
    chunkMergerWriterOptions.set_max_heavy_columns(Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager->MaxHeavyColumns);
    chunkMergerWriterOptions.set_max_block_count(GetDynamicConfig()->MaxBlockCount);

    TMergeJob::TChunkVector inputChunks;
    inputChunks.reserve(jobInfo.InputChunkIds.size());

    for (auto chunkId : jobInfo.InputChunkIds) {
        auto* chunk = chunkManager->FindChunk(chunkId);
        if (!IsObjectAlive(chunk)) {
            return false;
        }
        inputChunks.emplace_back(chunk);
    }

    const auto& requisition = outputChunk->GetAggregatedRequisition(chunkRequisitionRegistry);
    TChunkIdWithIndexes chunkIdWithIndexes(
        jobInfo.OutputChunkId,
        GenericChunkReplicaIndex,
        requisition.begin()->MediumIndex);
    int targetCount = erasureCodec == NErasure::ECodec::None
        ? outputChunk->GetAggregatedReplicationFactor(
            chunkIdWithIndexes.MediumIndex,
            chunkRequisitionRegistry)
        : NErasure::GetCodec(erasureCodec)->GetTotalPartCount();

    // TODO(gritukan): Support external media in chunk merger.
    auto* medium = chunkManager->GetMediumByIndexOrThrow(chunkIdWithIndexes.MediumIndex);
    if (medium->IsOffshore()) {
        YT_LOG_ALERT(
            "Chunk merger was run for table with offshore medium, ignored "
            "(ChunkId: %v, MediumIndex: %v, MediumName: %v, MediumType: %v)",
            jobInfo.OutputChunkId,
            medium->GetIndex(),
            medium->GetName(),
            medium->GetType());
        return false;
    }

    auto targetNodes = chunkManager->AllocateWriteTargets(
        medium->AsDomestic(),
        outputChunk,
        /*replicas*/ {}, // We know there are no replicas for output chunk yet.
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
        "(JobId: %v, JobEpoch: %v, Address: %v, NodeId: %v, InputChunkIds: %v, OutputChunkId: %v, ValidateShallowMerge: %v, AccointId: %v)",
        job->GetJobId(),
        job->GetJobEpoch(),
        context->GetNode()->GetDefaultAddress(),
        jobInfo.NodeId,
        jobInfo.InputChunkIds,
        jobInfo.OutputChunkId,
        validateShallowMerge,
        jobInfo.AccountId);

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
    const TChunkOwnerDataStatistics& oldStatistics,
    const TChunkOwnerDataStatistics& newStatistics)
{
    YT_LOG_ALERT_IF(oldStatistics.RowCount != newStatistics.RowCount,
        "Row count in new statistics is different (NodeId: %v, OldRowCount: %v, NewRowCount: %v)",
        nodeId,
        oldStatistics.RowCount,
        newStatistics.RowCount);

    YT_LOG_ALERT_IF(oldStatistics.DataWeight != newStatistics.DataWeight,
        "Data weight in new statistics is different (NodeId: %v, OldDataWeight: %v, NewDataWeight: %v)",
        nodeId,
        oldStatistics.DataWeight,
        newStatistics.DataWeight);
}

void TChunkMerger::RemoveNodeFromRescheduleMaps(TAccountId accountId, NCypressClient::TNodeId nodeId)
{
    // Doesn't guarantee the success of the operation, because nodeId is not always in the map when deleted.
    NodeToRescheduleCountAfterMaxBackoffDelay_.erase(nodeId);
    NodeToBackoffPeriod_.erase(nodeId);
    AccountIdToStuckNodes_[accountId].erase(nodeId);
    if (AccountIdToStuckNodes_[accountId].empty()) {
        AccountIdToStuckNodes_.erase(accountId);
    }
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

        auto eraseFromQueue = [&] {
            if (!IsLeader()) {
                return;
            }

            auto it = JobsUndergoingChunkCreation_.find(jobId);
            if (it == JobsUndergoingChunkCreation_.end()) {
                return;
            }

            auto accountId = it->second.AccountId;

            FinalizeJob(
                std::move(it->second),
                EMergeSessionResult::TransientFailure);

            DecrementTracker(
                &TAccountQueuesUsage::JobsUndergoingChunkCreation,
                accountId);

            JobsUndergoingChunkCreation_.erase(it);
        };

        if (!IsObjectAlive(transaction)) {
            YT_LOG_DEBUG("Chunk merger cannot create chunks: no such transaction (JobId: %v, TransactionId: %v)",
                jobId,
                transactionId);
            eraseFromQueue();
            continue;
        }

        auto chunkType = FromProto<EObjectType>(subrequest.type());

        auto mediumIndex = subrequest.medium_index();
        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!IsObjectAlive(medium)) {
            YT_LOG_DEBUG("Chunk merger cannot create chunks: no such medium (JobId: %v, MediumIndex: %v)",
                jobId,
                mediumIndex);
            eraseFromQueue();
            continue;
        }

        const auto& accountName = subrequest.account();
        auto* account = securityManager->FindAccountByName(accountName, /*activeLifeStageOnly*/ true);
        if (!IsObjectAlive(account)) {
            YT_LOG_DEBUG("Chunk merger cannot create chunks: no such account (JobId: %v, Account: %v)",
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
                auto accountId = jobInfo.AccountId;
                jobInfo.OutputChunkId = chunk->GetId();

                IncrementTracker(
                    &TAccountQueuesUsage::JobsAwaitingNodeHeartbeat,
                    accountId);
                JobsAwaitingNodeHeartbeat_.push(std::move(jobInfo));

                DecrementTracker(
                    &TAccountQueuesUsage::JobsUndergoingChunkCreation,
                    accountId);
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

    auto& statistics = AccountToChunkMergerStatistics_[accountId];
    auto updateFailedReplacements = [&] (int replacementCount) {
        if (!IsLeader()) {
            return;
        }
        statistics.ChunkReplacementsFailed += replacementCount;
    };

    auto replacementCount = request->replacements_size();
    auto* chunkOwner = FindChunkOwner(nodeId);
    if (!chunkOwner) {
        YT_LOG_DEBUG("Cannot replace chunks after merge: no such chunk owner node (NodeId: %v, AccountId: %v)",
            nodeId,
            accountId);
        updateFailedReplacements(replacementCount);
        FinalizeReplacement(nodeId, chunkListId, EMergeSessionResult::PermanentFailure);
        return;
    }

    if (chunkOwner->GetType() == EObjectType::Table) {
        auto* table = chunkOwner->As<TTableNode>();
        if (table->IsDynamic()) {
            YT_LOG_DEBUG("Cannot replace chunks after merge: table is dynamic (NodeId: %v, AccountId: %v)",
                chunkOwner->GetId(),
                accountId);
            updateFailedReplacements(replacementCount);
            FinalizeReplacement(nodeId, chunkListId, EMergeSessionResult::PermanentFailure);
            return;
        }
    }

    auto* rootChunkList = chunkOwner->GetChunkList();
    YT_LOG_DEBUG("Replacing chunks after merge (NodeId: %v, ChunkListId: %v, AccountId: %v)",
        nodeId,
        chunkListId,
        accountId);

    TChunkReplacer chunkReplacer(ChunkReplacerCallbacks_, Logger());
    if (!chunkReplacer.FindChunkList(rootChunkList, chunkListId)) {
        YT_LOG_DEBUG(
            "Cannot replace chunks after merge: parent chunk list is no longer there (NodeId: %v, ParentChunkListId: %v, AccountId: %v)",
            nodeId,
            chunkListId,
            accountId);
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
            YT_LOG_DEBUG("Cannot replace chunks after merge: no such chunk (NodeId: %v, ChunkId: %v, AccountId: %v)",
                nodeId,
                newChunkId,
                accountId);
            updateFailedReplacements(1);
            continue;
        }

        if (!newChunk->IsConfirmed()) {
            YT_LOG_ALERT("Cannot replace chunks after merge: new chunk is not confirmed (NodeId: %v, ChunkId: %v, AccountId: %v)",
                nodeId,
                newChunkId,
                accountId);
            updateFailedReplacements(1);
            continue;
        }

        auto chunkIds = FromProto<std::vector<TChunkId>>(replacement.old_chunk_ids());
        if (chunkReplacer.ReplaceChunkSequence(newChunk, chunkIds)) {
            YT_LOG_DEBUG(
                "Replaced chunks after merge (NodeId: %v, InputChunkIds: %v, ChunkId: %v, AccountId: %v)",
                nodeId,
                chunkIds,
                newChunkId,
                accountId);

            ++chunkReplacementsSucceeded;
            if (IsLeader()) {
                auto chunkCountDiff = std::ssize(chunkIds) - 1;

                ++statistics.ChunkReplacementsSucceeded;
                statistics.ChunkCountSaving += chunkCountDiff;

                auto& session = GetOrCrash(RunningSessions_, nodeId);
                if (session.TraversalStatistics.ChunkCount < 0) {
                    YT_LOG_ALERT("Node traversed chunk count is negative (NodeId: %v, TraversalStatistics: %v, AccountId: %v)",
                        chunkOwner->GetId(),
                        session.TraversalStatistics,
                        accountId);
                    session.TraversalStatistics.ChunkCount = 0;
                }
            }
        } else {
            YT_LOG_DEBUG(
                "Cannot replace chunks after merge: input chunk sequence is no longer there "
                "(NodeId: %v, InputChunkIds: %v, ChunkId: %v, AccountId: %v)",
                nodeId,
                chunkIds,
                newChunkId,
                accountId);
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

    YT_LOG_DEBUG(
        "Chunk list replaced after merge (NodeId: %v, OldChunkListId: %v, NewChunkListId: %v, AccountId: %v)",
        nodeId,
        rootChunkList->GetId(),
        newRootChunkList->GetId(),
        accountId);

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

        auto toErase = GetIteratorOrCrash(NodesBeingMerged_, nodeId);
        auto accountId = toErase->second;
        NodesBeingMerged_.erase(toErase);
        NodeToChunkMergerStatus_.erase(nodeId);
        DecrementPersistentTracker(accountId);

        auto* chunkOwner = FindChunkOwner(nodeId);
        if (!chunkOwner) {
            continue;
        }

        auto nodeTouched = chunkOwner->GetUpdatedSinceLastMerge();
        chunkOwner->SetUpdatedSinceLastMerge(false);

        YT_VERIFY(chunkOwner->GetType() == EObjectType::Table);
        auto* table = chunkOwner->As<TTableNode>();
        if (table->IsDynamic()) {
            YT_LOG_DEBUG(
                "Table became dynamic between chunk replacement and "
                "merge session finalization, ignored (TableId: %v, AccountId: %v)",
                table->GetId(),
                accountId);
            continue;
        }

        if (subrequest.has_traversal_info()) {
            FromProto(&chunkOwner->ChunkMergerTraversalInfo(), subrequest.traversal_info());
        }
        auto jobCount = subrequest.job_count();

        YT_LOG_DEBUG(
            "Finalizing merge session (NodeId: %v, NodeTouched: %v, Result: %v, JobCount: %v, AccountId: %v)",
            nodeId,
            nodeTouched,
            result,
            jobCount,
            accountId);

        const auto& config = GetDynamicConfig();
        if (config->RescheduleMergeOnSuccess && result == EMergeSessionResult::OK && jobCount > 0) {
            ScheduleMerge(nodeId);
            continue;
        }

        if (result == EMergeSessionResult::TransientFailure || (result == EMergeSessionResult::OK && nodeTouched)) {
            if (result == EMergeSessionResult::TransientFailure) {
                RescheduleMerge(nodeId, accountId);
            } else {
                ScheduleMerge(nodeId);
            }
            continue;
        } else if (result == EMergeSessionResult::PermanentFailure) {
            RemoveNodeFromRescheduleMaps(accountId, nodeId);
            continue;
        }

        YT_VERIFY(result == EMergeSessionResult::OK);
        RemoveNodeFromRescheduleMaps(accountId, nodeId);

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

    YT_LOG_INFO("Merge transaction updated (NewTransactionId: %v, PreviousTransactionId: %v)",
        TransactionRotator_.GetTransactionId(),
        TransactionRotator_.GetPreviousTransactionId());
}

void TChunkMerger::HydraRescheduleMerge(NProto::TReqRescheduleMerge* request)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(HasMutationContext());

    auto nodeId = FromProto<TObjectId>(request->node_id());
    auto accountId = FromProto<TAccountId>(request->account_id());

    if (!NodesBeingMerged_.contains(nodeId)) {
        YT_LOG_INFO("Node was removed from persistent merge state before merge was rescheduled (NodeId: %v, AccountId: %v)",
            nodeId,
            accountId);
        return;
    }

    if (RunningSessions_.contains(nodeId)) {
        YT_LOG_INFO("Node already has running merge session (NodeId: %v, AccountId: %v)",
            nodeId,
            accountId);
        return;
    }

    auto* trunkNode = FindChunkOwner(nodeId);
    if (!IsObjectAlive(trunkNode)) {
        YT_LOG_DEBUG(
            "Cannot reschedule merge: node was destroyed (NodeId: %v, AccountId: %v)",
            nodeId,
            accountId);
        EraseOrCrash(NodesBeingMerged_, nodeId);
        return;
    }

    YT_VERIFY(trunkNode->IsTrunk());
    if (!CanRegisterMergeSession(trunkNode)) {
        YT_LOG_DEBUG(
            "Cannot reschedule merge: unable to register merge session (NodeId: %v, AccountId: %v)",
            nodeId,
            accountId);
        EraseOrCrash(NodesBeingMerged_, nodeId);
        return;
    }

    DoRegisterSession(trunkNode);
}

void TChunkMerger::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    // Persist transactions so that we can commit them.
    Save(context, TransactionRotator_);
    Save(context, NodesBeingMerged_);
    Save(context, NodesBeingMergedPerAccount_);
    Save(context, ConfigVersion_);
}

void TChunkMerger::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, TransactionRotator_);

    // COMPAT(vovamelnikov)
    if (context.GetVersion() >= EMasterReign::ChunkMergerQueuesUsagePerAccount) {
        Load(context, NodesBeingMerged_);
    } else {
        OldNodesBeingMerged_ = std::make_unique<THashSet<TObjectId>>();
        Load(context, *OldNodesBeingMerged_);
    }
    if (context.GetVersion() >= EMasterReign::FixMergerStatistics) {
        Load(context, NodesBeingMergedPerAccount_);
    }
    NeedRestorePersistentStatistics_ = context.GetVersion() >= EMasterReign::ChunkMergerQueuesUsagePerAccount &&
        context.GetVersion() < EMasterReign::FixMergerStatistics;

    Load(context, ConfigVersion_);
}

void TChunkMerger::OnAfterSnapshotLoaded()
{
    if (OldNodesBeingMerged_) {
        NodesBeingMerged_.clear();
        NodesBeingMergedPerAccount_.clear();
        for (auto nodeId: *OldNodesBeingMerged_) {
            auto* chunkOwner = FindChunkOwner(nodeId);
            if (!chunkOwner) {
                continue;
            }

            auto accountId = chunkOwner->Account()->GetId();
            EmplaceOrCrash(NodesBeingMerged_, nodeId, accountId);
            IncrementPersistentTracker(accountId);
        }
        OldNodesBeingMerged_.reset();
    }

    if (NeedRestorePersistentStatistics_) {
        NodesBeingMergedPerAccount_.clear();
        for (const auto& [nodeId, accountId] : NodesBeingMerged_) {
            IncrementPersistentTracker(accountId);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TChunkMerger::IncrementTracker(
    int TAccountQueuesUsage::* queue,
    TAccountId accountId)
{
    ++(QueuesUsage_[accountId].*queue);
}

void TChunkMerger::DecrementTracker(
    int TAccountQueuesUsage::* queue,
    TAccountId accountId)
{
    auto it = QueuesUsage_.find(accountId);

    if (it == QueuesUsage_.end() || it->second.*queue <= 0) {
        if (it != QueuesUsage_.end()) {
            it->second.*queue = 0;
            if (it->second == TAccountQueuesUsage{}) {
                QueuesUsage_.erase(it);
            }
        }
        YT_LOG_ALERT("Chunk merger queues usage tracking is tainted, account seems to appear in queue negative times (AccountId: %v)",
            accountId);
        return;
    }

    --(it->second.*queue);
    if (it->second == TAccountQueuesUsage{}) {
        QueuesUsage_.erase(it);
    }
}

void TChunkMerger::IncrementPersistentTracker(TAccountId accountId)
{
    YT_VERIFY(HasHydraContext());

    ++(NodesBeingMergedPerAccount_[accountId]);
}

void TChunkMerger::DecrementPersistentTracker(TAccountId accountId)
{
    YT_VERIFY(HasHydraContext());

    auto it = NodesBeingMergedPerAccount_.find(accountId);
    if (it == NodesBeingMergedPerAccount_.end() || it->second <= 0) {
        if (it != NodesBeingMergedPerAccount_.end()) {
            NodesBeingMergedPerAccount_.erase(it);
        }
        YT_LOG_ALERT("Chunk merger persistent queue usage tracking is tainted, account seems to appear in queue negative times (AccountId: %v)",
            accountId);
        return;
    }

    --it->second;
    if (it->second == 0) {
        NodesBeingMergedPerAccount_.erase(it);
    }
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

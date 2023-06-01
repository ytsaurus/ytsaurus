#include "chunk_reincarnator.h"

#include "chunk_list.h"
#include "chunk_manager.h"
#include "chunk_requisition.h"
#include "chunk_scanner.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/epoch_history_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/chunk_server/proto/chunk_reincarnator.pb.h>

#include <yt/yt/server/master/node_tracker_server/node_directory_builder.h>

#include <yt/yt/server/master/table_server/table_node.h>

#include <yt/yt/server/master/transaction_server/transaction_manager.h>
#include <yt/yt/server/master/transaction_server/transaction_rotator.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NHydra;
using namespace NJobTrackerClient::NProto;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTableServer;
using namespace NTransactionServer;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EReincarnationResult,
    ((OK)                      (0))
    ((Transient)               (1))
    // Permanent failures.
    ((TooManyAncestors)        (2))
    ((DynamicTableChunk)       (3))
    ((Teleportations)          (4))
    ((NonTableChunk)           (5))
    ((GenericPermanentFailure) (6))
    ((WrongChunkFormat)        (7))
    ((TooManyFailedJobs)       (8))
);

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsPermanentFailure(EReincarnationResult result)
{
    return result != EReincarnationResult::OK && result != EReincarnationResult::Transient;
}

const std::pair<const char*, EReincarnationResult> ReincarnationMetrics[] = {
    {"/chunk_reincarnator/successful_reincarnations", EReincarnationResult::OK},
    {"/chunk_reincarnator/transiently_failed_reincarnations", EReincarnationResult::Transient},
    {"/chunk_reincarnator/permanently_failed_reincarnations", EReincarnationResult::GenericPermanentFailure},
    {"/chunk_reincarnator/permanent_failures/too_many_ancestors", EReincarnationResult::TooManyAncestors},
    {"/chunk_reincarnator/permanent_failures/dynamic_table_chunk", EReincarnationResult::DynamicTableChunk},
    {"/chunk_reincarnator/permanent_failures/teleportations", EReincarnationResult::Teleportations},
    {"/chunk_reincarnator/permanent_failures/non_table_chunk", EReincarnationResult::NonTableChunk},
    {"/chunk_reincarnator/permanent_failures/wrong_chunk_format", EReincarnationResult::WrongChunkFormat},
    {"/chunk_reincarnator/permanent_failures/too_many_failed_jobs", EReincarnationResult::TooManyFailedJobs},
};

////////////////////////////////////////////////////////////////////////////////

class TFailedJobRegistry
{
public:
    void Clear()
    {
        JobsInfos_.clear();
        History_.clear();
    }

    void SetMaxTrackedChunks(int maxTrackedChunks)
    {
        MaxTrackedChunks_ = maxTrackedChunks;

        EvictIfNeeded();
    }

    void SetMaxFailedJobs(int maxFailedJobs)
    {
        MaxFailedJobs_ = maxFailedJobs;

        if (MaxFailedJobs_ == 0) {
            Clear();
        }
    }

    void OnChunkDestroyed(TChunk* chunk)
    {
        if (MaxTrackedChunks_ == 0) {
            return;
        }

        auto it = JobsInfos_.find(chunk->GetId());
        if (it == JobsInfos_.end()) {
            return;
        }

        auto historyIt = it->second.HistoryIterator;
        History_.erase(historyIt);
        JobsInfos_.erase(it);
    }

    EReincarnationResult OnJobFailed(const TReincarnationJobPtr& job)
    {
        YT_LOG_DEBUG("Reincarnation job failed (JobId: %v, OldChunkId: %v, NewChunkId: %v)",
            job->GetJobId(),
            job->OldChunkId(),
            job->NewChunkId());
        if (MaxTrackedChunks_ == 0) {
            return EReincarnationResult::TooManyFailedJobs;
        }

        auto chunkId = job->OldChunkId();
        auto jobsInfoIt = JobsInfos_.find(chunkId);

        if (jobsInfoIt == JobsInfos_.end()) {
            if (MaxFailedJobs_ == 0) {
                return EReincarnationResult::TooManyFailedJobs;
            }

            auto historyIt = History_.insert(History_.end(), chunkId);
            EmplaceOrCrash(JobsInfos_, chunkId, TJobsInfo{1, historyIt});
            EvictIfNeeded();
            return EReincarnationResult::Transient;
        }

        ++jobsInfoIt->second.Count;
        if (jobsInfoIt->second.Count > MaxFailedJobs_) {
            History_.erase(jobsInfoIt->second.HistoryIterator);
            JobsInfos_.erase(jobsInfoIt);
            return EReincarnationResult::TooManyFailedJobs;
        }

        History_.erase(jobsInfoIt->second.HistoryIterator);
        jobsInfoIt->second.HistoryIterator = History_.insert(History_.end(), chunkId);
        return EReincarnationResult::Transient;
    }

private:
    int MaxTrackedChunks_ = 0;
    int MaxFailedJobs_ = 0;

    using TFailedJobHistory = std::list<TChunkId>;
    // Front chunk has the oldest failed job.
    TFailedJobHistory History_;

    struct TJobsInfo
    {
        int Count = 0;
        TFailedJobHistory::iterator HistoryIterator;
    };
    THashMap<TChunkId, TJobsInfo> JobsInfos_;

    void EvictIfNeeded()
    {
        while (std::ssize(History_) > MaxTrackedChunks_) {
            auto chunkId = History_.front();
            History_.pop_front();
            EraseOrCrash(JobsInfos_, chunkId);
        }

        ShrinkHashTable(JobsInfos_);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkAncestorTraverser
{
public:
    explicit TChunkAncestorTraverser(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    {
        YT_VERIFY(Bootstrap_);
    }

    struct TChunkReincarnationPreparationResult
    {
        EReincarnationResult Result;
        int VisitedChunkListCount;
    };

    //! Returns OK if chunk is suitable for reincarnation.
    // NB: This function must be called before CommitChunkReincarnation() is called.
    TChunkReincarnationPreparationResult TryPrepareChunkReincarnation(
        TChunk* chunk,
        int visitedChunkListCountLimit,
        bool chunkListCountLimitViolationIsPermanentFailure)
    {
        YT_VERIFY(IsObjectAlive(chunk));

        NotVisitedChildrenCounts_.clear();
        TraversalStack_.clear();
        int visitedChunkListCount = 0;

        bool foundTable = false;
        TraversalStack_.push_back(chunk);

        auto processParents = [&] (TChunkTree* chunkTree, auto callback) {
            switch (chunkTree->GetType()) {
                case EObjectType::Chunk: [[fallthrough]];
                case EObjectType::ErasureChunk:
                    for (auto [parent, refCounter] : chunk->Parents()) {
                        if (!callback(parent)) {
                            break;
                        }
                    }
                    break;

                case EObjectType::ChunkList:
                    for (auto* parent : chunkTree->AsChunkList()->Parents()) {
                        if (!callback(parent)) {
                            break;
                        }
                    }
                    break;
                default:
                    // Other chunk tree types are handled in TryProcessParent().
                    YT_ABORT();
            }
        };

        auto result = EReincarnationResult::OK;
        while (!TraversalStack_.empty() && result == EReincarnationResult::OK) {
            auto* chunkTree = TraversalStack_.back();
            TraversalStack_.pop_back();

            processParents(chunkTree, [&] (TChunkTree* parent) {
                result = TryProcessParent(
                    chunk->GetId(),
                    parent,
                    &visitedChunkListCount,
                    visitedChunkListCountLimit,
                    chunkListCountLimitViolationIsPermanentFailure,
                    &foundTable);
                return result == EReincarnationResult::OK;
            });
        }

        if (result != EReincarnationResult::OK) {
            return {result, visitedChunkListCount};
        }

        if (!foundTable) {
            YT_LOG_DEBUG("Chunk is not reachable from any table (ChunkId: %v)",
                chunk->GetId());
            return {EReincarnationResult::NonTableChunk, visitedChunkListCount};
        }

        return {EReincarnationResult::OK, visitedChunkListCount};
    }

    // NB: This function can be called only after successful call of TryPrepareChunkReincarnation().
    void CommitChunkReincarnation(TChunk* oldChunk, TChunk* newChunk)
    {
        YT_VERIFY(TraversalStack_.empty());
        YT_VERIFY(IsObjectAlive(oldChunk));
        YT_VERIFY(IsObjectAlive(newChunk));

        // |TraverseStack_| is reused here to store top sort order.

        auto visitParent = [&] (TChunkList* parent) {
            auto it = NotVisitedChildrenCounts_.find(parent);
            YT_VERIFY(it != NotVisitedChildrenCounts_.end());

            if (--it->second == 0) {
                TraversalStack_.push_back(parent);
                NotVisitedChildrenCounts_.erase(it);
            }
        };

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        struct TChunkParent {
            TChunkList* ChunkList;
            int Cardinality;
        };

        TCompactVector<TChunkParent, TypicalChunkParentCount> chunkParents;
        chunkParents.reserve(oldChunk->Parents().size());
        for (auto [parent, cardinality] : oldChunk->Parents()) {
            YT_VERIFY(parent->GetType() == EObjectType::ChunkList);

            auto* chunkList = parent->AsChunkList();
            // If chunk has non-static ancestor TryPrepare() returns an error.
            YT_VERIFY(chunkList->GetKind() == EChunkListKind::Static);
            chunkParents.push_back({chunkList, cardinality});
        }

        // NB: We cannot call ReplaceChunkListChild while iterating over chunk->Parents().
        for (auto [chunkList, cardinality] : chunkParents) {
            for (int i = 0; i < std::ssize(chunkList->Children()) && cardinality > 0; ++i) {
                if (chunkList->Children()[i] == oldChunk) {
                    chunkManager->ReplaceChunkListChild(chunkList, i, newChunk);
                    --cardinality;
                }
            }

            visitParent(chunkList);
        }

        while (!TraversalStack_.empty()) {
            auto* chunkList = TraversalStack_.back()->AsChunkList();
            TraversalStack_.pop_back();

            RecomputeChunkListStatistics(chunkList);
            for (auto* parent : chunkList->Parents()) {
                visitParent(parent);
            }
        }
    }

private:
    TBootstrap* const Bootstrap_;
    THashMap<TChunkList*, i64> NotVisitedChildrenCounts_;
    std::vector<TChunkTree*> TraversalStack_;

    EReincarnationResult TryProcessParent(
        TChunkId chunkId,
        TChunkTree* parentChunkTree,
        int* visitedChunkListCount,
        int visitedChunkListCountLimit,
        bool firstChunkInBatch,
        bool* foundTable)
    {
        if (parentChunkTree->GetType() != EObjectType::ChunkList ||
            parentChunkTree->AsChunkList()->GetKind() != EChunkListKind::Static)
        {
            return EReincarnationResult::DynamicTableChunk;
        }

        auto* parent = parentChunkTree->AsChunkList();

        if (auto it = NotVisitedChildrenCounts_.find(parent);
            it != NotVisitedChildrenCounts_.end())
        {
            ++it->second;
            return EReincarnationResult::OK;
        }

        if (++*visitedChunkListCount > visitedChunkListCountLimit) {
            if (firstChunkInBatch) {
                YT_LOG_DEBUG("Chunk has too many ancestors (ChunkId: %v, VisitedChunkListCount: %v)",
                    chunkId,
                    *visitedChunkListCount);
                return EReincarnationResult::TooManyAncestors;
            }

            return EReincarnationResult::Transient;
        }

        for (auto owners : {parent->TrunkOwningNodes(), parent->BranchedOwningNodes()}) {
            for (auto* owner : owners) {
                if (owner->GetType() == EObjectType::Table &&
                    owner->As<TTableNode>()->IsDynamic())
                {
                    return EReincarnationResult::DynamicTableChunk;
                }

                if (owner->GetType() == EObjectType::Table) {
                    *foundTable = true;
                }
            }
        }

        EmplaceOrCrash(NotVisitedChildrenCounts_, parent, 1);
        TraversalStack_.push_back(parent);

        return EReincarnationResult::OK;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TReincarnationJob::TReincarnationJob(
    TJobId jobId,
    TChunk* oldChunk,
    TJobEpoch jobEpoch,
    TChunkIdWithIndexes newChunkIdWithIndexes,
    TNode* node,
    TNodePtrWithReplicaIndexList sourceReplicas,
    TNodePtrWithReplicaAndMediumIndexList targetReplicas,
    int mediumIndex)
    : TJob(
        jobId,
        EJobType::ReincarnateChunk,
        jobEpoch,
        node,
        GetJobResourceUsage(),
        newChunkIdWithIndexes)
    , OldChunkId_(oldChunk->GetId())
    , NewChunkId_(newChunkIdWithIndexes.Id)
    , SourceReplicas_(std::move(sourceReplicas))
    , TargetReplicas_(std::move(targetReplicas))
    , ErasureCodec_(NErasure::ECodec::None)
    , CompressionCodec_(NCompression::ECodec::None)
    , EnableSkynetSharing_(false)
    , MediumIndex_(mediumIndex)
{
    auto miscExt = oldChunk->ChunkMeta()->FindExtension<TMiscExt>();

    if (miscExt) {
        ErasureCodec_ = CheckedEnumCast<NErasure::ECodec>(miscExt->erasure_codec());
        CompressionCodec_ = CheckedEnumCast<NCompression::ECodec>(miscExt->compression_codec());
        EnableSkynetSharing_ = miscExt->shared_to_skynet();
    }
}

bool TReincarnationJob::FillJobSpec(TBootstrap* bootstrap, TJobSpec* jobSpec) const
{
    auto* jobSpecExt = jobSpec->MutableExtension(
        TReincarnateChunkJobSpecExt::reincarnate_chunk_job_spec_ext);

    ToProto(jobSpecExt->mutable_old_chunk_id(), OldChunkId_);
    jobSpecExt->set_erasure_codec(ToProto<int>(ErasureCodec_));
    jobSpecExt->set_compression_codec(ToProto<int>(CompressionCodec_));
    ToProto(jobSpecExt->mutable_new_chunk_id(), NewChunkId_);

    for (auto replica : TargetReplicas_) {
        jobSpecExt->add_target_replicas(ToProto<ui64>(replica));
    }

    NNodeTrackerServer::TNodeDirectoryBuilder builder(jobSpecExt->mutable_node_directory());
    builder.Add(TargetReplicas_);

    const auto& chunkManager = bootstrap->GetChunkManager();
    if (auto* oldChunk = chunkManager->FindChunk(OldChunkId_)) {
        builder.Add(oldChunk->StoredReplicas());
    }

    jobSpecExt->set_medium_index(MediumIndex_);
    jobSpecExt->set_enable_skynet_sharing(EnableSkynetSharing_);
    ToProto(jobSpecExt->mutable_source_replicas(), SourceReplicas_);

    return true;
}


TNodeResources TReincarnationJob::GetJobResourceUsage()
{
    TNodeResources resourceUsage;
    resourceUsage.set_reincarnation_slots(1);
    return resourceUsage;
}

////////////////////////////////////////////////////////////////////////////////

class TChunkReincarnator
    : public IChunkReincarnator
    , public NCellMaster::TMasterAutomatonPart
{
public:
    explicit TChunkReincarnator(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::ChunkReincarnator)
        , ChunkScanner_(bootstrap->GetObjectManager(), EChunkScanKind::Reincarnation, /*journal*/ false)
        , TransactionRotator_(bootstrap, "Chunk reincarnator transaction")
    {
        YT_VERIFY(Bootstrap_);

        RegisterSaver(
            ESyncSerializationPriority::Values,
            "ChunkReincarnator",
            BIND(&TChunkReincarnator::Save, Unretained(this)));
        RegisterLoader(
            "ChunkReincarnator",
            BIND(&TChunkReincarnator::Load, Unretained(this)));

        RegisterMethod(BIND(&TChunkReincarnator::HydraUpdateChunkReincarnatorTransactions, Unretained(this)));
        RegisterMethod(BIND(&TChunkReincarnator::HydraCreateReincarnatedChunks, Unretained(this)));
        RegisterMethod(BIND(&TChunkReincarnator::HydraReincarnateChunks, Unretained(this)));

        for (auto [path, kind] : ReincarnationMetrics) {
            Metrics_[kind] = ChunkServerProfiler.Counter(path);
        }
    }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(BIND(&TChunkReincarnator::OnTransactionFinished, MakeWeak(this)));
        transactionManager->SubscribeTransactionAborted(BIND(&TChunkReincarnator::OnTransactionFinished, MakeWeak(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TChunkReincarnator::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        FailedJobRegistry_.Clear();

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            ChunkScanner_.Start(chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex));
        }

        const auto& config = GetDynamicConfig();

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        const auto& invoker = hydraFacade->GetEpochAutomatonInvoker(
            EAutomatonThreadQueue::ChunkReincarnator);

        auto startExecutor = [&] (TPeriodicExecutorPtr* executor, auto callback, TDuration period) {
            *executor = New<TPeriodicExecutor>(invoker, std::move(callback), period);
            (*executor)->Start();
        };

        startExecutor(
            &ChunkScanExecutor_,
            BIND(&TChunkReincarnator::OnChunkScan, MakeWeak(this)),
            config->ChunkScanPeriod);

        startExecutor(
            &UpdateTransactionsExecutor_,
            BIND(&TChunkReincarnator::UpdateTransactions, MakeWeak(this)),
            config->TransactionUpdatePeriod);

        YT_LOG_DEBUG("Chunk reincarnation scanning started");

        CancelScheduledReincarnationsAndRestartTransaction();

        YT_VERIFY(JobEpoch_ == InvalidJobEpoch);
        const auto& jobRegistry = Bootstrap_->GetChunkManager()->GetJobRegistry();
        JobEpoch_ = jobRegistry->StartEpoch();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        ScheduledJobs_ = {};
        FailedJobRegistry_.Clear();

        auto stopExecutor = [] (auto& executor) {
            if (executor) {
                executor->Stop();
                executor.Reset();
            }
        };

        stopExecutor(ChunkScanExecutor_);
        stopExecutor(UpdateTransactionsExecutor_);

        if (JobEpoch_ != InvalidJobEpoch) {
            const auto& jobRegistry = Bootstrap_->GetChunkManager()->GetJobRegistry();
            jobRegistry->OnEpochFinished(JobEpoch_);
            JobEpoch_ = InvalidJobEpoch;
        }

        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            ChunkScanner_.Stop(shardIndex);
        }
    }

    void OnChunkDestroyed(TChunk* chunk) override
    {
        ChunkScanner_.OnChunkDestroyed(chunk);
    }

private:
    TJobEpoch JobEpoch_ = InvalidJobEpoch;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    struct TJobInfo {
        TJobId JobId;
        TChunkId OldChunkId;
        TChunkId NewChunkId;
        int MediumIndex = 0;
    };
    std::queue<TJobInfo> ScheduledJobs_;

    TChunkScanner ChunkScanner_;
    TPeriodicExecutorPtr ChunkScanExecutor_;

    TPeriodicExecutorPtr UpdateTransactionsExecutor_;

    struct TChunkReplacementInfo
    {
        TChunkId OldChunkId;
        TChunkId NewChunkId;
    };
    std::queue<TChunkReplacementInfo> ChunksToReplace_;

    // Transient.
    TFailedJobRegistry FailedJobRegistry_;

    // Metrics.
    TEnumIndexedVector<EReincarnationResult, NProfiling::TCounter> Metrics_;

    // Persistent field.
    TTransactionRotator TransactionRotator_;

    void OnReincarnationFinished(EReincarnationResult result)
    {
        if (IsLeader()) {
            Metrics_[result].Increment();
            if (IsPermanentFailure(result) && result != EReincarnationResult::GenericPermanentFailure) {
                Metrics_[EReincarnationResult::GenericPermanentFailure].Increment();
            }
        }
    }

    bool IsEnabled() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        return GetDynamicConfig()->Enable;
    }

    void CancelScheduledReincarnationsAndRestartTransaction()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        ScheduledJobs_ = {};
        ChunksToReplace_ = {};
        UpdateChunkReincarnatorTransactions();
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldClusterConfig)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        if (!IsLeader()) {
            return;
        }

        const auto& config = GetDynamicConfig();
        const auto& oldConfig = oldClusterConfig->ChunkManager->ChunkReincarnator;
        auto wasEnabled = oldConfig->Enable;

        if (config->Enable && !wasEnabled) {
            YT_LOG_INFO("Chunk reincarnator is enabled");
        }

        if (!config->Enable && wasEnabled) {
            YT_LOG_INFO("Chunk reincarnator is disabled");
        }

        if (ChunkScanExecutor_) {
            ChunkScanExecutor_->SetPeriod(config->ChunkScanPeriod);
        }
        if (UpdateTransactionsExecutor_) {
            UpdateTransactionsExecutor_->SetPeriod(config->TransactionUpdatePeriod);
        }

        if (config->ShouldRescheduleAfterChange(*oldConfig)) {
            YT_LOG_INFO("Chunk reincarnation global scan restarted");
            CancelScheduledReincarnationsAndRestartTransaction();
            ScheduleGlobalChunkScan();
        }

        if (config->Enable) {
            FailedJobRegistry_.SetMaxFailedJobs(config->MaxFailedJobs);
            FailedJobRegistry_.SetMaxTrackedChunks(config->MaxTrackedChunks);
        }
    }

    void ScheduleGlobalChunkScan()
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            auto descriptor = chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex);
            ChunkScanner_.ScheduleGlobalScan(descriptor);
        }
    }

    const TDynamicChunkReincarnatorConfigPtr& GetDynamicConfig() const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& configManager = Bootstrap_->GetConfigManager();

        return configManager->GetConfig()->ChunkManager->ChunkReincarnator;
    }

    void OnTransactionFinished(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());
        YT_VERIFY(IsObjectAlive(transaction));

        auto transactionId = transaction->GetId();
        auto previousTransactionId = TransactionRotator_.GetPreviousTransactionId();
        auto currentTransactionId = TransactionRotator_.GetTransactionId();

        if (!TransactionRotator_.OnTransactionFinished(transaction)) {
            return;
        }

        YT_LOG_DEBUG(
            "Chunk reincarnator transaction finished "
            "(FinishedTransactionId: %v, ChunkReincarnatorCurrentTransactionId: %v, ChunkReincarnatorPreviousTransactionId: %v)",
            transactionId,
            currentTransactionId,
            previousTransactionId);

        if (IsLeader()) {
            UpdateTransactions();
        }
    }

    void UpdateChunkReincarnatorTransactions()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            NProto::TReqUpdateChunkReincarnatorTransactions())
            ->CommitAndLog(Logger);
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        TransactionRotator_.Clear();
    }

    // Transient.
    void RescheduleReincarnation(TChunk* chunk)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        ChunkScanner_.EnqueueChunk(chunk);
    }

    // Transient.
    void ScheduleChunkReplacement(TChunkId oldChunkId, TChunkId newChunkId)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        auto* oldChunk = chunkManager->FindChunk(oldChunkId);
        if (!IsObjectAlive(oldChunk)) {
            return;
        }

        auto* newChunk = chunkManager->FindChunk(newChunkId);
        if (!IsObjectAlive(newChunk)) {
            RescheduleReincarnation(oldChunk);
            return;
        }

        ChunksToReplace_.push({.OldChunkId = oldChunkId, .NewChunkId = newChunkId});

        ReplaceOldChunks();
    }

    void ReplaceOldChunks(bool allowUnderfilledBatch = false)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        const auto& config = GetDynamicConfig();
        if (!config->Enable) {
            return;
        }

        const int batchSize = config->ReplacedChunkBatchSize;

        auto hasReadyToReplaceChunks = [&] {
            if (allowUnderfilledBatch) {
                return !ChunksToReplace_.empty();
            } else {
                return std::ssize(ChunksToReplace_) >= batchSize;
            }
        };

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();

        while (hasReadyToReplaceChunks()) {
            NProto::TReqReincarnateChunks req;
            for (int i = 0; i < batchSize && !ChunksToReplace_.empty(); ++i) {
                auto replacementInfo = ChunksToReplace_.front();
                ChunksToReplace_.pop();

                auto* subrequest = req.add_subrequests();
                ToProto(subrequest->mutable_old_chunk_id(), replacementInfo.OldChunkId);
                ToProto(subrequest->mutable_new_chunk_id(), replacementInfo.NewChunkId);
            }

            CreateMutation(hydraManager, req)
                ->CommitAndLog(Logger);
        }
    }

    void TryScheduleJob(TJobInfo jobInfo, IJobSchedulingContext* context)
    {
        const auto chunkManager = Bootstrap_->GetChunkManager();
        const auto& requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();

        auto oldChunkId = jobInfo.OldChunkId;
        auto* oldChunk = chunkManager->FindChunk(oldChunkId);
        if (!IsObjectAlive(oldChunk)) {
            return;
        }

        auto newChunkId = jobInfo.NewChunkId;
        auto* newChunk = chunkManager->FindChunk(newChunkId);
        if (!IsObjectAlive(newChunk)) {
            OnReincarnationFinished(EReincarnationResult::Transient);
            RescheduleReincarnation(oldChunk);
            return;
        }

        int mediumIndex = jobInfo.MediumIndex;
        int targetCount = newChunk->GetPhysicalReplicationFactor(mediumIndex, requisitionRegistry);

        auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!medium) {
            YT_LOG_ALERT("Cannot schedule reincarnation job; medium not found (OldChunkId: %v, NewChunkId: %v, MediumIndex: %v)",
                oldChunkId,
                newChunkId,
                mediumIndex);
            OnReincarnationFinished(EReincarnationResult::GenericPermanentFailure);
            return;
        }

        if (medium->IsOffshore()) {
            YT_LOG_ALERT("Cannot schedule reincarnation job; medium is offshore "
                "(OldChunkId: %v, NewChunkId: %v, MediumIndex: %v, MediumName: %v, MediumType: %v)",
                oldChunkId,
                newChunkId,
                mediumIndex,
                medium->GetName(),
                medium->GetType());
            OnReincarnationFinished(EReincarnationResult::GenericPermanentFailure);
            return;
        }

        auto targetNodes = chunkManager->AllocateWriteTargets(
            medium->AsDomestic(),
            newChunk,
            GenericChunkReplicaIndex,
            targetCount,
            targetCount,
            /*replicationFactorOverride*/ std::nullopt);

        if (targetNodes.empty()) {
            OnReincarnationFinished(EReincarnationResult::Transient);
            return;
        }

        TNodePtrWithReplicaIndexList sourceReplicas;
        sourceReplicas.reserve(oldChunk->StoredReplicas().size());
        for (auto replica : oldChunk->StoredReplicas()) {
            sourceReplicas.emplace_back(replica.GetPtr()->GetNode(), replica.GetReplicaIndex());
        }

        TNodePtrWithReplicaAndMediumIndexList targetReplicas;
        int targetIndex = 0;
        for (auto* node : targetNodes) {
            targetReplicas.emplace_back(node,
                newChunk->GetErasureCodec() == NErasure::ECodec::None
                    ? GenericChunkReplicaIndex
                    : targetIndex++,
                mediumIndex);
        }

        auto job = New<TReincarnationJob>(
            jobInfo.JobId,
            oldChunk,
            JobEpoch_,
            TChunkIdWithIndexes(
                newChunkId,
                GenericChunkReplicaIndex,
                mediumIndex),
            context->GetNode(),
            std::move(sourceReplicas),
            std::move(targetReplicas),
            mediumIndex);
        context->ScheduleJob(job);

        YT_LOG_DEBUG("Chunk reincarnation job scheduled (JobId: %v, Address: %v, OldChunkId: %v, NewChunkId: %v)",
            job->GetJobId(),
            context->GetNode()->GetDefaultAddress(),
            oldChunkId,
            newChunkId);
    }

    // IJobController implementation.
    void ScheduleJobs(EJobType jobType, IJobSchedulingContext* context) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsMasterJobType(jobType));

        if (jobType != EJobType::ReincarnateChunk) {
            return;
        }

        if (!IsLeader()) {
            return;
        }

        if (!IsEnabled()) {
            return;
        }

        const auto& resourceUsage = context->GetNodeResourceUsage();
        const auto& resourceLimits = context->GetNodeResourceLimits();

        const auto& config = GetDynamicConfig();

        auto hasSpareResources = [&] {
            return resourceUsage.reincarnation_slots() < resourceLimits.reincarnation_slots() &&
                context->GetJobRegistry()->GetJobCount(EJobType::ReincarnateChunk) < config->MaxRunningJobCount;
        };

        while (!ScheduledJobs_.empty() && hasSpareResources()) {
            TryScheduleJob(std::move(ScheduledJobs_.front()), context);
            ScheduledJobs_.pop();
        }
    }

    void OnJobWaiting(const TReincarnationJobPtr& job, IJobControllerCallbacks* callbacks) override
    {
        OnJobRunning(job, callbacks);
    }

    void OnJobRunning(const TReincarnationJobPtr& job, IJobControllerCallbacks* callbacks) override
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        auto jobTimeout = configManager->GetConfig()->ChunkManager->JobTimeout;
        auto duration = TInstant::Now() - job->GetStartTime();
        if (duration > jobTimeout) {
            YT_LOG_WARNING(
                "Job timed out, aborting and rescheduling chunk reincarnation job "
                "(JobId: %v, JobType: %v, Address: %v, Duration: %v, ChunkId: %v)",
                job->GetJobId(),
                job->GetType(),
                job->NodeAddress(),
                duration,
                job->OldChunkId());

            callbacks->AbortJob(job);
        }
    }

    // Transient failure.
    void OnJobAborted(const TReincarnationJobPtr& job) override
    {
        if (!IsLeader()) {
            return;
        }

        YT_LOG_DEBUG("Reincarnation job failed (JobId: %v, OldChunkId: %v, NewChunkId: %v)",
            job->GetJobId(),
            job->OldChunkId(),
            job->NewChunkId());

        // We don't have to remove new chunks here. Unused new chunks will be removed during transaction rotation.

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto* chunk = chunkManager->FindChunk(job->OldChunkId());
        if (!IsObjectAlive(chunk)) {
            return;
        }

        auto result = FailedJobRegistry_.OnJobFailed(job);
        OnReincarnationFinished(result);

        if (result == EReincarnationResult::Transient) {
            RescheduleReincarnation(chunk);
        } else {
            YT_VERIFY(result == EReincarnationResult::TooManyFailedJobs);
            YT_LOG_DEBUG("Chunk reincarnation failed due too many failed jobs (ChunkId: %v)", job->OldChunkId());
        }
    }

    void OnJobFailed(const TReincarnationJobPtr& job) override
    {
        OnJobAborted(job);
    }

    void OnJobCompleted(const TReincarnationJobPtr& job) override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        if (!IsLeader()) {
            return;
        }

        YT_LOG_DEBUG("Reincarnation job completed (JobId: %v, OldChunkId: %v, NewChunkId: %v)",
            job->GetJobId(),
            job->OldChunkId(),
            job->NewChunkId());

        if (IsEnabled()) {
            ScheduleChunkReplacement(job->OldChunkId(), job->NewChunkId());
        }
    }

    void UpdateTransactions()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        NProto::TReqUpdateChunkReincarnatorTransactions request;

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();

        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
    }

    void OnChunkScan()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (!IsEnabled()) {
            return;
        }

        if (!ChunkScanner_.HasUnscannedChunk()) {
            YT_LOG_DEBUG("Chunk reincarnator has nothing to scan");
            // Ensure that last chunks will be replaced even if their count is less than batch size.
            ReplaceOldChunks(/*allowUnderfilledBatch*/ true);
            return;
        }

        const auto& config = GetDynamicConfig();
        const auto& epochHistoryManager = Bootstrap_->GetEpochHistoryManager();

        int chunkLimit = config->MaxChunksPerScan;
        int visitedChunkListLimit = config->MaxVisitedChunkListsPerScan;
        bool firstChunkInBatch = true;

        YT_LOG_DEBUG("Chunk reincarnator iteration started (MinAllowedCreationTime: %v, MaxVisitedChunkListPerScanCount: %v)",
            config->MinAllowedCreationTime,
            visitedChunkListLimit);

        TChunkAncestorTraverser traverser(Bootstrap_);

        std::vector<TChunkId> chunksToReincarnate;
        chunksToReincarnate.reserve(chunkLimit);

        while (visitedChunkListLimit > 0 &&
            chunkLimit-- > 0 &&
            ChunkScanner_.HasUnscannedChunk())
        {
            auto* chunk = ChunkScanner_.DequeueChunk();

            if (!IsObjectAlive(chunk)) {
                continue;
            }

            if (!chunk->IsConfirmed()) {
                continue;
            }

            auto estimatedCreationTime = epochHistoryManager->GetEstimatedCreationTime(
                chunk->GetId(),
                NProfiling::GetInstant()).second;

            if (estimatedCreationTime > config->MinAllowedCreationTime) {
                continue;
            }

            auto shouldReincarnate = [&] (TChunk* chunk) {
                if (chunk->IsForeign()) {
                    return false;
                }

                switch (chunk->GetChunkFormat()) {
                    case EChunkFormat::TableUnversionedSchemaful:
                    case EChunkFormat::TableUnversionedSchemalessHorizontal:
                    case EChunkFormat::TableUnversionedColumnar:
                    // Some old table chunks can have this format. Such chunks should be reincarnated.
                    case EChunkFormat::FileDefault:
                        break;
                    default:
                        OnReincarnationFinished(EReincarnationResult::WrongChunkFormat);
                        return false;
                }

                if (chunk->IsExported()) {
                    OnReincarnationFinished(EReincarnationResult::Teleportations);
                    return false;
                }

                return true;
            };

            if (!shouldReincarnate(chunk)) {
                continue;
            }

            auto [result, currentVisitedChunkListCount] = traverser.TryPrepareChunkReincarnation(
                chunk,
                visitedChunkListLimit,
                /*chunkListCountLimitViolationIsPermanentFailure*/ firstChunkInBatch);
            firstChunkInBatch = false;
            visitedChunkListLimit -= currentVisitedChunkListCount;

            switch (result) {
                case EReincarnationResult::OK:
                    YT_LOG_TRACE("Scheduling new chunk creation (OldChunkId: %v)",
                        chunk->GetId());
                    chunksToReincarnate.push_back(chunk->GetId());
                    break;
                case EReincarnationResult::Transient:
                    RescheduleReincarnation(chunk);
                    [[fallthrough]];
                default:
                    OnReincarnationFinished(result);
            }
        }

        if (!chunksToReincarnate.empty()) {
            NProto::TReqCreateReincarnatedChunks request;
            for (auto chunkId : chunksToReincarnate) {
                ToProto(request.add_subrequests()->mutable_old_chunk_id(), chunkId);
            }
            CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
                ->CommitAndLog(Logger);
        }

        YT_LOG_DEBUG("Chunk reincarnation scanner finished (ScheduledChunkIdsCount: %v, ScheduledChunkIds: %v)",
            chunksToReincarnate.size(),
            chunksToReincarnate);
    }

    void HydraUpdateChunkReincarnatorTransactions(NProto::TReqUpdateChunkReincarnatorTransactions* /*request*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());

        TransactionRotator_.Rotate();

        YT_LOG_INFO(
            "Chunk reincarnator transactions updated "
            "(TransactionId: %v, PreviousTransactionId: %v)",
            TransactionRotator_.GetTransactionId(),
            TransactionRotator_.GetPreviousTransactionId());
    }

    void HydraCreateReincarnatedChunks(NProto::TReqCreateReincarnatedChunks* request)
    {
        if (!IsObjectAlive(TransactionRotator_.GetTransaction())) {
            YT_LOG_DEBUG(
                "Chunk reincarnation transaction is not alive (TransactionId: %v)",
                TransactionRotator_.GetTransactionId());

            if (IsLeader()) {
                const auto& chunkManager = Bootstrap_->GetChunkManager();

                for (const auto& subrequest : request->subrequests()) {
                    auto oldChunkId = FromProto<TChunkId>(subrequest.old_chunk_id());
                    auto* oldChunk = chunkManager->FindChunk(oldChunkId);

                    if (!IsObjectAlive(oldChunk)) {
                        continue;
                    }

                    RescheduleReincarnation(oldChunk);
                }
            }

            return;
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();

        struct TReincarnatedChunkInfo
        {
            TChunkId OldChunkId;
            TChunkId NewChunkId;
            int MediumIndex;
        };
        std::vector<TReincarnatedChunkInfo> createdChunks;
        createdChunks.reserve(request->subrequests().size());

        for (const auto& subrequest : request->subrequests()) {
            auto oldChunkId = FromProto<TChunkId>(subrequest.old_chunk_id());
            auto* oldChunk = chunkManager->FindChunk(oldChunkId);

            if (!IsObjectAlive(oldChunk)) {
                continue;
            }

            auto requisitionIndex = oldChunk->GetLocalRequisitionIndex();
            if (requisitionIndex == EmptyChunkRequisitionIndex) {
                // We don't want to reincarnate unused chunks, do we?
                continue;
            }

            if (oldChunk->IsExported()) {
                YT_LOG_DEBUG(
                    "Chunk was teleported after reincarnated chunks creation has been scheduled (ChunkId: %v)",
                    oldChunkId);
                OnReincarnationFinished(EReincarnationResult::Teleportations);
                continue;
            }

            const auto& requisition = requisitionRegistry->GetRequisition(requisitionIndex);
            YT_VERIFY(requisition.GetEntryCount() > 0);
            const auto& requisitionEntry = *requisition.begin();
            auto mediumIndex = requisitionEntry.MediumIndex;
            const auto& account = requisitionEntry.Account;
            auto replicationFactor = requisitionEntry.ReplicationPolicy.GetReplicationFactor();
            auto vital = requisition.GetVital();

            auto* newChunk = chunkManager->CreateChunk(
                TransactionRotator_.GetTransaction(),
                /*chunkList*/ nullptr,
                oldChunk->GetType(),
                account,
                replicationFactor,
                oldChunk->GetErasureCodec(),
                chunkManager->GetMediumByIndexOrThrow(mediumIndex),
                /*readQuorum*/ 0,
                /*writeQuorum*/ 0,
                /*movable*/ true,
                vital);

            newChunk->SetLocalRequisitionIndex(requisitionIndex, requisitionRegistry, Bootstrap_->GetObjectManager());

            createdChunks.push_back({oldChunkId, newChunk->GetId(), mediumIndex});
        }

        if (IsLeader()) {
            for (const auto& [oldChunkId, newChunkId, mediumIndex] : createdChunks) {
                ScheduledJobs_.push({
                    .JobId = chunkManager->GenerateJobId(),
                    .OldChunkId = oldChunkId,
                    .NewChunkId = newChunkId,
                    .MediumIndex = mediumIndex,
                });
            }
        }
    }

    void HydraReincarnateChunks(NProto::TReqReincarnateChunks* request)
    {
        const auto& config = GetDynamicConfig();
        if (!config->Enable) {
            return;
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        TChunkAncestorTraverser traverser(Bootstrap_);

        auto rescheduleChunkIfNeeded = [&] (TChunk* chunk) {
            if (IsLeader()) {
                YT_LOG_DEBUG(
                    "Rescheduling chunk reincarnation (ChunkId: %v)",
                    chunk->GetId());
                RescheduleReincarnation(chunk);
            }
        };

        int visitedChunkListLimit = config->MaxVisitedChunkListsPerScan;

        bool firstChunkInBatch = true;
        for (const auto& subrequest : request->subrequests()) {
            auto oldChunkId = FromProto<TChunkId>(subrequest.old_chunk_id());
            auto* oldChunk = chunkManager->FindChunk(oldChunkId);
            if (!IsObjectAlive(oldChunk)) {
                YT_LOG_DEBUG(
                    "Chunk reincarnation skipped because old chunk is not alive (ChunkId: %v)",
                    oldChunkId);
                continue;
            }

            if (oldChunk->IsExported()) {
                OnReincarnationFinished(EReincarnationResult::Teleportations);
                YT_LOG_DEBUG(
                    "Chunk was teleported after reincarnation had been scheduled; chunk reincarnation skipped (ChunkId: %v)",
                    oldChunkId);
                continue;
            }

            auto newChunkId = FromProto<TChunkId>(subrequest.new_chunk_id());
            auto* newChunk = chunkManager->FindChunk(newChunkId);
            if (!IsObjectAlive(newChunk)) {
                OnReincarnationFinished(EReincarnationResult::Transient);
                YT_LOG_DEBUG(
                    "New chunk is not alive; chunk reincarnation skipped (OldChunkId: %v, NewChunkId: %v)",
                    oldChunk->GetId(),
                    newChunkId);
                rescheduleChunkIfNeeded(oldChunk);
                continue;
            }


            if (!newChunk->IsConfirmed()) {
                OnReincarnationFinished(EReincarnationResult::Transient);
                YT_LOG_DEBUG(
                    "New chunk is not confirmed; chunk reincarnation skipped (OldChunkId: %v, NewChunkId: %v)",
                    oldChunk->GetId(),
                    newChunkId);
                rescheduleChunkIfNeeded(oldChunk);
                continue;
            }

            if (newChunk->GetChunkFormat() == EChunkFormat::FileDefault) {
                YT_LOG_ALERT(
                    "Reincarnated chunk has weird format (NewChunkId: %v, Format: %v)",
                    newChunk->GetId(),
                    newChunk->GetChunkFormat());
            }

            // Check chunk ancestors again because they could be changed after
            // chunk reincarnation was scheduled.
            auto [result, visitedChunkListCount] = traverser.TryPrepareChunkReincarnation(
                oldChunk,
                visitedChunkListLimit,
                /*chunkListCountLimitViolationIsPermanentFailure*/ firstChunkInBatch);
            visitedChunkListLimit -= visitedChunkListCount;
            firstChunkInBatch = false;

            OnReincarnationFinished(result);

            switch (result) {
                case EReincarnationResult::OK:
                    traverser.CommitChunkReincarnation(oldChunk, newChunk);

                    YT_LOG_DEBUG("Chunk was successfully reincarnated (OldChunkId: %v, NewChunkId: %v)",
                        oldChunk->GetId(),
                        newChunk->GetId());
                    break;
                case EReincarnationResult::Transient:
                    rescheduleChunkIfNeeded(oldChunk);
                    break;
                default:;
            }
        }
    }

    void Save(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, TransactionRotator_);
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;

        // COMPAT(kvk1920)
        if (context.GetVersion() >= EMasterReign::ChunkReincarnator) {
            Load(context, TransactionRotator_);
        } else {
            TransactionRotator_.Clear();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////


IChunkReincarnatorPtr CreateChunkReincarnator(NCellMaster::TBootstrap* bootstrap)
{
    return New<TChunkReincarnator>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

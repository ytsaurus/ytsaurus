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

#include <yt/yt/server/master/table_server/helpers.h>

#include <yt/yt/server/master/transaction_server/transaction_manager.h>
#include <yt/yt/server/master/transaction_server/transaction_rotator.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/transaction_client/proto/transaction_service.pb.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NChunkServer {

using namespace NCellMaster;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NHydra;
using namespace NNodeTrackerServer;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NTableServer;
using namespace NTransactionServer;
using namespace NTransactionSupervisor;
using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger.WithTag("ChunkReincarnator");
inline constexpr int SampleChunkIdCount = 10;

////////////////////////////////////////////////////////////////////////////////

void FromProto(
    TChunkReincarnationOptions* options,
    const NProto::TChunkReincarnationOptions& protoOptions)
{
    *options = {
        .IgnoreCreationTime = protoOptions.ignore_creation_time(),
        .IgnoreAccountSettings = protoOptions.ignore_account_settings(),
    };
}

void ToProto(
    NProto::TChunkReincarnationOptions* protoOptions,
    const TChunkReincarnationOptions& options)
{
    protoOptions->set_ignore_creation_time(options.IgnoreCreationTime);
    protoOptions->set_ignore_account_settings(options.IgnoreAccountSettings);
}

namespace {

class TSerializableChunkReincarnationOptions
    : public TYsonStruct
    , public TChunkReincarnationOptions
{
    REGISTER_YSON_STRUCT(TSerializableChunkReincarnationOptions);

    static void Register(TRegistrar registrar)
    {
        registrar.BaseClassParameter("ignore_creation_time", &TThis::IgnoreCreationTime)
            .Default(false);
        registrar.BaseClassParameter("ignore_account_settings", &TThis::IgnoreAccountSettings)
            .Default(false);
    }
};

} // namespace

void Deserialize(TChunkReincarnationOptions& options, INodePtr node)
{
    auto serializableOptions = New<TSerializableChunkReincarnationOptions>();
    serializableOptions->Load(std::move(node));
    options = *serializableOptions;
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EReincarnationResult,
    ((OK)                      (0))
    ((Transient)               (1))
    // Permanent failures.
    ((TooManyAncestors)        (2))
    ((DynamicTableChunk)       (3))
    ((NoTableAncestors)        (5))
    ((GenericPermanentFailure) (6))
    ((WrongChunkFormat)        (7))
    ((TooManyFailedJobs)       (8))
    // Exported chunk reincarnation check.
    ((NoSuchChunk)             (9))
    ((InvalidTransaction)     (10))
    ((ReincarnationDisabled)  (11))
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
    {"/chunk_reincarnator/permanent_failures/no_table_ancestors", EReincarnationResult::NoTableAncestors},
    {"/chunk_reincarnator/permanent_failures/wrong_chunk_format", EReincarnationResult::WrongChunkFormat},
    {"/chunk_reincarnator/permanent_failures/too_many_failed_jobs", EReincarnationResult::TooManyFailedJobs},
    {"/chunk_reincarnator/permanent_failures/invalid_transaction", EReincarnationResult::InvalidTransaction},
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
        YT_LOG_DEBUG(
            "Reincarnation job failed "
            "(JobId: %v, OldChunkId: %v, NewChunkId: %v)",
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

struct TEmpty
{
    void Load(const auto&)
    { }

    void Save(const auto&) const
    { }
};

template <int Size>
using TCellTagSet = TCompactFlatMap<TCellTag, TEmpty, Size>;

////////////////////////////////////////////////////////////////////////////////

class TChunkAncestorTraverser
{
private:
    struct TTraversalState
    {
        int AncestorVisitBudget;
        bool TableFound = false;
    };

public:
    explicit TChunkAncestorTraverser(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    {
        YT_VERIFY(Bootstrap_);
    }

    //! Returns OK if chunk is suitable for reincarnation.
    // NB: This function must be called before CommitChunkReincarnation().
    EReincarnationResult TryPrepareChunkReincarnation(
        TChunk* chunk,
        int maxVisitedChunkAncestorCount)
    {
        YT_VERIFY(IsObjectAlive(chunk));
        ChunkId_ = chunk->GetId();

        NotVisitedChildrenCounts_.clear();
        TraversalStack_.clear();

        TTraversalState traversalState{maxVisitedChunkAncestorCount};
        TraversalStack_.push_back(chunk);

        while (!TraversalStack_.empty()) {
            auto* chunkTree = TraversalStack_.back();
            TraversalStack_.pop_back();

            switch (chunkTree->GetType()) {
                case EObjectType::Chunk: [[fallthrough]];
                case EObjectType::ErasureChunk:
                    for (auto [parent, refCounter] : chunk->Parents()) {
                        auto result = VisitChunkAncestorOnPrepare(
                            parent,
                            &traversalState);
                        if (result != EReincarnationResult::OK) {
                            return result;
                        }
                    }
                    break;

                case EObjectType::ChunkList:
                case EObjectType::ChunkView: {
                    auto parents = chunkTree->GetType() == EObjectType::ChunkList
                        ? chunkTree->AsChunkList()->Parents()
                        : TRange(chunkTree->AsChunkView()->Parents());

                    for (auto* parent : parents) {
                        auto result = VisitChunkAncestorOnPrepare(
                            parent,
                            &traversalState);
                        if (result != EReincarnationResult::OK) {
                            return result;
                        }
                    }
                    break;
                }
                default:
                    // Other chunk tree types are handled in |VisitChunkAncestorOnPrepare|.
                    YT_LOG_FATAL("Unexpected chunk tree type (Type: %v)",
                        chunkTree->GetType());
            }
        }

        if (!traversalState.TableFound) {
            // NB: This case is rare enough to log it.
            YT_LOG_DEBUG("Chunk is not reachable from any table (ChunkId: %v)",
                chunk->GetId());
            return EReincarnationResult::NoTableAncestors;
        }

        return EReincarnationResult::OK;
    }

    // NB: This function can be called only after
    // |TryPrepareChunkReincarnation| was successfully called.
    void CommitChunkReincarnation(TChunk* oldChunk, TChunk* newChunk)
    {
        YT_VERIFY(TraversalStack_.empty());
        YT_VERIFY(IsObjectAlive(oldChunk));
        YT_VERIFY(IsObjectAlive(newChunk));
        YT_VERIFY(oldChunk->GetId() == ChunkId_);

        // NB: |TraverseStack_| is reused here to store top sort order.

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        struct TChunkParent {
            TChunkList* ChunkList;
            int Cardinality;
        };

        TCompactVector<TChunkParent, TypicalChunkParentCount> chunkParents;
        chunkParents.reserve(oldChunk->Parents().size());

        for (auto [parent, cardinality] : oldChunk->Parents()) {
            YT_VERIFY(
                parent->GetType() == EObjectType::ChunkList ||
                parent->GetType() == EObjectType::ChunkView);

            chunkParents.push_back({parent->AsChunkList(), cardinality});
        }

        // NB: We cannot call ReplaceChunkListChild while iterating over
        // |chunk->Parents()|.
        for (auto [chunkList, cardinality] : chunkParents) {
            for (
                int i = 0;
                i < std::ssize(chunkList->Children()) && cardinality > 0;
                ++i)
            {
                if (chunkList->Children()[i] == oldChunk) {
                    chunkManager->ReplaceChunkListChild(chunkList, i, newChunk);
                    --cardinality;
                }
            }

            VisitChunkAncestorOnCommit(chunkList);
        }

        while (!TraversalStack_.empty()) {
            auto* chunkTree = TraversalStack_.back();
            TraversalStack_.pop_back();

            TRange<TChunkList*> parents;

            if (chunkTree->GetType() == EObjectType::ChunkList) {
                auto* chunkList = chunkTree->AsChunkList();
                RecomputeChunkListStatistics(chunkList);
                parents = chunkList->Parents();

                for (auto owners : {chunkList->TrunkOwningNodes(), chunkList->BranchedOwningNodes()}) {
                    for (auto* owner : owners) {
                        if (owner->GetType() == EObjectType::Table) {
                            RecomputeTabletStatistics(owner->As<TTableNode>());
                        }
                    }
                }
            } else {
                parents = chunkTree->AsChunkView()->Parents();
            }

            for (auto* parent : parents) {
                VisitChunkAncestorOnCommit(parent);
            }
        }
    }

private:
    TBootstrap* const Bootstrap_;
    // Key: Either TChunkView or TChunkList.
    THashMap<TChunkTree*, i64> NotVisitedChildrenCounts_;
    std::vector<TChunkTree*> TraversalStack_;
    TChunkId ChunkId_ = NullChunkId;

    EReincarnationResult VisitChunkAncestorOnPrepare(
        TChunkTree* parent,
        TTraversalState* traversalState)
    {
        if (auto it = NotVisitedChildrenCounts_.find(parent);
            it != NotVisitedChildrenCounts_.end())
        {
            ++it->second;
            return EReincarnationResult::OK;
        }

        auto decrementAncestorVisitBudgetAndWarning = [&] {
            if (--traversalState->AncestorVisitBudget == 0) {
                YT_LOG_WARNING(
                    "Cannot reincarnate chunk: too many ancestors "
                    "(ChunkId: %v)",
                    ChunkId_);
                return true;
            }
            return false;
        };

        // Take into account current chunk ancestor.
        if (decrementAncestorVisitBudgetAndWarning()) {
            return EReincarnationResult::TooManyAncestors;
        }

        if (parent->GetType() == EObjectType::ChunkList) {
            auto* chunkList = parent->AsChunkList();
            for (auto owners : {chunkList->TrunkOwningNodes(), chunkList->BranchedOwningNodes()})
            {
                for (auto* owner : owners) {
                    if (decrementAncestorVisitBudgetAndWarning()) {
                        return EReincarnationResult::TooManyAncestors;
                    }

                    if (owner->GetType() == EObjectType::Table) {
                        auto* table = owner->As<TTableNode>();
                        if (table->IsDynamic() &&
                            table->GetTabletState() != NTabletClient::ETabletState::Unmounted)
                        {
                            return EReincarnationResult::DynamicTableChunk;
                        }

                        traversalState->TableFound = true;
                    }
                }
            }
        } else if (parent->GetType() != EObjectType::ChunkView) {
            YT_LOG_FATAL("Unexpected chunk ancestor type (Type: %v, ChunkId: %v)",
                parent->GetType(),
                ChunkId_);
            return EReincarnationResult::DynamicTableChunk;
        }

        EmplaceOrCrash(NotVisitedChildrenCounts_, parent, 1);
        TraversalStack_.push_back(parent);

        return EReincarnationResult::OK;
    }

    void VisitChunkAncestorOnCommit(TChunkList* chunkList)
    {
        auto it = NotVisitedChildrenCounts_.find(chunkList);
        YT_VERIFY(it != NotVisitedChildrenCounts_.end());

        if (--it->second == 0) {
            TraversalStack_.push_back(chunkList);
            NotVisitedChildrenCounts_.erase(it);
        }
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
    TNodePtrWithReplicaAndMediumIndexList sourceReplicas,
    TNodePtrWithReplicaAndMediumIndexList targetReplicas,
    int mediumIndex,
    TChunkReincarnationOptions reincarnationOptions)
    : TJob(
        jobId,
        EJobType::ReincarnateChunk,
        jobEpoch,
        node,
        GetJobResourceUsage(),
        newChunkIdWithIndexes)
    , OldChunkId_(oldChunk->GetId())
    , NewChunkId_(newChunkIdWithIndexes.Id)
    , ReincarnationOptions_(reincarnationOptions)
    , SourceReplicas_(std::move(sourceReplicas))
    , TargetReplicas_(std::move(targetReplicas))
    , MediumIndex_(mediumIndex)
    , ErasureCodec_(NErasure::ECodec::None)
    , CompressionCodec_(NCompression::ECodec::None)
    , EnableSkynetSharing_(false)
{
    if (auto miscExt = oldChunk->ChunkMeta()->FindExtension<TMiscExt>()) {
        ErasureCodec_ = CheckedEnumCast<NErasure::ECodec>(miscExt->erasure_codec());
        CompressionCodec_ = CheckedEnumCast<NCompression::ECodec>(miscExt->compression_codec());
        EnableSkynetSharing_ = miscExt->shared_to_skynet();
    }
}

bool TReincarnationJob::FillJobSpec(
    TBootstrap* bootstrap,
    NProto::TJobSpec* jobSpec) const
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

    NNodeTrackerServer::TNodeDirectoryBuilder builder(
        jobSpecExt->mutable_node_directory());
    builder.Add(TargetReplicas_);

    const auto& chunkManager = bootstrap->GetChunkManager();
    if (auto* oldChunk = chunkManager->FindChunk(OldChunkId_)) {
        auto ephemeralChunk = TEphemeralObjectPtr<TChunk>(oldChunk);
        auto replicasOrError = chunkManager->GetChunkReplicas(ephemeralChunk);
        if (!replicasOrError.IsOK() || !IsObjectAlive(ephemeralChunk)) {
            return false;
        }
        const auto& replicas = replicasOrError.Value();
        builder.Add(replicas);
    }

    jobSpecExt->set_medium_index(MediumIndex_);
    jobSpecExt->set_enable_skynet_sharing(EnableSkynetSharing_);
    ToProto(jobSpecExt->mutable_source_replicas(), SourceReplicas_);
    ToProto(jobSpecExt->mutable_legacy_source_replicas(), SourceReplicas_);

    return true;
}

TNodeResources TReincarnationJob::GetJobResourceUsage()
{
    TNodeResources resourceUsage;
    resourceUsage.set_reincarnation_slots(1);
    return resourceUsage;
}

////////////////////////////////////////////////////////////////////////////////

// Typical chunk reincarnation path:
// 1. Chunk is scanned by periodic scan (see |OnChunkScan|). If chunk is not old
//    enough to be reincarnated then it is just skipped. Chunk's ancestors are
//    traversed to check if chunk can be reincarnated (e.g. chunk does not
//    belong to any mounted dynamic table). If chunk is not exported then chunk
//    reincarnation mutation is scheduled (step 4).
// 2. For exported chunks reincarnation check requests are
//    sent to all cells the chunk is exported to via Hive (see
//    |HydraCheckExportedChunkReincarnation|). Such checks are tracked on
//    chunk's native cell by |TExportedChunkReincarnationCheckRegistry|.
// 3. Every chunk's foreign cell sends reincarnation check result to native
//    cell (see |HydraCheckForeignChunkReincarnation|). When reincarnation check
//    is performed on every chunk's foreign cell, reincarnation of this chunk is
//    scheduled (see |HydraOnExportedChunkReincarnationCheckFinished|).
// 4. For every chunk to reincarnate new chunk object is created and
//    reincarnation job is scheduled.
// 5. After chunk is physically reincarnated old chunks are replaced by new
//    ones. If there are exported origin chunks:
// 5.1. Transaction is started.
// 5.2. Reincarnated chunks are exported.
// 5.3. Foreign chunk reincarnation request is sent to foreign cells.
// 5.4. Transaction is committed.
class TChunkReincarnator
    : public IChunkReincarnator
    , public NCellMaster::TMasterAutomatonPart
{
public:
    explicit TChunkReincarnator(TBootstrap* bootstrap)
        : TMasterAutomatonPart(
            bootstrap,
            EAutomatonThreadQueue::ChunkReincarnator)
        , ChunkScanner_(
            EChunkScanKind::Reincarnation,
            /*journal*/ false)
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

        RegisterMethod(
            BIND(&TChunkReincarnator::HydraCheckExportedChunkReincarnation, Unretained(this)));
        RegisterMethod(
            BIND(&TChunkReincarnator::HydraCheckForeignChunkReincarnation, Unretained(this)));
        RegisterMethod(
            BIND(&TChunkReincarnator::HydraOnExportedChunkReincarnationCheckFinished, Unretained(this)));
        RegisterMethod(
            BIND(&TChunkReincarnator::HydraUpdateChunkReincarnatorTransactions, Unretained(this)));
        RegisterMethod(
            BIND(&TChunkReincarnator::HydraCreateReincarnatedChunks, Unretained(this)));
        RegisterMethod(
            BIND(&TChunkReincarnator::HydraReincarnateChunks, Unretained(this)));
        RegisterMethod(
            BIND(&TChunkReincarnator::HydraReincarnateForeignChunks, Unretained(this)));

        for (auto [path, kind] : ReincarnationMetrics) {
            Metrics_[kind] = ChunkServerProfiler.Counter(path);
        }

        TeleportedReincarnationCounter_ = ChunkServerProfiler
            .WithGlobal()
            .WithTag("cell_tag", ToString(Bootstrap_->GetCellTag()))
            .Counter("/chunk_reincarnator/teleported_reincarnations");
    }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->SubscribeTransactionCommitted(
            BIND(&TChunkReincarnator::OnTransactionFinished, MakeWeak(this)));
        transactionManager->SubscribeTransactionAborted(
            BIND(&TChunkReincarnator::OnTransactionFinished, MakeWeak(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(
            BIND(&TChunkReincarnator::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnLeaderActive();

        FailedJobRegistry_.Clear();

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
            ChunkScanner_.Start(
                chunkManager->GetGlobalBlobChunkScanDescriptor(shardIndex));
        }

        const auto& config = GetDynamicConfig();

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        const auto& invoker = hydraFacade->GetEpochAutomatonInvoker(
            EAutomatonThreadQueue::ChunkReincarnator);

        auto startExecutor = [&] (
            TPeriodicExecutorPtr* executor,
            auto callback,
            TDuration period)
        {
            *executor = New<TPeriodicExecutor>(
                invoker,
                std::move(callback),
                period);
            (*executor)->Start();
        };

        startExecutor(
            &ChunkScanExecutor_,
            BIND(&TChunkReincarnator::OnChunkScan, MakeWeak(this)),
            config->ChunkScanPeriod);

        startExecutor(
            &UpdateTransactionsExecutor_,
            BIND(
                &TChunkReincarnator::UpdateTransactions,
                MakeWeak(this),
                /*reschedulerReincarnations*/ true),
            config->TransactionUpdatePeriod);

        CancelScheduledReincarnationsAndRestartTransaction();

        YT_VERIFY(JobEpoch_ == InvalidJobEpoch);
        const auto& jobRegistry = Bootstrap_->GetChunkManager()->GetJobRegistry();
        JobEpoch_ = jobRegistry->StartEpoch();

        LastReplacement_ = NProfiling::GetInstant();
    }

    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::OnStopLeading();

        ScheduledJobs_ = {};
        FailedJobRegistry_.Clear();
        ExportedChunkReincarnationCheckRegistry_.Clear();

        auto stopExecutor = [] (auto& executor) {
            if (executor) {
                YT_UNUSED_FUTURE(executor->Stop());
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
        if (!chunk->IsBlob() || !IsLeader()) {
            return;
        }

        ChunkScanner_.OnChunkDestroyed(chunk);
        FailedJobRegistry_.OnChunkDestroyed(chunk);
        ExportedChunkReincarnationCheckRegistry_.UnregisterChunk(chunk->GetId());
    }

    void ScheduleReincarnation(TChunkTree* chunkTree, TChunkReincarnationOptions options) override
    {
        if (!IsLeader() || !IsEnabled()) {
            return;
        }

        const auto initialTraverseBudget =
            static_cast<i64>(GetDynamicConfig()->MaxVisitedChunkAncestorsPerChunk) *
            GetDynamicConfig()->MaxVisitedChunkAncestorsPerChunk;

        auto traverseBudget = initialTraverseBudget;

        THashSet<TChunkTree*> seen;
        std::vector<TChunkTree*> pending;
        std::vector<TChunkId> chunks;

        auto visitChild = [&] (TChunkTree* chunkTree) {
            if (chunkTree->IsForeign()) {
                return;
            }

            if (traverseBudget < 0) {
                THROW_ERROR_EXCEPTION("Failed to schedule chunk tree reincarnation; traverse budget exceeded")
                    << TErrorAttribute("traverse_budget", initialTraverseBudget);
            }

            --traverseBudget;

            if (seen.insert(chunkTree).second) {
                pending.push_back(chunkTree);
            }
        };

        visitChild(chunkTree);

        while (!pending.empty()) {
            auto* chunkTree = pending.back();
            pending.pop_back();

            switch (chunkTree->GetType()) {
                case EObjectType::Chunk:
                case EObjectType::ErasureChunk:
                    ChunkScanner_.EnqueueChunk({chunkTree->AsChunk(), options});
                    break;
                case EObjectType::ChunkList:
                    for (auto* child : chunkTree->AsChunkList()->Children()) {
                        if (child) {
                            visitChild(child);
                        }
                    }
                    break;
                case EObjectType::ChunkView:
                    visitChild(chunkTree->AsChunkView()->GetUnderlyingTree());
                    break;
                default:
                    break;
            }
        }
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TChunkScannerWithPayload<TChunkReincarnationOptions> ChunkScanner_;
    TPeriodicExecutorPtr ChunkScanExecutor_;

    //! This class is responsible for tracking exported chunk reincarnation
    //! check. It remembers chunk and its foreign cells on check start. When
    //! check is finished on one foreign cell this class is used to determine
    //! if it was last reincarnation check and chunk is ready to be reincarnated.
    class TExportedChunkReincarnationCheckRegistry
    {
    public:
        using TCellTagSet = TCellTagSet<TypicalChunkExportFactor>;

        // NB: It's a caller's responsibility to fill cell set. The main reason
        // for that is to avoid code duplication: caller likely needs to
        // traverse all cell tags the chunk is exported to.
        TCellTagSet* RegisterChunk(TChunk* chunk, TChunkReincarnationOptions options)
        {
            YT_ASSERT(chunk->IsExported());

            Previous_.erase(chunk->GetId());
            auto [it, inserted] = Current_.insert({chunk->GetId(), {}});
            if (!inserted) {
                it->second.UncheckedCells.clear();
            }

            it->second.Options = options;

            return &it->second.UncheckedCells;
        }

        bool UnregisterChunk(TChunkId chunkId)
        {
            auto it = Previous_.find(chunkId);
            if (it != Previous_.end()) {
                // Chunk should not be present in two maps at the same time.
                YT_VERIFY(!Current_.contains(chunkId));
                Previous_.erase(it);
                return true;
            }

            it = Current_.find(chunkId);
            if (it != Current_.end()) {
                Current_.erase(it);
                return true;
            }

            return false;
        }

        bool ApproveForCell(TChunkId chunkId, TCellTag foreignCellTag)
        {
            auto chunkIt = Current_.find(chunkId);
            auto* hashMap = &Current_;
            if (chunkIt == Current_.end()) {
                chunkIt = Previous_.find(chunkId);
                hashMap = &Previous_;
                if (chunkIt == Previous_.end()) {
                    return false;
                }
            }

            auto& cellTagSet = chunkIt->second.UncheckedCells;
            cellTagSet.erase(foreignCellTag);
            if (cellTagSet.empty()) {
                hashMap->erase(chunkIt);
                return true;
            }

            return false;
        }

        void Rotate(const auto& onChunkExpired)
        {
            for (const auto& [chunkId, chunkInfo] : Previous_) {
                onChunkExpired(chunkId, chunkInfo.Options);
            }
            Previous_.clear();
            Previous_.swap(Current_);
        }

        void Clear()
        {
            Previous_.clear();
            Current_.clear();
        }

    private:
        struct TChunkInfo
        {
            TCellTagSet UncheckedCells;
            TChunkReincarnationOptions Options;
        };
        using TPendingChecks = THashMap<TChunkId, TChunkInfo>;
        TPendingChecks Previous_;
        TPendingChecks Current_;
    };

    TExportedChunkReincarnationCheckRegistry ExportedChunkReincarnationCheckRegistry_;

    TJobEpoch JobEpoch_ = InvalidJobEpoch;

    struct TJobInfo
    {
        TJobId JobId;
        TChunkId OldChunkId;
        TChunkId NewChunkId;
        int MediumIndex;
        TChunkReincarnationOptions Options;
    };
    std::queue<TJobInfo> ScheduledJobs_;
    TFailedJobRegistry FailedJobRegistry_;

    TPeriodicExecutorPtr UpdateTransactionsExecutor_;

    struct TReincarnationRequest
    {
        TChunkId OldChunkId;
        TChunkId NewChunkId;
        TChunkReincarnationOptions Options;
    };
    std::queue<TReincarnationRequest> ChunksToReplace_;

    TInstant LastReplacement_;

    // Metrics.
    TEnumIndexedArray<EReincarnationResult, NProfiling::TCounter> Metrics_;
    NProfiling::TCounter TeleportedReincarnationCounter_;

    // Persistent.
    TTransactionRotator TransactionRotator_;
    i64 ConfigVersion_ = 0;

    void OnReincarnationFinished(EReincarnationResult result)
    {
        if (IsLeader()) {
            Metrics_[result].Increment();
            if (IsPermanentFailure(result) && result != EReincarnationResult::GenericPermanentFailure)
            {
                auto& counter = Metrics_[EReincarnationResult::GenericPermanentFailure];
                counter.Increment();
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
        ExportedChunkReincarnationCheckRegistry_.Clear();
        UpdateTransactions(/*rescheduleReincarnations*/ false);
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldClusterConfig)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        const auto& config = GetDynamicConfig();
        const auto& oldConfig = oldClusterConfig->ChunkManager->ChunkReincarnator;

        if (config->ShouldRescheduleAfterChange(*oldConfig)) {
            ++ConfigVersion_;
        }

        if (!IsLeader()) {
            return;
        }

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
            CancelScheduledReincarnationsAndRestartTransaction();
            ScheduleGlobalChunkScan();

            YT_LOG_INFO("Chunk reincarnation global scan restarted");
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

    void DoReincarnateChunks(
        TRange<TReincarnationRequest> chunks,
        std::vector<std::pair<TChunk*, TChunk*>>* exportedChunks = nullptr)
    {
        YT_VERIFY(HasMutationContext());
        if (chunks.empty()) {
            return;
        }

        auto isNative = CellTagFromId(chunks[0].OldChunkId) == Bootstrap_->GetCellTag();

        YT_VERIFY(exportedChunks || !isNative);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& config = GetDynamicConfig();

        auto maybeRescheduleChunk = [&] (TChunk* chunk, TChunkReincarnationOptions options) {
            if (isNative && IsLeader()) {
                YT_LOG_DEBUG(
                    "Rescheduling chunk reincarnation (ChunkId: %v)",
                    chunk->GetId());
                RescheduleReincarnation(chunk, options);
            }
        };

        TChunkAncestorTraverser traverser(Bootstrap_);

        for (auto request : chunks) {
            auto* oldChunk = chunkManager->FindChunk(request.OldChunkId);
            if (!IsObjectAlive(oldChunk)) {
                YT_LOG_DEBUG(
                    "Chunk reincarnation skipped because old chunk is not alive (ChunkId: %v)",
                    request.OldChunkId);
                continue;
            }

            auto* newChunk = chunkManager->FindChunk(request.NewChunkId);
            if (!IsObjectAlive(newChunk)) {
                if (!isNative) {
                    // Foreign chunks should be holded by transaction.
                    YT_LOG_ALERT(
                        "Imported reincarnated chunk is not alive; chunk reincarnation skipped "
                        "(OldChunkId: %v, NewChunkId: %v)",
                        oldChunk->GetId(),
                        request.NewChunkId);
                    continue;
                }

                OnReincarnationFinished(EReincarnationResult::Transient);
                YT_LOG_DEBUG(
                    "New chunk is not alive; chunk reincarnation skipped (OldChunkId: %v, NewChunkId: %v)",
                    oldChunk->GetId(),
                    newChunk->GetId());
                maybeRescheduleChunk(oldChunk, request.Options);
                continue;
            }

            if (!newChunk->IsConfirmed()) {
                OnReincarnationFinished(EReincarnationResult::Transient);
                YT_LOG_DEBUG(
                    "New chunk is not confirmed; chunk reincarnation skipped (OldChunkId: %v, NewChunkId: %v)",
                    oldChunk->GetId(),
                    newChunk->GetId());
                maybeRescheduleChunk(oldChunk, request.Options);
                continue;
            }

            if (newChunk->GetChunkFormat() == EChunkFormat::FileDefault) {
                YT_LOG_ALERT(
                    "Reincarnated chunk has weird format (NewChunkId: %v, Format: %v)",
                    newChunk->GetId(),
                    newChunk->GetChunkFormat());
                OnReincarnationFinished(EReincarnationResult::GenericPermanentFailure);
                continue;
            }

            auto result = traverser.TryPrepareChunkReincarnation(oldChunk, config->MaxVisitedChunkAncestorsPerChunk);
            if (isNative && oldChunk->IsExported() && result == EReincarnationResult::OK) {
                exportedChunks->push_back({oldChunk, newChunk});
            }

            if (isNative) {
                OnReincarnationFinished(result);
            }

            if (result != EReincarnationResult::OK) {
                maybeRescheduleChunk(oldChunk, request.Options);
                YT_LOG_DEBUG(
                    "Failed to replace reincarnated chunk "
                    "(ChunkId: %v, NewChunkId: %v, ReincarnationResult: %v)",
                    request.OldChunkId,
                    request.NewChunkId,
                    result);
                continue;
            }

            if (!isNative) {
                TeleportedReincarnationCounter_.Increment();
            }

            traverser.CommitChunkReincarnation(oldChunk, newChunk);

            chunkManager->ScheduleChunkRequisitionUpdate(oldChunk);
            chunkManager->ScheduleChunkRequisitionUpdate(newChunk);

            YT_LOG_DEBUG(
                "Chunk was successfully reincarnated (OldChunkId: %v, NewChunkId: %v)",
                oldChunk->GetId(),
                newChunk->GetId());
        }
    }

    void OnTransactionFinished(TTransaction* transaction)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(HasMutationContext());
        YT_VERIFY(IsObjectAlive(transaction));

        auto transactionId = transaction->GetId();
        auto previousTransactionId = TransactionRotator_.GetPreviousTransactionId();
        auto currentTransactionId = TransactionRotator_.GetTransactionId();

        if (TransactionRotator_.OnTransactionFinished(transaction)) {
            YT_LOG_DEBUG(
                "Chunk reincarnator transaction finished "
                "(FinishedTransactionId: %v, ChunkReincarnatorCurrentTransactionId: %v, ChunkReincarnatorPreviousTransactionId: %v)",
                transactionId,
                currentTransactionId,
                previousTransactionId);

            if (IsLeader()) {
                UpdateTransactions(/*rescheduleReincarnations*/ true);
            }

            return;
        }
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TMasterAutomatonPart::Clear();

        TransactionRotator_.Clear();

        ConfigVersion_ = 0;

        ChunksToReplace_ = {};

        FailedJobRegistry_.Clear();

        ScheduledJobs_ = {};

        ExportedChunkReincarnationCheckRegistry_.Clear();
    }

    // Transient.
    void RescheduleReincarnation(TChunk* chunk, TChunkReincarnationOptions options)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        // NB: We don't unregister exported chunk check here because this method
        // can be called from
        // `TExportedChunkReincarnationCheckRegistry::Unregister`.

        ChunkScanner_.EnqueueChunk({.Chunk = chunk, .Payload = options});
    }

    // Transient.
    void ScheduleChunkReplacement(TReincarnationRequest request)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        auto* oldChunk = chunkManager->FindChunk(request.OldChunkId);
        if (!IsObjectAlive(oldChunk)) {
            return;
        }

        auto* newChunk = chunkManager->FindChunk(request.NewChunkId);
        if (!IsObjectAlive(newChunk)) {
            RescheduleReincarnation(oldChunk, request.Options);
            return;
        }

        ChunksToReplace_.push(request);

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

        if (NProfiling::GetInstant() - LastReplacement_ >= config->ForcedUnderfilledBatchReplacementPeriod) {
            allowUnderfilledBatch = true;
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
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        if (hasReadyToReplaceChunks()) {
            LastReplacement_ = NProfiling::GetInstant();
        }

        while (hasReadyToReplaceChunks()) {
            TCompactFlatMap<
                TCellTag,
                TCompactVector<TTransactionId, 2>,
                MaxSecondaryMasterCells> transactionsToReplicate;

            NProto::TReqReincarnateChunks replaceChunksMutation;

            for (int i = 0; i < batchSize && !ChunksToReplace_.empty(); ++i) {
                auto request = ChunksToReplace_.front();
                ChunksToReplace_.pop();

                auto* oldChunk = chunkManager->FindChunk(request.OldChunkId);
                if (!IsObjectAlive(oldChunk)) {
                    continue;
                }

                auto* newChunk = chunkManager->FindChunk(request.NewChunkId);
                if (!IsObjectAlive(newChunk)) {
                    continue;
                }

                auto* subrequest = replaceChunksMutation.add_subrequests();
                ToProto(subrequest->mutable_old_chunk_id(), request.OldChunkId);
                ToProto(subrequest->mutable_new_chunk_id(), request.NewChunkId);
                ToProto(subrequest->mutable_options(), request.Options);
            }

            YT_UNUSED_FUTURE(CreateMutation(hydraManager, replaceChunksMutation)
                ->CommitAndLog(Logger));
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
            RescheduleReincarnation(oldChunk, jobInfo.Options);
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
            /*replicas*/ {}, // We know there are no replicas for newChunk yet.
            GenericChunkReplicaIndex,
            targetCount,
            targetCount,
            /*replicationFactorOverride*/ std::nullopt);

        if (targetNodes.empty()) {
            OnReincarnationFinished(EReincarnationResult::Transient);
            return;
        }

        auto ephemeralChunk = TEphemeralObjectPtr<TChunk>(oldChunk);
        TNodePtrWithReplicaAndMediumIndexList sourceReplicas;
        // This is context switch, chunk may die.
        auto replicasOrError = chunkManager->GetChunkReplicas(ephemeralChunk);
        if (!replicasOrError.IsOK()) {
            return;
        }

        if (!IsObjectAlive(ephemeralChunk)) {
            return;
        }

        auto replicas = replicasOrError.Value();

        // Make sure chunk is still alive.
        oldChunk = chunkManager->FindChunk(oldChunkId);
        if (!IsObjectAlive(oldChunk)) {
            return;
        }

        sourceReplicas.reserve(replicas.size());
        for (auto replica : replicas) {
            const auto* location = replica.GetPtr();
            sourceReplicas.emplace_back(location->GetNode(), replica.GetReplicaIndex(), location->GetEffectiveMediumIndex());
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
            mediumIndex,
            jobInfo.Options);
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
            auto job = std::move(ScheduledJobs_.front());
            ScheduledJobs_.pop();
            TryScheduleJob(std::move(job), context);
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
            RescheduleReincarnation(chunk, job->GetReincarnationOptions());
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
            ScheduleChunkReplacement({
                .OldChunkId = job->OldChunkId(),
                .NewChunkId = job->NewChunkId(),
                .Options = job->GetReincarnationOptions(),
            });
        }
    }

    void UpdateTransactions(bool rescheduleReincarnations)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
        YT_VERIFY(IsLeader());

        if (rescheduleReincarnations) {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            ExportedChunkReincarnationCheckRegistry_.Rotate(
                [&] (TChunkId chunkId, TChunkReincarnationOptions options) {
                    auto* chunk = chunkManager->FindChunk(chunkId);
                    if (IsObjectAlive(chunk)) {
                        RescheduleReincarnation(chunk, options);
                    }
                });
        }

        YT_UNUSED_FUTURE(CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            NProto::TReqUpdateChunkReincarnatorTransactions())
            ->CommitAndLog(Logger));
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

        auto scannedChunkBudget = config->MaxChunksPerScan;

        YT_LOG_DEBUG(
            "Chunk reincarnator iteration started "
            "(MinAllowedCreationTime: %v, MaxVisitedChunkAncestorsPerChunk: %v)",
            config->MinAllowedCreationTime,
            config->MaxVisitedChunkAncestorsPerChunk);

        TChunkAncestorTraverser traverser(Bootstrap_);

        std::vector<TChunkId> chunksToReincarnate;
        std::vector<TChunkId> exportedChunks;
        chunksToReincarnate.reserve(scannedChunkBudget);
        exportedChunks.reserve(scannedChunkBudget);

        int scannedChunkCount = 0;
        struct {
            int NotAlive = 0;
            int NotConfirmed = 0;
            int Fresh = 0;
            int Foreign = 0;
            int WrongFormat = 0;
            int AccountSettings = 0;
            int AfterTraverse = 0;

            int GetTotal() const
            {
                return
                    NotAlive +
                    NotConfirmed +
                    Fresh +
                    Foreign +
                    WrongFormat +
                    AccountSettings +
                    AfterTraverse;
            }
        } skipped;

        auto verboseLoggingEnabled = config->EnableVerboseLogging;
        std::vector<TChunkId> skippedBecauseOfAccountSettings;

        while (
            scannedChunkBudget-- > 0 &&
            ChunkScanner_.HasUnscannedChunk() &&
            // To prevent uncontrolled |ScheduledJobs_|'s growth.
            ssize(chunksToReincarnate) + ssize(exportedChunks) + ssize(ScheduledJobs_) < 2 * config->MaxRunningJobCount)
        {
            auto [chunk, options] = ChunkScanner_.DequeueChunk();

            ++scannedChunkCount;

            if (!IsObjectAlive(chunk)) {
                ++skipped.NotAlive;
                continue;
            }

            if (!chunk->IsConfirmed()) {
                ++skipped.NotConfirmed;
                continue;
            }

            if (!options.IgnoreCreationTime) {
                auto estimatedCreationTime = epochHistoryManager->GetEstimatedCreationTime(
                    chunk->GetId(),
                    NProfiling::GetInstant()).second;
                if (estimatedCreationTime > config->MinAllowedCreationTime) {
                    ++skipped.Fresh;
                    continue;
                }
            }

            if (chunk->IsForeign()) {
                // Each cell is responsible only for its native chunks. Foreign
                // chunks can be only reincarnated by their native cells.
                ++skipped.Foreign;
                continue;
            }

            switch (chunk->GetChunkFormat()) {
                // Static tables.
                case EChunkFormat::TableUnversionedSchemaful:
                case EChunkFormat::TableUnversionedSchemalessHorizontal:
                case EChunkFormat::TableUnversionedColumnar:
                // Dynamic tables.
                case EChunkFormat::TableVersionedColumnar:
                case EChunkFormat::TableVersionedSlim:
                case EChunkFormat::TableVersionedSimple:
                case EChunkFormat::TableVersionedIndexed:
                // Some old table chunks can have this format. Such chunks should be reincarnated.
                case EChunkFormat::FileDefault:
                    break;
                default:
                    YT_LOG_DEBUG("Chunk has unsuitable format; skipping (ChunkId: %v, Format: %v)",
                        chunk->GetId(),
                        chunk->GetChunkFormat());
                    OnReincarnationFinished(EReincarnationResult::WrongChunkFormat);
                    ++skipped.WrongFormat;
                    continue;
            }

            if (!(config->IgnoreAccountSettings || options.IgnoreAccountSettings)) {
                if (!IsReincarnationAllowedByAccountSettings(chunk)) {
                    ++skipped.AccountSettings;

                    if (verboseLoggingEnabled) {
                        skippedBecauseOfAccountSettings.push_back(chunk->GetId());
                    }

                    continue;
                }
            }

            auto result = traverser.TryPrepareChunkReincarnation(
                chunk,
                config->MaxVisitedChunkAncestorsPerChunk);
            YT_ASSERT(result != EReincarnationResult::Transient);

            switch (result) {
                case EReincarnationResult::OK:
                    if (chunk->IsExported()) {
                        YT_LOG_DEBUG(
                            "Scheduling exported chunk reincarnation check (OldChunkId: %v)",
                            chunk->GetId());
                        exportedChunks.push_back(chunk->GetId());
                    } else {
                        YT_LOG_DEBUG(
                            "Scheduling reincarnated chunk creation (OldChunkId: %v)",
                            chunk->GetId());
                        chunksToReincarnate.push_back(chunk->GetId());
                    }
                    break;
                default:
                    YT_LOG_DEBUG(
                        "Chunk cannot be reincarnated (ChunkId: %v, Reason: %v)",
                        chunk->GetId(),
                        result);
                    OnReincarnationFinished(result);
                    ++skipped.AfterTraverse;
                    break;
            }
        }

        if (!chunksToReincarnate.empty()) {
            // NB: About 95% of all chunks have never been exported.
            NProto::TReqCreateReincarnatedChunks request;
            request.set_config_version(ConfigVersion_);
            for (auto chunkId : chunksToReincarnate) {
                ToProto(request.add_subrequests()->mutable_old_chunk_id(), chunkId);
            }
            YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), request)
                ->CommitAndLog(Logger));

            YT_LOG_DEBUG(
                "Chunk reincarnation scheduled (ChunkCount: %v, ChunkIds: %v)",
                chunksToReincarnate.size(),
                MakeShrunkFormattableView(chunksToReincarnate, TDefaultFormatter(), SampleChunkIdCount));
        }

        // Exported chunks have to be handled a bit differently. Chunk
        // reincarnation check has to be performed on every cell to which it
        // has been exported.
        if (!exportedChunks.empty()) {
            NProto::TReqCheckExportedChunkReincarnation mutationRequest;
            mutationRequest.set_config_version(ConfigVersion_);
            ToProto(mutationRequest.mutable_chunk_ids(), exportedChunks);
            YT_UNUSED_FUTURE(CreateMutation(
                Bootstrap_->GetHydraFacade()->GetHydraManager(),
                mutationRequest)
                ->CommitAndLog(Logger));

            YT_LOG_DEBUG(
                "Exported chunk reincarnation check scheduled (ChunkIdsCount: %v, ChunkIds: %v)",
                exportedChunks.size(),
                MakeShrunkFormattableView(exportedChunks, TDefaultFormatter(), SampleChunkIdCount));
        }

        YT_LOG_DEBUG(
            "Chunk reincarnator iteration finished "
            "(ScannedChunks: %v, Skipped: %v, NotAlive: %v, NotConfirmed: %v, Fresh: %v, "
            "Foreign: %v, WrongFormat: %v, AccountSettings: %v, AfterTraverse: %v)",
            scannedChunkCount,
            skipped.GetTotal(),
            skipped.NotAlive,
            skipped.NotConfirmed,
            skipped.Fresh,
            skipped.Foreign,
            skipped.WrongFormat,
            skipped.AccountSettings,
            skipped.AfterTraverse);

        YT_LOG_DEBUG_IF(!skippedBecauseOfAccountSettings.empty(),
            "Chunks were not reincarnated because of account settings (ChunkCount: %v, ChunkIds: %v)",
            skippedBecauseOfAccountSettings.size(),
            MakeShrunkFormattableView(skippedBecauseOfAccountSettings, TDefaultFormatter(), SampleChunkIdCount));
    }

    bool IsReincarnationAllowedByAccountSettings(TChunk* chunk)
    {
        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto* requisitionRegistry = chunkManager->GetChunkRequisitionRegistry();
        const auto& requisition = chunk->GetAggregatedRequisition(requisitionRegistry);
        return std::all_of(requisition.begin(), requisition.end(), [] (const TRequisitionEntry& entry) {
            return entry.Account->GetEnableChunkReincarnation();
        });
    }

    void HydraCheckExportedChunkReincarnation(
        NProto::TReqCheckExportedChunkReincarnation* request)
    {
        // NB: This mutation is needed to post reliable hive message to foreign
        // cells. Registering pending exported chunk reincarnation checks is
        // done transiently. See |ExportedChunkReincarnationCheckRegistry_|.

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        using TCellToChunksMap = TCompactFlatMap<
            TCellTag,
            std::vector<TChunkId>,
            MaxSecondaryMasterCells>;
        TCellToChunksMap cellToChunks;
        cellToChunks.reserve(multicellManager->GetRegisteredMasterCellTags().size());
        for (auto protoChunkId : request->chunk_ids()) {
            auto chunkId = FromProto<TChunkId>(protoChunkId);
            auto* chunk = chunkManager->FindChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                YT_LOG_DEBUG("Scheduling reincarnation check was skipped for missing exported chunk (ChunkId: %v)", chunkId);
                continue;
            }

            YT_ASSERT(chunk->IsExported());

            auto* cellTags = IsLeader()
                ? ExportedChunkReincarnationCheckRegistry_.RegisterChunk(
                    chunk,
                    FromProto<TChunkReincarnationOptions>(request->options()))
                : nullptr;

            for (auto cellTag : multicellManager->GetRegisteredMasterCellTags()) {
                if (!chunk->IsExportedToCell(cellTag)) {
                    continue;
                }

                cellToChunks[cellTag].push_back(chunkId);
                if (cellTags) {
                    cellTags->insert({cellTag, {}});
                }
            }

            YT_ASSERT(!cellTags || !cellTags->empty());
        }

        for (const auto& [cellTag, chunkIds] : cellToChunks) {
            NProto::TReqCheckForeignChunkReincarnation request;
            request.set_config_version(ConfigVersion_);
            ToProto(request.mutable_chunk_ids(), chunkIds);
            multicellManager->PostToMaster(request, cellTag);
        }
    }

    void HydraCheckForeignChunkReincarnation(NProto::TReqCheckForeignChunkReincarnation* request)
    {
        YT_ASSERT(!request->chunk_ids().empty());

        const auto& config = GetDynamicConfig();

        YT_LOG_DEBUG(
            "Checking if reincarnation is possible for foreign chunks "
            "(MinAllowedCreationTime: %v, MaxVisitedChunkAncestorsPerChunk: %v)",
            config->MinAllowedCreationTime,
            config->MaxVisitedChunkAncestorsPerChunk);

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        TChunkAncestorTraverser traverser(Bootstrap_);

        NProto::TReqOnExportedChunkReincarnationCheckFinished response;
        response.set_foreign_cell_tag(ToProto<int>(multicellManager->GetCellTag()));
        response.set_config_version(request->config_version());

        std::optional<TCellTag> nativeCellTag;

        int readyToReincarnateChunkCount = 0;

        for (auto chunkId : request->chunk_ids()) {
            auto* subresponse = response.add_subresponses();
            ToProto(subresponse->mutable_chunk_id(), chunkId);
            auto* chunk = chunkManager->FindChunk(FromProto<TChunkId>(chunkId));
            if (!IsObjectAlive(chunk)) {
                subresponse->set_result(ToProto<int>(EReincarnationResult::NoSuchChunk));
                continue;
            }

            if (!nativeCellTag) {
                nativeCellTag = chunk->GetNativeCellTag();
                YT_ASSERT(*nativeCellTag != multicellManager->GetCellTag());
            }
            YT_ASSERT(*nativeCellTag == chunk->GetNativeCellTag());

            auto result = traverser.TryPrepareChunkReincarnation(
                chunk,
                config->MaxVisitedChunkAncestorsPerChunk);

            if (result == EReincarnationResult::OK) {
                ++readyToReincarnateChunkCount;
            }

            subresponse->set_result(ToProto<int>(result));
        }

        if (nativeCellTag) [[likely]] {
            multicellManager->PostToMaster(response, *nativeCellTag);

            YT_LOG_DEBUG(
                "Foreign chunk reincarnation check finished "
                "(TotalChunkCount: %v, ReadyForReincarnationChunkCount: %v)",
                request->chunk_ids().size(),
                readyToReincarnateChunkCount);
        }
    }

    void HydraOnExportedChunkReincarnationCheckFinished(
        NProto::TReqOnExportedChunkReincarnationCheckFinished* response)
    {
        if (!IsLeader()) {
            return;
        }

        auto foreignCellTag = FromProto<TCellTag>(response->foreign_cell_tag());

        if (response->config_version() != ConfigVersion_) {
            YT_LOG_DEBUG(
                "Cannot prepare exported chunk reincarnation: config version mismatch ",
                "(ForeignCellTag: %v, CurrentConfigVersion: %v, ResponseConfigVersion: %v)",
                foreignCellTag,
                ConfigVersion_,
                response->config_version());
            return;
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        std::vector<TChunkId> chunksToReincarnate;
        chunksToReincarnate.reserve(response->subresponses().size());

        for (const auto& subresponse : response->subresponses()) {
            auto chunkId = FromProto<TChunkId>(subresponse.chunk_id());
            auto result = CheckedEnumCast<EReincarnationResult>(subresponse.result());

            auto* chunk = chunkManager->FindChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                continue;
            }

            bool logged = false;
            switch (result) {
                case EReincarnationResult::NoSuchChunk: [[fallthrough]];
                case EReincarnationResult::NoTableAncestors:
                    [[fallthrough]];
                case EReincarnationResult::OK:
                    if (ExportedChunkReincarnationCheckRegistry_.ApproveForCell(
                        chunkId,
                        foreignCellTag))
                    {
                        chunksToReincarnate.push_back(chunkId);
                    }
                    break;
                default:
                    YT_LOG_ALERT(
                        "Unexpected exported chunk reincarnation check result "
                        "(ChunkId: %v, ForeignCellTag: %v, ReincarnationResult: %v)",
                        chunkId,
                        foreignCellTag,
                        result);
                    logged = true;
                    [[fallthrough]];
                case EReincarnationResult::ReincarnationDisabled: [[fallthrough]];
                case EReincarnationResult::TooManyAncestors: [[fallthrough]];
                case EReincarnationResult::DynamicTableChunk:
                    // We don't want to handle the same chunk multiple times.
                    if (ExportedChunkReincarnationCheckRegistry_.UnregisterChunk(chunkId)) {
                        YT_LOG_DEBUG_UNLESS(logged,
                            "Exported chunk reincarnation check failed "
                            "(ChunkId: %v, ForeignCellTag: %v, ReincarnationResult: %v)",
                            chunkId,
                            foreignCellTag,
                            result);
                        OnReincarnationFinished(result);
                    }
                    break;
            }
        }

        if (chunksToReincarnate.empty()) {
            return;
        }

        NProto::TReqCreateReincarnatedChunks mutationRequest;
        mutationRequest.set_config_version(ConfigVersion_);
        for (auto chunkId : chunksToReincarnate) {
            auto* subrequest = mutationRequest.add_subrequests();
            ToProto(subrequest->mutable_old_chunk_id(), chunkId);
        }

        YT_UNUSED_FUTURE(CreateMutation(Bootstrap_->GetHydraFacade()->GetHydraManager(), mutationRequest)
            ->CommitAndLog(Logger));
    }

    void HydraUpdateChunkReincarnatorTransactions(
        NProto::TReqUpdateChunkReincarnatorTransactions* /*request*/)
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
        if (!GetDynamicConfig()->Enable) {
            return;
        }

        if (request->has_config_version() && request->config_version() != ConfigVersion_) {
            return;
        }

        if (!IsObjectAlive(TransactionRotator_.GetTransaction())) {
            YT_LOG_DEBUG(
                "Chunk reincarnation transaction is not alive (TransactionId: %v)",
                TransactionRotator_.GetTransactionId());

            if (IsLeader()) {
                const auto& chunkManager = Bootstrap_->GetChunkManager();

                for (const auto& subrequest : request->subrequests()) {
                    auto oldChunkId = FromProto<TChunkId>(subrequest.old_chunk_id());
                    auto* oldChunk = chunkManager->FindChunk(oldChunkId);
                    auto options = FromProto<TChunkReincarnationOptions>(subrequest.options());

                    if (!IsObjectAlive(oldChunk)) {
                        continue;
                    }

                    RescheduleReincarnation(oldChunk, options);
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

            auto requisitionIndex = oldChunk->GetAggregatedRequisitionIndex();

            if (requisitionIndex == EmptyChunkRequisitionIndex) {
                // We don't want to reincarnate unused chunks, do we?
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
                chunkManager->GetMediumByIndex(mediumIndex),
                /*readQuorum*/ 0,
                /*writeQuorum*/ 0,
                /*movable*/ true,
                vital);

            // NB: Chunk requisition is set on chunk replacement.

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

        if (request->has_config_version() && request->config_version() != ConfigVersion_) {
            return;
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        TChunkAncestorTraverser traverser(Bootstrap_);

        std::vector<TReincarnationRequest> parsedSubrequests;
        parsedSubrequests.reserve(request->subrequests_size());
        for (const auto& subrequest : request->subrequests()) {
            parsedSubrequests.push_back({
                .OldChunkId = FromProto<TChunkId>(subrequest.old_chunk_id()),
                .NewChunkId = FromProto<TChunkId>(subrequest.new_chunk_id()),
                .Options = FromProto<TChunkReincarnationOptions>(subrequest.options()),
            });
        }

        std::vector<std::pair<TChunk*, TChunk*>> multicellReincarnations;

        DoReincarnateChunks(
            parsedSubrequests,
            &multicellReincarnations);

        if (multicellReincarnations.empty()) {
            return;
        }

        // Reincarnation of exported chunks is a bit complicated.
        // 1. Native cell starts transaction and exports chunks under this
        //    transaction. Transaction has to be replicated to all cells.
        // 2. Native cell sends to other participants reincarnation request.
        //    Every foreign cell checks if reincarnation is possible, imports
        //    new chunks under reincarnation transaction and performs chunk
        //    replacement. Foreign cell _can_ ignore reincarnation request but
        //    such failures are expected to be extremely rare.
        // 3. Native cell commits transaction. Transaction commit is serialized
        //    with chunks export via Hive.

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        using TPerCellReincarnationInfo = TCompactFlatMap<
            TCellTag,
            std::pair<std::vector<TChunk*>, std::vector<TChunk*>>,
            MaxSecondaryMasterCells>;
        TPerCellReincarnationInfo perCellReincarnationInfo;

        for (auto [oldChunk, newChunk] : multicellReincarnations) {
            for (auto cellTag : multicellManager->GetRegisteredMasterCellTags()) {
                if (oldChunk->IsExportedToCell(cellTag)) {
                    auto& [oldChunks, newChunks] = perCellReincarnationInfo[cellTag];
                    oldChunks.push_back(oldChunk);
                    newChunks.push_back(newChunk);
                }
            }
        }

        TCellTagList foreignCells;
        foreignCells.reserve(perCellReincarnationInfo.size());
        for (const auto& [cellTag, reincarnationInfo] : perCellReincarnationInfo) {
            foreignCells.push_back(cellTag);
        }

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->StartTransaction(
            nullptr,
            /*prerequisiteTransactions*/ {},
            /*replicatedToCellTags*/ foreignCells,
            config->MulticellReincarnationTransactionTimeout,
            /*deadline*/ std::nullopt,
            "Multicell chunk reincarnation",
            EmptyAttributes(),
            /*isCypressTransaction*/ false);

        for (const auto& [cellTag, chunks] : perCellReincarnationInfo) {
            const auto& [oldChunks, newChunks] = chunks;
            YT_ASSERT(oldChunks.size() == newChunks.size());

            // Export chunks.

            NChunkClient::NProto::TReqImportChunks importChunks;
            ToProto(importChunks.mutable_transaction_id(), transaction->GetId());
            chunkManager->ExportChunks(transaction, newChunks, cellTag, importChunks.mutable_chunks());
            multicellManager->PostToMaster(importChunks, cellTag);

            // Send reincarnation request.

            NProto::TReqReincarnateForeignChunks reincarnationRequest;
            ToProto(reincarnationRequest.mutable_transaction_id(), transaction->GetId());
            reincarnationRequest.mutable_subrequests()->Reserve(oldChunks.size());
            for (int i = 0; i < std::ssize(oldChunks); ++i) {
                auto* subrequest = reincarnationRequest.add_subrequests();
                ToProto(subrequest->mutable_old_chunk_id(), oldChunks[i]->GetId());
                ToProto(subrequest->mutable_new_chunk_id(), newChunks[i]->GetId());
            }

            multicellManager->PostToMaster(reincarnationRequest, cellTag);
        }

        transactionManager->CommitMasterTransaction(transaction, /*options*/ {});
    }

    void HydraReincarnateForeignChunks(NProto::TReqReincarnateForeignChunks* request)
    {
        YT_VERIFY(HasMutationContext());

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        auto* transaction = transactionManager->FindTransaction(transactionId);
        if (!transaction || transaction->GetPersistentState() != ETransactionState::Active) {
            YT_LOG_ALERT("Attempted to reincarnate foreign chunks under invalid transaction (TransactionId: %v)",
                transactionId);
            return;
        }

        std::vector<TReincarnationRequest> parsedSubrequests;
        parsedSubrequests.reserve(request->subrequests().size());
        for (const auto& subrequest : request->subrequests()) {
            // Account's settings and creation time are always ignored on foreign cell.
            parsedSubrequests.push_back({
                .OldChunkId = FromProto<TChunkId>(subrequest.old_chunk_id()),
                .NewChunkId = FromProto<TChunkId>(subrequest.new_chunk_id()),
                .Options = {.IgnoreCreationTime = true, .IgnoreAccountSettings = true},
            });
        }

        DoReincarnateChunks(parsedSubrequests);
    }

    void Save(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, TransactionRotator_);
        Save(context, ConfigVersion_);
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;

        Load(context, TransactionRotator_);

        // COMPAT(kvk1920)
        if (context.GetVersion() >= EMasterReign::MulticellChunkReincarnator) {
            Load(context, ConfigVersion_);
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

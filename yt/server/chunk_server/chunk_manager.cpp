#include "stdafx.h"
#include "chunk_manager.h"
#include "config.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "job.h"
#include "chunk_placement.h"
#include "chunk_replicator.h"
#include "chunk_sealer.h"
#include "chunk_tree_balancer.h"
#include "chunk_proxy.h"
#include "chunk_list_proxy.h"
#include "private.h"
#include "helpers.h"

#include <core/misc/string.h>

#include <core/compression/codec.h>

#include <core/erasure/codec.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/chunk_client/chunk_ypath.pb.h>
#include <ytlib/chunk_client/chunk_list_ypath.pb.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/schema.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/table_client/table_ypath.pb.h>

#include <ytlib/journal_client/helpers.h>

#include <server/hydra/composite_automaton.h>
#include <server/hydra/entity_map.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <server/chunk_server/chunk_manager.pb.h>

#include <server/node_tracker_server/node_tracker.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/cell_master/serialize.h>
#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>

#include <server/transaction_server/transaction_manager.h>
#include <server/transaction_server/transaction.h>

#include <server/object_server/type_handler_detail.h>

#include <server/node_tracker_server/node_directory_builder.h>
#include <server/node_tracker_server/config.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/account.h>
#include <server/security_server/group.h>

namespace NYT {
namespace NChunkServer {

using namespace NConcurrency;
using namespace NRpc;
using namespace NHydra;
using namespace NNodeTrackerServer;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NYTree;
using namespace NCellMaster;
using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJournalClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;
static TDuration ProfilingPeriod = TDuration::MilliSeconds(100);
// NB: Changing this value will invalidate all changelogs!
static TDuration UnapprovedReplicaGracePeriod = TDuration::Seconds(15);

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkTypeHandlerBase
    : public TObjectTypeHandlerWithMapBase<TChunk>
{
public:
    explicit TChunkTypeHandlerBase(TImpl* owner);

    virtual TNullable<TTypeCreationOptions> GetCreationOptions() const override
    {
        return TTypeCreationOptions(
            EObjectTransactionMode::Required,
            EObjectAccountMode::Required);
    }

    virtual TObjectBase* Create(
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObjects* request,
        TRspCreateObjects* response) override;

protected:
    TImpl* Owner_;

    virtual IObjectProxyPtr DoGetProxy(TChunk* chunk, TTransaction* transaction) override;

    virtual void DoDestroy(TChunk* chunk) override;

    virtual TTransaction* DoGetStagingTransaction(TChunk* chunk)
    {
        return chunk->GetStagingTransaction();
    }

    virtual void DoUnstage(TChunk* chunk, bool recursive) override;

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkTypeHandler
    : public TChunkTypeHandlerBase
{
public:
    explicit TChunkTypeHandler(TImpl* owner)
        : TChunkTypeHandlerBase(owner)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Chunk;
    }

private:
    virtual Stroka DoGetName(TChunk* chunk) override
    {
        return Format("chunk %v", chunk->GetId());
    }

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TErasureChunkTypeHandler
    : public TChunkTypeHandlerBase
{
public:
    explicit TErasureChunkTypeHandler(TImpl* owner)
        : TChunkTypeHandlerBase(owner)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::ErasureChunk;
    }

private:
    virtual Stroka DoGetName(TChunk* chunk) override
    {
        return Format("erasure chunk %v", chunk->GetId());
    }

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TJournalChunkTypeHandler
    : public TChunkTypeHandlerBase
{
public:
    explicit TJournalChunkTypeHandler(TImpl* owner)
        : TChunkTypeHandlerBase(owner)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::JournalChunk;
    }

private:
    virtual Stroka DoGetName(TChunk* chunk) override
    {
        return Format("journal chunk %v", chunk->GetId());
    }

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkListTypeHandler
    : public TObjectTypeHandlerWithMapBase<TChunkList>
{
public:
    explicit TChunkListTypeHandler(TImpl* owner);

    virtual EObjectType GetType() const override
    {
        return EObjectType::ChunkList;
    }

    virtual TNullable<TTypeCreationOptions> GetCreationOptions() const override
    {
        return TTypeCreationOptions(
            EObjectTransactionMode::Required,
            EObjectAccountMode::Forbidden);
    }

    virtual TObjectBase* Create(
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObjects* request,
        TRspCreateObjects* response) override;

private:
    TImpl* Owner_;

    virtual Stroka DoGetName(TChunkList* chunkList) override
    {
        return Format("chunk list %v", chunkList->GetId());
    }

    virtual IObjectProxyPtr DoGetProxy(TChunkList* chunkList, TTransaction* transaction) override;

    virtual void DoDestroy(TChunkList* chunkList) override;

    virtual TTransaction* DoGetStagingTransaction(TChunkList* chunkList)
    {
        return chunkList->GetStagingTransaction();
    }

    virtual void DoUnstage(TChunkList* chunkList, bool recursive) override;

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TImpl
    : public TMasterAutomatonPart
{
public:
    TImpl(
        TChunkManagerConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap)
        , Config_(config)
        , ChunkTreeBalancer_(Bootstrap)
        , Profiler(ChunkServerProfiler)
        , AddChunkCounter_("/add_chunk_rate")
        , RemoveChunkCounter_("/remove_chunk_rate")
        , AddChunkReplicaCounter_("/add_chunk_replica_rate")
        , RemoveChunkReplicaCounter_("/remove_chunk_replica_rate")
    {
        RegisterMethod(BIND(&TImpl::UpdateChunkProperties, Unretained(this)));

        RegisterLoader(
            "ChunkManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "ChunkManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESerializationPriority::Keys,
            "ChunkManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESerializationPriority::Values,
            "ChunkManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));
    }

    void Initialize()
    {
        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->RegisterHandler(New<TChunkTypeHandler>(this));
        objectManager->RegisterHandler(New<TErasureChunkTypeHandler>(this));
        objectManager->RegisterHandler(New<TJournalChunkTypeHandler>(this));
        objectManager->RegisterHandler(New<TChunkListTypeHandler>(this));

        auto nodeTracker = Bootstrap->GetNodeTracker();
        nodeTracker->SubscribeNodeRegistered(BIND(&TImpl::OnNodeRegistered, MakeWeak(this)));
        nodeTracker->SubscribeNodeUnregistered(BIND(&TImpl::OnNodeUnregistered, MakeWeak(this)));
        nodeTracker->SubscribeNodeConfigUpdated(BIND(&TImpl::OnNodeConfigUpdated, MakeWeak(this)));
        nodeTracker->SubscribeFullHeartbeat(BIND(&TImpl::OnFullHeartbeat, MakeWeak(this)));
        nodeTracker->SubscribeIncrementalHeartbeat(BIND(&TImpl::OnIncrementalHeartbeat, MakeWeak(this)));

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap->GetHydraFacade()->GetAutomatonInvoker(),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();
    }


    TMutationPtr CreateUpdateChunkPropertiesMutation(
        const NProto::TReqUpdateChunkProperties& request)
    {
        return CreateMutation(
            Bootstrap->GetHydraFacade()->GetHydraManager(),
            request,
            this,
            &TImpl::UpdateChunkProperties);
    }


    TNodeList AllocateWriteTargets(
        int replicaCount,
        const TNodeSet* forbiddenNodes,
        const TNullable<Stroka>& preferredHostName,
        EObjectType chunkType)
    {
        return ChunkPlacement_->AllocateWriteTargets(
            replicaCount,
            forbiddenNodes,
            preferredHostName,
            EWriteSessionType::User,
            chunkType);
    }

    TChunk* CreateChunk(EObjectType type)
    {
        Profiler.Increment(AddChunkCounter_);
        auto id = Bootstrap->GetObjectManager()->GenerateId(type);
        auto* chunk = new TChunk(id);
        ChunkMap_.Insert(id, chunk);
        return chunk;
    }

    TChunkList* CreateChunkList()
    {
        auto objectManager = Bootstrap->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::ChunkList);
        auto* chunkList = new TChunkList(id);
        ChunkListMap_.Insert(id, chunkList);
        return chunkList;
    }


    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree** childrenBegin,
        TChunkTree** childrenEnd)
    {
        if (childrenBegin == childrenEnd)
            return;

        if (!chunkList->Statistics().Sealed) {
            THROW_ERROR_EXCEPTION("Cannot attach children to an unsealed chunk list %v",
                chunkList->GetId());
        }

        auto objectManager = Bootstrap->GetObjectManager();
        NChunkServer::AttachToChunkList(
            chunkList,
            childrenBegin,
            childrenEnd,
            [&] (TChunkTree* chunkTree) {
                objectManager->RefObject(chunkTree);
            });
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children)
    {
        AttachToChunkList(
            chunkList,
            const_cast<TChunkTree**>(children.data()),
            const_cast<TChunkTree**>(children.data() + children.size()));
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* child)
    {
        AttachToChunkList(
            chunkList,
            &child,
            &child + 1);
    }


    void DetachFromChunkList(
        TChunkList* chunkList,
        TChunkTree** childrenBegin,
        TChunkTree** childrenEnd)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        NChunkServer::DetachFromChunkList(
            chunkList,
            childrenBegin,
            childrenEnd,
            [&] (TChunkTree* chunkTree) {
                objectManager->UnrefObject(chunkTree);
            });
    }

    void DetachFromChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children)
    {
        DetachFromChunkList(
            chunkList,
            const_cast<TChunkTree**>(children.data()),
            const_cast<TChunkTree**>(children.data() + children.size()));
    }

    void DetachFromChunkList(
        TChunkList* chunkList,
        TChunkTree* child)
    {
        DetachFromChunkList(
            chunkList,
            &child,
            &child + 1);
    }


    void RebalanceChunkTree(TChunkList* chunkList)
    {
        if (!ChunkTreeBalancer_.IsRebalanceNeeded(chunkList))
            return;

        PROFILE_TIMING ("/chunk_tree_rebalance_time") {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree rebalancing started (RootId: %v)",
                chunkList->GetId());
            ChunkTreeBalancer_.Rebalance(chunkList);
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree rebalancing completed");
        }
    }


    void ConfirmChunk(
        TChunk* chunk,
        const std::vector<NChunkClient::TChunkReplica>& replicas,
        TChunkInfo* chunkInfo,
        TChunkMeta* chunkMeta)
    {
        YCHECK(!chunk->IsConfirmed());

        auto id = chunk->GetId();

        chunk->ChunkInfo().Swap(chunkInfo);
        chunk->ChunkMeta().Swap(chunkMeta);

        auto nodeTracker = Bootstrap->GetNodeTracker();

        auto* mutationContext = Bootstrap->GetHydraFacade()->GetHydraManager()->GetMutationContext();
        auto mutationTimestamp = mutationContext->GetTimestamp();

        for (auto replica : replicas) {
            auto* node = nodeTracker->FindNode(replica.GetNodeId());
            if (!node) {
                LOG_DEBUG_UNLESS(IsRecovery(), "Tried to confirm chunk %v at an unknown node %v",
                    id,
                    replica.GetNodeId());
                continue;
            }

            auto chunkWithIndex = chunk->IsJournal()
                ? TChunkPtrWithIndex(chunk, EJournalReplicaType::Active)
                : TChunkPtrWithIndex(chunk, replica.GetIndex());

            if (node->GetState() != ENodeState::Online) {
                LOG_DEBUG_UNLESS(IsRecovery(), "Tried to confirm chunk %v at %v which has invalid state %Qlv",
                    id,
                    node->GetAddress(),
                    node->GetState());
                continue;
            }

            if (!node->HasReplica(chunkWithIndex, false)) {
                AddChunkReplica(
                    node,
                    chunkWithIndex,
                    false,
                    EAddReplicaReason::Confirmation);
                node->AddUnapprovedReplica(chunkWithIndex, mutationTimestamp);
            }
        }

        // Increase staged resource usage.
        if (chunk->IsStaged() && !chunk->IsJournal()) {
            auto* stagingTransaction = chunk->GetStagingTransaction();
            auto* stagingAccount = chunk->GetStagingAccount();
            auto securityManager = Bootstrap->GetSecurityManager();
            auto delta = chunk->GetResourceUsage();
            securityManager->UpdateAccountStagingUsage(stagingTransaction, stagingAccount, delta);
        }

        if (ChunkReplicator_) {
            ChunkReplicator_->ScheduleChunkRefresh(chunk);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk confirmed (ChunkId: %v)", id);
    }


    void UnstageChunk(TChunk* chunk)
    {
        if (chunk->IsStaged() && chunk->IsConfirmed() && !chunk->IsJournal()) {
            auto* stagingTransaction = chunk->GetStagingTransaction();
            auto* stagingAccount = chunk->GetStagingAccount();
            auto securityManager = Bootstrap->GetSecurityManager();
            auto delta = -chunk->GetResourceUsage();
            securityManager->UpdateAccountStagingUsage(stagingTransaction, stagingAccount, delta);
        }

        chunk->SetStagingTransaction(nullptr);
        chunk->SetStagingAccount(nullptr);
    }

    void UnstageChunkList(TChunkList* chunkList, bool recursive)
    {
        chunkList->SetStagingTransaction(nullptr);
        chunkList->SetStagingAccount(nullptr);

        if (recursive) {
            auto transactionManager = Bootstrap->GetTransactionManager();
            for (auto* child : chunkList->Children()) {
                transactionManager->UnstageObject(child, recursive);
            }
        }
    }


    TNodePtrWithIndexList LocateChunk(TChunkPtrWithIndex chunkWithIndex)
    {
        auto* chunk = chunkWithIndex.GetPtr();
        int index = chunkWithIndex.GetIndex();

        if (ChunkReplicator_) {
            ChunkReplicator_->TouchChunk(chunk);
        }

        TNodePtrWithIndexList result;
        auto replicas = chunk->GetReplicas();
        for (auto replica : replicas) {
            if (index == AllChunkReplicasIndex || replica.GetIndex() == index) {
                result.push_back(replica);
            }
        }

        return result;
    }


    void ClearChunkList(TChunkList* chunkList)
    {
        // TODO(babenko): currently we only support clearing a chunklist with no parents.
        YCHECK(chunkList->Parents().empty());
        chunkList->IncrementVersion();

        auto objectManager = Bootstrap->GetObjectManager();
        for (auto* child : chunkList->Children()) {
            ResetChunkTreeParent(chunkList, child);
            objectManager->UnrefObject(child);
        }

        chunkList->Children().clear();
        ResetChunkListStatistics(chunkList);

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk list cleared (ChunkListId: %v)", chunkList->GetId());
    }


    TJobPtr FindJob(const TJobId& id)
    {
        return ChunkReplicator_->FindJob(id);
    }

    TJobListPtr FindJobList(TChunk* chunk)
    {
        return ChunkReplicator_->FindJobList(chunk);
    }


    void ScheduleJobs(
        TNode* node,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort,
        std::vector<TJobPtr>* jobsToRemove)
    {
        ChunkReplicator_->ScheduleJobs(
            node,
            currentJobs,
            jobsToStart,
            jobsToAbort,
            jobsToRemove);
    }


    const yhash_set<TChunk*>& LostChunks() const;
    const yhash_set<TChunk*>& LostVitalChunks() const;
    const yhash_set<TChunk*>& OverreplicatedChunks() const;
    const yhash_set<TChunk*>& UnderreplicatedChunks() const;
    const yhash_set<TChunk*>& DataMissingChunks() const;
    const yhash_set<TChunk*>& ParityMissingChunks() const;
    const yhash_set<TChunk*>& QuorumMissingChunks() const;


    int GetTotalReplicaCount()
    {
        return TotalReplicaCount_;
    }

    bool IsReplicatorEnabled()
    {
        return ChunkReplicator_->IsEnabled();
    }


    void ScheduleChunkRefresh(TChunk* chunk)
    {
        ChunkReplicator_->ScheduleChunkRefresh(chunk);
    }

    void ScheduleChunkPropertiesUpdate(TChunkTree* chunkTree)
    {
        ChunkReplicator_->SchedulePropertiesUpdate(chunkTree);
    }

    void ScheduleChunkSeal(TChunk* chunk)
    {
        ChunkSealer_->ScheduleSeal(chunk);
    }


    TChunk* GetChunkOrThrow(const TChunkId& id)
    {
        auto* chunk = FindChunk(id);
        if (!IsObjectAlive(chunk)) {
            THROW_ERROR_EXCEPTION("No such chunk %v", id);
        }
        return chunk;
    }


    TChunkTree* FindChunkTree(const TChunkTreeId& id)
    {
        auto type = TypeFromId(id);
        switch (type) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
            case EObjectType::JournalChunk:
                return FindChunk(id);
            case EObjectType::ChunkList:
                return FindChunkList(id);
            default:
                return nullptr;
        }
    }

    TChunkTree* GetChunkTree(const TChunkTreeId& id)
    {
        auto* chunkTree = FindChunkTree(id);
        YCHECK(chunkTree);
        return chunkTree;
    }

    TChunkTree* GetChunkTreeOrThrow(const TChunkTreeId& id)
    {
        auto* chunkTree = FindChunkTree(id);
        if (!IsObjectAlive(chunkTree)) {
            THROW_ERROR_EXCEPTION("No such chunk tree %v", id);
        }
        return chunkTree;
    }


    EChunkStatus ComputeChunkStatus(TChunk* chunk)
    {
        return ChunkReplicator_->ComputeChunkStatus(chunk);
    }


    void SealChunk(TChunk* chunk, const TMiscExt& info)
    {
        if (!chunk->IsJournal()) {
            THROW_ERROR_EXCEPTION("Not a journal chunk");
        }

        if (!chunk->IsConfirmed()) {
            THROW_ERROR_EXCEPTION("Chunk is not confirmed");
        }

        if (chunk->IsSealed()) {
            THROW_ERROR_EXCEPTION("Chunk is already sealed");
        }

        chunk->Seal(info);

        // Go upwards and apply delta.
        YCHECK(chunk->Parents().size() == 1);
        auto* chunkList = chunk->Parents()[0];
        
        TChunkTreeStatistics statisticsDelta;
        statisticsDelta.Sealed = true;
        statisticsDelta.RowCount = info.row_count();
        statisticsDelta.UncompressedDataSize = info.uncompressed_data_size();
        statisticsDelta.CompressedDataSize = info.compressed_data_size();
        statisticsDelta.RegularDiskSpace = info.compressed_data_size();

        VisitUniqueAncestors(
            chunkList,
            [&] (TChunkList* current) {
                ++statisticsDelta.Rank;
                current->Statistics().Accumulate(statisticsDelta);
            });

        auto owningNodes = GetOwningNodes(chunk);
        auto securityManager = Bootstrap->GetSecurityManager();
        for (auto* node : owningNodes) {
            securityManager->UpdateAccountNodeUsage(node);
        }

        if (IsLeader()) {
            ScheduleChunkRefresh(chunk);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk sealed (ChunkId: %v, RowCount: %v, UncompressedDataSize: %v, CompressedDataSize: %v)",
            chunk->GetId(),
            info.row_count(),
            info.uncompressed_data_size(),
            info.compressed_data_size());
    }

    TFuture<TErrorOr<TMiscExt>> GetChunkQuorumInfo(TChunk* chunk)
    {
        if (chunk->IsSealed()) {
            auto miscExt = GetProtoExtension<TMiscExt>(chunk->ChunkMeta().extensions());
            return MakeFuture<TErrorOr<TMiscExt>>(miscExt);
        }

        std::vector<NNodeTrackerClient::TNodeDescriptor> replicas;
        for (auto nodeWithIndex : chunk->StoredReplicas()) {
            const auto* node = nodeWithIndex.GetPtr();
            replicas.push_back(node->GetDescriptor());
        }

        return ComputeQuorumInfo(
            chunk->GetId(),
            replicas,
            Config_->JournalRpcTimeout,
            chunk->GetReadQuorum());
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Chunk, TChunk, TChunkId);
    DECLARE_ENTITY_MAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);

private:
    friend class TChunkTypeHandlerBase;
    friend class TChunkTypeHandler;
    friend class TErasureChunkTypeHandler;
    friend class TChunkListTypeHandler;

    TChunkManagerConfigPtr Config_;

    TChunkTreeBalancer ChunkTreeBalancer_;

    int TotalReplicaCount_ = 0;

    bool NeedToRecomputeStatistics_ = false;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;

    NProfiling::TProfiler Profiler;
    NProfiling::TRateCounter AddChunkCounter_;
    NProfiling::TRateCounter RemoveChunkCounter_;
    NProfiling::TRateCounter AddChunkReplicaCounter_;
    NProfiling::TRateCounter RemoveChunkReplicaCounter_;

    TChunkPlacementPtr ChunkPlacement_;
    TChunkReplicatorPtr ChunkReplicator_;
    TChunkSealerPtr ChunkSealer_;

    NHydra::TEntityMap<TChunkId, TChunk> ChunkMap_;
    NHydra::TEntityMap<TChunkListId, TChunkList> ChunkListMap_;


    void DestroyChunk(TChunk* chunk)
    {
        // Decrease staging resource usage.
        if (chunk->IsStaged()) {
            UnstageChunk(chunk);
        }

        // Cancel all jobs, reset status etc.
        if (ChunkReplicator_) {
            ChunkReplicator_->OnChunkDestroyed(chunk);
        }

        // Unregister chunk replicas from all known locations.
        auto unregisterReplica = [&] (TNodePtrWithIndex nodeWithIndex, bool cached) {
            auto* node = nodeWithIndex.GetPtr();
            TChunkPtrWithIndex chunkWithIndex(chunk, nodeWithIndex.GetIndex());
            node->RemoveReplica(chunkWithIndex, cached);
            if (ChunkReplicator_ && !cached) {
                ChunkReplicator_->ScheduleChunkRemoval(node, chunkWithIndex);
            }
        };

        for (auto replica : chunk->StoredReplicas()) {
            unregisterReplica(replica, false);
        }

        if (chunk->CachedReplicas()) {
            for (auto replica : *chunk->CachedReplicas()) {
                unregisterReplica(replica, true);
            }
        }

        Profiler.Increment(RemoveChunkCounter_);
    }

    void DestroyChunkList(TChunkList* chunkList)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        // Drop references to children.
        for (auto* child : chunkList->Children()) {
            ResetChunkTreeParent(chunkList, child);
            objectManager->UnrefObject(child);
        }
    }


    void OnNodeRegistered(TNode* node)
    {
        if (ChunkPlacement_) {
            ChunkPlacement_->OnNodeRegistered(node);
        }

        if (ChunkReplicator_) {
            ChunkReplicator_->OnNodeRegistered(node);
        }
    }

    void OnNodeUnregistered(TNode* node)
    {
        for (auto replica : node->StoredReplicas()) {
            RemoveChunkReplica(node, replica, false, ERemoveReplicaReason::NodeUnregistered);
        }

        for (auto replica : node->CachedReplicas()) {
            RemoveChunkReplica(node, replica, true, ERemoveReplicaReason::NodeUnregistered);
        }

        if (ChunkPlacement_) {
            ChunkPlacement_->OnNodeUnregistered(node);
        }

        if (ChunkReplicator_) {
            ChunkReplicator_->OnNodeUnregistered(node);
        }
    }

    void OnNodeConfigUpdated(TNode* node)
    {
        const auto& config = node->GetConfig();
        if (config->Decommissioned != node->GetDecommissioned()) {
            if (config->Decommissioned) {
                LOG_INFO_UNLESS(IsRecovery(), "Node decommissioned (Address: %v)", node->GetAddress());
            } else {
                LOG_INFO_UNLESS(IsRecovery(), "Node is no longer decommissioned (Address: %v)", node->GetAddress());
            }

            node->SetDecommissioned(config->Decommissioned);

            if (ChunkReplicator_) {
                ChunkReplicator_->ScheduleNodeRefresh(node);
            }
        }
    }

    void OnFullHeartbeat(TNode* node, const NNodeTrackerServer::NProto::TReqFullHeartbeat& request)
    {
        YCHECK(node->StoredReplicas().empty());
        YCHECK(node->CachedReplicas().empty());

        for (const auto& chunkInfo : request.chunks()) {
            ProcessAddedChunk(node, chunkInfo, false);
        }

        if (ChunkPlacement_) {
            ChunkPlacement_->OnNodeUpdated(node);
        }
    }

    void OnIncrementalHeartbeat(
        TNode* node,
        const TReqIncrementalHeartbeat& request,
        TRspIncrementalHeartbeat* /*response*/)
    {
        for (const auto& chunkInfo : request.added_chunks()) {
            ProcessAddedChunk(node, chunkInfo, true);
        }

        for (const auto& chunkInfo : request.removed_chunks()) {
            ProcessRemovedChunk(node, chunkInfo);
        }

        auto* mutationContext = Bootstrap->GetHydraFacade()->GetHydraManager()->GetMutationContext();
        auto mutationTimestamp = mutationContext->GetTimestamp();

        auto& unapprovedReplicas = node->UnapprovedReplicas();
        for (auto it = unapprovedReplicas.begin(); it != unapprovedReplicas.end();) {
            auto jt = it++;
            auto replica = jt->first;
            auto registerTimestamp = jt->second;
            auto reason = ERemoveReplicaReason::None;
            if (!IsObjectAlive(replica.GetPtr())) {
                reason = ERemoveReplicaReason::ChunkIsDead;
            } else if (mutationTimestamp > registerTimestamp + UnapprovedReplicaGracePeriod) {
                reason = ERemoveReplicaReason::FailedToApprove;
            }
            if (reason != ERemoveReplicaReason::None) {
                // This also removes replica from unapprovedReplicas.
                RemoveChunkReplica(node, replica, false, reason);
            }
        }

        if (ChunkPlacement_) {
            ChunkPlacement_->OnNodeUpdated(node);
        }
    }


    void UpdateChunkProperties(const NProto::TReqUpdateChunkProperties& request)
    {
        for (const auto& update : request.updates()) {
            auto chunkId = FromProto<TChunkId>(update.chunk_id());
            auto* chunk = FindChunk(chunkId);
            if (!IsObjectAlive(chunk))
                continue;

            if (chunk->IsStaged()) {
                LOG_WARNING("Updating properties for staged chunk %v", chunkId);
                continue;
            }

            bool changed = false;
            if (update.has_replication_factor() && chunk->GetReplicationFactor() != update.replication_factor()) {
                YCHECK(!chunk->IsErasure());
                changed = true;
                chunk->SetReplicationFactor(update.replication_factor());
            }

            if (update.has_vital() && chunk->GetVital() != update.vital()) {
                changed = true;
                chunk->SetVital(update.vital());
            }

            if (ChunkReplicator_ && changed) {
                ChunkReplicator_->ScheduleChunkRefresh(chunk);
            }
        }
    }


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        ChunkMap_.SaveKeys(context);
        ChunkListMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        ChunkMap_.SaveValues(context);
        ChunkListMap_.SaveValues(context);
    }


    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        ChunkMap_.LoadKeys(context);
        ChunkListMap_.LoadKeys(context);
        // COMPAT(babenko): required to properly initialize partial sums for chunk lists.
        if (context.GetVersion() < 100) {
            ScheduleRecomputeStatistics();
        }
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        // COMPAT(ignat)
        if (context.GetVersion() < 42) {
            NeedToRecomputeStatistics_ = true;
        }
        ChunkMap_.LoadValues(context);
        ChunkListMap_.LoadValues(context);
    }

    virtual void OnAfterSnapshotLoaded() override
    {
        // Compute chunk replica count.
        auto nodeTracker = Bootstrap->GetNodeTracker();
        TotalReplicaCount_ = 0;
        for (const auto& pair : nodeTracker->Nodes()) {
            auto* node = pair.second;
            TotalReplicaCount_ += node->StoredReplicas().size();
            TotalReplicaCount_ += node->CachedReplicas().size();
        }
    }


    virtual void Clear() override
    {
        ChunkMap_.Clear();
        ChunkListMap_.Clear();
        TotalReplicaCount_ = 0;
    }

    void ScheduleRecomputeStatistics()
    {
        NeedToRecomputeStatistics_ = true;
    }

    const TChunkTreeStatistics& ComputeStatisticsFor(TChunkList* chunkList, TAtomic visitMark)
    {
        auto& statistics = chunkList->Statistics();
        if (chunkList->GetVisitMark() != visitMark) {
            chunkList->SetVisitMark(visitMark);

            statistics = TChunkTreeStatistics();
            int childrenCount = chunkList->Children().size();

            auto& rowCountSums = chunkList->RowCountSums();
            rowCountSums.clear();

            auto& chunkCountSums = chunkList->ChunkCountSums();
            chunkCountSums.clear();

            auto& dataSizeSums = chunkList->DataSizeSums();
            dataSizeSums.clear();

            for (int childIndex = 0; childIndex < childrenCount; ++childIndex) {
                auto* child = chunkList->Children()[childIndex];
                TChunkTreeStatistics childStatistics;
                switch (child->GetType()) {
                    case EObjectType::Chunk:
                    case EObjectType::ErasureChunk:
                        childStatistics.Accumulate(child->AsChunk()->GetStatistics());
                        break;

                    case EObjectType::ChunkList:
                        childStatistics = ComputeStatisticsFor(child->AsChunkList(), visitMark);
                        break;

                    default:
                        YUNREACHABLE();
                }

                if (childIndex + 1 < childrenCount) {
                    rowCountSums.push_back(statistics.RowCount + childStatistics.RowCount);
                    chunkCountSums.push_back(statistics.ChunkCount + childStatistics.ChunkCount);
                    dataSizeSums.push_back(statistics.UncompressedDataSize + childStatistics.UncompressedDataSize);
                }

                statistics.Accumulate(childStatistics);
            }

            if (!chunkList->Children().empty()) {
                ++statistics.Rank;
            }
            ++statistics.ChunkListCount;
        }
        return statistics;
    }

    void RecomputeStatistics()
    {
        // Chunk trees traversal with memoization.

        LOG_INFO("Started recomputing statistics");

        auto mark = TChunkList::GenerateVisitMark();

        // Force all statistics to be recalculated.
        for (auto& pair : ChunkListMap_) {
            auto* chunkList = pair.second;
            ComputeStatisticsFor(chunkList, mark);
        }

        LOG_INFO("Finished recomputing statistics");
    }


    virtual void OnRecoveryStarted() override
    {
        Profiler.SetEnabled(false);

        NeedToRecomputeStatistics_ = false;

        // Reset runtime info.
        for (const auto& pair : ChunkMap_) {
            auto* chunk = pair.second;
            chunk->SetRefreshScheduled(false);
            chunk->SetPropertiesUpdateScheduled(false);
            chunk->SetSealScheduled(false);
            chunk->ResetWeakRefCounter();
            chunk->SetRepairQueueIterator(Null);
        }

        for (const auto& pair : ChunkListMap_) {
            auto* chunkList = pair.second;
            chunkList->ResetWeakRefCounter();
        }
    }

    virtual void OnRecoveryComplete() override
    {
        Profiler.SetEnabled(true);

        if (NeedToRecomputeStatistics_) {
            RecomputeStatistics();
            NeedToRecomputeStatistics_ = false;
        }
    }

    virtual void OnLeaderRecoveryComplete() override
    {
        LOG_INFO("Scheduling full chunk refresh");
        PROFILE_TIMING ("/full_chunk_refresh_schedule_time") {
            ChunkPlacement_ = New<TChunkPlacement>(Config_, Bootstrap);
            ChunkReplicator_ = New<TChunkReplicator>(Config_, Bootstrap, ChunkPlacement_);
            ChunkSealer_ = New<TChunkSealer>(Config_, Bootstrap);
        }
        LOG_INFO("Full chunk refresh scheduled");
    }

    virtual void OnLeaderActive() override
    {
        ChunkReplicator_->Start();
        ChunkSealer_->Start();
    }

    virtual void OnStopLeading() override
    {
        if (ChunkReplicator_) {
            ChunkReplicator_->Stop();
        }
    }


    DECLARE_ENUM(EAddReplicaReason,
        (IncrementalHeartbeat)
        (FullHeartbeat)
        (Confirmation)
    );

    void AddChunkReplica(TNode* node, TChunkPtrWithIndex chunkWithIndex, bool cached, EAddReplicaReason reason)
    {
        auto* chunk = chunkWithIndex.GetPtr();
        auto nodeId = node->GetId();
        TNodePtrWithIndex nodeWithIndex(node, chunkWithIndex.GetIndex());

        if (!node->AddReplica(chunkWithIndex, cached))
            return;

        chunk->AddReplica(nodeWithIndex, cached);

        if (!IsRecovery()) {
            LOG_EVENT(
                Logger,
                reason == EAddReplicaReason::FullHeartbeat ? NLog::ELogLevel::Trace : NLog::ELogLevel::Debug,
                "Chunk replica added (ChunkId: %v, Cached: %v, NodeId: %v, Address: %v)",
                chunkWithIndex,
                cached,
                nodeId,
                node->GetAddress());
        }

        if (ChunkReplicator_ && !cached) {
            ChunkReplicator_->ScheduleChunkRefresh(chunk);
        }

        if (ChunkSealer_ && !cached && chunk->IsJournal()) {
            ChunkSealer_->ScheduleSeal(chunk);
        }

        if (reason == EAddReplicaReason::IncrementalHeartbeat || reason == EAddReplicaReason::Confirmation) {
            Profiler.Increment(AddChunkReplicaCounter_);
        }
    }


    DECLARE_ENUM(ERemoveReplicaReason,
        (None)
        (IncrementalHeartbeat)
        (FailedToApprove)
        (ChunkIsDead)
        (NodeUnregistered)
    );

    void RemoveChunkReplica(TNode* node, TChunkPtrWithIndex chunkWithIndex, bool cached, ERemoveReplicaReason reason)
    {
        auto* chunk = chunkWithIndex.GetPtr();
        auto nodeId = node->GetId();
        TNodePtrWithIndex nodeWithIndex(node, chunkWithIndex.GetIndex());

        if (reason == ERemoveReplicaReason::IncrementalHeartbeat && !node->HasReplica(chunkWithIndex, cached)) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk replica is already removed (ChunkId: %v, Cached: %v, Reason: %v, NodeId: %v, Address: %v)",
                chunkWithIndex,
                cached,
                reason,
                nodeId,
                node->GetAddress());
            return;
        }

        switch (reason) {
            case ERemoveReplicaReason::IncrementalHeartbeat:
            case ERemoveReplicaReason::FailedToApprove:
            case ERemoveReplicaReason::ChunkIsDead:
                node->RemoveReplica(chunkWithIndex, cached);
                break;
            case ERemoveReplicaReason::NodeUnregistered:
                // Do nothing.
                break;
            default:
                YUNREACHABLE();
        }
        chunk->RemoveReplica(nodeWithIndex, cached);

        if (!IsRecovery()) {
            LOG_EVENT(
                Logger,
                reason == ERemoveReplicaReason::NodeUnregistered ||
                reason == ERemoveReplicaReason::ChunkIsDead
                ? NLog::ELogLevel::Trace : NLog::ELogLevel::Debug,
                "Chunk replica removed (ChunkId: %v, Cached: %v, Reason: %v, NodeId: %v, Address: %v)",
                chunkWithIndex,
                cached,
                reason,
                nodeId,
                node->GetAddress());
        }

        if (ChunkReplicator_ && !cached) {
            ChunkReplicator_->ScheduleChunkRefresh(chunk);
        }

        Profiler.Increment(RemoveChunkReplicaCounter_);
    }


    void ProcessAddedChunk(
        TNode* node,
        const TChunkAddInfo& chunkAddInfo,
        bool incremental)
    {
        auto nodeId = node->GetId();
        auto chunkId = FromProto<TChunkId>(chunkAddInfo.chunk_id());
        auto chunkIdWithIndex = DecodeChunkId(chunkId);
        bool cached = chunkAddInfo.cached();

        auto* chunk = FindChunk(chunkIdWithIndex.Id);
        if (!IsObjectAlive(chunk)) {
            if (cached) {
                // Nodes may still contain cached replicas of chunks that no longer exist.
                // We just silently ignore this case.
                return;
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Unknown chunk added, removal scheduled (NodeId: %v, Address: %v, ChunkId: %v, Cached: %v)",
                nodeId,
                node->GetAddress(),
                chunkIdWithIndex,
                cached);

            if (ChunkReplicator_) {
                ChunkReplicator_->ScheduleUnknownChunkRemoval(node, chunkIdWithIndex);
            }

            return;
        }

        int replicaIndex;
        if (chunk->IsJournal()) {
            if (chunkAddInfo.active()) {
                replicaIndex = EJournalReplicaType::Active;
            } else if (chunkAddInfo.chunk_info().sealed()) {
                replicaIndex = EJournalReplicaType::Sealed;
            } else {
                replicaIndex = EJournalReplicaType::Unsealed;
            }
        } else {
            replicaIndex = chunkIdWithIndex.Index;
        }

        TChunkPtrWithIndex chunkWithIndex(chunk, replicaIndex);
        TNodePtrWithIndex nodeWithIndex(node, replicaIndex);

        if (!cached && node->HasUnapprovedReplica(chunkWithIndex)) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk approved (NodeId: %v, Address: %v, ChunkId: %v)",
                nodeId,
                node->GetAddress(),
                chunkWithIndex);

            node->ApproveReplica(chunkWithIndex);
            chunk->ApproveReplica(nodeWithIndex);
            return;
        }

        AddChunkReplica(
            node,
            chunkWithIndex,
            cached,
            incremental ? EAddReplicaReason::IncrementalHeartbeat : EAddReplicaReason::FullHeartbeat);
    }

    void ProcessRemovedChunk(
        TNode* node,
        const TChunkRemoveInfo& chunkInfo)
    {
        auto nodeId = node->GetId();
        auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));
        bool cached = chunkInfo.cached();

        auto* chunk = FindChunk(chunkIdWithIndex.Id);
        if (!IsObjectAlive(chunk)) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Unknown chunk replica removed (ChunkId: %v, Cached: %v, Address: %v, NodeId: %v)",
                 chunkIdWithIndex,
                 cached,
                 node->GetAddress(),
                 nodeId);
            return;
        }

        TChunkPtrWithIndex chunkWithIndex(chunk, chunkIdWithIndex.Index);
        RemoveChunkReplica(
            node,
            chunkWithIndex,
            cached,
            ERemoveReplicaReason::IncrementalHeartbeat);
    }


    void OnProfiling()
    {
        if (ChunkReplicator_) {
            Profiler.Enqueue("/refresh_list_size", ChunkReplicator_->GetRefreshListSize());
            Profiler.Enqueue("/properties_update_list_size", ChunkReplicator_->GetPropertiesUpdateListSize());
        }
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TChunkManager::TImpl, Chunk, TChunk, TChunkId, ChunkMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TChunkManager::TImpl, ChunkList, TChunkList, TChunkListId, ChunkListMap_)

DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, LostChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, LostVitalChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, OverreplicatedChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, UnderreplicatedChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, DataMissingChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, ParityMissingChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, QuorumMissingChunks, *ChunkReplicator_);

///////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkTypeHandlerBase::TChunkTypeHandlerBase(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap, &owner->ChunkMap_)
    , Owner_(owner)
{ }

IObjectProxyPtr TChunkManager::TChunkTypeHandlerBase::DoGetProxy(
    TChunk* chunk,
    TTransaction* /*transaction*/)
{
    return CreateChunkProxy(Bootstrap, chunk);
}

TObjectBase* TChunkManager::TChunkTypeHandlerBase::Create(
    TTransaction* transaction,
    TAccount* account,
    IAttributeDictionary* /*attributes*/,
    TReqCreateObjects* request,
    TRspCreateObjects* response)
{
    YCHECK(transaction);
    YCHECK(account);

    account->ValidateDiskSpaceLimit();

    auto chunkType = GetType();
    bool isErasure = (chunkType == EObjectType::ErasureChunk);
    bool isJournal = (chunkType == EObjectType::JournalChunk);

    const auto& requestExt = request->GetExtension(TReqCreateChunkExt::create_chunk_ext);
    auto erasureCodecId = isErasure ? NErasure::ECodec(requestExt.erasure_codec()) : NErasure::ECodec(NErasure::ECodec::None);
    auto* erasureCodec = isErasure ? NErasure::GetCodec(erasureCodecId) : nullptr;
    int replicationFactor = isErasure ? 1 : requestExt.replication_factor();
    int readQuorum = isJournal ? requestExt.read_quorum() : 0;
    int writeQuorum = isJournal ? requestExt.write_quorum() : 0;

    auto* chunk = Owner_->CreateChunk(chunkType);
    chunk->SetReplicationFactor(replicationFactor);
    chunk->SetReadQuorum(readQuorum);
    chunk->SetWriteQuorum(writeQuorum);
    chunk->SetErasureCodec(erasureCodecId);
    chunk->SetMovable(requestExt.movable());
    chunk->SetVital(requestExt.vital());
    chunk->SetStagingTransaction(transaction);
    chunk->SetStagingAccount(account);

    LOG_DEBUG_UNLESS(Owner_->IsRecovery(),
        "Chunk created "
        "(ChunkId: %v, TransactionId: %v, Account: %v, ReplicationFactor: %v, "
        "ReadQuorum: %v, WriteQuorum: %v, ErasureCodec: %v, Movable: %v, Vital: %v)",
        chunk->GetId(),
        transaction->GetId(),
        account->GetName(),
        chunk->GetReplicationFactor(),
        chunk->GetReadQuorum(),
        chunk->GetWriteQuorum(),
        erasureCodecId,
        requestExt.movable(),
        requestExt.vital());

    if (Owner_->IsLeader()) {
        TNodeSet forbiddenNodeSet;
        TNodeList forbiddenNodeList;
        auto nodeTracker = Bootstrap->GetNodeTracker();
        for (const auto& address : requestExt.forbidden_addresses()) {
            auto* node = nodeTracker->FindNodeByAddress(address);
            if (node) {
                forbiddenNodeSet.insert(node);
                forbiddenNodeList.push_back(node);
            }
        }

        auto preferredHostName = requestExt.has_preferred_host_name()
            ? TNullable<Stroka>(requestExt.preferred_host_name())
            : Null;

        int uploadReplicationFactor = isErasure
            ? erasureCodec->GetDataPartCount() + erasureCodec->GetParityPartCount()
            : requestExt.upload_replication_factor();

        auto targets = Owner_->AllocateWriteTargets(
            uploadReplicationFactor,
            &forbiddenNodeSet,
            preferredHostName,
            chunkType);

        auto* responseExt = response->MutableExtension(TRspCreateChunkExt::create_chunk_ext);
        NNodeTrackerServer::TNodeDirectoryBuilder builder(responseExt->mutable_node_directory());
        for (int index = 0; index < static_cast<int>(targets.size()); ++index) {
            auto* target = targets[index];
            NChunkServer::TNodePtrWithIndex replica(
                target,
                isErasure ? index : 0);
            builder.Add(replica);
            responseExt->add_replicas(NYT::ToProto<ui32>(replica));
        }

        LOG_DEBUG_UNLESS(Owner_->IsRecovery(),
            "Allocated nodes for new chunk "
                "(ChunkId: %v, TransactionId: %v, Account: %v, Targets: [%v], "
                "ForbiddenAddresses: [%v], PreferredHostName: %v, ReplicationFactor: %v, "
                "UploadReplicationFactor: %v, ReadQuorum: %v, WriteQuorum: %v, "
                "ErasureCodec: %v, Movable: %v, Vital: %v)",
            chunk->GetId(),
            transaction->GetId(),
            account->GetName(),
            JoinToString(targets, TNodePtrAddressFormatter()),
            JoinToString(forbiddenNodeList, TNodePtrAddressFormatter()),
            preferredHostName,
            chunk->GetReplicationFactor(),
            uploadReplicationFactor,
            chunk->GetReadQuorum(),
            chunk->GetWriteQuorum(),
            erasureCodecId,
            requestExt.movable(),
            requestExt.vital());
    }

    return chunk;
}

void TChunkManager::TChunkTypeHandlerBase::DoDestroy(TChunk* chunk)
{
    Owner_->DestroyChunk(chunk);
}

void TChunkManager::TChunkTypeHandlerBase::DoUnstage(
    TChunk* chunk,
    bool /*recursive*/)
{
    Owner_->UnstageChunk(chunk);
}

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkListTypeHandler::TChunkListTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap, &owner->ChunkListMap_)
    , Owner_(owner)
{ }

IObjectProxyPtr TChunkManager::TChunkListTypeHandler::DoGetProxy(
    TChunkList* chunkList,
    TTransaction* /*transaction*/)
{
    return CreateChunkListProxy(Bootstrap, chunkList);
}

TObjectBase* TChunkManager::TChunkListTypeHandler::Create(
    TTransaction* transaction,
    TAccount* account,
    IAttributeDictionary* /*attributes*/,
    TReqCreateObjects* /*request*/,
    TRspCreateObjects* /*response*/)
{
    auto* chunkList = Owner_->CreateChunkList();
    chunkList->SetStagingTransaction(transaction);
    chunkList->SetStagingAccount(account);
    return chunkList;
}

void TChunkManager::TChunkListTypeHandler::DoDestroy(TChunkList* chunkList)
{
    Owner_->DestroyChunkList(chunkList);
}

void TChunkManager::TChunkListTypeHandler::DoUnstage(
    TChunkList* chunkList,
    bool recursive)
{
    Owner_->UnstageChunkList(chunkList, recursive);
}

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TChunkManager::~TChunkManager()
{ }

void TChunkManager::Initialize()
{
    Impl_->Initialize();
}

TChunk* TChunkManager::GetChunkOrThrow(const TChunkId& id)
{
    return Impl_->GetChunkOrThrow(id);
}

TChunkTree* TChunkManager::FindChunkTree(const TChunkTreeId& id)
{
    return Impl_->FindChunkTree(id);
}

TChunkTree* TChunkManager::GetChunkTree(const TChunkTreeId& id)
{
    return Impl_->GetChunkTree(id);
}

TChunkTree* TChunkManager::GetChunkTreeOrThrow(const TChunkTreeId& id)
{
    return Impl_->GetChunkTreeOrThrow(id);
}

TNodeList TChunkManager::AllocateWriteTargets(
    int replicaCount,
    const TNodeSet* forbiddenNodes,
    const TNullable<Stroka>& preferredHostName,
    EObjectType chunkType)
{
    return Impl_->AllocateWriteTargets(
        replicaCount,
        forbiddenNodes,
        preferredHostName,
        chunkType);
}

TMutationPtr TChunkManager::CreateUpdateChunkPropertiesMutation(
    const NProto::TReqUpdateChunkProperties& request)
{
    return Impl_->CreateUpdateChunkPropertiesMutation(request);
}

TChunk* TChunkManager::CreateChunk(EObjectType type)
{
    return Impl_->CreateChunk(type);
}

TChunkList* TChunkManager::CreateChunkList()
{
    return Impl_->CreateChunkList();
}

void TChunkManager::ConfirmChunk(
    TChunk* chunk,
    const std::vector<NChunkClient::TChunkReplica>& replicas,
    TChunkInfo* chunkInfo,
    TChunkMeta* chunkMeta)
{
    Impl_->ConfirmChunk(
        chunk,
        replicas,
        chunkInfo,
        chunkMeta);
}

void TChunkManager::UnstageChunk(TChunk* chunk)
{
    Impl_->UnstageChunk(chunk);
}

void TChunkManager::UnstageChunkList(TChunkList* chunkList, bool recursive)
{
    Impl_->UnstageChunkList(chunkList, recursive);
}

TNodePtrWithIndexList TChunkManager::LocateChunk(TChunkPtrWithIndex chunkWithIndex)
{
    return Impl_->LocateChunk(chunkWithIndex);
}

void TChunkManager::AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree** childrenBegin,
    TChunkTree** childrenEnd)
{
    Impl_->AttachToChunkList(chunkList, childrenBegin, childrenEnd);
}

void TChunkManager::AttachToChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children)
{
    Impl_->AttachToChunkList(chunkList, children);
}

void TChunkManager::AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree* child)
{
    Impl_->AttachToChunkList(chunkList, child);
}

void TChunkManager::DetachFromChunkList(
    TChunkList* chunkList,
    TChunkTree** childrenBegin,
    TChunkTree** childrenEnd)
{
    Impl_->DetachFromChunkList(chunkList, childrenBegin, childrenEnd);
}

void TChunkManager::DetachFromChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children)
{
    Impl_->DetachFromChunkList(chunkList, children);
}

void TChunkManager::DetachFromChunkList(
    TChunkList* chunkList,
    TChunkTree* child)
{
    Impl_->DetachFromChunkList(chunkList, child);
}

void TChunkManager::RebalanceChunkTree(TChunkList* chunkList)
{
    Impl_->RebalanceChunkTree(chunkList);
}

void TChunkManager::ClearChunkList(TChunkList* chunkList)
{
    Impl_->ClearChunkList(chunkList);
}

TJobPtr TChunkManager::FindJob(const TJobId& id)
{
    return Impl_->FindJob(id);
}

TJobListPtr TChunkManager::FindJobList(TChunk* chunk)
{
    return Impl_->FindJobList(chunk);
}

void TChunkManager::ScheduleJobs(
    TNode* node,
    const std::vector<TJobPtr>& currentJobs,
    std::vector<TJobPtr>* jobsToStart,
    std::vector<TJobPtr>* jobsToAbort,
    std::vector<TJobPtr>* jobsToRemove)
{
    Impl_->ScheduleJobs(
        node,
        currentJobs,
        jobsToStart,
        jobsToAbort,
        jobsToRemove);
}

bool TChunkManager::IsReplicatorEnabled()
{
    return Impl_->IsReplicatorEnabled();
}

void TChunkManager::ScheduleChunkRefresh(TChunk* chunk)
{
    Impl_->ScheduleChunkRefresh(chunk);
}

void TChunkManager::ScheduleChunkPropertiesUpdate(TChunkTree* chunkTree)
{
    Impl_->ScheduleChunkPropertiesUpdate(chunkTree);
}

void TChunkManager::ScheduleChunkSeal(TChunk* chunk)
{
    Impl_->ScheduleChunkSeal(chunk);
}

int TChunkManager::GetTotalReplicaCount()
{
    return Impl_->GetTotalReplicaCount();
}

EChunkStatus TChunkManager::ComputeChunkStatus(TChunk* chunk)
{
    return Impl_->ComputeChunkStatus(chunk);
}

void TChunkManager::SealChunk(TChunk* chunk, const TMiscExt& info)
{
    Impl_->SealChunk(chunk, info);
}

TFuture<TErrorOr<TMiscExt>> TChunkManager::GetChunkQuorumInfo(TChunk* chunk)
{
    return Impl_->GetChunkQuorumInfo(chunk);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TChunkManager, Chunk, TChunk, TChunkId, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TChunkManager, ChunkList, TChunkList, TChunkListId, *Impl_)

DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, LostChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, LostVitalChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, OverreplicatedChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, UnderreplicatedChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, DataMissingChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, ParityMissingChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, QuorumMissingChunks, *Impl_);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

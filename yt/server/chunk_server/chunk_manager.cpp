#include "stdafx.h"
#include "chunk_manager.h"
#include "config.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_owner_base.h"
#include "job.h"
#include "chunk_placement.h"
#include "chunk_replicator.h"
#include "chunk_tree_balancer.h"
#include "chunk_proxy.h"
#include "chunk_list_proxy.h"
#include "node_directory_builder.h"
#include "private.h"
#include "helpers.h"

#include <core/misc/string.h>
#include <core/misc/collection_helpers.h>

#include <core/compression/codec.h>

#include <core/erasure/codec.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/chunk_client/chunk_ypath.pb.h>
#include <ytlib/chunk_client/chunk_list_ypath.pb.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/schema.h>

#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/table_client/table_ypath.pb.h>

#include <ytlib/meta_state/meta_state_manager.h>
#include <ytlib/meta_state/composite_meta_state.h>
#include <ytlib/meta_state/map.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

#include <server/chunk_server/chunk_manager.pb.h>

#include <server/node_tracker_server/node_tracker.h>

#include <server/cypress_server/cypress_manager.h>

#include <server/cell_master/serialization_context.h>
#include <server/cell_master/bootstrap.h>
#include <server/cell_master/meta_state_facade.h>

#include <server/transaction_server/transaction_manager.h>
#include <server/transaction_server/transaction.h>

#include <server/object_server/type_handler_detail.h>

#include <server/security_server/security_manager.h>
#include <server/security_server/account.h>
#include <server/security_server/group.h>

namespace NYT {
namespace NChunkServer {

using namespace NRpc;
using namespace NMetaState;
using namespace NNodeTrackerServer;
using namespace NTransactionServer;
using namespace NTransactionClient;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NYTree;
using namespace NCellMaster;
using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = ChunkServerLogger;
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
            EObjectAccountMode::Required,
            true);
    }

    virtual TObjectBase* Create(
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObjects* request,
        TRspCreateObjects* response) override;

protected:
    TImpl* Owner;

    virtual IObjectProxyPtr DoGetProxy(TChunk* chunk, TTransaction* transaction) override;

    virtual void DoDestroy(TChunk* chunk) override;

    virtual void DoUnstage(TChunk* chunk, TTransaction* transaction, bool recursive) override;

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
        return Sprintf("chunk %s", ~ToString(chunk->GetId()));
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
        return Sprintf("erasure chunk %s", ~ToString(chunk->GetId()));
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
            EObjectAccountMode::Forbidden,
            true);
    }

    virtual TObjectBase* Create(
        TTransaction* transaction,
        TAccount* account,
        IAttributeDictionary* attributes,
        TReqCreateObjects* request,
        TRspCreateObjects* response) override;

private:
    TImpl* Owner;

    virtual Stroka DoGetName(TChunkList* chunkList) override
    {
        return Sprintf("chunk list %s", ~ToString(chunkList->GetId()));
    }

    virtual IObjectProxyPtr DoGetProxy(TChunkList* chunkList, TTransaction* transaction) override;

    virtual void DoDestroy(TChunkList* chunkList) override;

    virtual void DoUnstage(TChunkList* chunkList, TTransaction* transaction, bool recursive) override;

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TImpl
    : public TMetaStatePart
{
public:
    TImpl(
        TChunkManagerConfigPtr config,
        TBootstrap* bootstrap)
        : TMetaStatePart(
            bootstrap->GetMetaStateFacade()->GetManager(),
            bootstrap->GetMetaStateFacade()->GetState())
        , Config(config)
        , Bootstrap(bootstrap)
        , ChunkTreeBalancer(Bootstrap)
        , TotalReplicaCount(0)
        , NeedToRecomputeStatistics(false)
        , Profiler(ChunkServerProfiler)
        , AddChunkCounter("/add_chunk_rate")
        , RemoveChunkCounter("/remove_chunk_rate")
        , AddChunkReplicaCounter("/add_chunk_replica_rate")
        , RemoveChunkReplicaCounter("/remove_chunk_replica_rate")
    {
        YCHECK(config);
        YCHECK(bootstrap);

        RegisterMethod(BIND(&TImpl::UpdateChunkProperties, Unretained(this)));

        {
            NCellMaster::TLoadContext context;
            context.SetBootstrap(Bootstrap);

            RegisterLoader(
                "ChunkManager.Keys",
                SnapshotVersionValidator(),
                BIND(&TImpl::LoadKeys, MakeStrong(this)),
                context);
            RegisterLoader(
                "ChunkManager.Values",
                SnapshotVersionValidator(),
                BIND(&TImpl::LoadValues, MakeStrong(this)),
                context);
        }

        {
            NCellMaster::TSaveContext context;

            RegisterSaver(
                ESerializationPriority::Keys,
                "ChunkManager.Keys",
                GetCurrentSnapshotVersion(),
                BIND(&TImpl::SaveKeys, MakeStrong(this)),
                context);
            RegisterSaver(
                ESerializationPriority::Values,
                "ChunkManager.Values",
                GetCurrentSnapshotVersion(),
                BIND(&TImpl::SaveValues, MakeStrong(this)),
                context);
        }
    }

    void Initialize()
    {
        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->RegisterHandler(New<TChunkTypeHandler>(this));
        objectManager->RegisterHandler(New<TErasureChunkTypeHandler>(this));
        objectManager->RegisterHandler(New<TChunkListTypeHandler>(this));

        auto nodeTracker = Bootstrap->GetNodeTracker();
        nodeTracker->SubscribeNodeRegistered(BIND(&TThis::OnNodeRegistered, MakeWeak(this)));
        nodeTracker->SubscribeNodeUnregistered(BIND(&TThis::OnNodeUnregistered, MakeWeak(this)));
        nodeTracker->SubscribeNodeConfigUpdated(BIND(&TThis::OnNodeConfigUpdated, MakeWeak(this)));
        nodeTracker->SubscribeFullHeartbeat(BIND(&TThis::OnFullHeartbeat, MakeWeak(this)));
        nodeTracker->SubscribeIncrementalHeartbeat(BIND(&TThis::OnIncrementalHeartbeat, MakeWeak(this)));

        ProfilingExecutor = New<TPeriodicExecutor>(
            Bootstrap->GetMetaStateFacade()->GetInvoker(),
            BIND(&TThis::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor->Start();
    }


    TMutationPtr CreateUpdateChunkPropertiesMutation(
        const NProto::TMetaReqUpdateChunkProperties& request)
    {
        return Bootstrap
            ->GetMetaStateFacade()
            ->CreateMutation(this, request, &TThis::UpdateChunkProperties);
    }


    DECLARE_METAMAP_ACCESSORS(Chunk, TChunk, TChunkId);
    DECLARE_METAMAP_ACCESSORS(ChunkList, TChunkList, TChunkListId);

    TNodeList AllocateWriteTargets(
        int replicaCount,
        const TNullable<Stroka>& preferredHostName)
    {
        return ChunkPlacement->AllocateWriteTargets(
            replicaCount,
            nullptr,
            preferredHostName,
            EWriteSessionType::User);
    }

    TChunk* CreateChunk(EObjectType type)
    {
        Profiler.Increment(AddChunkCounter);
        auto id = Bootstrap->GetObjectManager()->GenerateId(type);
        auto* chunk = new TChunk(id);
        ChunkMap.Insert(id, chunk);
        return chunk;
    }

    TChunkList* CreateChunkList()
    {
        auto id = Bootstrap->GetObjectManager()->GenerateId(EObjectType::ChunkList);
        auto* chunkList = new TChunkList(id);
        ChunkListMap.Insert(id, chunkList);
        return chunkList;
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree** childrenBegin,
        TChunkTree** childrenEnd,
        bool resetSorted)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        NChunkServer::AttachToChunkList(
            chunkList,
            childrenBegin,
            childrenEnd,
            [&] (TChunkTree* chunk) {
                objectManager->RefObject(chunk);
            },
            resetSorted);
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children,
        bool resetSorted)
    {
        AttachToChunkList(
            chunkList,
            const_cast<TChunkTree**>(children.data()),
            const_cast<TChunkTree**>(children.data() + children.size()),
            resetSorted);
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* child,
        bool resetSorted)
    {
        AttachToChunkList(
            chunkList,
            &child,
            &child + 1,
            resetSorted);
    }


    void RebalanceChunkTree(TChunkList* chunkList)
    {
        if (!ChunkTreeBalancer.IsRebalanceNeeded(chunkList))
            return;

        PROFILE_TIMING ("/chunk_tree_rebalance_time") {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree rebalancing started (RootId: %s)",
                ~ToString(chunkList->GetId()));
            ChunkTreeBalancer.Rebalance(chunkList);
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree rebalancing completed");
        }
    }


    void ConfirmChunk(
        TChunk* chunk,
        const std::vector<NChunkClient::TChunkReplica>& replicas,
        NChunkClient::NProto::TChunkInfo* chunkInfo,
        NChunkClient::NProto::TChunkMeta* chunkMeta)
    {
        YCHECK(!chunk->IsConfirmed());

        const auto& id = chunk->GetId();

        chunk->Confirm(chunkInfo, chunkMeta);

        auto nodeTracker = Bootstrap->GetNodeTracker();

        auto* mutationContext = Bootstrap->GetMetaStateFacade()->GetManager()->GetMutationContext();
        auto mutationTimestamp = mutationContext->GetTimestamp();

        FOREACH (auto clientReplica, replicas) {
            auto* node = nodeTracker->FindNode(clientReplica.GetNodeId());
            if (!node) {
                LOG_DEBUG_UNLESS(IsRecovery(), "Tried to confirm chunk %s at an unknown node %d",
                    ~ToString(id),
                    ~clientReplica.GetNodeId());
                continue;
            }

            TNodePtrWithIndex nodeWithIndex(node, clientReplica.GetIndex());
            TChunkPtrWithIndex chunkWithIndex(chunk, clientReplica.GetIndex());

            if (node->GetState() != ENodeState::Online) {
                LOG_DEBUG_UNLESS(IsRecovery(), "Tried to confirm chunk %s at %s which has invalid state %s",
                    ~ToString(id),
                    ~node->GetAddress(),
                    ~FormatEnum(node->GetState()).Quote());
                continue;
            }

            if (!node->HasReplica(chunkWithIndex, false)) {
                AddChunkReplica(
                    node,
                    chunkWithIndex,
                    false,
                    EAddReplicaReason::Confirmation);
                node->MarkReplicaUnapproved(chunkWithIndex, mutationTimestamp);
            }
        }

        // Increase staged resource usage.
        if (chunk->IsStaged()) {
            auto* stagingTransaction = chunk->GetStagingTransaction();
            auto* stagingAccount = chunk->GetStagingAccount();
            auto securityManager = Bootstrap->GetSecurityManager();
            auto delta = chunk->GetResourceUsage();
            securityManager->UpdateAccountStagingUsage(stagingTransaction, stagingAccount, delta);
        }

        if (ChunkReplicator) {
            ChunkReplicator->ScheduleChunkRefresh(chunk);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk confirmed (ChunkId: %s)", ~ToString(id));
    }

    TNodePtrWithIndexList LocateChunk(TChunkPtrWithIndex chunkWithIndex)
    {
        auto* chunk = chunkWithIndex.GetPtr();
        int index = chunkWithIndex.GetIndex();

        if (ChunkReplicator) {
            ChunkReplicator->TouchChunk(chunk);
        }

        TNodePtrWithIndexList result;
        auto replicas = chunk->GetReplicas();
        FOREACH (auto replica, replicas) {
            if (index == GenericChunkPartIndex || replica.GetIndex() == index) {
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
        FOREACH (auto* child, chunkList->Children()) {
            ResetChunkTreeParent(chunkList, child);
            objectManager->UnrefObject(child);
        }
        chunkList->Children().clear();
        chunkList->RowCountSums().clear();
        chunkList->ChunkCountSums().clear();
        chunkList->DataSizeSums().clear();
        chunkList->Statistics() = TChunkTreeStatistics();
        chunkList->Statistics().ChunkListCount = 1;

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk list cleared (ChunkListId: %s)", ~ToString(chunkList->GetId()));
    }

    void ResetChunkTreeParent(TChunkList* parent, TChunkTree* child)
    {
        switch (child->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk: {
                auto& parents = child->AsChunk()->Parents();
                auto it = std::find(parents.begin(), parents.end(), parent);
                YASSERT(it != parents.end());
                parents.erase(it);
                break;
            }
            case EObjectType::ChunkList: {
                auto& parents = child->AsChunkList()->Parents();
                auto it = parents.find(parent);
                YASSERT(it != parents.end());
                parents.erase(it);
                break;
            }
            default:
                YUNREACHABLE();
        }
    }


    TJobPtr FindJob(const TJobId& id)
    {
        return ChunkReplicator->FindJob(id);
    }

    TJobListPtr FindJobList(TChunk* chunk)
    {
        return ChunkReplicator->FindJobList(chunk);
    }


    void ScheduleJobs(
        TNode* node,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort,
        std::vector<TJobPtr>* jobsToRemove)
    {
        ChunkReplicator->ScheduleJobs(
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


    int GetTotalReplicaCount()
    {
        return TotalReplicaCount;
    }

    bool IsReplicatorEnabled()
    {
        return ChunkReplicator->IsEnabled();
    }


    void SchedulePropertiesUpdate(TChunkTree* chunkTree)
    {
        ChunkReplicator->SchedulePropertiesUpdate(chunkTree);
    }


    TChunk* GetChunkOrThrow(const TChunkId& id)
    {
        auto* chunk = FindChunk(id);
        if (!IsObjectAlive(chunk)) {
            THROW_ERROR_EXCEPTION("No such chunk %s", ~ToString(id));
        }
        return chunk;
    }


    TChunkTree* FindChunkTree(const TChunkTreeId& id)
    {
        auto type = TypeFromId(id);
        switch (type) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
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
            THROW_ERROR_EXCEPTION("No such chunk tree %s", ~ToString(id));
        }
        return chunkTree;
    }


    EChunkStatus ComputeChunkStatus(TChunk* chunk)
    {
        return ChunkReplicator->ComputeChunkStatus(chunk);
    }

private:
    typedef TImpl TThis;
    friend class TChunkTypeHandlerBase;
    friend class TChunkTypeHandler;
    friend class TErasureChunkTypeHandler;
    friend class TChunkListTypeHandler;

    TChunkManagerConfigPtr Config;
    TBootstrap* Bootstrap;

    TChunkTreeBalancer ChunkTreeBalancer;

    int TotalReplicaCount;

    bool NeedToRecomputeStatistics;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor;

    NProfiling::TProfiler Profiler;
    NProfiling::TRateCounter AddChunkCounter;
    NProfiling::TRateCounter RemoveChunkCounter;
    NProfiling::TRateCounter AddChunkReplicaCounter;
    NProfiling::TRateCounter RemoveChunkReplicaCounter;

    TChunkPlacementPtr ChunkPlacement;
    TChunkReplicatorPtr ChunkReplicator;

    TMetaStateMap<TChunkId, TChunk> ChunkMap;
    TMetaStateMap<TChunkListId, TChunkList> ChunkListMap;

    void DestroyChunk(TChunk* chunk)
    {
        // Decrease staging resource usage.
        if (chunk->IsStaged()) {
            UnstageChunk(chunk);
        }

        // Cancel all jobs, reset status etc.
        if (ChunkReplicator) {
            ChunkReplicator->OnChunkDestroyed(chunk);
        }

        // Unregister chunk replicas from all known locations.
        auto unregisterReplica = [&] (TNodePtrWithIndex nodeWithIndex, bool cached) {
            auto* node = nodeWithIndex.GetPtr();
            TChunkPtrWithIndex chunkWithIndex(chunk, nodeWithIndex.GetIndex());
            node->RemoveReplica(chunkWithIndex, cached);
            if (ChunkReplicator && !cached) {
                ChunkReplicator->ScheduleChunkRemoval(node, chunkWithIndex);
            }
        };

        FOREACH (auto replica, chunk->StoredReplicas()) {
            unregisterReplica(replica, false);
        }

        if (~chunk->CachedReplicas()) {
            FOREACH (auto replica, *chunk->CachedReplicas()) {
                unregisterReplica(replica, true);
            }
        }

        Profiler.Increment(RemoveChunkCounter);
    }

    void DestroyChunkList(TChunkList* chunkList)
    {
        auto objectManager = Bootstrap->GetObjectManager();
        // Drop references to children.
        FOREACH (auto* child, chunkList->Children()) {
            ResetChunkTreeParent(chunkList, child);
            objectManager->UnrefObject(child);
        }
    }


    void UnstageChunk(TChunk* chunk)
    {
        if (chunk->IsStaged() && chunk->IsConfirmed()) {
            auto* stagingTransaction = chunk->GetStagingTransaction();
            auto* stagingAccount = chunk->GetStagingAccount();
            auto securityManager = Bootstrap->GetSecurityManager();
            auto delta = -chunk->GetResourceUsage();
            securityManager->UpdateAccountStagingUsage(stagingTransaction, stagingAccount, delta);
        }

        chunk->SetStagingTransaction(nullptr);
        chunk->SetStagingAccount(nullptr);
    }

    void UnstageChunkList(
        TChunkList* chunkList,
        TTransaction* transaction,
        bool recursive)
    {
        if (!recursive)
            return;

        auto transactionManager = Bootstrap->GetTransactionManager();
        FOREACH (auto* child, chunkList->Children()) {
            transactionManager->UnstageObject(transaction, child, true);
        }
    }


    void OnNodeRegistered(TNode* node)
    {
        if (ChunkPlacement) {
            ChunkPlacement->OnNodeRegistered(node);
        }

        if (ChunkReplicator) {
            ChunkReplicator->OnNodeRegistered(node);
        }
    }

    void OnNodeUnregistered(TNode* node)
    {
        FOREACH (auto replica, node->StoredReplicas()) {
            RemoveChunkReplica(node, replica, false, ERemoveReplicaReason::NodeUnregistered);
        }

        FOREACH (auto replica, node->CachedReplicas()) {
            RemoveChunkReplica(node, replica, true, ERemoveReplicaReason::NodeUnregistered);
        }

        if (ChunkPlacement) {
            ChunkPlacement->OnNodeUnregistered(node);
        }

        if (ChunkReplicator) {
            ChunkReplicator->OnNodeUnregistered(node);
        }
    }

    void OnNodeConfigUpdated(TNode* node)
    {
        const auto& config = node->GetConfig();
        if (config->Decommissioned != node->GetDecommissioned()) {
            if (config->Decommissioned) {
                LOG_INFO_UNLESS(IsRecovery(), "Node decommissioned (Address: %s)", ~node->GetAddress());
            } else {
                LOG_INFO_UNLESS(IsRecovery(), "Node is no longer decommissioned (Address: %s)", ~node->GetAddress());
            }

            node->SetDecommissioned(config->Decommissioned);

            if (ChunkReplicator) {
                ChunkReplicator->ScheduleNodeRefresh(node);
            }
        }
    }

    void OnFullHeartbeat(TNode* node, const NNodeTrackerServer::NProto::TMetaReqFullHeartbeat& request)
    {
        YCHECK(node->StoredReplicas().empty());
        YCHECK(node->CachedReplicas().empty());

        FOREACH (const auto& chunkInfo, request.chunks()) {
            ProcessAddedChunk(node, chunkInfo, false);
        }

        if (ChunkPlacement) {
            ChunkPlacement->OnNodeUpdated(node);
        }
    }

    void OnIncrementalHeartbeat(TNode* node, const NNodeTrackerServer::NProto::TMetaReqIncrementalHeartbeat& request)
    {
        // Shrink hashtables.
        // NB: Skip StoredReplicas, these are typically huge.
        ShrinkHashTable(&node->CachedReplicas());
        ShrinkHashTable(&node->UnapprovedReplicas());
        ShrinkHashTable(&node->Jobs());
        for (auto& queue : node->ChunkReplicationQueues()) {
            ShrinkHashTable(&queue);
        }
        ShrinkHashTable(&node->ChunkRemovalQueue());
        // TODO(babenko): add after merge into master
        // ShrinkHashTable(&node->ChunkSealQueue());

        FOREACH (const auto& chunkInfo, request.added_chunks()) {
            ProcessAddedChunk(node, chunkInfo, true);
        }

        FOREACH (const auto& chunkInfo, request.removed_chunks()) {
            ProcessRemovedChunk(node, chunkInfo);
        }

        auto* mutationContext = Bootstrap->GetMetaStateFacade()->GetManager()->GetMutationContext();
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

        if (ChunkPlacement) {
            ChunkPlacement->OnNodeUpdated(node);
        }
    }


    void UpdateChunkProperties(const NProto::TMetaReqUpdateChunkProperties& request)
    {
        FOREACH (const auto& update, request.updates()) {
            auto chunkId = FromProto<TChunkId>(update.chunk_id());
            auto* chunk = FindChunk(chunkId);
            if (!IsObjectAlive(chunk))
                continue;

            if (chunk->IsStaged()) {
                LOG_WARNING("Updating properties for staged chunk %s", ~ToString(chunkId));
                continue;
            }

            bool hasChanged = false;
            if (update.has_replication_factor() && chunk->GetReplicationFactor() != update.replication_factor()) {
                YCHECK(!chunk->IsErasure());
                hasChanged = true;
                chunk->SetReplicationFactor(update.replication_factor());
            }

            if (update.has_vital() && chunk->GetVital() != update.vital()) {
                hasChanged = true;
                chunk->SetVital(update.vital());
            }

            if (ChunkReplicator && hasChanged) {
                ChunkReplicator->ScheduleChunkRefresh(chunk);
            }
        }
    }


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        ChunkMap.SaveKeys(context);
        ChunkListMap.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        ChunkMap.SaveValues(context);
        ChunkListMap.SaveValues(context);
    }


    virtual void OnBeforeLoaded() override
    {
        DoClear();
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        ChunkMap.LoadKeys(context);
        ChunkListMap.LoadKeys(context);
        // COMPAT(babenko)
        if (context.GetVersion() < 20) {
            size_t nodeCount = TSizeSerializer::Load(context);
            YCHECK(nodeCount == 0);

            size_t jobCount = TSizeSerializer::Load(context);
            YCHECK(jobCount == 0);

            size_t jobListCount = TSizeSerializer::Load(context);
            YCHECK(jobListCount == 0);

            // COMPAT(psushin): required to properly initialize TChunkList::DataSizeSums.
            ScheduleRecomputeStatistics();
        }
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        // COMPAT(psushin)
        if (context.GetVersion() < 20) {
            // Skip NodeIdGenerator.
            context.GetInput()->Skip(8);
        }
        // COMPAT(ignat)
        if (context.GetVersion() < 45) {
            NeedToRecomputeStatistics = true;
        }
        ChunkMap.LoadValues(context);
        ChunkListMap.LoadValues(context);
    }

    virtual void OnAfterLoaded() override
    {
        // Compute chunk replica count.
        auto nodeTracker = Bootstrap->GetNodeTracker();
        TotalReplicaCount = 0;
        FOREACH (auto* node, nodeTracker->GetNodes()) {
            TotalReplicaCount += node->StoredReplicas().size();
            TotalReplicaCount += node->CachedReplicas().size();
        }
    }


    void DoClear()
    {
        ChunkMap.Clear();
        ChunkListMap.Clear();
        TotalReplicaCount = 0;
    }

    virtual void Clear() override
    {
        DoClear();
    }

    void ScheduleRecomputeStatistics()
    {
        NeedToRecomputeStatistics = true;
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
        FOREACH (auto& pair, ChunkListMap) {
            auto* chunkList = pair.second;
            ComputeStatisticsFor(chunkList, mark);
        }

        LOG_INFO("Finished recomputing statistics");
    }


    virtual void OnRecoveryStarted() override
    {
        Profiler.SetEnabled(false);

        NeedToRecomputeStatistics = false;

        // Reset runtime info.
        FOREACH (const auto& pair, ChunkMap) {
            auto* chunk = pair.second;
            chunk->SetRefreshScheduled(false);
            chunk->SetPropertiesUpdateScheduled(false);
            chunk->ResetWeakRefCounter();
            chunk->SetRepairQueueIterator(TChunkRepairQueueIterator());
        }

        FOREACH (const auto& pair, ChunkListMap) {
            auto* chunkList = pair.second;
            chunkList->ResetWeakRefCounter();
        }
    }

    virtual void OnRecoveryComplete() override
    {
        Profiler.SetEnabled(true);

        if (NeedToRecomputeStatistics) {
            RecomputeStatistics();
            NeedToRecomputeStatistics = false;
        }
    }

    virtual void OnLeaderRecoveryComplete() override
    {
        LOG_INFO("Full chunk refresh started");
        PROFILE_TIMING ("/full_chunk_refresh_time") {
            YCHECK(!ChunkPlacement);
            ChunkPlacement = New<TChunkPlacement>(Config, Bootstrap);
            ChunkPlacement->Initialize();

            YCHECK(!ChunkReplicator);
            ChunkReplicator = New<TChunkReplicator>(Config, Bootstrap, ChunkPlacement);
            ChunkReplicator->Initialize();
        }
        LOG_INFO("Full chunk refresh completed");
    }

    virtual void OnStopLeading() override
    {
        ChunkPlacement.Reset();

        if (ChunkReplicator) {
            ChunkReplicator->Finalize();
            ChunkReplicator.Reset();
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

        if (node->HasReplica(chunkWithIndex, cached)) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk replica is already added (ChunkId: %s, Cached: %s, Reason: %s, NodeId: %d, Address: %s)",
                ~ToString(chunkWithIndex),
                ~FormatBool(cached),
                ~reason.ToString(),
                nodeId,
                ~node->GetAddress());
            return;
        }

        node->AddReplica(chunkWithIndex, cached);
        chunk->AddReplica(nodeWithIndex, cached);

        if (!IsRecovery()) {
            LOG_EVENT(
                Logger,
                reason == EAddReplicaReason::FullHeartbeat ? NLog::ELogLevel::Trace : NLog::ELogLevel::Debug,
                "Chunk replica added (ChunkId: %s, Cached: %s, NodeId: %d, Address: %s)",
                ~ToString(chunkWithIndex),
                ~FormatBool(cached),
                nodeId,
                ~node->GetAddress());
        }

        if (ChunkReplicator && !cached) {
            ChunkReplicator->ScheduleChunkRefresh(chunkWithIndex.GetPtr());
        }

        if (reason == EAddReplicaReason::IncrementalHeartbeat || reason == EAddReplicaReason::Confirmation) {
            Profiler.Increment(AddChunkReplicaCounter);
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
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk replica is already removed (ChunkId: %s, Cached: %s, Reason: %s, NodeId: %d, Address: %s)",
                ~ToString(chunkWithIndex),
                ~FormatBool(cached),
                ~reason.ToString(),
                nodeId,
                ~node->GetAddress());
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
                "Chunk replica removed (ChunkId: %s, Cached: %s, Reason: %s, NodeId: %d, Address: %s)",
                ~ToString(chunkWithIndex),
                ~FormatBool(cached),
                ~reason.ToString(),
                nodeId,
                ~node->GetAddress());
        }

        if (ChunkReplicator && !cached) {
            ChunkReplicator->ScheduleChunkRefresh(chunk);
        }

        Profiler.Increment(RemoveChunkReplicaCounter);
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

            LOG_DEBUG_UNLESS(IsRecovery(), "Unknown chunk added, removal scheduled (NodeId: %d, Address: %s, ChunkId: %s, Cached: %s)",
                nodeId,
                ~node->GetAddress(),
                ~ToString(chunkIdWithIndex),
                ~FormatBool(cached));

            if (ChunkReplicator) {
                ChunkReplicator->ScheduleUnknownChunkRemoval(node, chunkIdWithIndex);
            }

            return;
        }

        TChunkPtrWithIndex chunkWithIndex(chunk, chunkIdWithIndex.Index);
        if (!cached && node->HasUnapprovedReplica(chunkWithIndex)) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk approved (NodeId: %d, Address: %s, ChunkId: %s)",
                nodeId,
                ~node->GetAddress(),
                ~ToString(chunkWithIndex));

            node->ApproveReplica(chunkWithIndex);
            return;
        }

        // Use the size reported by the node, but check it for consistency first.
        if (!chunk->IsErasure()) {
            if (!chunk->ValidateChunkInfo(chunkAddInfo.chunk_info())) {
                auto error = TError("Mismatched chunk info reported by node (ChunkId: %s, Cached: %s, ExpectedInfo: {%s}, ReceivedInfo: {%s}, NodeId: %d, Address: %s)",
                    ~ToString(chunkWithIndex),
                    ~FormatBool(cached),
                    ~chunk->ChunkInfo().DebugString(),
                    ~chunkAddInfo.chunk_info().DebugString(),
                    nodeId,
                    ~node->GetAddress());
                LOG_ERROR(error);
                // TODO(babenko): return error to node
                return;
            }
            chunk->ChunkInfo() = chunkAddInfo.chunk_info();
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
            LOG_DEBUG_UNLESS(IsRecovery(), "Unknown chunk replica removed (ChunkId: %s, Cached: %s, Address: %s, NodeId: %d)",
                 ~ToString(chunkIdWithIndex),
                 ~FormatBool(cached),
                 ~node->GetAddress(),
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
        if (ChunkReplicator) {
            Profiler.Enqueue("/refresh_list_size", ChunkReplicator->GetRefreshListSize());
            Profiler.Enqueue("/properties_update_list_size", ChunkReplicator->GetPropertiesUpdateListSize());
        }
    }

};

DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, Chunk, TChunk, TChunkId, ChunkMap)
DEFINE_METAMAP_ACCESSORS(TChunkManager::TImpl, ChunkList, TChunkList, TChunkListId, ChunkListMap)

DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, LostChunks, *ChunkReplicator);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, LostVitalChunks, *ChunkReplicator);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, OverreplicatedChunks, *ChunkReplicator);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, UnderreplicatedChunks, *ChunkReplicator);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, DataMissingChunks, *ChunkReplicator);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, yhash_set<TChunk*>, ParityMissingChunks, *ChunkReplicator);

///////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkTypeHandlerBase::TChunkTypeHandlerBase(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap, &owner->ChunkMap)
    , Owner(owner)
{ }

IObjectProxyPtr TChunkManager::TChunkTypeHandlerBase::DoGetProxy(
    TChunk* chunk,
    TTransaction* transaction)
{
    UNUSED(transaction);

    return CreateChunkProxy(Bootstrap, chunk);
}

TObjectBase* TChunkManager::TChunkTypeHandlerBase::Create(
    TTransaction* transaction,
    TAccount* account,
    IAttributeDictionary* attributes,
    TReqCreateObjects* request,
    TRspCreateObjects* response)
{
    YCHECK(transaction);
    YCHECK(account);
    UNUSED(attributes);

    account->ValidateDiskSpaceLimit();

    auto type = GetType();
    bool isErasure = (type == EObjectType::ErasureChunk);
    const auto* requestExt = &request->GetExtension(TReqCreateChunkExt::create_chunk_ext);

    auto erasureCodecId = isErasure ? NErasure::ECodec(requestExt->erasure_codec()) : NErasure::ECodec(NErasure::ECodec::None);
    auto* erasureCodec = isErasure ? NErasure::GetCodec(erasureCodecId) : nullptr;
    int replicationFactor = isErasure ? 1 : requestExt->replication_factor();

    auto* chunk = Owner->CreateChunk(type);
    chunk->SetReplicationFactor(replicationFactor);
    chunk->SetErasureCodec(erasureCodecId);
    chunk->SetMovable(requestExt->movable());
    chunk->SetVital(requestExt->vital());
    chunk->SetStagingTransaction(transaction);
    chunk->SetStagingAccount(account);

    if (Owner->IsLeader()) {
        auto preferredHostName = requestExt->has_preferred_host_name()
            ? TNullable<Stroka>(requestExt->preferred_host_name())
            : Null;

        int uploadReplicationFactor = isErasure
            ? erasureCodec->GetDataPartCount() + erasureCodec->GetParityPartCount()
            : requestExt->upload_replication_factor();

        auto targets = Owner->AllocateWriteTargets(uploadReplicationFactor, preferredHostName);

        auto* responseExt = response->MutableExtension(TRspCreateChunkExt::create_chunk_ext);
        TNodeDirectoryBuilder builder(responseExt->mutable_node_directory());
        for (int index = 0; index < static_cast<int>(targets.size()); ++index) {
            auto* target = targets[index];
            NChunkServer::TNodePtrWithIndex replica(
                target,
                isErasure ? index : 0);
            builder.Add(replica);
            responseExt->add_replicas(NYT::ToProto<ui32>(replica));
        }

        LOG_DEBUG_UNLESS(Owner->IsRecovery(),
            "Allocated nodes for new chunk "
            "(ChunkId: %s, TransactionId: %s, Account: %s, Targets: [%s], "
            "PreferredHostName: %s, ReplicationFactor: %d, UploadReplicationFactor: %d, ErasureCodec: %s, Movable: %s, Vital: %s)",
            ~ToString(chunk->GetId()),
            ~ToString(transaction->GetId()),
            ~account->GetName(),
            ~JoinToString(targets, TNodePtrAddressFormatter()),
            ~ToString(preferredHostName),
            chunk->GetReplicationFactor(),
            uploadReplicationFactor,
            ~erasureCodecId.ToString(),
            ~FormatBool(requestExt->movable()),
            ~FormatBool(requestExt->vital()));
    }

    return chunk;
}

void TChunkManager::TChunkTypeHandlerBase::DoDestroy(TChunk* chunk)
{
    Owner->DestroyChunk(chunk);
}

void TChunkManager::TChunkTypeHandlerBase::DoUnstage(
    TChunk* chunk,
    TTransaction* transaction,
    bool recursive)
{
    UNUSED(transaction);
    UNUSED(recursive);

    Owner->UnstageChunk(chunk);
}

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkListTypeHandler::TChunkListTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap, &owner->ChunkListMap)
    , Owner(owner)
{ }

IObjectProxyPtr TChunkManager::TChunkListTypeHandler::DoGetProxy(
    TChunkList* chunkList,
    TTransaction* transaction)
{
    UNUSED(transaction);

    return CreateChunkListProxy(Bootstrap, chunkList);
}

TObjectBase* TChunkManager::TChunkListTypeHandler::Create(
    TTransaction* transaction,
    TAccount* account,
    IAttributeDictionary* attributes,
    TReqCreateObjects* request,
    TRspCreateObjects* response)
{
    UNUSED(transaction);
    UNUSED(account);
    UNUSED(attributes);
    UNUSED(request);
    UNUSED(response);

    return Owner->CreateChunkList();
}

void TChunkManager::TChunkListTypeHandler::DoDestroy(TChunkList* chunkList)
{
    Owner->DestroyChunkList(chunkList);
}

void TChunkManager::TChunkListTypeHandler::DoUnstage(
    TChunkList* obj,
    TTransaction* transaction,
    bool recursive)
{
    Owner->UnstageChunkList(obj, transaction, recursive);
}

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl(New<TImpl>(config, bootstrap))
{ }

TChunkManager::~TChunkManager()
{ }

void TChunkManager::Initialize()
{
    Impl->Initialize();
}

TChunk* TChunkManager::GetChunkOrThrow(const TChunkId& id)
{
    return Impl->GetChunkOrThrow(id);
}

TChunkTree* TChunkManager::FindChunkTree(const TChunkTreeId& id)
{
    return Impl->FindChunkTree(id);
}

TChunkTree* TChunkManager::GetChunkTree(const TChunkTreeId& id)
{
    return Impl->GetChunkTree(id);
}

TChunkTree* TChunkManager::GetChunkTreeOrThrow(const TChunkTreeId& id)
{
    return Impl->GetChunkTreeOrThrow(id);
}

TNodeList TChunkManager::AllocateWriteTargets(
    int replicaCount,
    const TNullable<Stroka>& preferredHostName)
{
    return Impl->AllocateWriteTargets(replicaCount, preferredHostName);
}

TMutationPtr TChunkManager::CreateUpdateChunkPropertiesMutation(
    const NProto::TMetaReqUpdateChunkProperties& request)
{
    return Impl->CreateUpdateChunkPropertiesMutation(request);
}

TChunk* TChunkManager::CreateChunk(EObjectType type)
{
    return Impl->CreateChunk(type);
}

TChunkList* TChunkManager::CreateChunkList()
{
    return Impl->CreateChunkList();
}

void TChunkManager::ConfirmChunk(
    TChunk* chunk,
    const std::vector<NChunkClient::TChunkReplica>& replicas,
    NChunkClient::NProto::TChunkInfo* chunkInfo,
    NChunkClient::NProto::TChunkMeta* chunkMeta)
{
    Impl->ConfirmChunk(
        chunk,
        replicas,
        chunkInfo,
        chunkMeta);
}

TNodePtrWithIndexList TChunkManager::LocateChunk(TChunkPtrWithIndex chunkWithIndex)
{
    return Impl->LocateChunk(chunkWithIndex);
}

void TChunkManager::AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree** childrenBegin,
    TChunkTree** childrenEnd,
    bool resetSorted)
{
    Impl->AttachToChunkList(chunkList, childrenBegin, childrenEnd, resetSorted);
}

void TChunkManager::AttachToChunkList(
    TChunkList* chunkList,
    const std::vector<TChunkTree*>& children,
    bool resetSorted)
{
    Impl->AttachToChunkList(chunkList, children, resetSorted);
}

void TChunkManager::AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree* child,
    bool resetSorted)
{
    Impl->AttachToChunkList(chunkList, child, resetSorted);
}

void TChunkManager::RebalanceChunkTree(TChunkList* chunkList)
{
    Impl->RebalanceChunkTree(chunkList);
}

void TChunkManager::ClearChunkList(TChunkList* chunkList)
{
    Impl->ClearChunkList(chunkList);
}

TJobPtr TChunkManager::FindJob(const TJobId& id)
{
    return Impl->FindJob(id);
}

TJobListPtr TChunkManager::FindJobList(TChunk* chunk)
{
    return Impl->FindJobList(chunk);
}

void TChunkManager::ScheduleJobs(
    TNode* node,
    const std::vector<TJobPtr>& currentJobs,
    std::vector<TJobPtr>* jobsToStart,
    std::vector<TJobPtr>* jobsToAbort,
    std::vector<TJobPtr>* jobsToRemove)
{
    Impl->ScheduleJobs(
        node,
        currentJobs,
        jobsToStart,
        jobsToAbort,
        jobsToRemove);
}

bool TChunkManager::IsReplicatorEnabled()
{
    return Impl->IsReplicatorEnabled();
}

void TChunkManager::SchedulePropertiesUpdate(TChunkTree* chunkTree)
{
    Impl->SchedulePropertiesUpdate(chunkTree);
}

int TChunkManager::GetTotalReplicaCount()
{
    return Impl->GetTotalReplicaCount();
}

EChunkStatus TChunkManager::ComputeChunkStatus(TChunk* chunk)
{
    return Impl->ComputeChunkStatus(chunk);
}

DELEGATE_METAMAP_ACCESSORS(TChunkManager, Chunk, TChunk, TChunkId, *Impl)
DELEGATE_METAMAP_ACCESSORS(TChunkManager, ChunkList, TChunkList, TChunkListId, *Impl)

DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, LostChunks, *Impl);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, LostVitalChunks, *Impl);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, OverreplicatedChunks, *Impl);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, UnderreplicatedChunks, *Impl);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, DataMissingChunks, *Impl);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, yhash_set<TChunk*>, ParityMissingChunks, *Impl);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

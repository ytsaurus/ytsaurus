#include "chunk_manager.h"
#include "private.h"
#include "chunk.h"
#include "chunk_list.h"
#include "chunk_list_proxy.h"
#include "chunk_owner_base.h"
#include "chunk_placement.h"
#include "chunk_proxy.h"
#include "chunk_replicator.h"
#include "chunk_sealer.h"
#include "chunk_tree_balancer.h"
#include "config.h"
#include "helpers.h"
#include "job.h"
#include "medium.h"
#include "medium_proxy.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/multicell_manager.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/chunk_server/chunk_manager.pb.h>

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/hydra/composite_automaton.h>
#include <yt/server/hydra/entity_map.h>

#include <yt/server/node_tracker_server/config.h>
#include <yt/server/node_tracker_server/node_directory_builder.h>
#include <yt/server/node_tracker_server/node_tracker.h>
#include <yt/server/node_tracker_server/rack.h>

#include <yt/server/object_server/object_manager.h>
#include <yt/server/object_server/type_handler_detail.h>

#include <yt/server/security_server/account.h>
#include <yt/server/security_server/group.h>
#include <yt/server/security_server/security_manager.h>

#include <yt/server/transaction_server/transaction.h>
#include <yt/server/transaction_server/transaction_manager.h>

#include <yt/server/journal_server/journal_node.h>
#include <yt/server/journal_server/journal_manager.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/session_id.h>
#include <yt/ytlib/chunk_client/chunk_service.pb.h>

#include <yt/ytlib/journal_client/helpers.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/core/compression/codec.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/string.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT {
namespace NChunkServer {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NRpc;
using namespace NHydra;
using namespace NNodeTrackerServer;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NObjectClient::NProto;
using namespace NYTree;
using namespace NCellMaster;
using namespace NCypressServer;
using namespace NSecurityServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJournalClient;
using namespace NJournalServer;

using NChunkClient::TSessionId;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;
static const auto ProfilingPeriod = TDuration::MilliSeconds(1000);
// NB: Changing this value will invalidate all changelogs!
static const auto ReplicaApproveTimeout = TDuration::Seconds(60);

static NProfiling::TAggregateCounter ChunkTreeRebalacnceTimeCounter("/chunk_tree_rebalance_time");

////////////////////////////////////////////////////////////////////////////////

struct TChunkToAllLinkedListNode
{
    auto operator() (TChunk* chunk) const
    {
        return &chunk->GetDynamicData()->AllLinkedListNode;
    }
};

struct TChunkToJournalLinkedListNode
{
    auto operator() (TChunk* chunk) const
    {
        return &chunk->GetDynamicData()->JournalLinkedListNode;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkTreeBalancerCallbacks
    : public IChunkTreeBalancerCallbacks
{
public:
    explicit TChunkTreeBalancerCallbacks(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual void RefObject(TObjectBase* object) override
    {
        Bootstrap_->GetObjectManager()->RefObject(object);
    }

    virtual void UnrefObject(TObjectBase* object) override
    {
        Bootstrap_->GetObjectManager()->UnrefObject(object);
    }

    virtual int GetObjectRefCounter(TObjectBase* object) override
    {
        return Bootstrap_->GetObjectManager()->GetObjectRefCounter(object);
    }

    virtual TChunkList* CreateChunkList() override
    {
        return Bootstrap_->GetChunkManager()->CreateChunkList(EChunkListKind::Static);
    }

    virtual void ClearChunkList(TChunkList* chunkList) override
    {
        Bootstrap_->GetChunkManager()->ClearChunkList(chunkList);
    }

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

private:
    NCellMaster::TBootstrap* const Bootstrap_;

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkTypeHandlerBase
    : public TObjectTypeHandlerWithMapBase<TChunk>
{
public:
    explicit TChunkTypeHandlerBase(TImpl* owner);

    virtual TObjectBase* FindObject(const TObjectId& id) override
    {
        return Map_->Find(DecodeChunkId(id).Id);
    }

protected:
    TImpl* const Owner_;


    virtual IObjectProxyPtr DoGetProxy(TChunk* chunk, TTransaction* transaction) override;

    virtual void DoDestroyObject(TChunk* chunk) override;

    virtual void DoUnstageObject(TChunk* chunk, bool recursive) override;

    virtual void DoExportObject(
        TChunk* chunk,
        TCellTag destinationCellTag) override
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto cellIndex = multicellManager->GetRegisteredMasterCellIndex(destinationCellTag);
        chunk->Export(cellIndex);
    }

    virtual void DoUnexportObject(
        TChunk* chunk,
        TCellTag destinationCellTag,
        int importRefCounter) override
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto cellIndex = multicellManager->GetRegisteredMasterCellIndex(destinationCellTag);
        chunk->Unexport(cellIndex, importRefCounter);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TRegularChunkTypeHandler
    : public TChunkTypeHandlerBase
{
public:
    explicit TRegularChunkTypeHandler(TImpl* owner)
        : TChunkTypeHandlerBase(owner)
    { }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Chunk;
    }

private:
    virtual TString DoGetName(const TChunk* chunk) override
    {
        return Format("chunk %v", chunk->GetId());
    }

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TErasureChunkTypeHandler
    : public TChunkTypeHandlerBase
{
public:
    TErasureChunkTypeHandler(TImpl* owner, EObjectType type)
        : TChunkTypeHandlerBase(owner)
        , Type_(type)
    { }

    virtual EObjectType GetType() const override
    {
        return Type_;
    }

private:
    const EObjectType Type_;

    virtual TString DoGetName(const TChunk* chunk) override
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
    virtual TString DoGetName(const TChunk* chunk) override
    {
        return Format("journal chunk %v", chunk->GetId());
    }

};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TMediumTypeHandler
    : public TObjectTypeHandlerWithMapBase<TMedium>
{
public:
    explicit TMediumTypeHandler(TImpl* owner);

    virtual ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable;
    }

    virtual EObjectType GetType() const override
    {
        return EObjectType::Medium;
    }

    virtual TObjectBase* CreateObject(
        const TObjectId& hintId,
        IAttributeDictionary* attributes) override;

private:
    TImpl* const Owner_;

    virtual TCellTagList DoGetReplicationCellTags(const TMedium* /*medium*/) override
    {
        return AllSecondaryCellTags();
    }

    virtual TString DoGetName(const TMedium* medium) override
    {
        return Format("medium %Qv", medium->GetName());
    }

    virtual TAccessControlDescriptor* DoFindAcd(TMedium* medium) override
    {
        return &medium->Acd();
    }

    virtual IObjectProxyPtr DoGetProxy(TMedium* medium, TTransaction* transaction) override;

    virtual void DoZombifyObject(TMedium* medium) override;
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

private:
    TImpl* const Owner_;


    virtual TString DoGetName(const TChunkList* chunkList) override
    {
        return Format("chunk list %v", chunkList->GetId());
    }

    virtual IObjectProxyPtr DoGetProxy(TChunkList* chunkList, TTransaction* transaction) override;

    virtual void DoDestroyObject(TChunkList* chunkList) override;

    virtual void DoUnstageObject(TChunkList* chunkList, bool recursive) override;
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
        , ChunkTreeBalancer_(New<TChunkTreeBalancerCallbacks>(Bootstrap_))
    {
        RegisterMethod(BIND(&TImpl::HydraUpdateChunkProperties, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraExportChunks, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraImportChunks, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraExecuteBatch, Unretained(this)));

        RegisterLoader(
            "ChunkManager.Keys",
            BIND(&TImpl::LoadKeys, Unretained(this)));
        RegisterLoader(
            "ChunkManager.Values",
            BIND(&TImpl::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "ChunkManager.Keys",
            BIND(&TImpl::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "ChunkManager.Values",
            BIND(&TImpl::SaveValues, Unretained(this)));

        auto cellTag = Bootstrap_->GetPrimaryCellTag();
        DefaultStoreMediumId_ = MakeWellKnownId(EObjectType::Medium, cellTag, 0xffffffffffffffff);
        DefaultCacheMediumId_ = MakeWellKnownId(EObjectType::Medium, cellTag, 0xfffffffffffffffe);

        auto* profileManager = TProfileManager::Get();
        Profiler.TagIds().push_back(profileManager->RegisterTag("cell_tag", Bootstrap_->GetCellTag()));

        for (auto jobType = EJobType::ReplicatorFirst;
             jobType != EJobType::ReplicatorLast;
             jobType = static_cast<EJobType>(static_cast<TEnumTraits<EJobType>::TUnderlying>(jobType) + 1))
        {
            JobTypeToTag_[jobType] = profileManager->RegisterTag("job_type", jobType);
        }
    }

    void Initialize()
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TRegularChunkTypeHandler>(this));
        objectManager->RegisterHandler(New<TErasureChunkTypeHandler>(this, EObjectType::ErasureChunk));
        for (auto type = MinErasureChunkPartType;
             type <= MaxErasureChunkPartType;
             type = static_cast<EObjectType>(static_cast<int>(type) + 1))
        {
            objectManager->RegisterHandler(New<TErasureChunkTypeHandler>(this, type));
        }
        objectManager->RegisterHandler(New<TJournalChunkTypeHandler>(this));
        objectManager->RegisterHandler(New<TChunkListTypeHandler>(this));
        objectManager->RegisterHandler(New<TMediumTypeHandler>(this));

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->SubscribeNodeRegistered(BIND(&TImpl::OnNodeRegistered, MakeWeak(this)));
        nodeTracker->SubscribeNodeUnregistered(BIND(&TImpl::OnNodeUnregistered, MakeWeak(this)));
        nodeTracker->SubscribeNodeDisposed(BIND(&TImpl::OnNodeDisposed, MakeWeak(this)));
        nodeTracker->SubscribeNodeRackChanged(BIND(&TImpl::OnNodeRackChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeDataCenterChanged(BIND(&TImpl::OnNodeDataCenterChanged, MakeWeak(this)));
        nodeTracker->SubscribeNodeDecommissionChanged(BIND(&TImpl::OnNodeChanged, MakeWeak(this)));
        nodeTracker->SubscribeFullHeartbeat(BIND(&TImpl::OnFullHeartbeat, MakeWeak(this)));
        nodeTracker->SubscribeIncrementalHeartbeat(BIND(&TImpl::OnIncrementalHeartbeat, MakeWeak(this)));
        nodeTracker->SubscribeDataCenterCreated(BIND(&TImpl::OnDataCenterCreated, MakeWeak(this)));
        nodeTracker->SubscribeDataCenterRenamed(BIND(&TImpl::OnDataCenterRenamed, MakeWeak(this)));
        nodeTracker->SubscribeDataCenterDestroyed(BIND(&TImpl::OnDataCenterDestroyed, MakeWeak(this)));

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();
    }

    std::unique_ptr<TMutation> CreateUpdateChunkPropertiesMutation(const NProto::TReqUpdateChunkProperties& request)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TImpl::HydraUpdateChunkProperties,
            this);
    }

    std::unique_ptr<TMutation> CreateExportChunksMutation(TCtxExportChunksPtr context)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TImpl::HydraExportChunks,
            this);
    }

    std::unique_ptr<TMutation> CreateImportChunksMutation(TCtxImportChunksPtr context)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TImpl::HydraImportChunks,
            this);
    }

    std::unique_ptr<TMutation> CreateExecuteBatchMutation(TCtxExecuteBatchPtr context)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            std::move(context),
            &TImpl::HydraExecuteBatch,
            this);
    }

    TNodeList AllocateWriteTargets(
        TMedium* medium,
        TChunk* chunk,
        int desiredCount,
        int minCount,
        TNullable<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes,
        const TNullable<TString>& preferredHostName)
    {
        return ChunkPlacement_->AllocateWriteTargets(
            medium,
            chunk,
            desiredCount,
            minCount,
            replicationFactorOverride,
            forbiddenNodes,
            preferredHostName,
            ESessionType::User);
    }

    void ConfirmChunk(
        TChunk* chunk,
        const NChunkClient::TChunkReplicaList& replicas,
        TChunkInfo* chunkInfo,
        TChunkMeta* chunkMeta)
    {
        const auto& id = chunk->GetId();

        if (chunk->IsConfirmed()) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk is already confirmed (ChunkId: %v)",
                id);
            return;
        }

        chunk->Confirm(chunkInfo, chunkMeta);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        const auto* mutationContext = GetCurrentMutationContext();
        auto mutationTimestamp = mutationContext->GetTimestamp();

        for (auto replica : replicas) {
            auto nodeId = replica.GetNodeId();
            auto* node = nodeTracker->FindNode(nodeId);
            if (!IsObjectAlive(node)) {
                LOG_DEBUG_UNLESS(IsRecovery(), "Tried to confirm chunk at an unknown node (ChunkId: %v, NodeId: %v)",
                    id,
                    replica.GetNodeId());
                continue;
            }

            auto mediumIndex = replica.GetMediumIndex();
            const auto* medium = GetMediumByIndex(mediumIndex);
            if (medium->GetCache()) {
                LOG_DEBUG_UNLESS(IsRecovery(), "Tried to confirm chunk at a cache medium (ChunkId: %v, Medium: %v)",
                    id,
                    medium->GetName());
                continue;
            }

            int replicaIndex = chunk->IsJournal() ? ActiveChunkReplicaIndex : replica.GetReplicaIndex();
            auto chunkWithIndexes = TChunkPtrWithIndexes(chunk, replicaIndex, replica.GetMediumIndex());

            if (node->GetLocalState() != ENodeState::Online) {
                LOG_DEBUG_UNLESS(IsRecovery(), "Tried to confirm chunk %v at %v which has invalid state %Qlv",
                    id,
                    node->GetDefaultAddress(),
                    node->GetLocalState());
                continue;
            }

            if (!node->HasReplica(chunkWithIndexes)) {
                AddChunkReplica(
                    medium,
                    node,
                    chunkWithIndexes,
                    EAddReplicaReason::Confirmation);
                node->AddUnapprovedReplica(chunkWithIndexes, mutationTimestamp);
            }
        }

        // NB: This is true for non-journal chunks.
        if (chunk->IsSealed()) {
            OnChunkSealed(chunk);
        }

        // Increase staged resource usage.
        if (chunk->IsStaged() && !chunk->IsJournal()) {
            auto* stagingTransaction = chunk->GetStagingTransaction();
            auto* stagingAccount = chunk->GetStagingAccount();
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto delta = chunk->GetResourceUsage();
            securityManager->UpdateAccountStagingUsage(stagingTransaction, stagingAccount, delta);
        }

        ScheduleChunkRefresh(chunk);
    }

    void SealChunk(TChunk* chunk, const TMiscExt& miscExt)
    {
        if (!chunk->IsJournal()) {
            THROW_ERROR_EXCEPTION("Not a journal chunk");
        }

        if (!chunk->IsConfirmed()) {
            THROW_ERROR_EXCEPTION("Chunk is not confirmed");
        }

        if (chunk->IsSealed()) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk is already sealed (ChunkId: %v)",
                chunk->GetId());
            return;
        }

        chunk->Seal(miscExt);
        OnChunkSealed(chunk);

        ScheduleChunkRefresh(chunk);
    }

    TChunkList* CreateChunkList(EChunkListKind kind)
    {
        auto* chunkList = DoCreateChunkList(kind);
        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk list created (Id: %v, Kind: %v)",
            chunkList->GetId(),
            chunkList->GetKind());
        return chunkList;
    }


    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd)
    {
        NChunkServer::AttachToChunkList(chunkList, childrenBegin, childrenEnd);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto it = childrenBegin; it != childrenEnd; ++it) {
            auto* child = *it;
            objectManager->RefObject(child);
        }
    }

    void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children)
    {
        AttachToChunkList(
            chunkList,
            children.data(),
            children.data() + children.size());
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
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd)
    {
        NChunkServer::DetachFromChunkList(chunkList, childrenBegin, childrenEnd);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto it = childrenBegin; it != childrenEnd; ++it) {
            auto* child = *it;
            objectManager->UnrefObject(child);
        }
    }

    void DetachFromChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children)
    {
        DetachFromChunkList(
            chunkList,
            children.data(),
            children.data() + children.size());
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

        PROFILE_AGGREGATED_TIMING (ChunkTreeRebalacnceTimeCounter) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree rebalancing started (RootId: %v)",
                chunkList->GetId());
            ChunkTreeBalancer_.Rebalance(chunkList);
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree rebalancing completed");
        }
    }


    void StageChunkTree(TChunkTree* chunkTree, TTransaction* transaction, TAccount* account)
    {
        Y_ASSERT(transaction);
        Y_ASSERT(!chunkTree->IsStaged());

        chunkTree->SetStagingTransaction(transaction);

        if (account) {
            chunkTree->SetStagingAccount(account);
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->RefObject(account);
        }
    }

    void UnstageChunk(TChunk* chunk)
    {
        auto* transaction = chunk->GetStagingTransaction();
        auto* account = chunk->GetStagingAccount();

        if (account) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(account);
        }

        if (account && chunk->IsConfirmed() && !chunk->IsJournal()) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto delta = -chunk->GetResourceUsage();
            securityManager->UpdateAccountStagingUsage(transaction, account, delta);
        }

        chunk->SetStagingTransaction(nullptr);
        chunk->SetStagingAccount(nullptr);
    }

    void UnstageChunkList(TChunkList* chunkList, bool recursive)
    {
        auto* account = chunkList->GetStagingAccount();
        if (account) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(account);
        }

        chunkList->SetStagingTransaction(nullptr);
        chunkList->SetStagingAccount(nullptr);

        if (recursive) {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            for (auto* child : chunkList->Children()) {
                if (child) {
                    transactionManager->UnstageObject(child->GetStagingTransaction(), child, recursive);
                }
            }
        }
    }

    TNodePtrWithIndexesList LocateChunk(TChunkPtrWithIndexes chunkWithIndexes)
    {
        auto* chunk = chunkWithIndexes.GetPtr();
        auto replicaIndex = chunkWithIndexes.GetReplicaIndex();
        auto mediumIndex = chunkWithIndexes.GetMediumIndex();

        TouchChunk(chunk);

        TNodePtrWithIndexesList result;
        auto replicas = chunk->GetReplicas();
        for (auto replica : replicas) {
            if ((replicaIndex == GenericChunkReplicaIndex || replica.GetReplicaIndex() == replicaIndex) &&
                (mediumIndex == AllMediaIndex || replica.GetMediumIndex() == mediumIndex))
            {
                result.push_back(replica);
            }
        }

        return result;
    }

    void TouchChunk(TChunk* chunk)
    {
        if (chunk->IsErasure() && ChunkReplicator_) {
            ChunkReplicator_->TouchChunk(chunk);
        }
    }


    void ClearChunkList(TChunkList* chunkList)
    {
        // TODO(babenko): currently we only support clearing a chunklist with no parents.
        YCHECK(chunkList->Parents().Empty());
        chunkList->IncrementVersion();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* child : chunkList->Children()) {
            if (child) {
                ResetChunkTreeParent(chunkList, child);
                objectManager->UnrefObject(child);
            }
        }

        chunkList->Children().clear();
        ResetChunkListStatistics(chunkList);

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk list cleared (ChunkListId: %v)", chunkList->GetId());
    }


    TJobPtr FindJob(const TJobId& id)
    {
        return ChunkReplicator_->FindJob(id);
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


    DECLARE_BYREF_RO_PROPERTY(THashSet<TChunk*>, LostChunks);
    DECLARE_BYREF_RO_PROPERTY(THashSet<TChunk*>, LostVitalChunks);
    DECLARE_BYREF_RO_PROPERTY(THashSet<TChunk*>, OverreplicatedChunks);
    DECLARE_BYREF_RO_PROPERTY(THashSet<TChunk*>, UnderreplicatedChunks);
    DECLARE_BYREF_RO_PROPERTY(THashSet<TChunk*>, DataMissingChunks);
    DECLARE_BYREF_RO_PROPERTY(THashSet<TChunk*>, ParityMissingChunks);
    DECLARE_BYREF_RO_PROPERTY(THashSet<TChunk*>, PrecariousChunks);
    DECLARE_BYREF_RO_PROPERTY(THashSet<TChunk*>, PrecariousVitalChunks);
    DECLARE_BYREF_RO_PROPERTY(THashSet<TChunk*>, QuorumMissingChunks);
    DECLARE_BYREF_RO_PROPERTY(THashSet<TChunk*>, UnsafelyPlacedChunks);
    DEFINE_BYREF_RO_PROPERTY(THashSet<TChunk*>, ForeignChunks);


    int GetTotalReplicaCount()
    {
        return TotalReplicaCount_;
    }

    bool IsReplicatorEnabled()
    {
        return ChunkReplicator_ && ChunkReplicator_->IsEnabled();
    }


    void ScheduleChunkRefresh(TChunk* chunk)
    {
        if (ChunkReplicator_) {
            ChunkReplicator_->ScheduleChunkRefresh(chunk);
        }
    }

    void ScheduleNodeRefresh(TNode* node)
    {
        if (ChunkReplicator_) {
            ChunkReplicator_->ScheduleNodeRefresh(node);
        }
    }

    void ScheduleChunkPropertiesUpdate(TChunkTree* chunkTree)
    {
        if (ChunkReplicator_) {
            ChunkReplicator_->SchedulePropertiesUpdate(chunkTree);
        }
    }

    void ScheduleChunkSeal(TChunk* chunk)
    {
        if (ChunkSealer_) {
            ChunkSealer_->ScheduleSeal(chunk);
        }
    }


    TChunk* GetChunkOrThrow(const TChunkId& id)
    {
        auto* chunk = FindChunk(id);
        if (!IsObjectAlive(chunk)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchChunk,
                "No such chunk %v",
                id);
        }
        return chunk;
    }

    TChunkList* GetChunkListOrThrow(const TChunkListId& id)
    {
        auto* chunkList = FindChunkList(id);
        if (!IsObjectAlive(chunkList)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchChunkList,
                "No such chunk list %v",
                id);
        }
        return chunkList;
    }

    TMedium* CreateMedium(
        const TString& name,
        TNullable<bool> transient,
        TNullable<bool> cache,
        TNullable<int> priority,
        const TObjectId& hintId)
    {
        ValidateMediumName(name);

        if (FindMediumByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Medium %Qv already exists",
                name);
        }

        if (MediumMap_.GetSize() >= MaxMediumCount) {
            THROW_ERROR_EXCEPTION("Medium count limit %v is reached",
                MaxMediumCount);
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::Medium, hintId);
        auto mediumIndex = GetFreeMediumIndex();
        return DoCreateMedium(
            id,
            mediumIndex,
            name,
            transient,
            cache,
            priority);
    }

    void DestroyMedium(TMedium* medium)
    {
        UnregisterMedium(medium);
    }

    void RenameMedium(TMedium* medium, const TString& newName)
    {
        if (medium->GetName() == newName) {
            return;
        }

        if (medium->IsBuiltin()) {
            THROW_ERROR_EXCEPTION("Builtin medium cannot be renamed");
        }

        if (FindMediumByName(newName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Medium %Qv already exists",
                newName);
        }

        // Update name.
        YCHECK(NameToMediumMap_.erase(medium->GetName()) == 1);
        YCHECK(NameToMediumMap_.emplace(newName, medium).second);
        medium->SetName(newName);
    }

    void SetMediumPriority(TMedium* medium, int priority)
    {
        if (medium->GetPriority() == priority) {
            return;
        }

        ValidateMediumPriority(priority);

        medium->SetPriority(priority);
    }

    TMedium* FindMediumByName(const TString& name) const
    {
        auto it = NameToMediumMap_.find(name);
        return it == NameToMediumMap_.end() ? nullptr : it->second;
    }

    TMedium* GetMediumByNameOrThrow(const TString& name) const
    {
        auto* medium = FindMediumByName(name);
        if (!medium) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchMedium,
                "No such medium %Qv",
                name);
        }
        return medium;
    }

    TMedium* FindMediumByIndex(int index) const
    {
        return index >= 0 && index < MaxMediumCount
            ? IndexToMediumMap_[index]
            : nullptr;
    }

    TMedium* GetMediumByIndexOrThrow(int index) const
    {
        auto* medium = FindMediumByIndex(index);
        if (!medium) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchMedium,
                "No such medium %v",
                index);
        }
        return medium;
    }

    TMedium* GetMediumByIndex(int index) const
    {
        auto* medium = FindMediumByIndex(index);
        YCHECK(medium);
        return medium;
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
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchChunkTree,
                "No such chunk tree %v",
                id);
        }
        return chunkTree;
    }


    TPerMediumArray<EChunkStatus> ComputeChunkStatuses(TChunk* chunk)
    {
        return ChunkReplicator_->ComputeChunkStatuses(chunk);
    }


    TFuture<TMiscExt> GetChunkQuorumInfo(TChunk* chunk)
    {
        if (chunk->IsSealed()) {
            return MakeFuture(chunk->MiscExt());
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
            chunk->GetReadQuorum(),
            Bootstrap_->GetNodeChannelFactory());
    }


    DECLARE_ENTITY_MAP_ACCESSORS(Chunk, TChunk);
    DECLARE_ENTITY_MAP_ACCESSORS(ChunkList, TChunkList);
    DECLARE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(Medium, Media, TMedium)

private:
    friend class TChunkTypeHandlerBase;
    friend class TRegularChunkTypeHandler;
    friend class TErasureChunkTypeHandler;
    friend class TChunkListTypeHandler;
    friend class TMediumTypeHandler;

    const TChunkManagerConfigPtr Config_;

    TChunkTreeBalancer ChunkTreeBalancer_;

    int TotalReplicaCount_ = 0;

    bool NeedToRecomputeStatistics_ = false;
    bool NeedResetDataWeight_ = false;
    bool NeedInitializeMediumConfig_ = false;

    TPeriodicExecutorPtr ProfilingExecutor_;

    TProfiler Profiler = ChunkServerProfiler;
    i64 ChunksCreated_ = 0;
    i64 ChunksDestroyed_ = 0;
    i64 ChunkReplicasAdded_ = 0;
    i64 ChunkReplicasRemoved_ = 0;
    i64 ChunkListsCreated_ = 0;
    i64 ChunkListsDestroyed_ = 0;

    TChunkPlacementPtr ChunkPlacement_;
    TChunkReplicatorPtr ChunkReplicator_;
    TChunkSealerPtr ChunkSealer_;

    // Global chunk lists; cf. TChunkDynamicData.
    TIntrusiveLinkedList<TChunk, TChunkToAllLinkedListNode> AllChunks_;
    TIntrusiveLinkedList<TChunk, TChunkToJournalLinkedListNode> JournalChunks_;

    NHydra::TEntityMap<TChunk> ChunkMap_;
    NHydra::TEntityMap<TChunkList> ChunkListMap_;

    NHydra::TEntityMap<TMedium> MediumMap_;
    THashMap<TString, TMedium*> NameToMediumMap_;
    std::vector<TMedium*> IndexToMediumMap_;
    TMediumSet UsedMediumIndexes_;

    TMediumId DefaultStoreMediumId_;
    TMedium* DefaultStoreMedium_ = nullptr;

    TMediumId DefaultCacheMediumId_;
    TMedium* DefaultCacheMedium_ = nullptr;

    TEnumIndexedVector<TTagId, EJobType, EJobType::ReplicatorFirst, EJobType::ReplicatorLast> JobTypeToTag_;
    THashMap<const TDataCenter*, TTagId> SourceDataCenterToTag_;
    THashMap<const TDataCenter*, TTagId> DestinationDataCenterToTag_;

    TChunk* DoCreateChunk(EObjectType chunkType)
    {
        auto id = Bootstrap_->GetObjectManager()->GenerateId(chunkType, NullObjectId);
        return DoCreateChunk(id);
    }

    TChunk* DoCreateChunk(const TChunkId& chunkId)
    {
        auto chunkHolder = std::make_unique<TChunk>(chunkId);
        auto* chunk = ChunkMap_.Insert(chunkId, std::move(chunkHolder));
        RegisterChunk(chunk);
        ChunksCreated_++;
        return chunk;
    }

    void DestroyChunk(TChunk* chunk)
    {
        if (chunk->IsForeign()) {
            YCHECK(ForeignChunks_.erase(chunk) == 1);
        }

        // Decrease staging resource usage; release account.
        UnstageChunk(chunk);

        // Cancel all jobs, reset status etc.
        if (ChunkReplicator_) {
            ChunkReplicator_->OnChunkDestroyed(chunk);
        }
        if (ChunkSealer_) {
            ChunkSealer_->OnChunkDestroyed(chunk);
        }

        auto job = chunk->GetJob();

        // Unregister chunk replicas from all known locations.
        // Schedule removal jobs.
        auto unregisterReplica = [&] (TNodePtrWithIndexes nodeWithIndexes, bool cached) {
            auto* node = nodeWithIndexes.GetPtr();
            TChunkPtrWithIndexes chunkWithIndexes(
                chunk,
                nodeWithIndexes.GetReplicaIndex(),
                nodeWithIndexes.GetMediumIndex());
            TChunkIdWithIndexes chunkIdWithIndexes(
                chunk->GetId(),
                nodeWithIndexes.GetReplicaIndex(),
                nodeWithIndexes.GetMediumIndex());
            if (!node->RemoveReplica(chunkWithIndexes)) {
                return;
            }
            if (cached) {
                return;
            }
            if (!ChunkReplicator_) {
                return;
            }
            if (node->GetLocalState() != ENodeState::Online) {
                return;
            }
            if (job &&
                job->GetNode() == node &&
                job->GetType() == EJobType::RemoveChunk &&
                job->GetChunkIdWithIndexes() == chunkIdWithIndexes)
            {
                return;
            }
            ChunkReplicator_->ScheduleReplicaRemoval(node, chunkWithIndexes);
        };

        for (auto replica : chunk->StoredReplicas()) {
            unregisterReplica(replica, false);
        }

        for (auto replica : chunk->CachedReplicas()) {
            unregisterReplica(replica, true);
        }

        UnregisterChunk(chunk);

        ++ChunksDestroyed_;
    }


    TChunkList* DoCreateChunkList(EChunkListKind kind)
    {
        ++ChunkListsCreated_;
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::ChunkList, NullObjectId);
        auto chunkListHolder = std::make_unique<TChunkList>(id);
        auto* chunkList = ChunkListMap_.Insert(id, std::move(chunkListHolder));
        chunkList->SetKind(kind);
        return chunkList;
    }

    void DestroyChunkList(TChunkList* chunkList)
    {
        // Release account.
        UnstageChunkList(chunkList, false);

        // Drop references to children.
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* child : chunkList->Children()) {
            if (child) {
                ResetChunkTreeParent(chunkList, child);
                objectManager->UnrefObject(child);
            }
        }

        ++ChunkListsDestroyed_;
    }


    void OnNodeRegistered(TNode* node)
    {
        if (ChunkPlacement_) {
            ChunkPlacement_->OnNodeRegistered(node);
        }

        if (ChunkReplicator_) {
            ChunkReplicator_->OnNodeRegistered(node);
        }

        ScheduleNodeRefresh(node);
    }

    void OnNodeUnregistered(TNode* node)
    {
        if (ChunkPlacement_) {
            ChunkPlacement_->OnNodeUnregistered(node);
        }

        if (ChunkReplicator_) {
            ChunkReplicator_->OnNodeUnregistered(node);
        }
    }

    void OnNodeDisposed(TNode* node)
    {
        for (const auto& pair : Media()) {
            const auto* medium = pair.second;
            auto mediumIndex = medium->GetIndex();
            for (auto replica : node->Replicas()[mediumIndex]) {
                RemoveChunkReplica(medium, node, replica, ERemoveReplicaReason::NodeDisposed);
            }
        }

        node->ClearReplicas();

        if (ChunkPlacement_) {
            ChunkPlacement_->OnNodeDisposed(node);
        }

        if (ChunkReplicator_) {
            ChunkReplicator_->OnNodeDisposed(node);
        }
    }

    void OnNodeChanged(TNode* node)
    {
        if (node->GetLocalState() == ENodeState::Online) {
            ScheduleNodeRefresh(node);
        }
    }

    void OnNodeRackChanged(TNode* node, TRack* oldRack)
    {
        auto* newDataCenter = node->GetDataCenter();
        auto* oldDataCenter = oldRack ? oldRack->GetDataCenter() : nullptr;
        if (newDataCenter != oldDataCenter) {
            ChunkReplicator_->HandleNodeDataCenterChange(node, oldDataCenter);
        }

        OnNodeChanged(node);
    }

    void OnNodeDataCenterChanged(TNode* node, TDataCenter* oldDataCenter)
    {
        YCHECK(node->GetDataCenter() != oldDataCenter);
        ChunkReplicator_->HandleNodeDataCenterChange(node, oldDataCenter);

        OnNodeChanged(node);
    }

    void OnFullHeartbeat(
        TNode* node,
        NNodeTrackerServer::NProto::TReqFullHeartbeat* request)
    {
        for (const auto& mediumReplicas : node->Replicas()) {
            YCHECK(mediumReplicas.empty());
        }

        for (const auto& stats : request->chunk_statistics()) {
            auto mediumIndex = stats.medium_index();
            node->ReserveReplicas(mediumIndex, stats.chunk_count());
        }

        for (const auto& chunkInfo : request->chunks()) {
            ProcessAddedChunk(node, chunkInfo, false);
        }

        if (ChunkPlacement_) {
            ChunkPlacement_->OnNodeUpdated(node);
        }
    }

    void OnIncrementalHeartbeat(
        TNode* node,
        TReqIncrementalHeartbeat* request,
        TRspIncrementalHeartbeat* /*response*/)
    {
        node->ShrinkHashTables();

        for (const auto& chunkInfo : request->added_chunks()) {
            ProcessAddedChunk(node, chunkInfo, true);
        }

        for (const auto& chunkInfo : request->removed_chunks()) {
            ProcessRemovedChunk(node, chunkInfo);
        }

        const auto* mutationContext = GetCurrentMutationContext();
        auto mutationTimestamp = mutationContext->GetTimestamp();

        auto& unapprovedReplicas = node->UnapprovedReplicas();
        for (auto it = unapprovedReplicas.begin(); it != unapprovedReplicas.end();) {
            auto jt = it++;
            auto replica = jt->first;
            auto registerTimestamp = jt->second;
            auto reason = ERemoveReplicaReason::None;
            if (!IsObjectAlive(replica.GetPtr())) {
                reason = ERemoveReplicaReason::ChunkDestroyed;
            } else if (mutationTimestamp > registerTimestamp + ReplicaApproveTimeout) {
                reason = ERemoveReplicaReason::ApproveTimeout;
            }
            if (reason != ERemoveReplicaReason::None) {
                // This also removes replica from unapproved set.
                auto mediumIndex = replica.GetMediumIndex();
                const auto* medium = GetMediumByIndex(mediumIndex);
                RemoveChunkReplica(medium, node, replica, reason);
            }
        }

        if (ChunkPlacement_) {
            ChunkPlacement_->OnNodeUpdated(node);
        }
    }

    void RegisterTagForDataCenter(const TDataCenter* dataCenter, THashMap<const TDataCenter*, TTagId>* dataCenterToTag)
    {
        auto dataCenterName = dataCenter ? dataCenter->GetName() : TString();
        auto tagId = TProfileManager::Get()->RegisterTag("source_data_center", dataCenterName);
        YCHECK(dataCenterToTag->emplace(dataCenter, tagId).second);
    }

    void RegisterTagsForDataCenter(const TDataCenter* dataCenter)
    {
        RegisterTagForDataCenter(dataCenter, &SourceDataCenterToTag_);
        RegisterTagForDataCenter(dataCenter, &DestinationDataCenterToTag_);
    }

    void UnregisterTagsForDataCenter(const TDataCenter* dataCenter)
    {
        // NB: just cleaning maps here, profile manager doesn't support unregistering tags.
        SourceDataCenterToTag_.erase(dataCenter);
        DestinationDataCenterToTag_.erase(dataCenter);
    }

    bool EnsureDataCenterTagsInitialized()
    {
        YCHECK(SourceDataCenterToTag_.empty() == DestinationDataCenterToTag_.empty());

        if (!SourceDataCenterToTag_.empty()) {
            return false;
        }

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto& pair : nodeTracker->DataCenters()) {
            RegisterTagsForDataCenter(pair.second);
        }
        RegisterTagsForDataCenter(nullptr);

        return true;
    }

    void OnDataCenterCreated(TDataCenter* dataCenter)
    {
        if (!EnsureDataCenterTagsInitialized()) {
            RegisterTagsForDataCenter(dataCenter);
        }
    }

    void OnDataCenterRenamed(TDataCenter* dataCenter)
    {
        if (!EnsureDataCenterTagsInitialized()) {
            UnregisterTagsForDataCenter(dataCenter);
            RegisterTagsForDataCenter(dataCenter);
        }
    }

    void OnDataCenterDestroyed(TDataCenter* dataCenter)
    {
        if (!EnsureDataCenterTagsInitialized()) {
            UnregisterTagsForDataCenter(dataCenter);
        }
    }

    TTagId GetDataCenterTag(const TDataCenter* dataCenter, THashMap<const TDataCenter*, TTagId>& dataCenterToTag)
    {
        auto it = dataCenterToTag.find(dataCenter);
        YCHECK(it != dataCenterToTag.end());
        return it->second;
    }

    TTagId GetSourceDataCenterTag(const TDataCenter* dataCenter)
    {
        EnsureDataCenterTagsInitialized();
        return GetDataCenterTag(dataCenter, SourceDataCenterToTag_);
    }

    TTagId GetDestinationDataCenterTag(const TDataCenter* dataCenter)
    {
        EnsureDataCenterTagsInitialized();
        return GetDataCenterTag(dataCenter, DestinationDataCenterToTag_);
    }

    void HydraUpdateChunkProperties(NProto::TReqUpdateChunkProperties* request)
    {
        // NB: Ordered map is a must to make the behavior deterministic.
        std::map<TCellTag, NProto::TReqUpdateChunkProperties> crossCellRequestMap;
        auto getCrossCellRequest = [&] (const TChunk* chunk) -> NProto::TReqUpdateChunkProperties& {
            auto cellTag = CellTagFromId(chunk->GetId());
            auto it = crossCellRequestMap.find(cellTag);
            if (it == crossCellRequestMap.end()) {
                it = crossCellRequestMap.emplace(cellTag, NProto::TReqUpdateChunkProperties()).first;
                it->second.set_cell_tag(Bootstrap_->GetCellTag());
            }
            return it->second;
        };

        bool local = request->cell_tag() == Bootstrap_->GetCellTag();

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        int cellIndex = local ? -1 : multicellManager->GetRegisteredMasterCellIndex(request->cell_tag());

        for (const auto& update : request->updates()) {
            auto chunkId = FromProto<TChunkId>(update.chunk_id());
            auto* chunk = FindChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                continue;
            }

            TChunkProperties newProperties;
            newProperties.SetVital(update.vital());
            for (const auto& mediumUpdate : update.medium_updates()) {
                if (mediumUpdate.has_medium_index() &&
                    mediumUpdate.has_replication_factor() &&
                    mediumUpdate.has_data_parts_only())
                {
                    auto mediumIndex = mediumUpdate.medium_index();
                    newProperties[mediumIndex].SetReplicationFactor(mediumUpdate.replication_factor());
                    newProperties[mediumIndex].SetDataPartsOnly(mediumUpdate.data_parts_only());
                }
            }

            Y_ASSERT(!local || newProperties.IsValid());

            auto& curProperties = local ? chunk->LocalProperties() : chunk->ExternalProperties(cellIndex);
            if (newProperties == curProperties) {
                continue;
            }

            curProperties = newProperties;
            if (chunk->IsForeign()) {
                Y_ASSERT(local);
                auto& crossCellRequest = getCrossCellRequest(chunk);
                *crossCellRequest.add_updates() = update;
            } else {
                ScheduleChunkRefresh(chunk);
            }
        }

        for (const auto& pair : crossCellRequestMap) {
            auto cellTag = pair.first;
            const auto& request = pair.second;
            multicellManager->PostToMaster(request, cellTag);
            LOG_DEBUG_UNLESS(IsRecovery(), "Requesting to update properties of imported chunks (CellTag: %v, Count: %v)",
                cellTag,
                request.updates_size());
        }
    }

    void HydraExportChunks(const TCtxExportChunksPtr& /*context*/, TReqExportChunks* request, TRspExportChunks* response)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);
        if (transaction->GetPersistentState() != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        std::vector<TChunkId> chunkIds;
        for (const auto& exportData : request->chunks()) {
            auto chunkId = FromProto<TChunkId>(exportData.id());
            auto* chunk = GetChunkOrThrow(chunkId);

            if (chunk->IsForeign()) {
                THROW_ERROR_EXCEPTION("Cannot export a foreign chunk %v", chunkId);
            }

            auto cellTag = exportData.destination_cell_tag();
            if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
                THROW_ERROR_EXCEPTION("Cell %v is not registered");
            }

            transactionManager->ExportObject(transaction, chunk, cellTag);

            if (response) {
                auto* importData = response->add_chunks();
                ToProto(importData->mutable_id(), chunkId);
                importData->mutable_info()->CopyFrom(chunk->ChunkInfo());
                importData->mutable_meta()->CopyFrom(chunk->ChunkMeta());
                importData->set_erasure_codec(static_cast<int>(chunk->GetErasureCodec()));
            }

            chunkIds.push_back(chunk->GetId());
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunks exported (TransactionId: %v, ChunkIds: %v)",
            transactionId,
            chunkIds);
    }

    void HydraImportChunks(const TCtxImportChunksPtr& /*context*/, TReqImportChunks* request, TRspImportChunks* /*response*/)
    {
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

        if (transaction->GetPersistentState() != ETransactionState::Active) {
            transaction->ThrowInvalidState();
        }

        std::vector<TChunkId> chunkIds;
        for (auto& importData : *request->mutable_chunks()) {
            auto chunkId = FromProto<TChunkId>(importData.id());
            if (CellTagFromId(chunkId) == Bootstrap_->GetCellTag()) {
                THROW_ERROR_EXCEPTION("Cannot import a native chunk %v", chunkId);
            }

            auto* chunk = ChunkMap_.Find(chunkId);
            if (!chunk) {
                chunk = DoCreateChunk(chunkId);
                chunk->SetForeign();
                chunk->Confirm(importData.mutable_info(), importData.mutable_meta());
                chunk->SetErasureCodec(NErasure::ECodec(importData.erasure_codec()));
                YCHECK(ForeignChunks_.insert(chunk).second);
            }

            transactionManager->ImportObject(transaction, chunk);

            chunkIds.push_back(chunk->GetId());
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunks imported (TransactionId: %v, ChunkIds: %v)",
            transactionId,
            chunkIds);
    }

    void HydraExecuteBatch(const TCtxExecuteBatchPtr& /*context*/, TReqExecuteBatch* request, TRspExecuteBatch* response)
    {
        auto executeSubrequests = [&] (
            auto* subrequests,
            auto* subresponses,
            auto handler,
            const char* errorMessage)
        {
            for (auto& subrequest : *subrequests) {
                auto* subresponse = subresponses ? subresponses->Add() : nullptr;
                try {
                    (this->*handler)(&subrequest, subresponse);
                } catch (const std::exception& ex) {
                    LOG_DEBUG_UNLESS(IsRecovery(), ex, errorMessage);
                    if (subresponse) {
                        ToProto(subresponse->mutable_error(), TError(ex));
                    }
                }
            }
        };

        executeSubrequests(
            request->mutable_create_chunk_subrequests(),
            response ? response->mutable_create_chunk_subresponses() : nullptr,
            &TImpl::ExecuteCreateChunkSubrequest,
            "Error creating chunk");

        executeSubrequests(
            request->mutable_confirm_chunk_subrequests(),
            response ? response->mutable_confirm_chunk_subresponses() : nullptr,
            &TImpl::ExecuteConfirmChunkSubrequest,
            "Error confirming chunk");

        executeSubrequests(
            request->mutable_seal_chunk_subrequests(),
            response ? response->mutable_seal_chunk_subresponses() : nullptr,
            &TImpl::ExecuteSealChunkSubrequest,
            "Error sealing chunk");

        executeSubrequests(
            request->mutable_create_chunk_lists_subrequests(),
            response ? response->mutable_create_chunk_lists_subresponses() : nullptr,
            &TImpl::ExecuteCreateChunkListsSubrequest,
            "Error creating chunk lists");

        executeSubrequests(
            request->mutable_unstage_chunk_tree_subrequests(),
            response ? response->mutable_unstage_chunk_tree_subresponses() : nullptr,
            &TImpl::ExecuteUnstageChunkTreeSubrequest,
            "Error unstaging chunk tree");

        executeSubrequests(
            request->mutable_attach_chunk_trees_subrequests(),
            response ? response->mutable_attach_chunk_trees_subresponses() : nullptr,
            &TImpl::ExecuteAttachChunkTreesSubrequest,
            "Error attaching chunk trees");
    }

    void ExecuteCreateChunkSubrequest(
        TReqExecuteBatch::TCreateChunkSubrequest* subrequest,
        TRspExecuteBatch::TCreateChunkSubresponse* subresponse)
    {
        auto transactionId = FromProto<TTransactionId>(subrequest->transaction_id());
        auto chunkType = EObjectType(subrequest->type());
        bool isErasure = (chunkType == EObjectType::ErasureChunk);
        bool isJournal = (chunkType == EObjectType::JournalChunk);
        auto erasureCodecId = isErasure ? NErasure::ECodec(subrequest->erasure_codec()) : NErasure::ECodec::None;
        int replicationFactor = isErasure ? 1 : subrequest->replication_factor();
        const auto& mediumName = subrequest->medium_name();
        int readQuorum = isJournal ? subrequest->read_quorum() : 0;
        int writeQuorum = isJournal ? subrequest->write_quorum() : 0;

        auto* medium = GetMediumByNameOrThrow(mediumName);
        int mediumIndex = medium->GetIndex();

        ValidateReplicationFactor(replicationFactor);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = securityManager->GetAccountByNameOrThrow(subrequest->account());
        auto resourceUsageIncrease = TClusterResources()
            .SetChunkCount(1)
            .SetMediumDiskSpace(mediumIndex, 1);
        if (subrequest->validate_resource_usage_increase()) {
            securityManager->ValidateResourceUsageIncrease(account, resourceUsageIncrease);
        }

        TChunkList* chunkList = nullptr;
        if (subrequest->has_chunk_list_id()) {
            auto chunkListId = FromProto<TChunkListId>(subrequest->chunk_list_id());
            chunkList = GetChunkListOrThrow(chunkListId);
            chunkList->ValidateSealed();
            chunkList->ValidateUniqueAncestors();
        }

        // NB: Once the chunk is created, no exceptions could be thrown.
        auto* chunk = DoCreateChunk(chunkType);
        chunk->SetReadQuorum(readQuorum);
        chunk->SetWriteQuorum(writeQuorum);
        chunk->SetErasureCodec(erasureCodecId);
        chunk->SetMovable(subrequest->movable());
        chunk->SetLocalVital(subrequest->vital());

        auto sessionId = TSessionId(chunk->GetId(), mediumIndex);

        auto& chunkProperties = chunk->LocalProperties();
        chunkProperties[mediumIndex].SetReplicationFactor(replicationFactor);
        chunkProperties.SetVital(subrequest->vital());

        StageChunkTree(chunk, transaction, account);

        transactionManager->StageObject(transaction, chunk);

        if (chunkList) {
            AttachToChunkList(chunkList, chunk);
        }

        if (subresponse) {
            ToProto(subresponse->mutable_session_id(), sessionId);
        }

        LOG_DEBUG_UNLESS(IsRecovery(),
            "Chunk created "
            "(ChunkId: %v, ChunkListId: %v, TransactionId: %v, Account: %v, "
            "ReplicationFactor: %v, ReadQuorum: %v, WriteQuorum: %v, ErasureCodec: %v, Movable: %v, Vital: %v)",
            sessionId,
            GetObjectId(chunkList),
            transaction->GetId(),
            account->GetName(),
            replicationFactor,
            readQuorum,
            writeQuorum,
            erasureCodecId,
            subrequest->movable(),
            subrequest->vital());
    }

    void ExecuteConfirmChunkSubrequest(
        TReqExecuteBatch::TConfirmChunkSubrequest* subrequest,
        TRspExecuteBatch::TConfirmChunkSubresponse* subresponse)
    {
        auto chunkId = FromProto<TChunkId>(subrequest->chunk_id());
        auto replicas = FromProto<TChunkReplicaList>(subrequest->replicas());

        auto* chunk = GetChunkOrThrow(chunkId);

        ConfirmChunk(
            chunk,
            replicas,
            subrequest->mutable_chunk_info(),
            subrequest->mutable_chunk_meta());

        if (subresponse && subrequest->request_statistics()) {
            *subresponse->mutable_statistics() = chunk->GetStatistics().ToDataStatistics();
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk confirmed (ChunkId: %v, Replicas: %v)",
            chunkId,
            replicas);
    }

    void ExecuteSealChunkSubrequest(
        TReqExecuteBatch::TSealChunkSubrequest* subrequest,
        TRspExecuteBatch::TSealChunkSubresponse* subresponse)
    {
        auto chunkId = FromProto<TChunkId>(subrequest->chunk_id());
        auto* chunk = GetChunkOrThrow(chunkId);

        const auto& miscExt = subrequest->misc();

        SealChunk(
            chunk,
            miscExt);

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk sealed "
            "(ChunkId: %v, RowCount: %v, UncompressedDataSize: %v, CompressedDataSize: %v)",
            chunk->GetId(),
            miscExt.row_count(),
            miscExt.uncompressed_data_size(),
            miscExt.compressed_data_size());
    }

    void ExecuteCreateChunkListsSubrequest(
        TReqExecuteBatch::TCreateChunkListsSubrequest* subrequest,
        TRspExecuteBatch::TCreateChunkListsSubresponse* subresponse)
    {
        auto transactionId = FromProto<TTransactionId>(subrequest->transaction_id());
        int count = subrequest->count();

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

        std::vector<TChunkListId> chunkListIds;
        chunkListIds.reserve(count);
        for (int index = 0; index < count; ++index) {
            auto* chunkList = DoCreateChunkList(EChunkListKind::Static);
            StageChunkTree(chunkList, transaction, nullptr);
            transactionManager->StageObject(transaction, chunkList);
            ToProto(subresponse->add_chunk_list_ids(), chunkList->GetId());
            chunkListIds.push_back(chunkList->GetId());
        }

        LOG_DEBUG_UNLESS(IsRecovery(),
            "Chunk lists created (ChunkListIds: %v, TransactionId: %v)",
            chunkListIds,
            transaction->GetId());
    }

    void ExecuteUnstageChunkTreeSubrequest(
        TReqExecuteBatch::TUnstageChunkTreeSubrequest* subrequest,
        TRspExecuteBatch::TUnstageChunkTreeSubresponse* subresponse)
    {
        auto chunkTreeId = FromProto<TTransactionId>(subrequest->chunk_tree_id());
        auto recursive = subrequest->recursive();

        auto* chunkTree = GetChunkTreeOrThrow(chunkTreeId);
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->UnstageObject(chunkTree->GetStagingTransaction(), chunkTree, recursive);

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree unstaged (ChunkTreeId: %v, Recursive: %v)",
            chunkTreeId,
            recursive);
    }

    void ExecuteAttachChunkTreesSubrequest(
        TReqExecuteBatch::TAttachChunkTreesSubrequest* subrequest,
        TRspExecuteBatch::TAttachChunkTreesSubresponse* subresponse)
    {
        auto parentId = FromProto<TTransactionId>(subrequest->parent_id());
        auto* parent = GetChunkListOrThrow(parentId);

        std::vector<TChunkTree*> children;
        children.reserve(subrequest->child_ids_size());
        for (const auto& protoChildId : subrequest->child_ids()) {
            auto childId = FromProto<TChunkTreeId>(protoChildId);
            auto* child = GetChunkTreeOrThrow(childId);
            children.push_back(child);
            // YT-6542: Make sure we never attach a chunk list to its parent more than once.
            if (child->GetType() == EObjectType::ChunkList) {
                auto* chunkListChild = child->AsChunkList();
                for (auto* someParent : chunkListChild->Parents()) {
                    if (someParent == parent) {
                        THROW_ERROR_EXCEPTION("Chunk list %v already has %v as its parent",
                            chunkListChild->GetId(),
                            parent->GetId());
                    }
                }
            }
        }

        AttachToChunkList(parent, children);

        if (subrequest->request_statistics()) {
            *subresponse->mutable_statistics() = parent->Statistics().ToDataStatistics();
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk trees attached (ParentId: %v, ChildIds: %v)",
            parentId,
            MakeFormattableRange(children, TObjectIdFormatter()));
    }


    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        ChunkMap_.SaveKeys(context);
        ChunkListMap_.SaveKeys(context);
        MediumMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        ChunkMap_.SaveValues(context);
        ChunkListMap_.SaveValues(context);
        MediumMap_.SaveValues(context);
    }


    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        ChunkMap_.LoadKeys(context);
        ChunkListMap_.LoadKeys(context);
        // COMPAT(shakurov)
        if (context.GetVersion() >= 400) {
            MediumMap_.LoadKeys(context);
        }
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        ChunkMap_.LoadValues(context);
        ChunkListMap_.LoadValues(context);
        // COMPAT(shakurov)
        if (context.GetVersion() >= 400) {
            MediumMap_.LoadValues(context);
        }
        //COMPAT(savrus)
        NeedResetDataWeight_ = context.GetVersion() < 612;
        //COMPAT(savrus)
        NeedInitializeMediumConfig_ = context.GetVersion() < 629;
    }

    virtual void OnBeforeSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnBeforeSnapshotLoaded();

        NeedToRecomputeStatistics_ = false;
        NeedResetDataWeight_ = false;
        NeedInitializeMediumConfig_ = false;
    }

    virtual void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        //COMPAT(savrus)
        if (NeedResetDataWeight_) {
            for (const auto& pair : ChunkListMap_) {
                auto* chunkList = pair.second;
                chunkList->Statistics().DataWeight = -1;
            }

            const auto& cypressManager = Bootstrap_->GetCypressManager();
            for (const auto& pair : cypressManager->Nodes()) {
                if (auto* node = dynamic_cast<TChunkOwnerBase*>(pair.second)) {
                    node->SnapshotStatistics().set_data_weight(-1);
                    node->DeltaStatistics().set_data_weight(-1);
                }
            }
        }

        // Populate nodes' chunk replica sets.
        // Compute chunk replica count.

        LOG_INFO("Started initializing chunks");

        for (const auto& pair : ChunkMap_) {
            auto* chunk = pair.second;

            RegisterChunk(chunk);

            auto addReplicas = [&] (const auto& replicas) {
                for (auto replica : replicas) {
                    TChunkPtrWithIndexes chunkWithIndexes(
                        chunk,
                        replica.GetReplicaIndex(),
                        replica.GetMediumIndex());
                    replica.GetPtr()->AddReplica(chunkWithIndexes);
                    ++TotalReplicaCount_;
                }
            };
            addReplicas(chunk->StoredReplicas());
            addReplicas(chunk->CachedReplicas());

            if (chunk->IsForeign()) {
                YCHECK(ForeignChunks_.insert(chunk).second);
            }
        }

        for (const auto& pair : MediumMap_) {
            auto* medium = pair.second;
            RegisterMedium(medium);
        }

        InitBuiltins();

        if (NeedToRecomputeStatistics_) {
            RecomputeStatistics();
            NeedToRecomputeStatistics_ = false;
        }

        if (NeedInitializeMediumConfig_) {
            for (const auto& pair : MediumMap_) {
                auto* medium = pair.second;
                InitializeMediumConfig(medium);
            }
            NeedInitializeMediumConfig_ = false;
        }

        LOG_INFO("Finished initializing chunks");
    }


    virtual void Clear() override
    {
        TMasterAutomatonPart::Clear();

        AllChunks_.Clear();
        JournalChunks_.Clear();
        ChunkMap_.Clear();
        ChunkListMap_.Clear();
        ForeignChunks_.clear();
        TotalReplicaCount_ = 0;

        MediumMap_.Clear();
        NameToMediumMap_.clear();
        IndexToMediumMap_ = std::vector<TMedium*>(MaxMediumCount, nullptr);
        UsedMediumIndexes_.reset();

        ChunksCreated_ = 0;
        ChunksDestroyed_ = 0;
        ChunkReplicasAdded_ = 0;
        ChunkReplicasRemoved_ = 0;
        ChunkListsCreated_ = 0;
        ChunkListsDestroyed_ = 0;

        DefaultStoreMedium_ = nullptr;
        DefaultCacheMedium_ = nullptr;
    }

    virtual void SetZeroState() override
    {
        TMasterAutomatonPart::SetZeroState();

        InitBuiltins();
    }


    void InitBuiltins()
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        // Media

        // default
        if (EnsureBuiltinMediumInitialized(
            DefaultStoreMedium_,
            DefaultStoreMediumId_,
            DefaultStoreMediumIndex,
            DefaultStoreMediumName,
            false))
        {
            DefaultStoreMedium_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                securityManager->GetUsersGroup(),
                EPermission::Use));
        }

        // cache
        if (EnsureBuiltinMediumInitialized(
            DefaultCacheMedium_,
            DefaultCacheMediumId_,
            DefaultCacheMediumIndex,
            DefaultCacheMediumName,
            true))
        {
            DefaultCacheMedium_->Acd().AddEntry(TAccessControlEntry(
                ESecurityAction::Allow,
                securityManager->GetUsersGroup(),
                EPermission::Use));
        }
    }

    bool EnsureBuiltinMediumInitialized(
        TMedium*& medium,
        const TMediumId& id,
        int mediumIndex,
        const TString& name,
        bool cache)
    {
        if (medium) {
            return false;
        }
        medium = FindMedium(id);
        if (medium) {
            return false;
        }
        medium = DoCreateMedium(id, mediumIndex, name, false, cache, Null);
        return true;
    }


    void ScheduleRecomputeStatistics()
    {
        NeedToRecomputeStatistics_ = true;
    }

    void RecomputeStatistics()
    {
        LOG_INFO("Started recomputing statistics");

        auto visitMark = TChunkList::GenerateVisitMark();

        std::vector<TChunkList*> chunkLists;
        std::vector<std::pair<TChunkList*, int>> stack;

        auto visit = [&] (TChunkList* chunkList) {
            if (chunkList->GetVisitMark() != visitMark) {
                chunkList->SetVisitMark(visitMark);
                stack.emplace_back(chunkList, 0);
            }
        };

        // Sort chunk lists in topological order
        for (const auto& pair : ChunkListMap_) {
            auto* chunkList = pair.second;
            visit(chunkList);

            while (!stack.empty()) {
                chunkList = stack.back().first;
                int childIndex = stack.back().second;
                int childCount = chunkList->Children().size();

                if (childIndex == childCount) {
                    chunkLists.push_back(chunkList);
                    stack.pop_back();
                } else {
                    ++stack.back().second;
                    auto* child = chunkList->Children()[childIndex];
                    if (child && child->GetType() == EObjectType::ChunkList) {
                        visit(child->AsChunkList());
                    }
                }
            }
        }

        // Recompute statistics
        for (auto* chunkList : chunkLists) {
            auto& statistics = chunkList->Statistics();
            auto oldStatistics = statistics;
            statistics = TChunkTreeStatistics();
            int childCount = chunkList->Children().size();

            auto& cumulativeStatistics = chunkList->CumulativeStatistics();
            cumulativeStatistics.clear();

            for (int childIndex = 0; childIndex < childCount; ++childIndex) {
                auto* child = chunkList->Children()[childIndex];
                if (!child) {
                    continue;
                }

                TChunkTreeStatistics childStatistics;
                switch (child->GetType()) {
                    case EObjectType::Chunk:
                    case EObjectType::ErasureChunk:
                    case EObjectType::JournalChunk:
                        childStatistics.Accumulate(child->AsChunk()->GetStatistics());
                        break;

                    case EObjectType::ChunkList:
                        childStatistics.Accumulate(child->AsChunkList()->Statistics());
                        break;

                    default:
                        Y_UNREACHABLE();
                }

                if (childIndex + 1 < childCount) {
                    cumulativeStatistics.push_back({
                        statistics.LogicalRowCount + childStatistics.LogicalRowCount,
                        statistics.LogicalChunkCount + childStatistics.LogicalChunkCount,
                        statistics.UncompressedDataSize + childStatistics.UncompressedDataSize
                    });
                }

                statistics.Accumulate(childStatistics);
            }

            ++statistics.Rank;
            ++statistics.ChunkListCount;

            if (statistics != oldStatistics) {
                LOG_DEBUG("Chunk list statistics changed (ChunkList: %v, OldStatistics: %v, NewStatistics: %v)",
                    chunkList->GetId(),
                    oldStatistics,
                    statistics);
            }
        }

        LOG_INFO("Finished recomputing statistics");
    }

    virtual void OnRecoveryStarted() override
    {
        TMasterAutomatonPart::OnRecoveryStarted();

        Profiler.SetEnabled(false);
    }

    virtual void OnRecoveryComplete() override
    {
        TMasterAutomatonPart::OnRecoveryComplete();

        Profiler.SetEnabled(true);
    }

    virtual void OnLeaderRecoveryComplete() override
    {
        TMasterAutomatonPart::OnLeaderRecoveryComplete();

        ChunkPlacement_ = New<TChunkPlacement>(Config_, Bootstrap_);
        ChunkReplicator_ = New<TChunkReplicator>(Config_, Bootstrap_, ChunkPlacement_);
        ChunkSealer_ = New<TChunkSealer>(Config_, Bootstrap_);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto& pair : nodeTracker->Nodes()) {
            auto* node = pair.second;
            ChunkReplicator_->OnNodeRegistered(node);
            ChunkPlacement_->OnNodeRegistered(node);
        }
    }

    virtual void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        ChunkReplicator_->Start(AllChunks_.GetFront(), AllChunks_.GetSize());
        ChunkSealer_->Start(JournalChunks_.GetFront(), JournalChunks_.GetSize());
    }

    virtual void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        ChunkPlacement_.Reset();

        if (ChunkReplicator_) {
            ChunkReplicator_->Stop();
            ChunkReplicator_.Reset();
        }

        if (ChunkSealer_) {
            ChunkSealer_->Stop();
            ChunkSealer_.Reset();
        }
    }


    void RegisterChunk(TChunk* chunk)
    {
        AllChunks_.PushFront(chunk);
        if (chunk->IsJournal()) {
            JournalChunks_.PushFront(chunk);
        }
    }

    void UnregisterChunk(TChunk* chunk)
    {
        AllChunks_.Remove(chunk);
        if (chunk->IsJournal()) {
            JournalChunks_.Remove(chunk);
        }
    }


    void AddChunkReplica(
        const TMedium* medium,
        TNode* node,
        TChunkPtrWithIndexes chunkWithIndexes,
        EAddReplicaReason reason)
    {
        auto* chunk = chunkWithIndexes.GetPtr();
        bool cached = medium->GetCache();
        auto nodeId = node->GetId();
        TNodePtrWithIndexes nodeWithIndexes(
            node,
            chunkWithIndexes.GetReplicaIndex(),
            chunkWithIndexes.GetMediumIndex());

        if (!node->AddReplica(chunkWithIndexes)) {
            return;
        }

        chunk->AddReplica(nodeWithIndexes, medium);

        if (!IsRecovery()) {
            LOG_EVENT(
                Logger,
                reason == EAddReplicaReason::FullHeartbeat ? NLogging::ELogLevel::Trace : NLogging::ELogLevel::Debug,
                "Chunk replica added (ChunkId: %v, NodeId: %v, Address: %v)",
                chunkWithIndexes,
                nodeId,
                node->GetDefaultAddress());
        }

        if (!cached) {
            ScheduleChunkRefresh(chunk);
        }

        if (ChunkSealer_ && !cached && chunk->IsJournal()) {
            ChunkSealer_->ScheduleSeal(chunk);
        }

        if (reason == EAddReplicaReason::IncrementalHeartbeat || reason == EAddReplicaReason::Confirmation) {
            ++ChunkReplicasAdded_;
        }
    }

    void RemoveChunkReplica(
        const TMedium* medium,
        TNode* node,
        TChunkPtrWithIndexes chunkWithIndexes,
        ERemoveReplicaReason reason)
    {
        auto* chunk = chunkWithIndexes.GetPtr();
        bool cached = medium->GetCache();
        auto nodeId = node->GetId();
        TNodePtrWithIndexes nodeWithIndexes(
            node,
            chunkWithIndexes.GetReplicaIndex(),
            chunkWithIndexes.GetMediumIndex());

        if (reason == ERemoveReplicaReason::IncrementalHeartbeat && !node->HasReplica(chunkWithIndexes)) {
            return;
        }

        chunk->RemoveReplica(nodeWithIndexes, medium);

        switch (reason) {
            case ERemoveReplicaReason::IncrementalHeartbeat:
            case ERemoveReplicaReason::ApproveTimeout:
            case ERemoveReplicaReason::ChunkDestroyed:
                node->RemoveReplica(chunkWithIndexes);
                if (ChunkReplicator_ && !cached) {
                    ChunkReplicator_->OnReplicaRemoved(node, chunkWithIndexes, reason);
                }
                break;
            case ERemoveReplicaReason::NodeDisposed:
                // Do nothing.
                break;
            default:
                Y_UNREACHABLE();
        }

        if (!IsRecovery()) {
            LOG_EVENT(
                Logger,
                reason == ERemoveReplicaReason::NodeDisposed ||
                reason == ERemoveReplicaReason::ChunkDestroyed
                ? NLogging::ELogLevel::Trace : NLogging::ELogLevel::Debug,
                "Chunk replica removed (ChunkId: %v, Reason: %v, NodeId: %v, Address: %v)",
                chunkWithIndexes,
                reason,
                nodeId,
                node->GetDefaultAddress());
        }

        if (!cached) {
            ScheduleChunkRefresh(chunk);
        }

        ++ChunkReplicasRemoved_;
    }


    static std::pair<int,int> GetAddedChunkIndexes(
        TChunk* chunk,
        const TChunkAddInfo& chunkAddInfo,
        const TChunkIdWithIndexes& chunkIdWithIndexes)
    {
        int mediumIndex = chunkIdWithIndexes.MediumIndex;
        int replicaIndex;
        if (chunk->IsJournal()) {
            if (chunkAddInfo.active()) {
                replicaIndex = ActiveChunkReplicaIndex;
            } else if (chunkAddInfo.sealed()) {
                replicaIndex = SealedChunkReplicaIndex;
            } else {
                replicaIndex = UnsealedChunkReplicaIndex;
            }
        } else {
            replicaIndex = chunkIdWithIndexes.ReplicaIndex;
        }
        return {replicaIndex, mediumIndex};
    }

    void ProcessAddedChunk(
        TNode* node,
        const TChunkAddInfo& chunkAddInfo,
        bool incremental)
    {
        auto nodeId = node->GetId();
        auto chunkId = FromProto<TChunkId>(chunkAddInfo.chunk_id());
        auto chunkIdWithIndex = DecodeChunkId(chunkId);
        TChunkIdWithIndexes chunkIdWithIndexes(chunkIdWithIndex, chunkAddInfo.medium_index());

        auto* medium = FindMediumByIndex(chunkIdWithIndexes.MediumIndex);
        if (!medium) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Cannot add chunk with unknown medium (NodeId: %v, Address: %v, ChunkId: %v)",
                nodeId,
                node->GetDefaultAddress(),
                chunkIdWithIndexes);
            return;
        }

        bool cached = medium->GetCache();

        auto* chunk = FindChunk(chunkIdWithIndexes.Id);
        if (!IsObjectAlive(chunk)) {
            if (cached) {
                // Nodes may still contain cached replicas of chunks that no longer exist.
                // We just silently ignore this case.
                return;
            }

            LOG_DEBUG_UNLESS(IsRecovery(), "Unknown chunk added, removal scheduled (NodeId: %v, Address: %v, ChunkId: %v)",
                nodeId,
                node->GetDefaultAddress(),
                chunkIdWithIndexes);

            if (ChunkReplicator_) {
                ChunkReplicator_->ScheduleUnknownReplicaRemoval(node, chunkIdWithIndexes);
            }

            return;
        }

        auto indexes = GetAddedChunkIndexes(chunk, chunkAddInfo, chunkIdWithIndexes);
        TChunkPtrWithIndexes chunkWithIndexes(chunk, indexes.first, indexes.second);
        TNodePtrWithIndexes nodeWithIndexes(node, indexes.first, indexes.second);

        if (!cached && node->HasUnapprovedReplica(chunkWithIndexes)) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Chunk approved (NodeId: %v, Address: %v, ChunkId: %v)",
                nodeId,
                node->GetDefaultAddress(),
                chunkWithIndexes);

            node->ApproveReplica(chunkWithIndexes);
            chunk->ApproveReplica(nodeWithIndexes);
            return;
        }

        AddChunkReplica(
            medium,
            node,
            chunkWithIndexes,
            incremental ? EAddReplicaReason::IncrementalHeartbeat : EAddReplicaReason::FullHeartbeat);
    }

    void ProcessRemovedChunk(
        TNode* node,
        const TChunkRemoveInfo& chunkInfo)
    {
        auto nodeId = node->GetId();
        auto chunkIdWithIndex = DecodeChunkId(FromProto<TChunkId>(chunkInfo.chunk_id()));
        TChunkIdWithIndexes chunkIdWithIndexes(chunkIdWithIndex, chunkInfo.medium_index());

        auto* medium = FindMediumByIndex(chunkIdWithIndexes.MediumIndex);
        if (!medium) {
            LOG_WARNING_UNLESS(IsRecovery(), "Cannot remove chunk with unknown medium (NodeId: %v, Address: %v, ChunkId: %v)",
                nodeId,
                node->GetDefaultAddress(),
                chunkIdWithIndexes);
            return;
        }

        auto* chunk = FindChunk(chunkIdWithIndex.Id);
        // NB: Chunk could already be a zombie but we still need to remove the replica.
        if (!chunk) {
            LOG_DEBUG_UNLESS(IsRecovery(), "Unknown chunk replica removed (ChunkId: %v, Address: %v, NodeId: %v)",
                chunkIdWithIndexes,
                node->GetDefaultAddress(),
                nodeId);
            return;
        }

        TChunkPtrWithIndexes chunkWithIndexes(
            chunk,
            chunkIdWithIndexes.ReplicaIndex,
            chunkIdWithIndexes.MediumIndex);
        RemoveChunkReplica(
            medium,
            node,
            chunkWithIndexes,
            ERemoveReplicaReason::IncrementalHeartbeat);
    }


    void OnChunkSealed(TChunk* chunk)
    {
        Y_ASSERT(chunk->IsSealed());

        if (chunk->Parents().empty())
            return;

        // Go upwards and apply delta.
        YCHECK(chunk->Parents().size() == 1);
        auto* chunkList = chunk->Parents()[0];

        auto statisticsDelta = chunk->GetStatistics();
        AccumulateUniqueAncestorsStatistics(chunkList, statisticsDelta);

        const auto& securityManager = Bootstrap_->GetSecurityManager();

        auto owningNodes = GetOwningNodes(chunk);

        bool journalNodeLocked = false;
        TJournalNode* trunkJournalNode = nullptr;
        for (auto* node : owningNodes) {
            securityManager->UpdateAccountNodeUsage(node);
            if (node->GetType() == EObjectType::Journal) {
                auto* journalNode = node->As<TJournalNode>();
                if (journalNode->GetUpdateMode() != EUpdateMode::None) {
                    journalNodeLocked = true;
                }
                if (trunkJournalNode) {
                    YCHECK(journalNode->GetTrunkNode() == trunkJournalNode);
                } else {
                    trunkJournalNode = journalNode->GetTrunkNode();
                }
            }
        }

        if (!journalNodeLocked && IsObjectAlive(trunkJournalNode)) {
            const auto& journalManager = Bootstrap_->GetJournalManager();
            journalManager->SealJournal(trunkJournalNode, nullptr);
        }
    }


    void OnProfiling()
    {
        if (!IsLeader()) {
            return;
        }

        Profiler.Enqueue("/refresh_queue_size", ChunkReplicator_->GetRefreshQueueSize(), EMetricType::Gauge);
        Profiler.Enqueue("/properties_update_queue_size", ChunkReplicator_->GetPropertiesUpdateQueueSize(), EMetricType::Gauge);
        Profiler.Enqueue("/seal_queue_size", ChunkSealer_->GetQueueSize(), EMetricType::Gauge);

        Profiler.Enqueue("/chunk_count", ChunkMap_.GetSize(), EMetricType::Gauge);
        Profiler.Enqueue("/chunks_created", ChunksCreated_, EMetricType::Counter);
        Profiler.Enqueue("/chunks_destroyed", ChunksDestroyed_, EMetricType::Counter);

        Profiler.Enqueue("/chunk_replica_count", TotalReplicaCount_, EMetricType::Gauge);
        Profiler.Enqueue("/chunk_replicas_added", ChunkReplicasAdded_, EMetricType::Counter);
        Profiler.Enqueue("/chunk_replicas_removed", ChunkReplicasRemoved_, EMetricType::Counter);

        Profiler.Enqueue("/chunk_list_count", ChunkListMap_.GetSize(), EMetricType::Gauge);
        Profiler.Enqueue("/chunk_lists_created", ChunkListsCreated_, EMetricType::Counter);
        Profiler.Enqueue("/chunk_lists_destroyed", ChunkListsDestroyed_, EMetricType::Counter);

        Profiler.Enqueue("/lost_chunk_count", LostChunks().size(), EMetricType::Gauge);
        Profiler.Enqueue("/lost_vital_chunk_count", LostVitalChunks().size(), EMetricType::Gauge);
        Profiler.Enqueue("/overreplicated_chunk_count", OverreplicatedChunks().size(), EMetricType::Gauge);
        Profiler.Enqueue("/underreplicated_chunk_count", UnderreplicatedChunks().size(), EMetricType::Gauge);
        Profiler.Enqueue("/data_missing_chunk_count", DataMissingChunks().size(), EMetricType::Gauge);
        Profiler.Enqueue("/parity_missing_chunk_count", ParityMissingChunks().size(), EMetricType::Gauge);
        Profiler.Enqueue("/precarious_chunk_count", PrecariousChunks().size(), EMetricType::Gauge);
        Profiler.Enqueue("/precarious_vital_chunk_count", PrecariousVitalChunks().size(), EMetricType::Gauge);
        Profiler.Enqueue("/quorum_missing_chunk_count", QuorumMissingChunks().size(), EMetricType::Gauge);
        Profiler.Enqueue("/unsafely_placed_chunk_count", UnsafelyPlacedChunks().size(), EMetricType::Gauge);

        const auto& jobCounters = ChunkReplicator_->JobCounters();
        for (auto jobType = EJobType::ReplicatorFirst;
             jobType != EJobType::ReplicatorLast;
             jobType = static_cast<EJobType>(static_cast<TEnumTraits<EJobType>::TUnderlying>(jobType) + 1))
        {
            Profiler.Enqueue("/job_count", jobCounters[jobType], EMetricType::Gauge, {JobTypeToTag_[jobType]});
        }

        for (const auto& srcPair : ChunkReplicator_->InterDCEdgeConsumption()) {
            const auto* src = srcPair.first;
            for (const auto& dstPair : srcPair.second) {
                const auto* dst = dstPair.first;
                const auto consumption = dstPair.second;

                TTagIdList tags = {GetSourceDataCenterTag(src), GetDestinationDataCenterTag(dst)};
                Profiler.Enqueue("/inter_dc_edge_consumption", consumption, EMetricType::Gauge, tags);
            }
        }
        for (const auto& srcPair : ChunkReplicator_->InterDCEdgeCapacities()) {
            const auto* src = srcPair.first;
            for (const auto& dstPair : srcPair.second) {
                const auto* dst = dstPair.first;
                const auto capacity = dstPair.second;

                TTagIdList tags = {GetSourceDataCenterTag(src), GetDestinationDataCenterTag(dst)};
                Profiler.Enqueue("/inter_dc_edge_capacity", capacity, EMetricType::Gauge, tags);
            }
        }
    }


    int GetFreeMediumIndex()
    {
        for (int index = 0; index < MaxMediumCount; ++index) {
            if (!UsedMediumIndexes_[index]) {
                return index;
            }
        }
        Y_UNREACHABLE();
    }

    TMedium* DoCreateMedium(
        const TMediumId& id,
        int mediumIndex,
        const TString& name,
        TNullable<bool> transient,
        TNullable<bool> cache,
        TNullable<int> priority)
    {
        auto mediumHolder = std::make_unique<TMedium>(id);
        mediumHolder->SetName(name);
        mediumHolder->SetIndex(mediumIndex);
        if (transient) {
            mediumHolder->SetTransient(*transient);
        }
        if (cache) {
            mediumHolder->SetCache(*cache);
        }
        if (priority) {
            ValidateMediumPriority(*priority);
            mediumHolder->SetPriority(*priority);
        }

        auto* medium = MediumMap_.Insert(id, std::move(mediumHolder));
        RegisterMedium(medium);
        InitializeMediumConfig(medium);

        // Make the fake reference.
        YCHECK(medium->RefObject() == 1);

        return medium;
    }

    void RegisterMedium(TMedium* medium)
    {
        YCHECK(NameToMediumMap_.emplace(medium->GetName(), medium).second);

        auto mediumIndex = medium->GetIndex();
        YCHECK(!UsedMediumIndexes_[mediumIndex]);
        UsedMediumIndexes_.set(mediumIndex);

        YCHECK(!IndexToMediumMap_[mediumIndex]);
        IndexToMediumMap_[mediumIndex] = medium;
    }

    void UnregisterMedium(TMedium* medium)
    {
        YCHECK(NameToMediumMap_.erase(medium->GetName()) == 1);

        auto mediumIndex = medium->GetIndex();
        YCHECK(UsedMediumIndexes_[mediumIndex]);
        UsedMediumIndexes_.reset(mediumIndex);

        YCHECK(IndexToMediumMap_[mediumIndex] == medium);
        IndexToMediumMap_[mediumIndex] = nullptr;
    }

    void InitializeMediumConfig(TMedium* medium)
    {
        medium->Config()->MaxReplicasPerRack = Config_->MaxReplicasPerRack;
        medium->Config()->MaxRegularReplicasPerRack = Config_->MaxRegularReplicasPerRack;
        medium->Config()->MaxJournalReplicasPerRack = Config_->MaxJournalReplicasPerRack;
        medium->Config()->MaxErasureReplicasPerRack = Config_->MaxErasureReplicasPerRack;
    }

    static void ValidateMediumName(const TString& name)
    {
        if (name.empty()) {
            THROW_ERROR_EXCEPTION("Medium name cannot be empty");
        }
    }

    static void ValidateMediumPriority(int priority)
    {
        if (priority < 0 || priority > MaxMediumPriority) {
            THROW_ERROR_EXCEPTION("Medium priority must be in range [0,%v]", MaxMediumPriority);
        }
    }

};

DEFINE_ENTITY_MAP_ACCESSORS(TChunkManager::TImpl, Chunk, TChunk, ChunkMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TChunkManager::TImpl, ChunkList, TChunkList, ChunkListMap_)

DEFINE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(TChunkManager::TImpl, Medium, Media, TMedium, MediumMap_)

DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, THashSet<TChunk*>, LostChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, THashSet<TChunk*>, LostVitalChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, THashSet<TChunk*>, OverreplicatedChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, THashSet<TChunk*>, UnderreplicatedChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, THashSet<TChunk*>, DataMissingChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, THashSet<TChunk*>, ParityMissingChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, THashSet<TChunk*>, PrecariousChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, THashSet<TChunk*>, PrecariousVitalChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, THashSet<TChunk*>, QuorumMissingChunks, *ChunkReplicator_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager::TImpl, THashSet<TChunk*>, UnsafelyPlacedChunks, *ChunkReplicator_);

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkTypeHandlerBase::TChunkTypeHandlerBase(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->ChunkMap_)
    , Owner_(owner)
{ }

IObjectProxyPtr TChunkManager::TChunkTypeHandlerBase::DoGetProxy(
    TChunk* chunk,
    TTransaction* /*transaction*/)
{
    return CreateChunkProxy(Bootstrap_, &Metadata_, chunk);
}

void TChunkManager::TChunkTypeHandlerBase::DoDestroyObject(TChunk* chunk)
{
    TObjectTypeHandlerWithMapBase::DoDestroyObject(chunk);
    Owner_->DestroyChunk(chunk);
}

void TChunkManager::TChunkTypeHandlerBase::DoUnstageObject(
    TChunk* chunk,
    bool recursive)
{
    TObjectTypeHandlerWithMapBase::DoUnstageObject(chunk, recursive);
    Owner_->UnstageChunk(chunk);
}

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkListTypeHandler::TChunkListTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->ChunkListMap_)
    , Owner_(owner)
{ }

IObjectProxyPtr TChunkManager::TChunkListTypeHandler::DoGetProxy(
    TChunkList* chunkList,
    TTransaction* /*transaction*/)
{
    return CreateChunkListProxy(Bootstrap_, &Metadata_, chunkList);
}

void TChunkManager::TChunkListTypeHandler::DoDestroyObject(TChunkList* chunkList)
{
    TObjectTypeHandlerWithMapBase::DoDestroyObject(chunkList);
    Owner_->DestroyChunkList(chunkList);
}

void TChunkManager::TChunkListTypeHandler::DoUnstageObject(
    TChunkList* chunkList,
    bool recursive)
{
    TObjectTypeHandlerWithMapBase::DoUnstageObject(chunkList, recursive);
    Owner_->UnstageChunkList(chunkList, recursive);
}

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TMediumTypeHandler::TMediumTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->MediumMap_)
    , Owner_(owner)
{ }

IObjectProxyPtr TChunkManager::TMediumTypeHandler::DoGetProxy(
    TMedium* medium,
    TTransaction* /*transaction*/)
{
    return CreateMediumProxy(Bootstrap_, &Metadata_, medium);
}

TObjectBase* TChunkManager::TMediumTypeHandler::CreateObject(
    const TObjectId& hintId,
    IAttributeDictionary* attributes)
{
    auto name = attributes->GetAndRemove<TString>("name");
    // These three are optional.
    auto priority = attributes->FindAndRemove<int>("priority");
    auto transient = attributes->FindAndRemove<bool>("transient");
    auto cache = attributes->FindAndRemove<bool>("cache");
    if (cache) {
        THROW_ERROR_EXCEPTION("Cannot create a new cache medium");
    }

    return Owner_->CreateMedium(name, transient, cache, priority, hintId);
}

void TChunkManager::TMediumTypeHandler::DoZombifyObject(TMedium* medium)
{
    // NB: Destroying arbitrary media is not currently supported.
    // This handler, however, is needed to destroy just-created media
    // for which attribute initialization has failed.
    Owner_->DestroyMedium(medium);
}

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TChunkManager(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TChunkManager::~TChunkManager() = default;

void TChunkManager::Initialize()
{
    Impl_->Initialize();
}

TChunk* TChunkManager::GetChunkOrThrow(const TChunkId& id)
{
    return Impl_->GetChunkOrThrow(id);
}

TChunkList* TChunkManager::GetChunkListOrThrow(const TChunkListId& id)
{
    return Impl_->GetChunkListOrThrow(id);
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
    TMedium* medium,
    TChunk* chunk,
    int desiredCount,
    int minCount,
    TNullable<int> replicationFactorOverride,
    const TNodeList* forbiddenNodes,
    const TNullable<TString>& preferredHostName)
{
    return Impl_->AllocateWriteTargets(
        medium,
        chunk,
        desiredCount,
        minCount,
        replicationFactorOverride,
        forbiddenNodes,
        preferredHostName);
}

std::unique_ptr<TMutation> TChunkManager::CreateUpdateChunkPropertiesMutation(const NProto::TReqUpdateChunkProperties& request)
{
    return Impl_->CreateUpdateChunkPropertiesMutation(request);
}

std::unique_ptr<TMutation> TChunkManager::CreateExportChunksMutation(TCtxExportChunksPtr context)
{
    return Impl_->CreateExportChunksMutation(std::move(context));
}

std::unique_ptr<TMutation> TChunkManager::CreateImportChunksMutation(TCtxImportChunksPtr context)
{
    return Impl_->CreateImportChunksMutation(std::move(context));
}

std::unique_ptr<TMutation> TChunkManager::CreateExecuteBatchMutation(TCtxExecuteBatchPtr context)
{
    return Impl_->CreateExecuteBatchMutation(std::move(context));
}

TChunkList* TChunkManager::CreateChunkList(EChunkListKind kind)
{
    return Impl_->CreateChunkList(kind);
}

void TChunkManager::UnstageChunk(TChunk* chunk)
{
    Impl_->UnstageChunk(chunk);
}

void TChunkManager::UnstageChunkList(TChunkList* chunkList, bool recursive)
{
    Impl_->UnstageChunkList(chunkList, recursive);
}

TNodePtrWithIndexesList TChunkManager::LocateChunk(TChunkPtrWithIndexes chunkWithIndexes)
{
    return Impl_->LocateChunk(chunkWithIndexes);
}

void TChunkManager::TouchChunk(TChunk* chunk)
{
    Impl_->TouchChunk(chunk);
}

void TChunkManager::AttachToChunkList(
    TChunkList* chunkList,
    TChunkTree* const* childrenBegin,
    TChunkTree* const* childrenEnd)
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
    TChunkTree* const* childrenBegin,
    TChunkTree* const* childrenEnd)
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

void TChunkManager::ScheduleNodeRefresh(TNode* node)
{
    Impl_->ScheduleNodeRefresh(node);
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

TPerMediumArray<EChunkStatus> TChunkManager::ComputeChunkStatuses(TChunk* chunk)
{
    return Impl_->ComputeChunkStatuses(chunk);
}

TFuture<TMiscExt> TChunkManager::GetChunkQuorumInfo(TChunk* chunk)
{
    return Impl_->GetChunkQuorumInfo(chunk);
}

TMedium* TChunkManager::FindMediumByIndex(int index) const
{
    return Impl_->FindMediumByIndex(index);
}

TMedium* TChunkManager::GetMediumByIndex(int index) const
{
    return Impl_->GetMediumByIndex(index);
}

TMedium* TChunkManager::GetMediumByIndexOrThrow(int index) const
{
    return Impl_->GetMediumByIndexOrThrow(index);
}

void TChunkManager::RenameMedium(TMedium* medium, const TString& newName)
{
    Impl_->RenameMedium(medium, newName);
}

void TChunkManager::SetMediumPriority(TMedium* medium, int newPriority)
{
    Impl_->SetMediumPriority(medium, newPriority);
}

TMedium* TChunkManager::FindMediumByName(const TString& name) const
{
    return Impl_->FindMediumByName(name);
}

TMedium* TChunkManager::GetMediumByNameOrThrow(const TString& name) const
{
    return Impl_->GetMediumByNameOrThrow(name);
}

DELEGATE_ENTITY_MAP_ACCESSORS(TChunkManager, Chunk, TChunk, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TChunkManager, ChunkList, TChunkList, *Impl_)

DELEGATE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(TChunkManager, Medium, Media, TMedium, *Impl_)

DELEGATE_BYREF_RO_PROPERTY(TChunkManager, THashSet<TChunk*>, LostChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, THashSet<TChunk*>, LostVitalChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, THashSet<TChunk*>, OverreplicatedChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, THashSet<TChunk*>, UnderreplicatedChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, THashSet<TChunk*>, DataMissingChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, THashSet<TChunk*>, ParityMissingChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, THashSet<TChunk*>, PrecariousChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, THashSet<TChunk*>, PrecariousVitalChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, THashSet<TChunk*>, QuorumMissingChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, THashSet<TChunk*>, UnsafelyPlacedChunks, *Impl_);
DELEGATE_BYREF_RO_PROPERTY(TChunkManager, THashSet<TChunk*>, ForeignChunks, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT

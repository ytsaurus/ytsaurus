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
#include "chunk_tree_traverser.h"
#include "chunk_view.h"
#include "chunk_view_proxy.h"
#include "config.h"
#include "dynamic_store.h"
#include "dynamic_store_proxy.h"
#include "expiration_tracker.h"
#include "helpers.h"
#include "job.h"
#include "medium.h"
#include "medium_proxy.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/multicell_manager.h>
#include <yt/server/master/cell_master/serialize.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/config.h>

#include <yt/server/master/chunk_server/proto/chunk_manager.pb.h>

#include <yt/server/master/cypress_server/cypress_manager.h>

#include <yt/server/lib/hydra/composite_automaton.h>
#include <yt/server/lib/hydra/entity_map.h>

#include <yt/server/master/node_tracker_server/config.h>
#include <yt/server/master/node_tracker_server/node_directory_builder.h>
#include <yt/server/master/node_tracker_server/node_tracker.h>
#include <yt/server/master/node_tracker_server/rack.h>

#include <yt/server/master/object_server/object_manager.h>
#include <yt/server/master/object_server/type_handler_detail.h>

#include <yt/server/master/security_server/account.h>
#include <yt/server/master/security_server/group.h>
#include <yt/server/master/security_server/security_manager.h>

#include <yt/server/master/tablet_server/tablet.h>

#include <yt/server/master/transaction_server/transaction.h>
#include <yt/server/master/transaction_server/transaction_manager.h>

#include <yt/server/master/journal_server/journal_node.h>
#include <yt/server/master/journal_server/journal_manager.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/session_id.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/proto/chunk_service.pb.h>

#include <yt/ytlib/journal_client/helpers.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/client/tablet_client/public.h>

#include <yt/core/compression/codec.h>

#include <yt/library/erasure/codec.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/string.h>

#include <yt/core/profiling/profile_manager.h>

#include <util/generic/cast.h>

namespace NYT::NChunkServer {

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
using namespace NTabletServer;
using namespace NTabletClient;

using NChunkClient::TSessionId;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;
static const auto ProfilingPeriod = TDuration::MilliSeconds(1000);

static NProfiling::TAggregateGauge ChunkTreeRebalanceTimeCounter("/chunk_tree_rebalance_time");

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

    virtual void RefObject(TObject* object) override
    {
        Bootstrap_->GetObjectManager()->RefObject(object);
    }

    virtual void UnrefObject(TObject* object) override
    {
        Bootstrap_->GetObjectManager()->UnrefObject(object);
    }

    virtual int GetObjectRefCounter(TObject* object) override
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

class TChunkManager::TChunkTypeHandler
    : public TObjectTypeHandlerWithMapBase<TChunk>
{
public:
    TChunkTypeHandler(TImpl* owner, EObjectType type);

    virtual TObject* FindObject(TObjectId id) override
    {
        return Map_->Find(DecodeChunkId(id).Id);
    }

    virtual EObjectType GetType() const override
    {
        return Type_;
    }

private:
    TImpl* const Owner_;
    const EObjectType Type_;

    virtual IObjectProxyPtr DoGetProxy(TChunk* chunk, TTransaction* transaction) override;
    virtual void DoDestroyObject(TChunk* chunk) override;
    virtual void DoUnstageObject(TChunk* chunk, bool recursive) override;
    virtual void DoExportObject(TChunk* chunk, TCellTag destinationCellTag) override;
    virtual void DoUnexportObject(TChunk* chunk, TCellTag destinationCellTag, int importRefCounter) override;
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

    virtual TObject* CreateObject(
        TObjectId hintId,
        IAttributeDictionary* attributes) override;

private:
    TImpl* const Owner_;

    virtual TCellTagList DoGetReplicationCellTags(const TMedium* /*medium*/) override
    {
        return AllSecondaryCellTags();
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

    virtual IObjectProxyPtr DoGetProxy(TChunkList* chunkList, TTransaction* transaction) override;

    virtual void DoDestroyObject(TChunkList* chunkList) override;

    virtual void DoUnstageObject(TChunkList* chunkList, bool recursive) override;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TChunkViewTypeHandler
    : public TObjectTypeHandlerWithMapBase<TChunkView>
{
public:
    explicit TChunkViewTypeHandler(TImpl* owner);

    virtual EObjectType GetType() const override
    {
        return EObjectType::ChunkView;
    }

private:
    TImpl* const Owner_;

    virtual IObjectProxyPtr DoGetProxy(TChunkView* chunkView, TTransaction* transaction) override;

    virtual void DoDestroyObject(TChunkView* chunkView) override;

    virtual void DoUnstageObject(TChunkView* chunkView, bool recursive) override;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TDynamicStoreTypeHandler
    : public TObjectTypeHandlerWithMapBase<TDynamicStore>
{
public:
    TDynamicStoreTypeHandler(TImpl* owner, EObjectType type);

    virtual EObjectType GetType() const override
    {
        return Type_;
    }

private:
    TImpl* const Owner_;
    const EObjectType Type_;

    virtual IObjectProxyPtr DoGetProxy(TDynamicStore* dynamicStore, TTransaction* transaction) override;

    virtual void DoDestroyObject(TDynamicStore* dynamicStore) override;

    virtual void DoUnstageObject(TDynamicStore* dynamicStore, bool recursive) override;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkManager::TImpl
    : public TMasterAutomatonPart
{
public:
    TImpl(
        TChunkManagerConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::ChunkManager)
        , Config_(config)
        , ChunkTreeBalancer_(New<TChunkTreeBalancerCallbacks>(Bootstrap_))
        , ExpirationTracker_(New<TExpirationTracker>(bootstrap))
    {
        RegisterMethod(BIND(&TImpl::HydraConfirmChunkListsRequisitionTraverseFinished, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUpdateChunkRequisition, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraExportChunks, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraImportChunks, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraExecuteBatch, Unretained(this)));
        RegisterMethod(BIND(&TImpl::HydraUnstageExpiredChunks, Unretained(this)));

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

        auto primaryCellTag = Bootstrap_->GetMulticellManager()->GetPrimaryCellTag();
        DefaultStoreMediumId_ = MakeWellKnownId(EObjectType::Medium, primaryCellTag, 0xffffffffffffffff);
        DefaultCacheMediumId_ = MakeWellKnownId(EObjectType::Medium, primaryCellTag, 0xfffffffffffffffe);

        auto* profileManager = TProfileManager::Get();
        Profiler.TagIds().push_back(profileManager->RegisterTag("cell_tag", primaryCellTag));

        for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
            if (jobType >= NJobTrackerClient::FirstMasterJobType && jobType <= NJobTrackerClient::LastMasterJobType) {
                JobTypeToTag_[jobType] = profileManager->RegisterTag("job_type", jobType);
            }
        }
    }

    void Initialize()
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(New<TChunkTypeHandler>(this, EObjectType::Chunk));
        objectManager->RegisterHandler(New<TChunkTypeHandler>(this, EObjectType::ErasureChunk));
        objectManager->RegisterHandler(New<TChunkTypeHandler>(this, EObjectType::JournalChunk));
        objectManager->RegisterHandler(New<TChunkTypeHandler>(this, EObjectType::ErasureJournalChunk));
        for (auto type = MinErasureChunkPartType;
             type <= MaxErasureChunkPartType;
             type = static_cast<EObjectType>(static_cast<int>(type) + 1))
        {
            objectManager->RegisterHandler(New<TChunkTypeHandler>(this, type));
        }
        for (auto type = MinErasureJournalChunkPartType;
             type <= MaxErasureJournalChunkPartType;
             type = static_cast<EObjectType>(static_cast<int>(type) + 1))
        {
            objectManager->RegisterHandler(New<TChunkTypeHandler>(this, type));
        }
        objectManager->RegisterHandler(New<TChunkViewTypeHandler>(this));
        objectManager->RegisterHandler(New<TDynamicStoreTypeHandler>(this, EObjectType::SortedDynamicTabletStore));
        objectManager->RegisterHandler(New<TDynamicStoreTypeHandler>(this, EObjectType::OrderedDynamicTabletStore));
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

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND(&TImpl::OnReplicateKeysToSecondaryMaster, MakeWeak(this)));
            multicellManager->SubscribeReplicateValuesToSecondaryMaster(
                BIND(&TImpl::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
        }

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::Periodic),
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();
    }

    void OnReplicateKeysToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        // Sort media by index to make sure they have the same indexing in the secondary
        // cell. Also, this makes the code deterministic which is a must.
        std::vector<TMedium*> media;
        media.reserve(MediumMap_.size());
        for (auto pair : MediumMap_) {
            auto* medium = pair.second;
            if (IsObjectAlive(medium)) {
                media.push_back(medium);
            }
        }
        auto indexLess = [] (TMedium* lhs, TMedium* rhs) {
            return lhs->GetIndex() < rhs->GetIndex();
        };
        std::sort(media.begin(), media.end(), indexLess);

        for (auto* medium : media) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(medium, cellTag);
        }
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();

        auto media = GetValuesSortedByKey(MediumMap_);
        for (auto* medium : media) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(medium, cellTag);
        }
    }

    IYPathServicePtr GetOrchidService()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND(&TImpl::BuildOrchidYson, MakeStrong(this)))
            ->Via(Bootstrap_->GetHydraFacade()->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ChunkManager));
    }

    void BuildOrchidYson(NYson::IYsonConsumer* consumer)
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .DoIf(DefaultStoreMedium_, [&] (auto fluent) {
                    fluent
                        .Item("requisition_registry").Value(TSerializableChunkRequisitionRegistry(Bootstrap_->GetChunkManager()));
                })
            .EndMap();
    }

    std::unique_ptr<TMutation> CreateUpdateChunkRequisitionMutation(const NProto::TReqUpdateChunkRequisition& request)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TImpl::HydraUpdateChunkRequisition,
            this);
    }

    std::unique_ptr<TMutation> CreateConfirmChunkListsRequisitionTraverseFinishedMutation(
        const NProto::TReqConfirmChunkListsRequisitionTraverseFinished& request)
    {
        return CreateMutation(
            Bootstrap_->GetHydraFacade()->GetHydraManager(),
            request,
            &TImpl::HydraConfirmChunkListsRequisitionTraverseFinished,
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
        std::optional<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes,
        const std::optional<TString>& preferredHostName)
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
        const NChunkClient::TChunkReplicaWithMediumList& replicas,
        TChunkInfo* chunkInfo,
        TChunkMeta* chunkMeta)
    {
        const auto& id = chunk->GetId();

        if (chunk->IsConfirmed()) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk is already confirmed (ChunkId: %v)",
                id);
            return;
        }

        chunk->Confirm(chunkInfo, chunkMeta);

        CancelChunkExpiration(chunk);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        const auto* mutationContext = GetCurrentMutationContext();
        auto mutationTimestamp = mutationContext->GetTimestamp();

        for (auto replica : replicas) {
            auto nodeId = replica.GetNodeId();
            auto* node = nodeTracker->FindNode(nodeId);
            if (!IsObjectAlive(node)) {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tried to confirm chunk at an unknown node (ChunkId: %v, NodeId: %v)",
                    id,
                    replica.GetNodeId());
                continue;
            }

            auto mediumIndex = replica.GetMediumIndex();
            const auto* medium = GetMediumByIndexOrThrow(mediumIndex);
            if (medium->GetCache()) {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tried to confirm chunk at a cache medium (ChunkId: %v, Medium: %v)",
                    id,
                    medium->GetName());
                continue;
            }

            auto chunkWithIndexes = TChunkPtrWithIndexes(
                chunk,
                replica.GetReplicaIndex(),
                replica.GetMediumIndex(),
                chunk->IsJournal() ? EChunkReplicaState::Active : EChunkReplicaState::Generic);

            if (node->GetLocalState() != ENodeState::Online) {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Tried to confirm chunk at non-online node (ChunkId: %v, Address: %v, State: %v)",
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

        if (!chunk->IsJournal()) {
            if (chunk->IsStaged()) {
                UpdateTransactionResourceUsage(chunk, +1);
            }
            UpdateAccountResourceUsage(chunk, +1);
        }

        ScheduleChunkRefresh(chunk);
    }

    // Adds #chunk to its staging transaction resource usage.
    void UpdateTransactionResourceUsage(const TChunk* chunk, i64 delta)
    {
        YT_ASSERT(chunk->IsStaged());
        YT_ASSERT(chunk->IsDiskSizeFinal());

        // NB: Use just the local replication as this only makes sense for staged chunks.
        const auto& requisition = ChunkRequisitionRegistry_.GetRequisition(chunk->GetLocalRequisitionIndex());
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateTransactionResourceUsage(chunk, requisition, delta);
    }

    // Adds #chunk to accounts' resource usage.
    void UpdateAccountResourceUsage(const TChunk* chunk, i64 delta, const TChunkRequisition* forcedRequisition = nullptr)
    {
        YT_ASSERT(chunk->IsDiskSizeFinal());

        const auto& requisition = forcedRequisition
            ? *forcedRequisition
            : chunk->GetAggregatedRequisition(GetChunkRequisitionRegistry());
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateResourceUsage(chunk, requisition, delta);
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
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk is already sealed (ChunkId: %v)",
                chunk->GetId());
            return;
        }

        chunk->Seal(miscExt);
        OnChunkSealed(chunk);

        ScheduleChunkRefresh(chunk);
    }

    TChunkView* CreateChunkView(TChunkTree* underlyingTree, NChunkClient::TReadRange readRange, TTransactionId transactionId = {})
    {
        switch (underlyingTree->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk: {
                auto* underlyingChunk = underlyingTree->As<TChunk>();
                auto* chunkView = DoCreateChunkView(underlyingChunk, std::move(readRange), transactionId);
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk view created (Id: %v, ChunkId: %v, TransactionId: %v)",
                    chunkView->GetId(),
                    underlyingChunk->GetId(),
                    transactionId);
                return chunkView;
            }

            case EObjectType::ChunkView: {
                YT_VERIFY(!transactionId);

                auto* underlyingChunkView = underlyingTree->As<TChunkView>();
                auto* chunkView = DoCreateChunkView(underlyingChunkView, std::move(readRange));
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk view created (Id: %v, ChunkId: %v, BaseChunkViewId: %v)",
                    chunkView->GetId(),
                    underlyingChunkView->GetUnderlyingChunk()->GetId(),
                    underlyingChunkView->GetId());
                return chunkView;
            }

            default:
                YT_ABORT();
        }
    }

    TChunkView* CloneChunkView(TChunkView* chunkView, NChunkClient::TReadRange readRange)
    {
        return CreateChunkView(chunkView->GetUnderlyingChunk(), readRange, chunkView->GetTransactionId());
    }

    TDynamicStore* CreateDynamicStore(TDynamicStoreId storeId, const TTablet* tablet)
    {
        auto* dynamicStore = DoCreateDynamicStore(storeId, tablet);
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Dynamic store created (StoreId: %v, TabletId: %v)",
            dynamicStore->GetId(),
            tablet->GetId());
        return dynamicStore;
    }

    TChunkList* CreateChunkList(EChunkListKind kind)
    {
        auto* chunkList = DoCreateChunkList(kind);
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk list created (Id: %v, Kind: %v)",
            chunkList->GetId(),
            chunkList->GetKind());
        return chunkList;
    }

    TChunkList* CloneTabletChunkList(TChunkList* chunkList)
    {
        auto* newChunkList = CreateChunkList(chunkList->GetKind());

        if (chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet) {
            AttachToChunkList(
                newChunkList,
                chunkList->Children().data() + chunkList->GetTrimmedChildCount(),
                chunkList->Children().data() + chunkList->Children().size());

            // Restoring statistics.
            newChunkList->Statistics().LogicalRowCount = chunkList->Statistics().LogicalRowCount;
            newChunkList->Statistics().LogicalChunkCount = chunkList->Statistics().LogicalChunkCount;
            newChunkList->CumulativeStatistics() = chunkList->CumulativeStatistics();
            newChunkList->CumulativeStatistics().TrimFront(chunkList->GetTrimmedChildCount());
        } else if (chunkList->GetKind() == EChunkListKind::SortedDynamicTablet) {
            newChunkList->SetPivotKey(chunkList->GetPivotKey());
            auto children = EnumerateStoresInChunkTree(chunkList);
            AttachToChunkList(newChunkList, children);
        } else {
            YT_ABORT();
        }

        return newChunkList;
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


    void ReplaceChunkListChild(
        TChunkList* chunkList,
        int childIndex,
        TChunkTree* child)
    {
        auto* oldChild = chunkList->Children()[childIndex];

        if (oldChild == child) {
            return;
        }

        NChunkServer::ReplaceChunkListChild(chunkList, childIndex, child);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(child);
        objectManager->UnrefObject(oldChild);
    }


    void RebalanceChunkTree(TChunkList* chunkList)
    {
        if (!ChunkTreeBalancer_.IsRebalanceNeeded(chunkList))
            return;

        PROFILE_AGGREGATED_TIMING (ChunkTreeRebalanceTimeCounter) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree rebalancing started (RootId: %v)",
                chunkList->GetId());
            ChunkTreeBalancer_.Rebalance(chunkList);
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree rebalancing completed");
        }
    }


    void StageChunkList(TChunkList* chunkList, TTransaction* transaction, TAccount* account)
    {
        StageChunkTree(chunkList, transaction, account);
    }

    void StageChunk(TChunk* chunk, TTransaction* transaction, TAccount* account)
    {
        StageChunkTree(chunk, transaction, account);

        if (chunk->IsDiskSizeFinal()) {
            UpdateTransactionResourceUsage(chunk, +1);
        }
    }

    void StageChunkTree(TChunkTree* chunkTree, TTransaction* transaction, TAccount* account)
    {
        YT_ASSERT(transaction);
        YT_ASSERT(!chunkTree->IsStaged());

        chunkTree->SetStagingTransaction(transaction);

        if (!account) {
            return;
        }

        chunkTree->SetStagingAccount(account);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        // XXX(portals)
        objectManager->RefObject(account);
    }

    void UnstageChunk(TChunk* chunk)
    {
        CancelChunkExpiration(chunk);
        UnstageChunkTree(chunk);
    }

    void UnstageChunkList(TChunkList* chunkList, bool recursive)
    {
        UnstageChunkTree(chunkList);

        if (recursive) {
            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            for (auto* child : chunkList->Children()) {
                if (child) {
                    transactionManager->UnstageObject(child->GetStagingTransaction(), child, recursive);
                }
            }
        }
    }

    void UnstageChunkTree(TChunkTree* chunkTree)
    {
        if (auto* account = chunkTree->GetStagingAccount()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(account);
        }

        chunkTree->SetStagingTransaction(nullptr);
        chunkTree->SetStagingAccount(nullptr);
    }

    void ScheduleChunkExpiration(TChunk* chunk)
    {
        YT_VERIFY(HasMutationContext());
        YT_VERIFY(chunk->IsStaged());
        YT_VERIFY(!chunk->IsConfirmed());

        auto now = GetCurrentMutationContext()->GetTimestamp();
        chunk->SetExpirationTime(now + GetDynamicConfig()->StagedChunkExpirationTimeout);
        ExpirationTracker_->ScheduleExpiration(chunk);
    }

    void CancelChunkExpiration(TChunk* chunk)
    {
        if (chunk->IsStaged()) {
            ExpirationTracker_->CancelExpiration(chunk);
            chunk->SetExpirationTime(TInstant::Zero());
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

    void ExportChunk(TChunk* chunk, TCellTag destinationCellTag)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto cellIndex = multicellManager->GetRegisteredMasterCellIndex(destinationCellTag);
        chunk->Export(cellIndex, GetChunkRequisitionRegistry());
    }

    void UnexportChunk(TChunk* chunk, TCellTag destinationCellTag, int importRefCounter)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto cellIndex = multicellManager->GetRegisteredMasterCellIndex(destinationCellTag);

        if (!chunk->IsExportedToCell(cellIndex)) {
            YT_LOG_ALERT("Chunk is not exported and cannot be unexported "
                "(ChunkId: %v, CellTag: %v, CellIndex: %v, ImportRefCounter: %v)",
                chunk->GetId(),
                destinationCellTag,
                cellIndex,
                importRefCounter);
            return;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* requisitionRegistry = GetChunkRequisitionRegistry();

        auto unexportChunk = [&] () {
            chunk->Unexport(cellIndex, importRefCounter, requisitionRegistry, objectManager);
        };

        if (chunk->GetExternalRequisitionIndex(cellIndex) == EmptyChunkRequisitionIndex) {
            // Unexporting will effectively do nothing from the replication and
            // accounting standpoints.
            unexportChunk();
        } else {
            const auto isChunkDiskSizeFinal = chunk->IsDiskSizeFinal();
            if (isChunkDiskSizeFinal) {
                const auto& requisitionBefore = chunk->GetAggregatedRequisition(requisitionRegistry);
                UpdateAccountResourceUsage(chunk, -1, &requisitionBefore);
                // Don't use the old requisition after unexporting the chunk.
            }

            unexportChunk();

            if (isChunkDiskSizeFinal) {
                UpdateAccountResourceUsage(chunk, +1, nullptr);
            }

            ScheduleChunkRefresh(chunk);
        }
    }


    void ClearChunkList(TChunkList* chunkList)
    {
        // TODO(babenko): currently we only support clearing a chunklist with no parents.
        YT_VERIFY(chunkList->Parents().Empty());
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk list cleared (ChunkListId: %v)", chunkList->GetId());
    }


    void ScheduleJobs(
        TNode* node,
        const TNodeResources& resourceUsage,
        const TNodeResources& resourceLimits,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort,
        std::vector<TJobPtr>* jobsToRemove)
    {
        ChunkReplicator_->ScheduleJobs(
            node,
            resourceUsage,
            resourceLimits,
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


    bool IsChunkReplicatorEnabled()
    {
        return ChunkReplicator_ && ChunkReplicator_->IsReplicatorEnabled();
    }

    bool IsChunkRefreshEnabled()
    {
        return ChunkReplicator_ && ChunkReplicator_->IsRefreshEnabled();
    }

    bool IsChunkRequisitionUpdateEnabled()
    {
        return ChunkReplicator_ && ChunkReplicator_->IsRequisitionUpdateEnabled();
    }

    bool IsChunkSealerEnabled()
    {
        return ChunkSealer_ && ChunkSealer_->IsEnabled();
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

    void ScheduleChunkRequisitionUpdate(TChunkTree* chunkTree)
    {
        switch (chunkTree->GetType()) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
            case EObjectType::JournalChunk:
            case EObjectType::ErasureJournalChunk:
                ScheduleChunkRequisitionUpdate(chunkTree->AsChunk());
                break;

            case EObjectType::ChunkView:
                ScheduleChunkRequisitionUpdate(chunkTree->AsChunkView()->GetUnderlyingChunk());
                break;

            case EObjectType::ChunkList:
                ScheduleChunkRequisitionUpdate(chunkTree->AsChunkList());
                break;

            case EObjectType::SortedDynamicTabletStore:
            case EObjectType::OrderedDynamicTabletStore:
                break;

            default:
                YT_ABORT();
        }
    }

    void ScheduleChunkRequisitionUpdate(TChunkList* chunkList)
    {
        YT_VERIFY(HasMutationContext());

        if (!IsObjectAlive(chunkList)) {
            return;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RefObject(chunkList);

        ChunkListsAwaitingRequisitionTraverse_.insert(chunkList);

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk list is awaiting requisition traverse (ChunkListId: %v)",
            chunkList->GetId());

        if (ChunkReplicator_) {
            ChunkReplicator_->ScheduleRequisitionUpdate(chunkList);
        }
    }

    void ScheduleChunkRequisitionUpdate(TChunk* chunk)
    {
        if (ChunkReplicator_) {
            ChunkReplicator_->ScheduleRequisitionUpdate(chunk);
        }
    }

    void ScheduleChunkSeal(TChunk* chunk)
    {
        if (ChunkSealer_) {
            ChunkSealer_->ScheduleSeal(chunk);
        }
    }


    TChunk* GetChunkOrThrow(TChunkId id)
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

    TChunkView* GetChunkViewOrThrow(TChunkViewId id)
    {
        auto* chunkView = FindChunkView(id);
        if (!IsObjectAlive(chunkView)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchChunkView,
                "No such chunk view %v",
                id);
        }
        return chunkView;
    }

    TDynamicStore* GetDynamicStoreOrThrow(TDynamicStoreId id)
    {
        auto* dynamicStore = FindDynamicStore(id);
        if (!IsObjectAlive(dynamicStore)) {
             THROW_ERROR_EXCEPTION(
                 NTabletClient::EErrorCode::NoSuchDynamicStore,
                 "No such dynamic store %v",
                 id);
        }
        return dynamicStore;
    }

    TChunkList* GetChunkListOrThrow(TChunkListId id)
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
        std::optional<bool> transient,
        std::optional<bool> cache,
        std::optional<int> priority,
        TObjectId hintId)
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
        YT_VERIFY(NameToMediumMap_.erase(medium->GetName()) == 1);
        YT_VERIFY(NameToMediumMap_.emplace(newName, medium).second);
        medium->SetName(newName);
        InitMediumProfiling(medium);
    }

    void SetMediumPriority(TMedium* medium, int priority)
    {
        if (medium->GetPriority() == priority) {
            return;
        }

        ValidateMediumPriority(priority);

        medium->SetPriority(priority);
    }

    void SetMediumConfig(TMedium* medium, TMediumConfigPtr newConfig)
    {
        auto oldMaxReplicationFactor = medium->Config()->MaxReplicationFactor;
        medium->Config() = std::move(newConfig);
        if (ChunkReplicator_ && medium->Config()->MaxReplicationFactor != oldMaxReplicationFactor) {
            ChunkReplicator_->ScheduleGlobalChunkRefresh(AllChunks_.GetFront(), AllChunks_.GetSize());
        }
    }

    TMedium* FindMediumByName(const TString& name) const
    {
        auto it = NameToMediumMap_.find(name);
        return it == NameToMediumMap_.end() ? nullptr : it->second;
    }

    TMedium* GetMediumByNameOrThrow(const TString& name) const
    {
        auto* medium = FindMediumByName(name);
        if (!IsObjectAlive(medium)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchMedium,
                "No such medium %Qv",
                name);
        }
        return medium;
    }

    TMedium* GetMediumOrThrow(TMediumId id) const
    {
        auto* medium = FindMedium(id);
        if (!IsObjectAlive(medium)) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoSuchMedium,
                "No such medium %v",
                id);
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
        if (!IsObjectAlive(medium)) {
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
        YT_VERIFY(medium);
        return medium;
    }

    TChunkTree* FindChunkTree(TChunkTreeId id)
    {
        auto type = TypeFromId(id);
        switch (type) {
            case EObjectType::Chunk:
            case EObjectType::ErasureChunk:
            case EObjectType::JournalChunk:
            case EObjectType::ErasureJournalChunk:
                return FindChunk(id);
            case EObjectType::ChunkList:
                return FindChunkList(id);
            case EObjectType::ChunkView:
                return FindChunkView(id);
            case EObjectType::SortedDynamicTabletStore:
            case EObjectType::OrderedDynamicTabletStore:
                return FindDynamicStore(id);
            default:
                return nullptr;
        }
    }

    TChunkTree* GetChunkTree(TChunkTreeId id)
    {
        auto* chunkTree = FindChunkTree(id);
        YT_VERIFY(chunkTree);
        return chunkTree;
    }

    TChunkTree* GetChunkTreeOrThrow(TChunkTreeId id)
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


    TMediumMap<EChunkStatus> ComputeChunkStatuses(TChunk* chunk)
    {
        return ChunkReplicator_->ComputeChunkStatuses(chunk);
    }


    TFuture<TMiscExt> GetChunkQuorumInfo(TChunk* chunk)
    {
        if (chunk->IsSealed()) {
            return MakeFuture(chunk->MiscExt());
        }

        return ComputeQuorumInfo(
            chunk->GetId(),
            chunk->GetErasureCodec(),
            GetChunkReplicaDescriptors(chunk),
            GetDynamicConfig()->JournalRpcTimeout,
            chunk->GetReadQuorum(),
            Bootstrap_->GetNodeChannelFactory());
    }

    TChunkRequisitionRegistry* GetChunkRequisitionRegistry()
    {
        return &ChunkRequisitionRegistry_;
    }

    DECLARE_ENTITY_MAP_ACCESSORS(Chunk, TChunk);
    DECLARE_ENTITY_MAP_ACCESSORS(ChunkView, TChunkView);
    DECLARE_ENTITY_MAP_ACCESSORS(DynamicStore, TDynamicStore);
    DECLARE_ENTITY_MAP_ACCESSORS(ChunkList, TChunkList);
    DECLARE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(Medium, Media, TMedium)

private:
    friend class TChunkTypeHandler;
    friend class TRegularChunkTypeHandler;
    friend class TChunkListTypeHandler;
    friend class TChunkViewTypeHandler;
    friend class TDynamicStoreTypeHandler;
    friend class TMediumTypeHandler;

    const TChunkManagerConfigPtr Config_;

    TChunkTreeBalancer ChunkTreeBalancer_;

    int TotalReplicaCount_ = 0;

    bool NeedToComputeCumulativeStatisticsForDynamicTables_ = false;

    TPeriodicExecutorPtr ProfilingExecutor_;

    TProfiler Profiler = ChunkServerProfiler;
    i64 ChunksCreated_ = 0;
    i64 ChunksDestroyed_ = 0;
    i64 ChunkReplicasAdded_ = 0;
    i64 ChunkReplicasRemoved_ = 0;
    i64 ChunkViewsCreated_ = 0;
    i64 ChunkViewsDestroyed_ = 0;
    i64 ChunkListsCreated_ = 0;
    i64 ChunkListsDestroyed_ = 0;

    TChunkPlacementPtr ChunkPlacement_;
    TChunkReplicatorPtr ChunkReplicator_;
    TChunkSealerPtr ChunkSealer_;

    const TExpirationTrackerPtr ExpirationTracker_;

    // Global chunk lists; cf. TChunkDynamicData.
    TIntrusiveLinkedList<TChunk, TChunkToAllLinkedListNode> AllChunks_;
    TIntrusiveLinkedList<TChunk, TChunkToJournalLinkedListNode> JournalChunks_;

    NHydra::TEntityMap<TChunk> ChunkMap_;
    NHydra::TEntityMap<TChunkView> ChunkViewMap_;
    NHydra::TEntityMap<TDynamicStore> DynamicStoreMap_;
    NHydra::TEntityMap<TChunkList> ChunkListMap_;

    NHydra::TEntityMap<TMedium> MediumMap_;
    THashMap<TString, TMedium*> NameToMediumMap_;
    std::vector<TMedium*> IndexToMediumMap_;
    TMediumSet UsedMediumIndexes_;

    TMediumId DefaultStoreMediumId_;
    TMedium* DefaultStoreMedium_ = nullptr;

    TMediumId DefaultCacheMediumId_;
    TMedium* DefaultCacheMedium_ = nullptr;

    TChunkRequisitionRegistry ChunkRequisitionRegistry_;

    TEnumIndexedVector<EJobType, TTagId, NJobTrackerClient::FirstMasterJobType, NJobTrackerClient::LastMasterJobType> JobTypeToTag_;
    THashMap<const TDataCenter*, TTagId> SourceDataCenterToTag_;
    THashMap<const TDataCenter*, TTagId> DestinationDataCenterToTag_;

    // Each requisition update scheduled for a chunk list should eventually be
    // converted into a number of requisition update requests scheduled for its
    // chunks. Before that conversion happens, however, the chunk list must be
    // kept alive. Each chunk list in this multiset carries additional (strong) references
    // (whose number coincides with the chunk list's multiplicity) to ensure that.
    THashMultiSet<TChunkList*> ChunkListsAwaitingRequisitionTraverse_;


    const TDynamicChunkManagerConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->ChunkManager;
    }


    TChunk* DoCreateChunk(EObjectType chunkType)
    {
        auto id = Bootstrap_->GetObjectManager()->GenerateId(chunkType, NullObjectId);
        return DoCreateChunk(id);
    }

    TChunk* DoCreateChunk(TChunkId chunkId)
    {
        auto chunkHolder = std::make_unique<TChunk>(chunkId);
        auto* chunk = ChunkMap_.Insert(chunkId, std::move(chunkHolder));
        RegisterChunk(chunk);
        chunk->RefUsedRequisitions(GetChunkRequisitionRegistry());
        ChunksCreated_++;
        return chunk;
    }

    void DestroyChunk(TChunk* chunk)
    {
        if (chunk->IsForeign()) {
            YT_VERIFY(ForeignChunks_.erase(chunk) == 1);
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

        if (chunk->IsNative() && chunk->IsDiskSizeFinal()) {
            UpdateAccountResourceUsage(chunk, -1);
        }

        auto job = chunk->GetJob();

        // Unregister chunk replicas from all known locations.
        // Schedule removal jobs.
        auto unregisterReplica = [&] (TNodePtrWithIndexes nodeWithIndexes, bool cached) {
            auto* node = nodeWithIndexes.GetPtr();
            TChunkPtrWithIndexes chunkWithIndexes(
                chunk,
                nodeWithIndexes.GetReplicaIndex(),
                nodeWithIndexes.GetMediumIndex(),
                nodeWithIndexes.GetState());
            if (!node->RemoveReplica(chunkWithIndexes)) {
                return;
            }
            if (cached) {
                return;
            }

            TChunkIdWithIndexes chunkIdWithIndexes(
                chunk->GetId(),
                nodeWithIndexes.GetReplicaIndex(),
                nodeWithIndexes.GetMediumIndex());
            node->AddDestroyedReplica(chunkIdWithIndexes);

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

        chunk->UnrefUsedRequisitions(
            GetChunkRequisitionRegistry(),
            Bootstrap_->GetObjectManager());

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


    TChunkView* DoCreateChunkView(TChunk* underlyingChunk, NChunkClient::TReadRange readRange, TTransactionId transactionId)
    {
        ++ChunkViewsCreated_;
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::ChunkView, NullObjectId);
        auto chunkViewHolder = std::make_unique<TChunkView>(id);
        auto* chunkView = ChunkViewMap_.Insert(id, std::move(chunkViewHolder));
        chunkView->SetUnderlyingChunk(underlyingChunk);
        underlyingChunk->AddParent(chunkView);
        chunkView->SetReadRange(std::move(readRange));
        Bootstrap_->GetObjectManager()->RefObject(underlyingChunk);
        if (transactionId) {
            auto& transactionManager = Bootstrap_->GetTransactionManager();
            transactionManager->CreateOrRefTimestampHolder(transactionId);
            chunkView->SetTransactionId(transactionId);
        }
        return chunkView;
    }

    TChunkView* DoCreateChunkView(TChunkView* underlyingChunkView, NChunkClient::TReadRange readRange)
    {
        readRange.LowerLimit() = underlyingChunkView->GetAdjustedLowerReadLimit(readRange.LowerLimit());
        readRange.UpperLimit() = underlyingChunkView->GetAdjustedUpperReadLimit(readRange.UpperLimit());
        auto transactionId = underlyingChunkView->GetTransactionId();
        return DoCreateChunkView(underlyingChunkView->GetUnderlyingChunk(), readRange, transactionId);
    }

    void DestroyChunkView(TChunkView* chunkView)
    {
        YT_VERIFY(!chunkView->GetStagingTransaction());

        auto* underlyingChunk = chunkView->GetUnderlyingChunk();
        const auto& objectManager = Bootstrap_->GetObjectManager();
        underlyingChunk->RemoveParent(chunkView);
        objectManager->UnrefObject(underlyingChunk);

        auto& transactionManager = Bootstrap_->GetTransactionManager();
        transactionManager->UnrefTimestampHolder(chunkView->GetTransactionId());

        ++ChunkViewsDestroyed_;
    }

    TDynamicStore* DoCreateDynamicStore(TDynamicStoreId storeId, const TTablet* tablet)
    {
        auto holder = std::make_unique<TDynamicStore>(storeId);
        auto* dynamicStore = DynamicStoreMap_.Insert(storeId, std::move(holder));
        dynamicStore->SetTablet(tablet);
        return dynamicStore;
    }

    void DestroyDynamicStore(TDynamicStore* dynamicStore)
    {
        YT_VERIFY(!dynamicStore->GetStagingTransaction());

        if (auto* chunk = dynamicStore->GetFlushedChunk()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->UnrefObject(chunk);
        }
    }

    void OnNodeRegistered(TNode* node)
    {
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
        for (const auto& [mediumIndex, replicas] : node->Replicas()) {
            const auto* medium = FindMediumByIndex(mediumIndex);
            if (!medium) {
                continue;
            }
            for (auto replica : replicas) {
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
            HandleNodeDataCenterChange(node, oldDataCenter);
        }

        OnNodeChanged(node);
    }

    void OnNodeDataCenterChanged(TNode* node, TDataCenter* oldDataCenter)
    {
        HandleNodeDataCenterChange(node, oldDataCenter);

        OnNodeChanged(node);
    }

    void HandleNodeDataCenterChange(TNode* node, TDataCenter* oldDataCenter)
    {
        YT_VERIFY(node->GetDataCenter() != oldDataCenter);

        if (ChunkReplicator_) {
            ChunkReplicator_->OnNodeDataCenterChanged(node, oldDataCenter);
        }

        if (ChunkPlacement_) {
            ChunkPlacement_->OnNodeDataCenterChanged(node, oldDataCenter);
        }
    }

    void OnFullHeartbeat(
        TNode* node,
        NNodeTrackerServer::NProto::TReqFullHeartbeat* request)
    {
        for (const auto& mediumReplicas : node->Replicas()) {
            YT_VERIFY(mediumReplicas.second.empty());
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
        const auto& dynamicConfig = GetDynamicConfig();
        for (auto it = unapprovedReplicas.begin(); it != unapprovedReplicas.end();) {
            auto jt = it++;
            auto replica = jt->first;
            auto registerTimestamp = jt->second;
            auto reason = ERemoveReplicaReason::None;
            if (!IsObjectAlive(replica.GetPtr())) {
                reason = ERemoveReplicaReason::ChunkDestroyed;
            } else if (mutationTimestamp > registerTimestamp + dynamicConfig->ReplicaApproveTimeout) {
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

    void RegisterTagForSourceDataCenter(const TDataCenter* dataCenter)
    {
        auto dataCenterName = dataCenter ? dataCenter->GetName() : TString();
        auto tagId = TProfileManager::Get()->RegisterTag("source_data_center", dataCenterName);
        YT_VERIFY(SourceDataCenterToTag_.emplace(dataCenter, tagId).second);
    }

    void RegisterTagForDestinationDataCenter(const TDataCenter* dataCenter)
    {
        auto dataCenterName = dataCenter ? dataCenter->GetName() : TString();
        auto tagId = TProfileManager::Get()->RegisterTag("destination_data_center", dataCenterName);
        YT_VERIFY(DestinationDataCenterToTag_.emplace(dataCenter, tagId).second);
    }

    void RegisterTagsForDataCenter(const TDataCenter* dataCenter)
    {
        RegisterTagForSourceDataCenter(dataCenter);
        RegisterTagForDestinationDataCenter(dataCenter);
    }

    void UnregisterTagsForDataCenter(const TDataCenter* dataCenter)
    {
        // NB: just cleaning maps here, profile manager doesn't support unregistering tags.
        SourceDataCenterToTag_.erase(dataCenter);
        DestinationDataCenterToTag_.erase(dataCenter);
    }

    bool EnsureDataCenterTagsInitialized()
    {
        YT_VERIFY(SourceDataCenterToTag_.empty() == DestinationDataCenterToTag_.empty());

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

        if (ChunkReplicator_) {
            ChunkReplicator_->OnDataCenterDestroyed(dataCenter);
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

        if (ChunkReplicator_) {
            ChunkReplicator_->OnDataCenterDestroyed(dataCenter);
        }
    }

    TTagId GetDataCenterTag(const TDataCenter* dataCenter, THashMap<const TDataCenter*, TTagId>& dataCenterToTag)
    {
        return GetOrCrash(dataCenterToTag, dataCenter);
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

    void HydraConfirmChunkListsRequisitionTraverseFinished(NProto::TReqConfirmChunkListsRequisitionTraverseFinished* request)
    {
        auto chunkListIds = FromProto<std::vector<TChunkListId>>(request->chunk_list_ids());

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Confirming finished chunk lists requisition traverse (ChunkListIds: %v)",
            chunkListIds);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto chunkListId : chunkListIds) {
            auto* chunkList = FindChunkList(chunkListId);
            if (!chunkList) {
                YT_LOG_ALERT_UNLESS(IsRecovery(), "Chunk list is missing during requisition traverse finish confirmation (ChunkListId: %v)",
                    chunkListId);
                continue;
            }

            auto it = ChunkListsAwaitingRequisitionTraverse_.find(chunkList);
            if (it == ChunkListsAwaitingRequisitionTraverse_.end()) {
                YT_LOG_ALERT_UNLESS(IsRecovery(), "Chunk list does not hold an additional strong ref during requisition traverse finish confirmation (ChunkListId: %v)",
                    chunkListId);
                continue;
            }

            ChunkListsAwaitingRequisitionTraverse_.erase(it);
            objectManager->UnrefObject(chunkList);
        }
    }

    void HydraUpdateChunkRequisition(NProto::TReqUpdateChunkRequisition* request)
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        // NB: Ordered map is a must to make the behavior deterministic.
        std::map<TCellTag, NProto::TReqUpdateChunkRequisition> crossCellRequestMap;
        auto getCrossCellRequest = [&] (const TChunk* chunk) -> NProto::TReqUpdateChunkRequisition& {
            auto cellTag = chunk->GetNativeCellTag();
            auto it = crossCellRequestMap.find(cellTag);
            if (it == crossCellRequestMap.end()) {
                it = crossCellRequestMap.emplace(cellTag, NProto::TReqUpdateChunkRequisition()).first;
                it->second.set_cell_tag(multicellManager->GetCellTag());
            }
            return it->second;
        };

        auto local = request->cell_tag() == multicellManager->GetCellTag();
        int cellIndex = local ? -1 : multicellManager->GetRegisteredMasterCellIndex(request->cell_tag());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* requisitionRegistry = GetChunkRequisitionRegistry();

        auto setChunkRequisitionIndex = [&] (TChunk* chunk, TChunkRequisitionIndex requisitionIndex) {
            if (local) {
                chunk->SetLocalRequisitionIndex(requisitionIndex, requisitionRegistry, objectManager);
            } else {
                chunk->SetExternalRequisitionIndex(cellIndex, requisitionIndex, requisitionRegistry, objectManager);
            }
        };

        const auto updates = TranslateChunkRequisitionUpdateRequest(request);

        // Below, we ref chunks' new requisitions and unref old ones. Such unreffing may
        // remove a requisition which may happen to be the new requisition of subsequent chunks.
        // To avoid such thrashing, ref everything here and unref it afterwards.
        for (const auto& update : updates) {
            requisitionRegistry->Ref(update.TranslatedRequisitionIndex);
        }

        for (const auto& update : updates) {
            auto* chunk = update.Chunk;
            auto newRequisitionIndex = update.TranslatedRequisitionIndex;

            if (!local && !chunk->IsExportedToCell(cellIndex)) {
                // The chunk has already been unexported from that cell.
                continue;
            }

            auto curRequisitionIndex = local ? chunk->GetLocalRequisitionIndex() : chunk->GetExternalRequisitionIndex(cellIndex);

            if (newRequisitionIndex == curRequisitionIndex) {
                continue;
            }

            if (chunk->IsForeign()) {
                setChunkRequisitionIndex(chunk, newRequisitionIndex);

                YT_ASSERT(local);
                auto& crossCellRequest = getCrossCellRequest(chunk);
                auto* crossCellUpdate = crossCellRequest.add_updates();
                ToProto(crossCellUpdate->mutable_chunk_id(), chunk->GetId());
                crossCellUpdate->set_chunk_requisition_index(newRequisitionIndex);
            } else {
                const auto isChunkDiskSizeFinal = chunk->IsDiskSizeFinal();
                if (isChunkDiskSizeFinal) {
                    // NB: changing chunk's requisition may unreference and destroy the old requisition.
                    // Worse yet, this may, in turn, weak-unreference some accounts, thus triggering
                    // destruction of their control blocks (that hold strong and weak counters).
                    // So be sure to use the old requisition *before* setting the new one.
                    const auto& requisitionBefore = chunk->GetAggregatedRequisition(requisitionRegistry);
                    UpdateAccountResourceUsage(chunk, -1, &requisitionBefore);
                }

                setChunkRequisitionIndex(chunk, newRequisitionIndex); // potentially destroys the old requisition

                if (isChunkDiskSizeFinal) {
                    UpdateAccountResourceUsage(chunk, +1, nullptr);
                }

                ScheduleChunkRefresh(chunk);
            }
        }

        for (auto& pair : crossCellRequestMap) {
            auto cellTag = pair.first;
            auto& request = pair.second;
            FillChunkRequisitionDict(&request, *requisitionRegistry);
            multicellManager->PostToMaster(request, cellTag);
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Requesting to update requisition of imported chunks (CellTag: %v, Count: %v)",
                cellTag,
                request.updates_size());
        }

        for (const auto& update : updates) {
            requisitionRegistry->Unref(update.TranslatedRequisitionIndex, objectManager);
        }
    }

    struct TRequisitionUpdate
    {
        TChunk* Chunk;
        TChunkRequisitionIndex TranslatedRequisitionIndex;
    };

    std::vector<TRequisitionUpdate> TranslateChunkRequisitionUpdateRequest(NProto::TReqUpdateChunkRequisition* request)
    {
        // NB: this is necessary even for local requests as requisition indexes
        // in the request are different from those in the registry.
        auto translateRequisitionIndex = BuildChunkRequisitionIndexTranslator(*request);

        std::vector<TRequisitionUpdate> updates;
        updates.reserve(request->updates_size());

        for (const auto& update : request->updates()) {
            auto chunkId = FromProto<TChunkId>(update.chunk_id());
            auto* chunk = FindChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                continue;
            }

            auto newRequisitionIndex = translateRequisitionIndex(update.chunk_requisition_index());
            updates.emplace_back(TRequisitionUpdate{chunk, newRequisitionIndex});
        }

        return updates;
    }

    std::function<TChunkRequisitionIndex(TChunkRequisitionIndex)>
    BuildChunkRequisitionIndexTranslator(const NProto::TReqUpdateChunkRequisition& request)
    {
        THashMap<TChunkRequisitionIndex, TChunkRequisitionIndex> remoteToLocalIndexMap;
        remoteToLocalIndexMap.reserve(request.chunk_requisition_dict_size());
        for (const auto& pair : request.chunk_requisition_dict()) {
            auto remoteIndex = pair.index();

            TChunkRequisition requisition;
            FromProto(&requisition, pair.requisition(), Bootstrap_->GetSecurityManager());
            auto localIndex = ChunkRequisitionRegistry_.GetOrCreate(requisition, Bootstrap_->GetObjectManager());

            YT_VERIFY(remoteToLocalIndexMap.emplace(remoteIndex, localIndex).second);
        }

        return [remoteToLocalIndexMap = std::move(remoteToLocalIndexMap)] (TChunkRequisitionIndex remoteIndex) {
            // The remote side must provide a dictionary entry for every index it sends us.
            return GetOrCrash(remoteToLocalIndexMap, remoteIndex);
        };
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunks exported (TransactionId: %v, ChunkIds: %v)",
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

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        std::vector<TChunkId> chunkIds;
        for (auto& importData : *request->mutable_chunks()) {
            auto chunkId = FromProto<TChunkId>(importData.id());
            if (CellTagFromId(chunkId) == multicellManager->GetCellTag()) {
                THROW_ERROR_EXCEPTION("Cannot import a native chunk %v", chunkId);
            }

            auto* chunk = ChunkMap_.Find(chunkId);
            if (!chunk) {
                chunk = DoCreateChunk(chunkId);
                chunk->SetForeign();
                chunk->Confirm(importData.mutable_info(), importData.mutable_meta());
                chunk->SetErasureCodec(NErasure::ECodec(importData.erasure_codec()));
                YT_VERIFY(ForeignChunks_.insert(chunk).second);
            }

            transactionManager->ImportObject(transaction, chunk);

            chunkIds.push_back(chunk->GetId());
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunks imported (TransactionId: %v, ChunkIds: %v)",
            transactionId,
            chunkIds);
    }

    void HydraUnstageExpiredChunks(NProto::TReqUnstageExpiredChunks* request) noexcept
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();

        for (const auto& protoId : request->chunk_ids()) {
            auto chunkId = FromProto<TChunkId>(protoId);
            auto* chunk = FindChunk(chunkId);
            if (!IsObjectAlive(chunk)) {
                continue;
            }

            if (!chunk->IsStaged()) {
                continue;
            }

            if (chunk->IsConfirmed()) {
                continue;
            }

            transactionManager->UnstageObject(chunk->GetStagingTransaction(), chunk, false /*recursive*/);

            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Unstaged expired chunk (ChunkId: %v)",
                chunkId);
        }
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
                    YT_LOG_DEBUG_UNLESS(IsRecovery(), TError(errorMessage) << ex);
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
        auto chunkType = CheckedEnumCast<EObjectType>(subrequest->type());
        bool isErasure = IsErasureChunkType(chunkType);
        bool isJournal = IsJournalChunkType(chunkType);
        auto erasureCodecId = isErasure ? CheckedEnumCast<NErasure::ECodec>(subrequest->erasure_codec()) : NErasure::ECodec::None;
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
        account->ValidateActiveLifeStage();

        if (subrequest->validate_resource_usage_increase()) {
            auto resourceUsageIncrease = TClusterResources()
                .SetChunkCount(1)
                .SetMediumDiskSpace(mediumIndex, 1)
                .SetMasterMemory(1);
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

        YT_ASSERT(chunk->GetLocalRequisitionIndex() ==
                 (isErasure
                      ? MigrationErasureChunkRequisitionIndex
                      : MigrationChunkRequisitionIndex));

        TChunkRequisition requisition(
            account,
            mediumIndex,
            TReplicationPolicy(replicationFactor, false /* dataPartsOnly */),
            false /* committed */);
        requisition.SetVital(subrequest->vital());
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto requisitionIndex = ChunkRequisitionRegistry_.GetOrCreate(requisition, objectManager);
        chunk->SetLocalRequisitionIndex(requisitionIndex, GetChunkRequisitionRegistry(), objectManager);

        auto sessionId = TSessionId(chunk->GetId(), mediumIndex);

        StageChunk(chunk, transaction, account);

        transactionManager->StageObject(transaction, chunk);

        if (chunkList) {
            AttachToChunkList(chunkList, chunk);
        }

        if (subresponse) {
            ToProto(subresponse->mutable_session_id(), sessionId);
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(),
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
        auto replicas = FromProto<TChunkReplicaWithMediumList>(subrequest->replicas());

        auto* chunk = GetChunkOrThrow(chunkId);

        ConfirmChunk(
            chunk,
            replicas,
            subrequest->mutable_chunk_info(),
            subrequest->mutable_chunk_meta());

        if (subresponse && subrequest->request_statistics()) {
            *subresponse->mutable_statistics() = chunk->GetStatistics().ToDataStatistics();
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk confirmed (ChunkId: %v, Replicas: %v)",
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk sealed "
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
            StageChunkList(chunkList, transaction, nullptr);
            transactionManager->StageObject(transaction, chunkList);
            ToProto(subresponse->add_chunk_list_ids(), chunkList->GetId());
            chunkListIds.push_back(chunkList->GetId());
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(),
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk tree unstaged (ChunkTreeId: %v, Recursive: %v)",
            chunkTreeId,
            recursive);
    }

    void ExecuteAttachChunkTreesSubrequest(
        TReqExecuteBatch::TAttachChunkTreesSubrequest* subrequest,
        TRspExecuteBatch::TAttachChunkTreesSubresponse* subresponse)
    {
        auto parentId = FromProto<TTransactionId>(subrequest->parent_id());
        auto* parent = GetChunkListOrThrow(parentId);
        auto transactionId = subrequest->has_transaction_id()
            ? FromProto<TTransactionId>(subrequest->transaction_id())
            : NullTransactionId;

        std::vector<TChunkTree*> children;
        children.reserve(subrequest->child_ids_size());
        for (const auto& protoChildId : subrequest->child_ids()) {
            auto childId = FromProto<TChunkTreeId>(protoChildId);
            auto* child = GetChunkTreeOrThrow(childId);
            if (parent->GetKind() == EChunkListKind::SortedDynamicSubtablet) {
                YT_VERIFY(child->GetType() == EObjectType::Chunk);

                auto chunkView = CreateChunkView(child->AsChunk(), NChunkClient::TReadRange(), transactionId);
                children.push_back(chunkView);
            } else {
                children.push_back(child);
            }
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

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk trees attached (ParentId: %v, ChildIds: %v, TransactionId: %v)",
            parentId,
            MakeFormattableView(children, TObjectIdFormatter()),
            transactionId);
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        ChunkMap_.SaveKeys(context);
        ChunkListMap_.SaveKeys(context);
        MediumMap_.SaveKeys(context);
        ChunkViewMap_.SaveKeys(context);
        DynamicStoreMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        ChunkMap_.SaveValues(context);
        ChunkListMap_.SaveValues(context);
        MediumMap_.SaveValues(context);
        Save(context, ChunkRequisitionRegistry_);
        Save(context, ChunkListsAwaitingRequisitionTraverse_);
        ChunkViewMap_.SaveValues(context);
        DynamicStoreMap_.SaveValues(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        ChunkMap_.LoadKeys(context);
        ChunkListMap_.LoadKeys(context);
        MediumMap_.LoadKeys(context);
        ChunkViewMap_.LoadKeys(context);

        // COMPAT(ifsmirnov)
        if (context.GetVersion() >= EMasterReign::DynamicStoreRead) {
            DynamicStoreMap_.LoadKeys(context);
        }
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        ChunkMap_.LoadValues(context);

        ChunkListMap_.LoadValues(context);
        MediumMap_.LoadValues(context);

        Load(context, ChunkRequisitionRegistry_);
        Load(context, ChunkListsAwaitingRequisitionTraverse_);

        ChunkViewMap_.LoadValues(context);

        // COMPAT(ifsmirnov)
        if (context.GetVersion() >= EMasterReign::DynamicStoreRead) {
            DynamicStoreMap_.LoadValues(context);
        }

        // COMPAT(ifsmirnov)
        NeedToComputeCumulativeStatisticsForDynamicTables_ = context.GetVersion() <
            EMasterReign::YT_10639_CumulativeStatisticsInDynamicTables;
    }

    virtual void OnBeforeSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnBeforeSnapshotLoaded();

        NeedToComputeCumulativeStatisticsForDynamicTables_ = false;
    }

    virtual void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        // Populate nodes' chunk replica sets.
        // Compute chunk replica count.

        YT_LOG_INFO("Started initializing chunks");

        for (const auto& pair : ChunkMap_) {
            auto* chunk = pair.second;

            RegisterChunk(chunk);

            auto addReplicas = [&] (const auto& replicas) {
                for (auto replica : replicas) {
                    TChunkPtrWithIndexes chunkWithIndexes(
                        chunk,
                        replica.GetReplicaIndex(),
                        replica.GetMediumIndex(),
                        replica.GetState());
                    replica.GetPtr()->AddReplica(chunkWithIndexes);
                    ++TotalReplicaCount_;
                }
            };
            addReplicas(chunk->StoredReplicas());
            addReplicas(chunk->CachedReplicas());

            if (chunk->IsForeign()) {
                YT_VERIFY(ForeignChunks_.insert(chunk).second);
            }

            // COMPAT(shakurov)
            if (chunk->GetExpirationTime()) {
                ExpirationTracker_->ScheduleExpiration(chunk);
            }
        }

        for (const auto& pair : MediumMap_) {
            auto* medium = pair.second;
            InitMediumProfiling(medium);
            RegisterMedium(medium);
        }

        InitBuiltins();

        if (NeedToComputeCumulativeStatisticsForDynamicTables_) {
            ComputeCumulativeStatisticsForDynamicTables();
            NeedToComputeCumulativeStatisticsForDynamicTables_ = false;
        }

        YT_LOG_INFO("Finished initializing chunks");
    }


    virtual void Clear() override
    {
        TMasterAutomatonPart::Clear();

        AllChunks_.Clear();
        JournalChunks_.Clear();
        ChunkMap_.Clear();
        ChunkListMap_.Clear();
        ChunkViewMap_.Clear();
        ForeignChunks_.clear();
        TotalReplicaCount_ = 0;

        ChunkRequisitionRegistry_.Clear();

        SourceDataCenterToTag_.clear();
        DestinationDataCenterToTag_.clear();

        ChunkListsAwaitingRequisitionTraverse_.clear();

        MediumMap_.Clear();
        NameToMediumMap_.clear();
        IndexToMediumMap_ = std::vector<TMedium*>(MaxMediumCount, nullptr);
        UsedMediumIndexes_.reset();

        ChunksCreated_ = 0;
        ChunksDestroyed_ = 0;
        ChunkReplicasAdded_ = 0;
        ChunkReplicasRemoved_ = 0;
        ChunkViewsCreated_ = 0;
        ChunkViewsDestroyed_ = 0;
        ChunkListsCreated_ = 0;
        ChunkListsDestroyed_ = 0;

        DefaultStoreMedium_ = nullptr;
        DefaultCacheMedium_ = nullptr;

        ExpirationTracker_->Clear();
    }

    virtual void SetZeroState() override
    {
        TMasterAutomatonPart::SetZeroState();

        InitBuiltins();
    }


    void InitBuiltins()
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        const auto& objectManager = Bootstrap_->GetObjectManager();

        // Chunk requisition registry
        ChunkRequisitionRegistry_.EnsureBuiltinRequisitionsInitialized(
            securityManager->GetChunkWiseAccountingMigrationAccount(),
            objectManager);

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
        TMediumId id,
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
        medium = DoCreateMedium(id, mediumIndex, name, false, cache, std::nullopt);
        return true;
    }

    void RecomputeStatistics(TChunkList* chunkList)
    {
        YT_VERIFY(chunkList->GetKind() != EChunkListKind::OrderedDynamicTablet);

        auto& statistics = chunkList->Statistics();
        auto oldStatistics = statistics;
        statistics = TChunkTreeStatistics{};

        auto& cumulativeStatistics = chunkList->CumulativeStatistics();
        cumulativeStatistics.Clear();
        if (chunkList->HasModifyableCumulativeStatistics()) {
            cumulativeStatistics.DeclareModifiable();
        } else if (chunkList->HasAppendableCumulativeStatistics()) {
            cumulativeStatistics.DeclareAppendable();
        } else {
            YT_ABORT();
        }

        for (auto* child : chunkList->Children()) {
            YT_VERIFY(child);
            auto childStatistics = GetChunkTreeStatistics(child);
            statistics.Accumulate(childStatistics);
            if (chunkList->HasCumulativeStatistics()) {
                cumulativeStatistics.PushBack(TCumulativeStatisticsEntry{childStatistics});
            }
        }

        ++statistics.Rank;
        ++statistics.ChunkListCount;

        if (statistics != oldStatistics) {
            YT_LOG_DEBUG("Chunk list statistics changed (ChunkList: %v, OldStatistics: %v, NewStatistics: %v)",
                chunkList->GetId(),
                oldStatistics,
                statistics);
        }

        if (!chunkList->Children().empty() && chunkList->HasCumulativeStatistics()) {
            auto ultimateCumulativeEntry = chunkList->CumulativeStatistics().Back();
            if (ultimateCumulativeEntry != TCumulativeStatisticsEntry{statistics}) {
                YT_LOG_FATAL(
                    "Chunk list cumulative statistics do not match statistics "
                    "(ChunkListId: %v, Statistics: %v, UltimateCumulativeEntry: %v)",
                    chunkList->GetId(),
                    statistics,
                    ultimateCumulativeEntry);
            }
        }
    }

    // Fix for YT-10619.
    void RecomputeOrderedTabletCumulativeStatistics(TChunkList* chunkList)
    {
        YT_VERIFY(chunkList->GetKind() == EChunkListKind::OrderedDynamicTablet);


        auto getChildStatisticsEntry = [] (TChunkTree* child) {
            return child
                ? TCumulativeStatisticsEntry{child->AsChunk()->GetStatistics()}
                : TCumulativeStatisticsEntry(0 /*RowCount*/, 1 /*ChunkCount*/, 0 /*DataSize*/);
        };

        TCumulativeStatisticsEntry beforeFirst{chunkList->Statistics()};
        for (auto* child : chunkList->Children()) {
            auto childEntry = getChildStatisticsEntry(child);
            beforeFirst = beforeFirst - childEntry;
        }

        YT_VERIFY(chunkList->HasTrimmableCumulativeStatistics());
        auto& cumulativeStatistics = chunkList->CumulativeStatistics();
        cumulativeStatistics.Clear();
        cumulativeStatistics.DeclareTrimmable();
        // Replace default-constructed auxiliary 'before-first' entry.
        cumulativeStatistics.PushBack(beforeFirst);
        cumulativeStatistics.TrimFront(1);

        auto currentStatistics = beforeFirst;
        for (auto* child : chunkList->Children()) {
            auto childEntry = getChildStatisticsEntry(child);
            currentStatistics = currentStatistics + childEntry;
            cumulativeStatistics.PushBack(childEntry);
        }

        YT_VERIFY(currentStatistics == TCumulativeStatisticsEntry{chunkList->Statistics()});
        auto ultimateCumulativeEntry = chunkList->CumulativeStatistics().Empty()
            ? chunkList->CumulativeStatistics().GetPreviousSum(0)
            : chunkList->CumulativeStatistics().Back();
        if (ultimateCumulativeEntry != TCumulativeStatisticsEntry{chunkList->Statistics()}) {
            YT_LOG_FATAL(
                "Chunk list cumulative statistics do not match statistics "
                "(ChunkListId: %v, Statistics: %v, UltimateCumulativeEntry: %v)",
                chunkList->GetId(),
                chunkList->Statistics(),
                ultimateCumulativeEntry);
        }
    }

    // Compat for YT-10639.
    void ComputeCumulativeStatisticsForDynamicTables()
    {
        for (auto [id, chunkList] : ChunkListMap_) {
            switch (chunkList->GetKind()) {
                case EChunkListKind::OrderedDynamicTablet:
                    RecomputeOrderedTabletCumulativeStatistics(chunkList);
                    break;

                case EChunkListKind::OrderedDynamicRoot:
                case EChunkListKind::SortedDynamicRoot:
                case EChunkListKind::SortedDynamicTablet:
                    RecomputeStatistics(chunkList);
                    break;

                default:
                    break;
            }

            if (chunkList->HasCumulativeStatistics()) {
                YT_VERIFY(chunkList->Children().size() == chunkList->CumulativeStatistics().Size());
                if (chunkList->Statistics().ChunkCount > 0 && !chunkList->CumulativeStatistics().Empty()) {
                    YT_VERIFY(TCumulativeStatisticsEntry{chunkList->Statistics()} ==
                        chunkList->CumulativeStatistics().Back());
                }
            }
        }
    }

    // NB(ifsmirnov): This code was used 3 years ago as an ancient COMPAT but might soon
    // be reused when cumulative stats for dyntables come.
    void RecomputeStatistics()
    {
        YT_LOG_INFO("Started recomputing statistics");

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
            RecomputeStatistics(chunkList);
            auto& statistics = chunkList->Statistics();
            auto oldStatistics = statistics;
            statistics = TChunkTreeStatistics();
            int childCount = chunkList->Children().size();

            auto& cumulativeStatistics = chunkList->CumulativeStatistics();
            cumulativeStatistics.Clear();

            for (int childIndex = 0; childIndex < childCount; ++childIndex) {
                // TODO(ifsmirnov): think of it in context of nullptrs and cumulative statistics.
                auto* child = chunkList->Children()[childIndex];
                if (!child) {
                    continue;
                }

                TChunkTreeStatistics childStatistics;
                switch (child->GetType()) {
                    case EObjectType::Chunk:
                    case EObjectType::ErasureChunk:
                    case EObjectType::JournalChunk:
                    case EObjectType::ErasureJournalChunk:
                        childStatistics.Accumulate(child->AsChunk()->GetStatistics());
                        break;

                    case EObjectType::ChunkList:
                        childStatistics.Accumulate(child->AsChunkList()->Statistics());
                        break;

                    case EObjectType::ChunkView:
                        childStatistics.Accumulate(child->AsChunkView()->GetStatistics());
                        break;

                    default:
                        YT_ABORT();
                }

                if (childIndex + 1 < childCount && chunkList->HasCumulativeStatistics()) {
                    cumulativeStatistics.PushBack(TCumulativeStatisticsEntry{
                        childStatistics.LogicalRowCount,
                        childStatistics.LogicalChunkCount,
                        childStatistics.UncompressedDataSize
                    });
                }

                statistics.Accumulate(childStatistics);
            }

            ++statistics.Rank;
            ++statistics.ChunkListCount;

            if (statistics != oldStatistics) {
                YT_LOG_DEBUG("Chunk list statistics changed (ChunkList: %v, OldStatistics: %v, NewStatistics: %v)",
                    chunkList->GetId(),
                    oldStatistics,
                    statistics);
            }
        }

        YT_LOG_INFO("Finished recomputing statistics");
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

        ExpirationTracker_->Start();
    }

    virtual void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        ChunkReplicator_->Start(AllChunks_.GetFront(), AllChunks_.GetSize());
        ChunkSealer_->Start(JournalChunks_.GetFront(), JournalChunks_.GetSize());

        {
            NProto::TReqConfirmChunkListsRequisitionTraverseFinished request;
            std::vector<TChunkListId> chunkListIds;
            for (auto* chunkList : ChunkListsAwaitingRequisitionTraverse_) {
                ToProto(request.add_chunk_list_ids(), chunkList->GetId());
            }

            YT_LOG_INFO("Scheduling chunk lists requisition traverse confirmation (Count: %v)",
                request.chunk_list_ids_size());

            CreateConfirmChunkListsRequisitionTraverseFinishedMutation(request)
                ->CommitAndLog(Logger);
        }
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

        ExpirationTracker_->Stop();
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
        CancelChunkExpiration(chunk);

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
            chunkWithIndexes.GetMediumIndex(),
            chunkWithIndexes.GetState());

        if (!node->AddReplica(chunkWithIndexes)) {
            return;
        }

        chunk->AddReplica(nodeWithIndexes, medium);

        if (!IsRecovery()) {
            YT_LOG_EVENT(
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

        if (chunk->IsStaged() && !chunk->IsConfirmed() && !chunk->GetExpirationTime()) {
            ScheduleChunkExpiration(chunk);
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
            chunkWithIndexes.GetMediumIndex(),
            chunkWithIndexes.GetState());

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
                YT_ABORT();
        }

        if (!IsRecovery()) {
            YT_LOG_EVENT(
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


    static EChunkReplicaState GetAddedChunkReplicaState(
        TChunk* chunk,
        const TChunkAddInfo& chunkAddInfo)
    {
        if (chunk->IsJournal()) {
            if (chunkAddInfo.active()) {
                return EChunkReplicaState::Active;
            } else if (chunkAddInfo.sealed()) {
                return EChunkReplicaState::Sealed;
            } else {
                return EChunkReplicaState::Unsealed;
            }
        } else {
            return EChunkReplicaState::Generic;
        }
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
        if (!IsObjectAlive(medium)) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Cannot add chunk with unknown medium (NodeId: %v, Address: %v, ChunkId: %v)",
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

            auto isUnknown = node->AddDestroyedReplica(chunkIdWithIndexes);
            YT_LOG_DEBUG_UNLESS(IsRecovery(),
                "%v removal scheduled (NodeId: %v, Address: %v, ChunkId: %v)",
                isUnknown ? "Unknown chunk added," : "Destroyed chunk",
                nodeId,
                node->GetDefaultAddress(),
                chunkIdWithIndexes);

            if (ChunkReplicator_) {
                ChunkReplicator_->ScheduleUnknownReplicaRemoval(node, chunkIdWithIndexes);
            }

            return;
        }

        auto state = GetAddedChunkReplicaState(chunk, chunkAddInfo);
        TChunkPtrWithIndexes chunkWithIndexes(chunk, chunkIdWithIndexes.ReplicaIndex, chunkIdWithIndexes.MediumIndex, state);
        TNodePtrWithIndexes nodeWithIndexes(node, chunkIdWithIndexes.ReplicaIndex, chunkIdWithIndexes.MediumIndex, state);

        if (!cached && node->HasUnapprovedReplica(chunkWithIndexes)) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Chunk approved (NodeId: %v, Address: %v, ChunkId: %v)",
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
        if (!IsObjectAlive(medium)) {
            YT_LOG_WARNING_UNLESS(IsRecovery(), "Cannot remove chunk with unknown medium (NodeId: %v, Address: %v, ChunkId: %v)",
                nodeId,
                node->GetDefaultAddress(),
                chunkIdWithIndexes);
            return;
        }

        auto* chunk = FindChunk(chunkIdWithIndex.Id);
        // NB: Chunk could already be a zombie but we still need to remove the replica.
        if (!chunk) {
            auto isDestroyed = node->RemoveDestroyedReplica(chunkIdWithIndexes);
            YT_LOG_DEBUG_UNLESS(IsRecovery(),
                "%v chunk replica removed (ChunkId: %v, Address: %v, NodeId: %v)",
                isDestroyed ? "Destroyed" : "Unknown",
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
        YT_ASSERT(chunk->IsSealed());

        if (!chunk->HasParents())
            return;

        // Go upwards and apply delta.
        YT_VERIFY(chunk->GetParentCount() == 1);
        auto statisticsDelta = chunk->GetStatistics();
        AccumulateUniqueAncestorsStatistics(chunk, statisticsDelta);

        auto owningNodes = GetOwningNodes(chunk);

        bool journalNodeLocked = false;
        TJournalNode* trunkJournalNode = nullptr;
        for (auto* node : owningNodes) {
            if (node->GetType() == EObjectType::Journal) {
                auto* journalNode = node->As<TJournalNode>();
                if (journalNode->GetUpdateMode() != EUpdateMode::None) {
                    journalNodeLocked = true;
                }
                if (trunkJournalNode) {
                    YT_VERIFY(journalNode->GetTrunkNode() == trunkJournalNode);
                } else {
                    trunkJournalNode = journalNode->GetTrunkNode();
                }
            }
        }

        if (chunk->IsJournal()) {
            if (chunk->IsStaged()) {
                UpdateTransactionResourceUsage(chunk, +1);
            }
            UpdateAccountResourceUsage(chunk, +1);
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
        Profiler.Enqueue("/requisition_update_queue_size", ChunkReplicator_->GetRequisitionUpdateQueueSize(), EMetricType::Gauge);
        Profiler.Enqueue("/seal_queue_size", ChunkSealer_->GetQueueSize(), EMetricType::Gauge);

        Profiler.Enqueue("/chunk_count", ChunkMap_.GetSize(), EMetricType::Gauge);
        Profiler.Enqueue("/chunks_created", ChunksCreated_, EMetricType::Counter);
        Profiler.Enqueue("/chunks_destroyed", ChunksDestroyed_, EMetricType::Counter);

        Profiler.Enqueue("/chunk_replica_count", TotalReplicaCount_, EMetricType::Gauge);
        Profiler.Enqueue("/chunk_replicas_added", ChunkReplicasAdded_, EMetricType::Counter);
        Profiler.Enqueue("/chunk_replicas_removed", ChunkReplicasRemoved_, EMetricType::Counter);

        Profiler.Enqueue("/chunk_view_count", ChunkViewMap_.GetSize(), EMetricType::Gauge);
        Profiler.Enqueue("/chunk_views_created", ChunkViewsCreated_, EMetricType::Counter);
        Profiler.Enqueue("/chunk_views_destroyed", ChunkViewsDestroyed_, EMetricType::Counter);

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

        const auto& jobCounters = ChunkReplicator_->RunningJobs();
        const auto& jobsStarted = ChunkReplicator_->JobsStarted();
        const auto& jobsCompleted = ChunkReplicator_->JobsCompleted();
        const auto& jobsFailed = ChunkReplicator_->JobsFailed();
        const auto& jobsAborted = ChunkReplicator_->JobsAborted();
        for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
            if (jobType >= NJobTrackerClient::FirstMasterJobType && jobType <= NJobTrackerClient::LastMasterJobType) {
                Profiler.Enqueue("/running_job_count", jobCounters[jobType], EMetricType::Gauge, {JobTypeToTag_[jobType]});
                Profiler.Enqueue("/jobs_started", jobsStarted[jobType], EMetricType::Counter, {JobTypeToTag_[jobType]});
                Profiler.Enqueue("/jobs_completed", jobsCompleted[jobType], EMetricType::Counter, {JobTypeToTag_[jobType]});
                Profiler.Enqueue("/jobs_failed", jobsFailed[jobType], EMetricType::Counter, {JobTypeToTag_[jobType]});
                Profiler.Enqueue("/jobs_aborted", jobsAborted[jobType], EMetricType::Counter, {JobTypeToTag_[jobType]});
            }
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
        YT_ABORT();
    }

    TMedium* DoCreateMedium(
        TMediumId id,
        int mediumIndex,
        const TString& name,
        std::optional<bool> transient,
        std::optional<bool> cache,
        std::optional<int> priority)
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
        InitMediumProfiling(medium);
        RegisterMedium(medium);
        InitializeMediumConfig(medium);

        // Make the fake reference.
        YT_VERIFY(medium->RefObject() == 1);

        return medium;
    }

    void InitMediumProfiling(TMedium* medium)
    {
        medium->SetProfilingTag(TProfileManager::Get()->RegisterTag("medium", medium->GetName()));
    }

    void RegisterMedium(TMedium* medium)
    {
        YT_VERIFY(NameToMediumMap_.emplace(medium->GetName(), medium).second);

        auto mediumIndex = medium->GetIndex();
        YT_VERIFY(!UsedMediumIndexes_[mediumIndex]);
        UsedMediumIndexes_.set(mediumIndex);

        YT_VERIFY(!IndexToMediumMap_[mediumIndex]);
        IndexToMediumMap_[mediumIndex] = medium;
    }

    void UnregisterMedium(TMedium* medium)
    {
        YT_VERIFY(NameToMediumMap_.erase(medium->GetName()) == 1);

        auto mediumIndex = medium->GetIndex();
        YT_VERIFY(UsedMediumIndexes_[mediumIndex]);
        UsedMediumIndexes_.reset(mediumIndex);

        YT_VERIFY(IndexToMediumMap_[mediumIndex] == medium);
        IndexToMediumMap_[mediumIndex] = nullptr;
    }

    void InitializeMediumConfig(TMedium* medium)
    {
        InitializeMediumMaxReplicasPerRack(medium);
        InitializeMediumMaxReplicationFactor(medium);
    }

    void InitializeMediumMaxReplicasPerRack(TMedium* medium)
    {
        medium->Config()->MaxReplicasPerRack = Config_->MaxReplicasPerRack;
        medium->Config()->MaxRegularReplicasPerRack = Config_->MaxRegularReplicasPerRack;
        medium->Config()->MaxJournalReplicasPerRack = Config_->MaxJournalReplicasPerRack;
        medium->Config()->MaxErasureReplicasPerRack = Config_->MaxErasureReplicasPerRack;
    }

    // COMPAT(shakurov)
    void InitializeMediumMaxReplicationFactor(TMedium* medium)
    {
        medium->Config()->MaxReplicationFactor = Config_->MaxReplicationFactor;
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
DEFINE_ENTITY_MAP_ACCESSORS(TChunkManager::TImpl, ChunkView, TChunkView, ChunkViewMap_)
DEFINE_ENTITY_MAP_ACCESSORS(TChunkManager::TImpl, DynamicStore, TDynamicStore, DynamicStoreMap_)
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

TChunkManager::TChunkTypeHandler::TChunkTypeHandler(TImpl* owner, EObjectType type)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->ChunkMap_)
    , Owner_(owner)
    , Type_(type)
{ }

IObjectProxyPtr TChunkManager::TChunkTypeHandler::DoGetProxy(
    TChunk* chunk,
    TTransaction* /*transaction*/)
{
    return CreateChunkProxy(Bootstrap_, &Metadata_, chunk);
}

void TChunkManager::TChunkTypeHandler::DoDestroyObject(TChunk* chunk)
{
    // NB: TObjectTypeHandlerWithMapBase::DoDestroyObject will release
    // the runtime data; postpone its call.
    Owner_->DestroyChunk(chunk);
    TObjectTypeHandlerWithMapBase::DoDestroyObject(chunk);
}

void TChunkManager::TChunkTypeHandler::DoUnstageObject(
    TChunk* chunk,
    bool recursive)
{
    TObjectTypeHandlerWithMapBase::DoUnstageObject(chunk, recursive);
    Owner_->UnstageChunk(chunk);
}

void TChunkManager::TChunkTypeHandler::DoExportObject(
    TChunk* chunk,
    TCellTag destinationCellTag)
{
    Owner_->ExportChunk(chunk, destinationCellTag);
}

void TChunkManager::TChunkTypeHandler::DoUnexportObject(
    TChunk* chunk,
    TCellTag destinationCellTag,
    int importRefCounter)
{
    Owner_->UnexportChunk(chunk, destinationCellTag, importRefCounter);
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

TChunkManager::TChunkViewTypeHandler::TChunkViewTypeHandler(TImpl* owner)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->ChunkViewMap_)
    , Owner_(owner)
{ }

IObjectProxyPtr TChunkManager::TChunkViewTypeHandler::DoGetProxy(
    TChunkView* chunkView,
    TTransaction* /*transaction*/)
{
    return CreateChunkViewProxy(Bootstrap_, &Metadata_, chunkView);
}

void TChunkManager::TChunkViewTypeHandler::DoDestroyObject(TChunkView* chunkView)
{
    TObjectTypeHandlerWithMapBase::DoDestroyObject(chunkView);
    Owner_->DestroyChunkView(chunkView);
}

void TChunkManager::TChunkViewTypeHandler::DoUnstageObject(
    TChunkView* /*chunkView*/,
    bool /*recursive*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

TChunkManager::TDynamicStoreTypeHandler::TDynamicStoreTypeHandler(TImpl* owner, EObjectType type)
    : TObjectTypeHandlerWithMapBase(owner->Bootstrap_, &owner->DynamicStoreMap_)
    , Owner_(owner)
    , Type_(type)
{ }

IObjectProxyPtr TChunkManager::TDynamicStoreTypeHandler::DoGetProxy(
    TDynamicStore* dynamicStore,
    TTransaction* /*transaction*/)
{
    return CreateDynamicStoreProxy(Bootstrap_, &Metadata_, dynamicStore);
}

void TChunkManager::TDynamicStoreTypeHandler::DoDestroyObject(TDynamicStore* dynamicStore)
{
    TObjectTypeHandlerWithMapBase::DoDestroyObject(dynamicStore);
    Owner_->DestroyDynamicStore(dynamicStore);
}

void TChunkManager::TDynamicStoreTypeHandler::DoUnstageObject(
    TDynamicStore* /*dynamicStore*/,
    bool /*recursive*/)
{
    YT_ABORT();
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

TObject* TChunkManager::TMediumTypeHandler::CreateObject(
    TObjectId hintId,
    IAttributeDictionary* attributes)
{
    auto name = attributes->GetAndRemove<TString>("name");
    // These three are optional.
    auto priority = attributes->FindAndRemove<int>("priority");
    auto transient = attributes->FindAndRemove<bool>("transient");
    auto cache = attributes->FindAndRemove<bool>("cache");
    if (cache && *cache) {
        THROW_ERROR_EXCEPTION("Cannot create a new cache medium");
    }

    return Owner_->CreateMedium(name, transient, cache, priority, hintId);
}

void TChunkManager::TMediumTypeHandler::DoZombifyObject(TMedium* medium)
{
    TObjectTypeHandlerBase::DoZombifyObject(medium);
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

TChunk* TChunkManager::GetChunkOrThrow(TChunkId id)
{
    return Impl_->GetChunkOrThrow(id);
}

TChunkView* TChunkManager::GetChunkViewOrThrow(TChunkViewId id)
{
    return Impl_->GetChunkViewOrThrow(id);
}

TDynamicStore* TChunkManager::GetDynamicStoreOrThrow(TDynamicStoreId id)
{
    return Impl_->GetDynamicStoreOrThrow(id);
}

TChunkList* TChunkManager::GetChunkListOrThrow(TChunkListId id)
{
    return Impl_->GetChunkListOrThrow(id);
}

TChunkTree* TChunkManager::FindChunkTree(TChunkTreeId id)
{
    return Impl_->FindChunkTree(id);
}

TChunkTree* TChunkManager::GetChunkTree(TChunkTreeId id)
{
    return Impl_->GetChunkTree(id);
}

TChunkTree* TChunkManager::GetChunkTreeOrThrow(TChunkTreeId id)
{
    return Impl_->GetChunkTreeOrThrow(id);
}

TNodeList TChunkManager::AllocateWriteTargets(
    TMedium* medium,
    TChunk* chunk,
    int desiredCount,
    int minCount,
    std::optional<int> replicationFactorOverride,
    const TNodeList* forbiddenNodes,
    const std::optional<TString>& preferredHostName)
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

NYTree::IYPathServicePtr TChunkManager::GetOrchidService()
{
    return Impl_->GetOrchidService();
}

std::unique_ptr<TMutation> TChunkManager::CreateUpdateChunkRequisitionMutation(
    const NProto::TReqUpdateChunkRequisition& request)
{
    return Impl_->CreateUpdateChunkRequisitionMutation(request);
}

std::unique_ptr<NHydra::TMutation> TChunkManager::CreateConfirmChunkListsRequisitionTraverseFinishedMutation(
    const NProto::TReqConfirmChunkListsRequisitionTraverseFinished& request)
{
    return Impl_->CreateConfirmChunkListsRequisitionTraverseFinishedMutation(request);
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

TChunkList* TChunkManager::CloneTabletChunkList(TChunkList* chunkList)
{
    return Impl_->CloneTabletChunkList(chunkList);
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

void TChunkManager::ReplaceChunkListChild(
    TChunkList* chunkList,
    int childIndex,
    TChunkTree* newChild)
{
    Impl_->ReplaceChunkListChild(chunkList, childIndex, newChild);
}

TChunkView* TChunkManager::CreateChunkView(
    TChunkTree* underlyingTree,
    NChunkClient::TReadRange readRange)
{
    return Impl_->CreateChunkView(underlyingTree, std::move(readRange));
}

TChunkView* TChunkManager::CloneChunkView(
    TChunkView* chunkView,
    NChunkClient::TReadRange readRange)
{
    return Impl_->CloneChunkView(chunkView, std::move(readRange));
}

TDynamicStore* TChunkManager::CreateDynamicStore(TDynamicStoreId storeId, const TTablet* tablet)
{
    return Impl_->CreateDynamicStore(storeId, tablet);
}

void TChunkManager::RebalanceChunkTree(TChunkList* chunkList)
{
    Impl_->RebalanceChunkTree(chunkList);
}

void TChunkManager::ClearChunkList(TChunkList* chunkList)
{
    Impl_->ClearChunkList(chunkList);
}

void TChunkManager::ScheduleJobs(
    TNode* node,
    const TNodeResources& resourceUsage,
    const TNodeResources& resourceLimits,
    const std::vector<TJobPtr>& currentJobs,
    std::vector<TJobPtr>* jobsToStart,
    std::vector<TJobPtr>* jobsToAbort,
    std::vector<TJobPtr>* jobsToRemove)
{
    Impl_->ScheduleJobs(
        node,
        resourceUsage,
        resourceLimits,
        currentJobs,
        jobsToStart,
        jobsToAbort,
        jobsToRemove);
}

bool TChunkManager::IsChunkReplicatorEnabled()
{
    return Impl_->IsChunkReplicatorEnabled();
}

bool TChunkManager::IsChunkRefreshEnabled()
{
    return Impl_->IsChunkRefreshEnabled();
}

bool TChunkManager::IsChunkRequisitionUpdateEnabled()
{
    return Impl_->IsChunkRequisitionUpdateEnabled();
}

bool TChunkManager::IsChunkSealerEnabled()
{
    return Impl_->IsChunkSealerEnabled();
}

void TChunkManager::ScheduleChunkRefresh(TChunk* chunk)
{
    Impl_->ScheduleChunkRefresh(chunk);
}

void TChunkManager::ScheduleNodeRefresh(TNode* node)
{
    Impl_->ScheduleNodeRefresh(node);
}

void TChunkManager::ScheduleChunkRequisitionUpdate(TChunkTree* chunkTree)
{
    Impl_->ScheduleChunkRequisitionUpdate(chunkTree);
}

void TChunkManager::ScheduleChunkSeal(TChunk* chunk)
{
    Impl_->ScheduleChunkSeal(chunk);
}

int TChunkManager::GetTotalReplicaCount()
{
    return Impl_->GetTotalReplicaCount();
}

TMediumMap<EChunkStatus> TChunkManager::ComputeChunkStatuses(TChunk* chunk)
{
    return Impl_->ComputeChunkStatuses(chunk);
}

TFuture<TMiscExt> TChunkManager::GetChunkQuorumInfo(TChunk* chunk)
{
    return Impl_->GetChunkQuorumInfo(chunk);
}

TMedium* TChunkManager::GetMediumOrThrow(TMediumId id) const
{
    return Impl_->GetMediumOrThrow(id);
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

void TChunkManager::SetMediumConfig(TMedium* medium, TMediumConfigPtr newConfig)
{
    Impl_->SetMediumConfig(medium, newConfig);
}

TMedium* TChunkManager::FindMediumByName(const TString& name) const
{
    return Impl_->FindMediumByName(name);
}

TMedium* TChunkManager::GetMediumByNameOrThrow(const TString& name) const
{
    return Impl_->GetMediumByNameOrThrow(name);
}

TChunkRequisitionRegistry* TChunkManager::GetChunkRequisitionRegistry()
{
    return Impl_->GetChunkRequisitionRegistry();
}

DELEGATE_ENTITY_MAP_ACCESSORS(TChunkManager, Chunk, TChunk, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TChunkManager, ChunkView, TChunkView, *Impl_)
DELEGATE_ENTITY_MAP_ACCESSORS(TChunkManager, DynamicStore, TDynamicStore, *Impl_)
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

} // namespace NYT::NChunkServer

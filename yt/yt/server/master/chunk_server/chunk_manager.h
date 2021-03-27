#pragma once

#include "public.h"
#include "chunk_replica.h"
#include "chunk_requisition.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/proto/chunk_manager.pb.h>

#include <yt/yt/server/lib/hydra/composite_automaton.h>
#include <yt/yt/server/lib/hydra/entity_map.h>
#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/ytlib/journal_client/helpers.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/small_vector.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkManager
    : public TRefCounted
{
public:
    TChunkManager(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    ~TChunkManager();

    void Initialize();

    NYTree::IYPathServicePtr GetOrchidService();

    std::unique_ptr<NHydra::TMutation> CreateUpdateChunkRequisitionMutation(
        const NProto::TReqUpdateChunkRequisition& request);
    std::unique_ptr<NHydra::TMutation> CreateConfirmChunkListsRequisitionTraverseFinishedMutation(
        const NProto::TReqConfirmChunkListsRequisitionTraverseFinished& request);

    using TCtxExportChunks = NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqExportChunks,
        NChunkClient::NProto::TRspExportChunks>;
    using TCtxExportChunksPtr = TIntrusivePtr<TCtxExportChunks>;
    std::unique_ptr<NHydra::TMutation> CreateExportChunksMutation(
        TCtxExportChunksPtr context);

    using TCtxImportChunks = NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqImportChunks,
        NChunkClient::NProto::TRspImportChunks>;
    using TCtxImportChunksPtr = TIntrusivePtr<TCtxImportChunks>;
    std::unique_ptr<NHydra::TMutation> CreateImportChunksMutation(
        TCtxImportChunksPtr context);

    using TCtxExecuteBatch = NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqExecuteBatch,
        NChunkClient::NProto::TRspExecuteBatch>;
    using TCtxExecuteBatchPtr = TIntrusivePtr<TCtxExecuteBatch>;
    std::unique_ptr<NHydra::TMutation> CreateExecuteBatchMutation(
        TCtxExecuteBatchPtr context);

    DECLARE_ENTITY_MAP_ACCESSORS(Chunk, TChunk);
    TChunk* GetChunkOrThrow(TChunkId id);

    DECLARE_ENTITY_MAP_ACCESSORS(ChunkView, TChunkView);
    TChunkView* GetChunkViewOrThrow(TChunkViewId id);

    DECLARE_ENTITY_MAP_ACCESSORS(DynamicStore, TDynamicStore);
    TDynamicStore* GetDynamicStoreOrThrow(TDynamicStoreId id);

    DECLARE_ENTITY_MAP_ACCESSORS(ChunkList, TChunkList);
    TChunkList* GetChunkListOrThrow(TChunkListId id);

    DECLARE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(Medium, Media, TMedium)

    TChunkTree* FindChunkTree(TChunkTreeId id);
    TChunkTree* GetChunkTree(TChunkTreeId id);
    TChunkTree* GetChunkTreeOrThrow(TChunkTreeId id);

    TNodeList AllocateWriteTargets(
        TMedium* medium,
        TChunk* chunk,
        int desiredCount,
        int minCount,
        std::optional<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes,
        const std::optional<TString>& preferredHostName);

    TChunkList* CreateChunkList(EChunkListKind kind);

    //! For ordered tablets, copies all chunks taking trimmed chunks into account
    //! and updates cumulative statistics accordingly. If all chunks were trimmed
    //! then a nullptr chunk is appended to a cloned chunk list.
    //!
    //! For sorted tablets, cloned chunk list is flattened.
    TChunkList* CloneTabletChunkList(TChunkList* chunkList);

    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd);
    void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children);
    void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* child);

    void DetachFromChunkList(
        TChunkList* chunkList,
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd);
    void DetachFromChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children);
    void DetachFromChunkList(
        TChunkList* chunkList,
        TChunkTree* child);
    void ReplaceChunkListChild(
        TChunkList* chunkList,
        int childIndex,
        TChunkTree* newChild);

    //! Creates #EChunkListKind::HunkRoot child of #tabletChunkList (if missing).
    TChunkList* GetOrCreateHunkChunkList(TChunkList* tabletChunkList);
    //! Similar to #AttachToChunkList but also handles hunk chunks in #children
    //! by attaching them to a separate hunk root child of #chunkList (creating it on demand).
    void AttachToTabletChunkList(
        TChunkList* tabletChunkList,
        const std::vector<TChunkTree*>& children);

    TChunkView* CreateChunkView(TChunkTree* underlyingTree, NChunkClient::TLegacyReadRange readRange);
    TChunkView* CloneChunkView(TChunkView* chunkView, NChunkClient::TLegacyReadRange readRange);

    TChunk* CreateChunk(
        NTransactionServer::TTransaction* transaction,
        TChunkList* chunkList,
        NObjectClient::EObjectType chunkType,
        NSecurityServer::TAccount* account,
        int replicationFactor,
        NErasure::ECodec erasureCodecId,
        TMedium* medium,
        int readQuorum,
        int writeQuorum,
        bool movable,
        bool vital,
        bool overlayed = false,
        NChunkClient::TConsistentPlacementHash consistentPlacementHash = NChunkClient::NullConsistentPlacementHash);

    TDynamicStore* CreateDynamicStore(TDynamicStoreId storeId, const NTabletServer::TTablet* tablet);

    void RebalanceChunkTree(TChunkList* chunkList);

    void UnstageChunk(TChunk* chunk);
    void UnstageChunkList(TChunkList* chunkList, bool recursive);

    TNodePtrWithIndexesList LocateChunk(TChunkPtrWithIndexes chunkWithIndexes);
    void TouchChunk(TChunk* chunk);

    void ClearChunkList(TChunkList* chunkList);

    void ScheduleJobs(
        TNode* node,
        const NNodeTrackerClient::NProto::TNodeResources& resourceUsage,
        const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
        const std::vector<TJobPtr>& currentJobs,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort,
        std::vector<TJobPtr>* jobsToRemove);

    bool IsChunkReplicatorEnabled();
    bool IsChunkRefreshEnabled();
    bool IsChunkRequisitionUpdateEnabled();
    bool IsChunkSealerEnabled();

    void ScheduleChunkRefresh(TChunk* chunk);
    void ScheduleNodeRefresh(TNode* node);
    void ScheduleChunkRequisitionUpdate(TChunkTree* chunkTree);
    void ScheduleChunkSeal(TChunk* chunk);
    void ScheduleChunkMerge(TChunkOwnerBase* node);
    TChunkRequisitionRegistry* GetChunkRequisitionRegistry();

    const THashSet<TChunk*>& LostVitalChunks() const;
    const THashSet<TChunk*>& LostChunks() const;
    const THashSet<TChunk*>& OverreplicatedChunks() const;
    const THashSet<TChunk*>& UnderreplicatedChunks() const;
    const THashSet<TChunk*>& DataMissingChunks() const;
    const THashSet<TChunk*>& ParityMissingChunks() const;
    const TOldestPartMissingChunkSet& OldestPartMissingChunks() const;
    const THashSet<TChunk*>& PrecariousChunks() const;
    const THashSet<TChunk*>& PrecariousVitalChunks() const;
    const THashSet<TChunk*>& QuorumMissingChunks() const;
    const THashSet<TChunk*>& UnsafelyPlacedChunks() const;
    const THashSet<TChunk*>& ForeignChunks() const;

    //! Returns the total number of all chunk replicas.
    int GetTotalReplicaCount();

    TMediumMap<EChunkStatus> ComputeChunkStatuses(TChunk* chunk);

    //! Computes quorum info for a given journal chunk
    //! by querying a quorum of replicas.
    TFuture<NJournalClient::TChunkQuorumInfo> GetChunkQuorumInfo(
        NChunkServer::TChunk* chunk);
    TFuture<NJournalClient::TChunkQuorumInfo> GetChunkQuorumInfo(
        TChunkId chunkId,
        bool overlayed,
        NErasure::ECodec codecId,
        int readQuorum,
        const std::vector<NJournalClient::TChunkReplicaDescriptor>& replicaDescriptors);

    //! Returns the medium with a given id (throws if none).
    TMedium* GetMediumOrThrow(TMediumId id) const;

    //! Returns the medium with a given index (|nullptr| if none).
    TMedium* FindMediumByIndex(int index) const;

    //! Returns the medium with a given index (fails if none).
    TMedium* GetMediumByIndex(int index) const;

    //! Returns the medium with a given index (throws if none).
    TMedium* GetMediumByIndexOrThrow(int index) const;

    //! Renames an existing medium. Throws on name conflict.
    void RenameMedium(TMedium* medium, const TString& newName);

    //! Validates and changes medium priority.
    void SetMediumPriority(TMedium* medium, int priority);

    //! Changes medium config. Triggers global chunk refresh if necessary.
    void SetMediumConfig(TMedium* medium, TMediumConfigPtr newConfig);

    //! Returns the medium with a given name (|nullptr| if none).
    TMedium* FindMediumByName(const TString& name) const;

    //! Returns the medium with a given name (throws if none).
    TMedium* GetMediumByNameOrThrow(const TString& name) const;

private:
    class TImpl;
    class TChunkTypeHandler;
    class TChunkViewTypeHandler;
    class TDynamicStoreTypeHandler;
    class TChunkListTypeHandler;
    class TMediumTypeHandler;

    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TChunkManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

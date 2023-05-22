#pragma once

#include "public.h"
#include "chunk_placement.h"
#include "chunk_replica.h"
#include "chunk_view.h"
#include "chunk_requisition.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/proto/chunk_manager.pb.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/yt/ytlib/job_tracker_client/proto/job_tracker_service.pb.h>

#include <yt/yt/ytlib/journal_client/helpers.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/library/erasure/impl/public.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct IChunkManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    virtual const IJobRegistryPtr& GetJobRegistry() const = 0;

    virtual std::unique_ptr<NHydra::TMutation> CreateUpdateChunkRequisitionMutation(
        const NProto::TReqUpdateChunkRequisition& request) = 0;
    virtual std::unique_ptr<NHydra::TMutation> CreateConfirmChunkListsRequisitionTraverseFinishedMutation(
        const NProto::TReqConfirmChunkListsRequisitionTraverseFinished& request) = 0;
    virtual std::unique_ptr<NHydra::TMutation> CreateRegisterChunkEndorsementsMutation(
        const NProto::TReqRegisterChunkEndorsements& request) = 0;

    using TCtxExportChunks = NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqExportChunks,
        NChunkClient::NProto::TRspExportChunks>;
    using TCtxExportChunksPtr = TIntrusivePtr<TCtxExportChunks>;
    virtual std::unique_ptr<NHydra::TMutation> CreateExportChunksMutation(
        TCtxExportChunksPtr context) = 0;

    using TCtxImportChunks = NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqImportChunks,
        NChunkClient::NProto::TRspImportChunks>;
    using TCtxImportChunksPtr = TIntrusivePtr<TCtxImportChunks>;
    virtual std::unique_ptr<NHydra::TMutation> CreateImportChunksMutation(
        TCtxImportChunksPtr context) = 0;

    using TCtxExecuteBatch = NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqExecuteBatch,
        NChunkClient::NProto::TRspExecuteBatch>;
    using TCtxExecuteBatchPtr = TIntrusivePtr<TCtxExecuteBatch>;

    using TCtxCreateChunk = NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqCreateChunk,
        NChunkClient::NProto::TRspCreateChunk>;
    using TCtxCreateChunkPtr = TIntrusivePtr<TCtxCreateChunk>;

    using TCtxConfirmChunk = NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqConfirmChunk,
        NChunkClient::NProto::TRspConfirmChunk>;
    using TCtxConfirmChunkPtr = TIntrusivePtr<TCtxConfirmChunk>;

    using TCtxSealChunk = NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqSealChunk,
        NChunkClient::NProto::TRspSealChunk>;
    using TCtxSealChunkPtr = TIntrusivePtr<TCtxSealChunk>;

    using TCtxCreateChunkLists = NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqCreateChunkLists,
        NChunkClient::NProto::TRspCreateChunkLists>;
    using TCtxCreateChunkListsPtr = TIntrusivePtr<TCtxCreateChunkLists>;

    using TCtxUnstageChunkTree = NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqUnstageChunkTree,
        NChunkClient::NProto::TRspUnstageChunkTree>;
    using TCtxUnstageChunkTreePtr = TIntrusivePtr<TCtxUnstageChunkTree>;

    using TCtxAttachChunkTrees = NRpc::TTypedServiceContext<
        NChunkClient::NProto::TReqAttachChunkTrees,
        NChunkClient::NProto::TRspAttachChunkTrees>;
    using TCtxAttachChunkTreesPtr = TIntrusivePtr<TCtxAttachChunkTrees>;

    virtual std::unique_ptr<NHydra::TMutation> CreateExecuteBatchMutation(
        TCtxExecuteBatchPtr context) = 0;

    virtual std::unique_ptr<NHydra::TMutation> CreateCreateChunkMutation(
        TCtxCreateChunkPtr context) = 0;
    virtual std::unique_ptr<NHydra::TMutation> CreateConfirmChunkMutation(
        TCtxConfirmChunkPtr context) = 0;
    virtual std::unique_ptr<NHydra::TMutation> CreateSealChunkMutation(
        TCtxSealChunkPtr context) = 0;
    virtual std::unique_ptr<NHydra::TMutation> CreateCreateChunkListsMutation(
        TCtxCreateChunkListsPtr context) = 0;
    virtual std::unique_ptr<NHydra::TMutation> CreateUnstageChunkTreeMutation(
        TCtxUnstageChunkTreePtr context) = 0;
    virtual std::unique_ptr<NHydra::TMutation> CreateAttachChunkTreesMutation(
        TCtxAttachChunkTreesPtr context) = 0;

    virtual std::unique_ptr<NHydra::TMutation> CreateExecuteBatchMutation(
        NChunkClient::NProto::TReqExecuteBatch* request,
        NChunkClient::NProto::TRspExecuteBatch* response) = 0;

    struct TSequoiaExecuteBatchRequest
    {
        std::vector<NChunkClient::NProto::TReqCreateChunk> CreateChunkSubrequests;
        std::vector<NChunkClient::NProto::TReqConfirmChunk> ConfirmChunkSubrequests;
    };

    struct TSequoiaExecuteBatchResponse
    {
        std::vector<NChunkClient::NProto::TRspCreateChunk> CreateChunkSubresponses;
        std::vector<NChunkClient::NProto::TRspConfirmChunk> ConfirmChunkSubresponses;
    };

    struct TPreparedExecuteBatchRequest final
    {
        //! Mutation for non-Sequoia requests.
        NChunkClient::NProto::TReqExecuteBatch MutationRequest;
        NChunkClient::NProto::TRspExecuteBatch MutationResponse;

        //! Sequoia subrequests.
        TSequoiaExecuteBatchRequest SequoiaRequest;
        TSequoiaExecuteBatchResponse SequoiaResponse;

        //! Original request split info.
        std::vector<bool> IsCreateChunkSubrequestSequoia;
        std::vector<bool> IsConfirmChunkSubrequestSequoia;
    };
    using TPreparedExecuteBatchRequestPtr = TIntrusivePtr<TPreparedExecuteBatchRequest>;

    virtual TPreparedExecuteBatchRequestPtr PrepareExecuteBatchRequest(
        const NChunkClient::NProto::TReqExecuteBatch& request) = 0;

    virtual void PrepareExecuteBatchResponse(
        TPreparedExecuteBatchRequestPtr request,
        NChunkClient::NProto::TRspExecuteBatch* response) = 0;

    virtual TFuture<void> ExecuteBatchSequoia(TPreparedExecuteBatchRequestPtr request) = 0;

    // COMPAT(aleksandra-zh)
    virtual TFuture<NChunkClient::NProto::TRspCreateChunk> CreateChunk(
        const NChunkClient::NProto::TReqCreateChunk& request) = 0;
    virtual TFuture<NChunkClient::NProto::TRspConfirmChunk> ConfirmChunk(
        const NChunkClient::NProto::TReqConfirmChunk& request) = 0;

    virtual TFuture<NChunkClient::NProto::TRspCreateChunk> SequoiaCreateChunk(
        const NChunkClient::NProto::TReqCreateChunk& request) = 0;
    virtual TFuture<NChunkClient::NProto::TRspConfirmChunk> SequoiaConfirmChunk(
        const NChunkClient::NProto::TReqConfirmChunk& request) = 0;

    virtual bool IsSequoiaCreateChunkRequest(const NChunkClient::NProto::TReqCreateChunk& request) = 0;
    virtual bool IsSequoiaConfirmChunkRequest(const NChunkClient::NProto::TReqConfirmChunk& request) = 0;

    using TCtxJobHeartbeat = NRpc::TTypedServiceContext<
        NJobTrackerClient::NProto::TReqHeartbeat,
        NJobTrackerClient::NProto::TRspHeartbeat>;
    using TCtxJobHeartbeatPtr = TIntrusivePtr<TCtxJobHeartbeat>;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Chunk, TChunk);
    virtual TChunk* GetChunkOrThrow(TChunkId id) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(ChunkView, TChunkView);
    virtual TChunkView* GetChunkViewOrThrow(TChunkViewId id) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(DynamicStore, TDynamicStore);
    virtual TDynamicStore* GetDynamicStoreOrThrow(TDynamicStoreId id) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(ChunkList, TChunkList);
    virtual TChunkList* GetChunkListOrThrow(TChunkListId id) = 0;

    DECLARE_INTERFACE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(Medium, Media, TMedium);

    virtual TChunkTree* FindChunkTree(TChunkTreeId id) = 0;
    virtual TChunkTree* GetChunkTree(TChunkTreeId id) = 0;
    virtual TChunkTree* GetChunkTreeOrThrow(TChunkTreeId id) = 0;

    //! This function returns a list of nodes where the replicas can be allocated
    //! or an empty list if the search has not succeeded.
    virtual TNodeList AllocateWriteTargets(
        TMedium* medium,
        TChunk* chunk,
        int desiredCount,
        int minCount,
        std::optional<int> replicationFactorOverride,
        const TNodeList* forbiddenNodes,
        const TNodeList* allocatedNodes,
        const std::optional<TString>& preferredHostName) = 0;

    virtual TNodeList AllocateWriteTargets(
        TMedium* medium,
        TChunk* chunk,
        int replicaIndex,
        int desiredCount,
        int minCount,
        std::optional<int> replicationFactorOverride) = 0;

    virtual TChunkList* CreateChunkList(EChunkListKind kind) = 0;

    //! For ordered tablets, copies all chunks taking trimmed chunks into account
    //! and updates cumulative statistics accordingly. If all chunks were trimmed
    //! then a nullptr chunk is appended to a cloned chunk list.
    //!
    //! For sorted tablets, cloned chunk list is flattened.
    virtual TChunkList* CloneTabletChunkList(TChunkList* chunkList) = 0;

    virtual void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd) = 0;
    virtual void AttachToChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children) = 0;
    virtual void AttachToChunkList(
        TChunkList* chunkList,
        TChunkTree* child) = 0;

    virtual void DetachFromChunkList(
        TChunkList* chunkList,
        TChunkTree* const* childrenBegin,
        TChunkTree* const* childrenEnd,
        EChunkDetachPolicy policy) = 0;
    virtual void DetachFromChunkList(
        TChunkList* chunkList,
        const std::vector<TChunkTree*>& children,
        EChunkDetachPolicy policy) = 0;
    virtual void DetachFromChunkList(
        TChunkList* chunkList,
        TChunkTree* child,
        EChunkDetachPolicy policy) = 0;
    // NB: Keep in mind that cumulative chunk list statistics will not be
    // recalculated.
    virtual void ReplaceChunkListChild(
        TChunkList* chunkList,
        int childIndex,
        TChunkTree* newChild) = 0;

    virtual TChunkView* CreateChunkView(TChunkTree* underlyingTree, TChunkViewModifier modifier) = 0;
    virtual TChunkView* CloneChunkView(TChunkView* chunkView, NChunkClient::TLegacyReadRange readRange) = 0;

    virtual TChunk* CreateChunk(
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
        NChunkClient::TConsistentReplicaPlacementHash consistentReplicaPlacementHash = NChunkClient::NullConsistentReplicaPlacementHash,
        i64 replicaLagLimit = 0,
        TChunkId hintId = NullChunkId) = 0;

    virtual TDynamicStore* CreateDynamicStore(TDynamicStoreId storeId, NTabletServer::TTablet* tablet) = 0;

    virtual void RebalanceChunkTree(TChunkList* chunkList, EChunkTreeBalancerMode settingsMode) = 0;

    virtual void UnstageChunk(TChunk* chunk) = 0;
    virtual void UnstageChunkList(TChunkList* chunkList, bool recursive) = 0;

    virtual TNodePtrWithReplicaIndexList LocateChunk(TChunkPtrWithReplicaIndex chunkWithIndexes) = 0;
    virtual void TouchChunk(TChunk* chunk) = 0;

    virtual void ClearChunkList(TChunkList* chunkList) = 0;

    virtual void ProcessJobHeartbeat(TNode* node, const TCtxJobHeartbeatPtr& context) = 0;

    virtual TJobId GenerateJobId() const = 0;

    virtual void SealChunk(TChunk* chunk, const NChunkClient::NProto::TChunkSealInfo& info) = 0;

    virtual const IChunkAutotomizerPtr& GetChunkAutotomizer() const = 0;

    virtual const TChunkReplicatorPtr& GetChunkReplicator() const = 0;

    virtual bool IsChunkReplicatorEnabled() = 0;
    virtual bool IsChunkRefreshEnabled() = 0;
    virtual bool IsChunkRequisitionUpdateEnabled() = 0;
    virtual bool IsChunkSealerEnabled() = 0;

    virtual void ScheduleChunkRefresh(TChunk* chunk) = 0;
    virtual void ScheduleChunkRequisitionUpdate(TChunkTree* chunkTree) = 0;
    virtual void ScheduleChunkSeal(TChunk* chunk) = 0;
    virtual void ScheduleChunkMerge(TChunkOwnerBase* node) = 0;
    virtual bool IsNodeBeingMerged(NCypressClient::TObjectId nodeId) const = 0;
    virtual TChunkRequisitionRegistry* GetChunkRequisitionRegistry() = 0;

    virtual const THashSet<TChunk*>& ForeignChunks() const = 0;

    //! Returns the total number of all chunk replicas.
    virtual int GetTotalReplicaCount() = 0;

    virtual void ScheduleGlobalChunkRefresh() = 0;

    //! Computes quorum info for a given journal chunk
    //! by querying a quorum of replicas.
    virtual TFuture<NJournalClient::TChunkQuorumInfo> GetChunkQuorumInfo(
        NChunkServer::TChunk* chunk) = 0;
    virtual TFuture<NJournalClient::TChunkQuorumInfo> GetChunkQuorumInfo(
        TChunkId chunkId,
        bool overlayed,
        NErasure::ECodec codecId,
        int readQuorum,
        i64 replicaLagLimit,
        const std::vector<NJournalClient::TChunkReplicaDescriptor>& replicaDescriptors) = 0;

    //! Returns the medium with a given id (throws if none).
    virtual TMedium* GetMediumOrThrow(TMediumId id) const = 0;

    //! Returns the medium with a given index (|nullptr| if none).
    virtual TMedium* FindMediumByIndex(int index) const = 0;

    //! Returns the medium with a given index (fails if none).
    virtual TMedium* GetMediumByIndex(int index) const = 0;

    //! Returns the medium with a given index (throws if none).
    virtual TMedium* GetMediumByIndexOrThrow(int index) const = 0;

    //! Renames an existing medium. Throws on name conflict.
    virtual void RenameMedium(TMedium* medium, const TString& newName) = 0;

    //! Validates and changes medium priority.
    virtual void SetMediumPriority(TMedium* medium, int priority) = 0;

    //! Changes medium config. Triggers global chunk refresh if necessary.
    virtual void SetMediumConfig(TMedium* medium, TMediumConfigPtr newConfig) = 0;

    //! Returns the medium with a given name (|nullptr| if none).
    virtual TMedium* FindMediumByName(const TString& name) const = 0;

    //! Returns the medium with a given name (throws if none).
    virtual TMedium* GetMediumByNameOrThrow(const TString& name) const = 0;

    //! Returns chunk replicas "ideal" from CRP point of view.
    //! This reflects the target chunk placement, not the actual one.
    virtual TNodePtrWithReplicaInfoAndMediumIndexList GetConsistentChunkReplicas(TChunk* chunk) const = 0;

    //! Returns global chunk scan descriptor for journal chunks.
    virtual TGlobalChunkScanDescriptor GetGlobalJournalChunkScanDescriptor(int shardIndex) const = 0;

    //! Returns global chunk scan descriptor for blob chunks.
    virtual TGlobalChunkScanDescriptor GetGlobalBlobChunkScanDescriptor(int shardIndex) const = 0;

    //! Aborts job both in job controller and job registry.
    virtual void AbortAndRemoveJob(const TJobPtr& job) = 0;

    // TODO(gritukan): This is a mock for future incumbent manager. Remove it.
    virtual NRpc::IChannelPtr FindChunkReplicatorChannel(TChunk* chunk) = 0;
    virtual NRpc::IChannelPtr GetChunkReplicatorChannelOrThrow(TChunk* chunk) = 0;

    virtual std::vector<NRpc::IChannelPtr> GetChunkReplicatorChannels() = 0;

    //! Returns number of lost vital chunks at current master cell.
    // NB: This function sums amounts of lost vital chunks from all alive
    // chunk replicators only and never returns an error.
    virtual TFuture<i64> GetCellLostVitalChunkCount() = 0;

    virtual void DisposeNode(TNode* node) = 0;
    virtual void DisposeLocation(NChunkServer::TChunkLocation* location) = 0;

private:
    friend class TChunkTypeHandler;
    friend class TChunkListTypeHandler;
    friend class TChunkViewTypeHandler;
    friend class TDynamicStoreTypeHandler;
    friend class TMediumTypeHandler;

    virtual NHydra::TEntityMap<TChunk>& MutableChunks() = 0;
    virtual void DestroyChunk(TChunk* chunk) = 0;
    virtual void ExportChunk(TChunk* chunk, NObjectClient::TCellTag destinationCellTag) = 0;
    virtual void UnexportChunk(TChunk* chunk, NObjectClient::TCellTag destinationCellTag, int importRefCounter) = 0;

    virtual NHydra::TEntityMap<TChunkList>& MutableChunkLists() = 0;
    virtual void DestroyChunkList(TChunkList* chunkList) = 0;

    virtual NHydra::TEntityMap<TChunkView>& MutableChunkViews() = 0;
    virtual void DestroyChunkView(TChunkView* chunkView) = 0;

    virtual NHydra::TEntityMap<TDynamicStore>& MutableDynamicStores() = 0;
    virtual void DestroyDynamicStore(TDynamicStore* dynamicStore) = 0;

    virtual NHydra::TEntityMap<TMedium>& MutableMedia() = 0;
    virtual TMedium* CreateMedium(
        const TString& name,
        std::optional<bool> transient,
        std::optional<int> priority,
        std::optional<int> hintIndex,
        NObjectClient::TObjectId hintId) = 0;
    virtual void DestroyMedium(TMedium* medium) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkManager)

////////////////////////////////////////////////////////////////////////////////

IChunkManagerPtr CreateChunkManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer

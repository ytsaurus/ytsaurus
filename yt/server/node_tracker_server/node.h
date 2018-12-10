#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/chunk_server/chunk_replica.h>

#include <yt/server/hydra/entity_map.h>

#include <yt/server/node_tracker_server/node_tracker.pb.h>

#include <yt/server/tablet_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/server/object_server/object.h>

#include <yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>
#include <yt/client/node_tracker_client/node_directory.h>
#include <yt/ytlib/node_tracker_client/node_statistics.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

#include <array>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ENodeState,
    // Used internally.
    ((Unknown)    (-1))
    // Not registered.
    ((Offline)     (0))
    // Registered but did not report the first heartbeat yet.
    ((Registered)  (1))
    // Registered and reported the first heartbeat.
    ((Online)      (2))
    // Unregistered and placed into disposal queue.
    ((Unregistered)(3))
    // Indicates that state varies across cells.
    ((Mixed)       (4))
);

struct TCellNodeStatistics
{
    NChunkServer::TPerMediumArray<i64> ChunkReplicaCount = {};
};

TCellNodeStatistics& operator+=(TCellNodeStatistics& lhs, const TCellNodeStatistics& rhs);

void ToProto(NProto::TReqSetCellNodeDescriptors::TStatistics* protoStatistics, const TCellNodeStatistics& statistics);
void FromProto(TCellNodeStatistics* statistics, const NProto::TReqSetCellNodeDescriptors::TStatistics& protoStatistics);

struct TCellNodeDescriptor
{
    ENodeState State = ENodeState::Unknown;
    TCellNodeStatistics Statistics;
};

void ToProto(NProto::TReqSetCellNodeDescriptors::TNodeDescriptor* protoDescriptor, const TCellNodeDescriptor& descriptor);
void FromProto(TCellNodeDescriptor* descriptor, const NProto::TReqSetCellNodeDescriptors::TNodeDescriptor& protoDescriptor);

class TNode
    : public NObjectServer::TObjectBase
    , public TRefTracked<TNode>
{
public:
    // Import third-party types into the scope.
    using TChunkPtrWithIndexes = NChunkServer::TChunkPtrWithIndexes;
    using TChunkPtrWithIndex = NChunkServer::TChunkPtrWithIndex;
    using TChunkId = NChunkServer::TChunkId;
    using TChunk = NChunkServer::TChunk;
    using TJobId = NChunkServer::TJobId;
    using TJobPtr = NChunkServer::TJobPtr;
    template <typename T>
    using TPerMediumArray = NChunkServer::TPerMediumArray<T>;
    using TMediumIndexSet = std::bitset<NChunkClient::MaxMediumCount>;

    // Transient properties.
    DEFINE_BYREF_RW_PROPERTY(TPerMediumArray<double>, IOWeights);
    DEFINE_BYVAL_RW_PROPERTY(ENodeState, LastGossipState, ENodeState::Unknown);

    ui64 GetVisitMark(int mediumIndex);
    void SetVisitMark(int mediumIndex, ui64 mark);

    using TMulticellDescriptors = THashMap<NObjectClient::TCellTag, TCellNodeDescriptor>;
    DEFINE_BYREF_RO_PROPERTY(TMulticellDescriptors, MulticellDescriptors);

    //! Tags specified by user in "user_tags" attribute.
    DEFINE_BYREF_RO_PROPERTY(std::vector<TString>, UserTags);
    //! Tags received from node during registration (those typically come from config).
    DEFINE_BYREF_RO_PROPERTY(std::vector<TString>, NodeTags);
    //! User tags plus node tags.
    DEFINE_BYREF_RO_PROPERTY(THashSet<TString>, Tags);

    DEFINE_BYVAL_RW_PROPERTY(TInstant, RegisterTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastSeenTime);

    DEFINE_BYVAL_RW_PROPERTY(NYson::TYsonString, Annotations);
    DEFINE_BYVAL_RW_PROPERTY(TString, Version);

    DEFINE_BYREF_RO_PROPERTY(NNodeTrackerClient::NProto::TNodeStatistics, Statistics);
    void SetStatistics(NNodeTrackerClient::NProto::TNodeStatistics&& statistics);

    DEFINE_BYREF_RW_PROPERTY(std::vector<TError>, Alerts);

    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceLimits);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides, ResourceLimitsOverrides);

    DEFINE_BYVAL_RO_PROPERTY(TRack*, Rack);

    // Lease tracking.
    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, LeaseTransaction);

    // Chunk Manager stuff.
    DEFINE_BYVAL_RO_PROPERTY(bool, Banned);
    void ValidateNotBanned();

    DEFINE_BYVAL_RO_PROPERTY(bool, Decommissioned);

    using TFillFactorIterator = std::optional<NChunkServer::TFillFactorToNodeIterator>;
    using TFillFactorIterators = TPerMediumArray<TFillFactorIterator>;

    using TLoadFactorIterator = std::optional<NChunkServer::TLoadFactorToNodeIterator>;
    using TLoadFactorIterators = TPerMediumArray<TLoadFactorIterator>;

    DEFINE_BYREF_RW_PROPERTY(TFillFactorIterators, FillFactorIterators);
    DEFINE_BYREF_RW_PROPERTY(TLoadFactorIterators, LoadFactorIterators);
    TFillFactorIterator GetFillFactorIterator(int mediumIndex);
    void SetFillFactorIterator(int mediumIndex, TFillFactorIterator iter);
    TLoadFactorIterator GetLoadFactorIterator(int mediumIndex);
    void SetLoadFactorIterator(int mediumIndex, TLoadFactorIterator iter);

    DEFINE_BYVAL_RO_PROPERTY(bool, DisableWriteSessions);

    // Used for graceful restart.
    DEFINE_BYVAL_RW_PROPERTY(bool, DisableSchedulerJobs);

    DEFINE_BYVAL_RW_PROPERTY(bool, DisableTabletCells);

    // NB: Randomize replica hashing to avoid collisions during balancing.
    using TMediumReplicaSet = THashSet<TChunkPtrWithIndexes>;
    using TReplicaSet = TPerMediumArray<TMediumReplicaSet>;
    DEFINE_BYREF_RO_PROPERTY(TReplicaSet, Replicas);

    //! Maps replicas to the leader timestamp when this replica was registered by a client.
    using TUnapprovedReplicaMap = THashMap<TChunkPtrWithIndexes, TInstant>;
    DEFINE_BYREF_RW_PROPERTY(TUnapprovedReplicaMap, UnapprovedReplicas);

    using TJobMap = THashMap<TJobId, TJobPtr>;
    DEFINE_BYREF_RO_PROPERTY(TJobMap, IdToJob);

    //! Indexed by priority. Each map is as follows:
    //! Key:
    //!   Encodes chunk and one of its parts (for erasure chunks only, others use GenericChunkReplicaIndex).
    //!   Medium index indicates the medium where this replica is being stored.
    //! Value:
    //!   Indicates media where acting as replication targets for this chunk.
    using TChunkReplicationQueues = std::vector<THashMap<TChunkPtrWithIndexes, TMediumIndexSet>>;
    DEFINE_BYREF_RW_PROPERTY(TChunkReplicationQueues, ChunkReplicationQueues);

    //! Key:
    //!   Encodes chunk and one of its parts (for erasure chunks only, others use GenericChunkReplicaIndex).
    //! Value:
    //!   Indicates media where removal of this chunk is scheduled.
    using TChunkRemovalQueue = THashMap<NChunkClient::TChunkIdWithIndex, TMediumIndexSet>;
    DEFINE_BYREF_RW_PROPERTY(TChunkRemovalQueue, ChunkRemovalQueue);

    //! Key:
    //!   Indicates an unsealed chunk.
    //! Value:
    //!   Indicates media where seal of this chunk is scheduled.
    using TChunkSealQueue = THashMap<TChunk*, TMediumIndexSet>;
    DEFINE_BYREF_RW_PROPERTY(TChunkSealQueue, ChunkSealQueue);

    // Tablet Manager stuff.
    struct TTabletSlot
    {
        NTabletServer::TTabletCell* Cell = nullptr;
        NHydra::EPeerState PeerState = NHydra::EPeerState::None;
        int PeerId = NHydra::InvalidPeerId;

        void Persist(NCellMaster::TPersistenceContext& context);
    };

    using TTabletSlotList = SmallVector<TTabletSlot, NTabletClient::TypicalTabletSlotCount>;
    DEFINE_BYREF_RW_PROPERTY(TTabletSlotList, TabletSlots);

public:
    explicit TNode(const NObjectServer::TObjectId& objectId);

    TNodeId GetId() const;

    TNodeDescriptor GetDescriptor(NNodeTrackerClient::EAddressType addressType = NNodeTrackerClient::EAddressType::InternalRpc) const;

    const TNodeAddressMap& GetNodeAddresses() const;
    void SetNodeAddresses(const TNodeAddressMap& nodeAddresses);
    const TAddressMap& GetAddressesOrThrow(NNodeTrackerClient::EAddressType addressType) const;
    const TString& GetDefaultAddress() const;

    //! Get data center to which this node belongs.
    /*!
     *  May return nullptr if the node belongs to no rack or its rack belongs to
     *  no data center.
     */
    TDataCenter* GetDataCenter() const;

    bool HasTag(const std::optional<TString>& tag) const;

    //! Prepares per-cell state map.
    //! Inserts new entries into the map, fills missing onces with ENodeState::Offline value.
    void InitializeStates(NObjectClient::TCellTag cellTag, const NObjectClient::TCellTagList& secondaryCellTags);
    //! Gets the local state by dereferencing local state pointer.
    ENodeState GetLocalState() const;
    //! Sets the local state by dereferencing local state pointer.
    void SetLocalState(ENodeState state);

    //! Sets the state and statistics for the given cell.
    void SetCellDescriptor(NObjectClient::TCellTag cellTag, const TCellNodeDescriptor& descriptor);

    //! If states are same for all cells then returns this common value.
    //! Otherwise returns "mixed" state.
    ENodeState GetAggregatedState() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    TJobPtr FindJob(const TJobId& jobId);
    void RegisterJob(const TJobPtr& job);
    void UnregisterJob(const TJobPtr& job);

    // Chunk Manager stuff.
    void ReserveReplicas(int mediumIndex, int sizeHint);
    //! Returns |true| if the replica was actually added.
    bool AddReplica(TChunkPtrWithIndexes replica);
    //! Returns |true| if the replica was approved
    bool RemoveReplica(TChunkPtrWithIndexes replica);

    bool HasReplica(TChunkPtrWithIndexes) const;
    TChunkPtrWithIndexes PickRandomReplica(int mediumIndex);
    void ClearReplicas();

    void AddUnapprovedReplica(TChunkPtrWithIndexes replica, TInstant timestamp);
    bool HasUnapprovedReplica(TChunkPtrWithIndexes replica) const;
    void ApproveReplica(TChunkPtrWithIndexes replica);

    void AddToChunkRemovalQueue(const NChunkClient::TChunkIdWithIndexes& replica);
    void RemoveFromChunkRemovalQueue(const NChunkClient::TChunkIdWithIndexes& replica);

    void AddToChunkReplicationQueue(TChunkPtrWithIndexes replica, int targetMediumIndex, int priority);
    //! Handles the case |targetMediumIndex == AllMediaIndex| correctly.
    void RemoveFromChunkReplicationQueues(TChunkPtrWithIndexes replica, int targetMediumIndex);

    void AddToChunkSealQueue(TChunkPtrWithIndexes chunkWithIndexes);
    void RemoveFromChunkSealQueue(TChunkPtrWithIndexes chunkWithIndexes);

    void ClearSessionHints();
    void AddSessionHint(int mediumIndex, NChunkClient::ESessionType sessionType);

    int GetSessionCount(NChunkClient::ESessionType sessionType) const;
    int GetTotalSessionCount() const;

    int GetTotalTabletSlots() const;

    // Returns true iff the node has at least one location belonging to the
    // specified medium.
    bool HasMedium(int mediumIndex) const;

    //! Returns null if there's no storage of specified medium on this node.
    std::optional<double> GetFillFactor(int mediumIndex) const;
    //! Returns null if there's no storage of specified medium on this node.
    std::optional<double> GetLoadFactor(int mediumIndex) const;

    bool IsWriteEnabled(int mediumIndex) const;

    TTabletSlot* FindTabletSlot(const NTabletServer::TTabletCell* cell);
    TTabletSlot* GetTabletSlot(const NTabletServer::TTabletCell* cell);

    void DetachTabletCell(const NTabletServer::TTabletCell* cell);

    void InitTabletSlots();
    void ClearTabletSlots();

    void ShrinkHashTables();

    void Reset();

    static ui64 GenerateVisitMark();

    // Computes node statistics for the local cell.
    TCellNodeStatistics ComputeCellStatistics() const;
    // Computes total cluster statistics (over all cells, including the local one).
    TCellNodeStatistics ComputeClusterStatistics() const;

    void ClearCellStatistics();

private:
    NNodeTrackerClient::TNodeAddressMap NodeAddresses_;
    TString DefaultAddress_;

    TPerMediumArray<int> HintedUserSessionCount_;
    TPerMediumArray<int> HintedReplicationSessionCount_;
    TPerMediumArray<int> HintedRepairSessionCount_;

    int TotalHintedUserSessionCount_;
    int TotalHintedReplicationSessionCount_;
    int TotalHintedRepairSessionCount_;

    TPerMediumArray<TMediumReplicaSet::iterator> RandomReplicaIters_;

    TPerMediumArray<ui64> VisitMarks_{};

    TPerMediumArray<std::optional<double>> FillFactors_;
    TPerMediumArray<std::optional<int>> SessionCount_;

    ENodeState* LocalStatePtr_ = nullptr;
    ENodeState AggregatedState_ = ENodeState::Unknown;


    int GetHintedSessionCount(int mediumIndex) const;

    void ComputeAggregatedState();
    void ComputeDefaultAddress();
    void ComputeFillFactors();
    void ComputeSessionCount();

    bool DoAddReplica(TChunkPtrWithIndexes replica);
    bool DoRemoveReplica(TChunkPtrWithIndexes replica);
    bool DoHasReplica(TChunkPtrWithIndexes replica) const;

    // Private accessors for TNodeTracker.
    friend class TNodeTracker;

    void SetRack(TRack* rack);
    void SetBanned(bool value);
    void SetDecommissioned(bool value);
    void SetDisableWriteSessions(bool value);

    void SetNodeTags(const std::vector<TString>& tags);
    void SetUserTags(const std::vector<TString>& tags);
    void RebuildTags();

};

////////////////////////////////////////////////////////////////////////////////

struct TNodePtrAddressFormatter
{
    void operator()(TStringBuilder* builder, TNode* node) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer

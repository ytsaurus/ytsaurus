#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/chunk_server/chunk_location.h>

#include <yt/yt/server/master/maintenance_tracker_server/maintenance_target.h>

#include <yt/yt/server/master/node_tracker_server/proto/node_tracker.pb.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/ytlib/node_tracker_client/node_statistics.h>
#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>

#include <optional>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

struct TCellNodeStatistics
{
    NChunkClient::TMediumMap<i64> ChunkReplicaCount;
    i64 DestroyedChunkReplicaCount = 0;
    i64 ChunkPushReplicationQueuesSize = 0;
    i64 ChunkPullReplicationQueuesSize = 0;
    i64 PullReplicationChunkCount = 0;
};

TCellNodeStatistics& operator+=(TCellNodeStatistics& lhs, const TCellNodeStatistics& rhs);

void ToProto(NProto::TNodeStatistics* protoStatistics, const TCellNodeStatistics& statistics);
void FromProto(TCellNodeStatistics* statistics, const NProto::TNodeStatistics& protoStatistics);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EWriteTargetValidityChange,
    ((None)                      (0))
    ((ReportedDataNodeHeartbeat) (1))
    ((Decommissioned)            (2))
    ((WriteSessionsDisabled)     (3))
);

////////////////////////////////////////////////////////////////////////////////

struct TIncrementalHeartbeatCounters
{
    NProfiling::TCounter RemovedChunks;
    NProfiling::TCounter RemovedUnapprovedReplicas;
    NProfiling::TCounter ApprovedReplicas;
    NProfiling::TCounter AddedReplicas;
    NProfiling::TCounter AddedDestroyedReplicas;

    explicit TIncrementalHeartbeatCounters(const NProfiling::TProfiler& profiler);
};

////////////////////////////////////////////////////////////////////////////////

class TNode
    : public NObjectServer::TObject
    , public NMaintenanceTrackerServer::TMaintenanceTarget<
        TNode,
        NMaintenanceTrackerServer::EMaintenanceType::Ban,
        NMaintenanceTrackerServer::EMaintenanceType::Decommission,
        NMaintenanceTrackerServer::EMaintenanceType::DisableSchedulerJobs,
        NMaintenanceTrackerServer::EMaintenanceType::DisableWriteSessions,
        NMaintenanceTrackerServer::EMaintenanceType::DisableTabletCells,
        NMaintenanceTrackerServer::EMaintenanceType::PendingRestart>
    , public TRefTracked<TNode>
{
public:
    // Import third-party types into the scope.
    using TChunkPtrWithReplicaInfo = NChunkServer::TChunkPtrWithReplicaInfo;
    using TChunkPtrWithReplicaIndex = NChunkServer::TChunkPtrWithReplicaIndex;
    using TChunkPtrWithReplicaAndMediumIndex = NChunkServer::TChunkPtrWithReplicaAndMediumIndex;
    using TChunkId = NChunkServer::TChunkId;
    using TChunk = NChunkServer::TChunk;
    template <typename T>
    using TMediumMap = NChunkClient::TMediumMap<T>;
    using TMediumSet = NChunkServer::TMediumSet;

    DEFINE_BYREF_RO_PROPERTY(TMediumMap<double>, IOWeights);
    DEFINE_BYREF_RO_PROPERTY(TMediumMap<i64>, TotalSpace);
    DEFINE_BYREF_RW_PROPERTY(TMediumMap<int>, ConsistentReplicaPlacementTokenCount);

    // Returns the number of tokens for this node that should be placed on the
    // consistent replica placement ring. For media that are absent on the node,
    // returns zero.
    int GetConsistentReplicaPlacementTokenCount(int mediumIndex) const;

    ui64 GetVisitMark(int mediumIndex);
    void SetVisitMark(int mediumIndex, ui64 mark);

    struct TCellNodeDescriptor
    {
        ENodeState State = ENodeState::Unknown;
        TCellNodeStatistics Statistics;
        bool RegistrationPending = false;

        void Persist(const NCellMaster::TPersistenceContext& context);
    };
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

    DEFINE_BYREF_RW_PROPERTY(THashSet<ENodeFlavor>, Flavors);

    //! Helpers for |Flavors| access.
    bool IsDataNode() const;
    bool IsExecNode() const;
    bool IsTabletNode() const;
    bool IsChaosNode() const;
    bool IsCellarNode() const;

    //! This set contains heartbeat types that were reported by the node since last registration.
    //! Node is considered online iff it received all heartbeats corresponded to its flavors.
    DEFINE_BYREF_RW_PROPERTY(THashSet<ENodeHeartbeatType>, ReportedHeartbeats);

    // COMPAT(gritukan)
    DEFINE_BYVAL_RW_PROPERTY(bool, ExecNodeIsNotDataNode);

    //! Helpers for |ReportedHeartbeats| access.
    bool ReportedClusterNodeHeartbeat() const;
    bool ReportedDataNodeHeartbeat() const;
    bool ReportedExecNodeHeartbeat() const;
    bool ReportedCellarNodeHeartbeat() const;
    bool ReportedTabletNodeHeartbeat() const;

    void ValidateRegistered();

    DEFINE_BYREF_RO_PROPERTY(NNodeTrackerClient::NProto::TClusterNodeStatistics, ClusterNodeStatistics);
    void SetClusterNodeStatistics(NNodeTrackerClient::NProto::TClusterNodeStatistics&& statistics);

    DEFINE_BYREF_RW_PROPERTY(std::vector<TError>, Alerts);

    DEFINE_BYREF_RO_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceLimits);
    DEFINE_BYREF_RO_PROPERTY(NNodeTrackerClient::NProto::TNodeResources, ResourceUsage);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides, ResourceLimitsOverrides);

    DEFINE_BYREF_RO_PROPERTY(std::vector<NChunkServer::TRealChunkLocation*>, RealChunkLocations);

    //! Detaches real locations from this node. Deletes imaginary locations.
    void ClearChunkLocations();

    // COMPAT(kvk1920)
    void AddRealChunkLocation(NChunkServer::TRealChunkLocation* location);
    void RemoveRealChunkLocation(NChunkServer::TRealChunkLocation* location);

    // COMPAT(kvk1920)
    // NB: This field is loaded during TNodeTracker::LoadKeys().
    DEFINE_BYREF_RW_PROPERTY(bool, UseImaginaryChunkLocations);
    DEFINE_BYREF_RW_PROPERTY(std::vector<NChunkServer::TChunkLocation*>, ChunkLocations);
    // COMPAT(shakurov)
    DEFINE_BYREF_RW_PROPERTY(TMediumMap<std::unique_ptr<NChunkServer::TImaginaryChunkLocation>>, ImaginaryChunkLocations);

    NChunkServer::TImaginaryChunkLocation* GetOrCreateImaginaryChunkLocation(int mediumIndex, bool ignoreHydraContext = false);
    // COMPAT(danilalexeev)
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    NChunkServer::TImaginaryChunkLocation* GetOrCreateImaginaryChunkLocationDuringSnapshotLoading(int mediumIndex);
    NChunkServer::TImaginaryChunkLocation* GetImaginaryChunkLocation(int mediumIndex);

    DEFINE_BYVAL_RO_PROPERTY(THost*, Host);

    // Lease tracking.
    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, LeaseTransaction);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TDuration>, LastSeenLeaseTransactionTimeout);

    // Exec Node stuff.
    DEFINE_BYREF_RO_PROPERTY(NNodeTrackerClient::NProto::TExecNodeStatistics, ExecNodeStatistics);
    void SetExecNodeStatistics(NNodeTrackerClient::NProto::TExecNodeStatistics&& statistics);

    DEFINE_BYREF_RW_PROPERTY(std::optional<TString>, JobProxyVersion);

    // Chunk Manager stuff.
    DEFINE_BYREF_RO_PROPERTY(NNodeTrackerClient::NProto::TDataNodeStatistics, DataNodeStatistics);
    void SetDataNodeStatistics(
        NNodeTrackerClient::NProto::TDataNodeStatistics&& statistics,
        const NChunkServer::IChunkManagerPtr& chunkManager);

    void ValidateNotBanned();

    using TLoadFactorIterator = std::optional<NChunkServer::TLoadFactorToNodeIterator>;
    using TLoadFactorIterators = TMediumMap<TLoadFactorIterator>;

    DEFINE_BYREF_RW_PROPERTY(TLoadFactorIterators, LoadFactorIterators);
    TLoadFactorIterator GetLoadFactorIterator(int mediumIndex) const;
    void SetLoadFactorIterator(int mediumIndex, TLoadFactorIterator iter);

    bool GetEffectiveDisableWriteSessions() const;

    // Transient copies of DisableWriteSessions.
    DEFINE_BYVAL_RO_PROPERTY(bool, DisableWriteSessionsSentToNode);
    void SetDisableWriteSessionsSentToNode(bool value);

    DEFINE_BYVAL_RO_PROPERTY(bool, DisableWriteSessionsReportedByNode);
    void SetDisableWriteSessionsReportedByNode(bool value);

    bool IsValidWriteTarget() const;
    bool WasValidWriteTarget(EWriteTargetValidityChange change) const;

    //! Indexed by priority. Each map is as follows:
    //! Key:
    //!   Encodes chunk and one of its parts (for erasure chunks only, others use GenericChunkReplicaIndex).
    //! Value:
    //!   Indicates media where acting as replication targets for this chunk.
    using TChunkPushReplicationQueue = THashMap<NChunkClient::TChunkIdWithIndex, TMediumSet>;
    using TChunkPushReplicationQueues = std::vector<TChunkPushReplicationQueue>;
    DEFINE_BYREF_RW_PROPERTY(TChunkPushReplicationQueues, ChunkPushReplicationQueues);

    //! Has the same structure as push replication queues.
    using TChunkPullReplicationQueue = THashMap<NChunkClient::TChunkIdWithIndex, TMediumSet>;
    using TChunkPullReplicationQueues = std::vector<TChunkPullReplicationQueue>;
    DEFINE_BYREF_RW_PROPERTY(TChunkPullReplicationQueues, ChunkPullReplicationQueues);

    // For chunks in push queue, its correspondent pull queue node id.
    // Used for CRP-enabled chunks only.
    using TChunkNodeIds = THashMap<TChunkId, THashMap<int, TNodeId>>;
    DEFINE_BYREF_RW_PROPERTY(TChunkNodeIds, PushReplicationTargetNodeIds);

    // A set of chunk ids with an ongoing replication to this node as a destination.
    // Used for CRP-enabled chunks only.
    using TChunkPullReplicationSet = THashMap<TChunkId, TMediumMap<int>>;
    DEFINE_BYREF_RW_PROPERTY(TChunkPullReplicationSet, ChunksBeingPulled);

    //! Chunk replica announcement requests that should be sent to the node upon next heartbeat.
    //! Non-null revision means that the request was already sent and is pending confirmation.
    using TEndorsementMap = THashMap<TChunk*, NHydra::TRevision>;
    DEFINE_BYREF_RW_PROPERTY(TEndorsementMap, ReplicaEndorsements);

    // Cell Manager stuff.
    struct TCellSlot
    {
        NCellServer::TCellBase* Cell = nullptr;
        NHydra::EPeerState PeerState = NHydra::EPeerState::None;
        int PeerId = NHydra::InvalidPeerId;

        //! Sum of `PreloadPendingStoreCount` over all tablets in slot.
        int PreloadPendingStoreCount = 0;

        //! Sum of `PreloadCompletedStoreCount` over all tablets in slot.
        int PreloadCompletedStoreCount = 0;

        //! Sum of `PreloadFailedStoreCount` over all tablets in slot.
        int PreloadFailedStoreCount = 0;

        void Persist(const NCellMaster::TPersistenceContext& context);

        // Used in cell balancer to check peer state.
        bool IsWarmedUp() const;
    };

    using TCellar = TCompactVector<TCellSlot, NCellarClient::TypicalCellarSize>;
    using TCellarMap = THashMap<NCellarClient::ECellarType, TCellar>;
    DEFINE_BYREF_RW_PROPERTY(TCellarMap, Cellars);

    DEFINE_BYREF_RW_PROPERTY(std::optional<TIncrementalHeartbeatCounters>, IncrementalHeartbeatCounters);

    DEFINE_BYVAL_RW_PROPERTY(ENodeState, LastGossipState, ENodeState::Unknown);

    DEFINE_BYVAL_RW_PROPERTY(int, NextDisposedLocationIndex);
    DEFINE_BYVAL_RW_PROPERTY(bool, DisposalTickScheduled);

    using TSequenceNumberMap = std::multimap<NChunkServer::THeartbeatSequenceNumber, TChunkId>;
    DEFINE_BYREF_RW_PROPERTY(TSequenceNumberMap, AwaitingHeartbeatChunkIds);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TChunkId>, RemovalJobScheduledChunkIds);

public:
    explicit TNode(NObjectServer::TObjectId objectId);

    TNodeId GetId() const;

    TNodeDescriptor GetDescriptor(NNodeTrackerClient::EAddressType addressType = NNodeTrackerClient::EAddressType::InternalRpc) const;

    const TNodeAddressMap& GetNodeAddresses() const;
    void SetNodeAddresses(const TNodeAddressMap& nodeAddresses);
    const TAddressMap& GetAddressesOrThrow(NNodeTrackerClient::EAddressType addressType) const;
    const TString& GetDefaultAddress() const;

    //! Get rack to which this node belongs.
    /*!
     *  May return nullptr if the node belongs to no rack
     */
    TRack* GetRack() const;

    //! Get data center to which this node belongs.
    /*!
     *  May return nullptr if the node belongs to no rack or its rack belongs to
     *  no data center.
     */
    TDataCenter* GetDataCenter() const;

    bool HasTag(const std::optional<TString>& tag) const;

    //! Prepares per-cell state map.
    //! Inserts new entries into the map, fills missing ones with ENodeState::Offline value.
    void InitializeStates(
        NObjectClient::TCellTag cellTag,
        const NObjectClient::TCellTagList& secondaryCellTags);

    //! Recomputes node IO weights from statistics.
    void RecomputeIOWeights(const NChunkServer::IChunkManagerPtr& chunkManager);

    //! Gets the local state by dereferencing local state pointer.
    ENodeState GetLocalState() const;
    //! Sets the local state by dereferencing local state pointer.
    void SetLocalState(ENodeState state);

    //! Sets state for the given cell.
    void SetState(
        NObjectClient::TCellTag cellTag,
        ENodeState state,
        bool redundant);
    //! Sets statistics for the given cell.
    void SetStatistics(
        NObjectClient::TCellTag cellTag,
        const TCellNodeStatistics& descriptor);
    void SetRegistrationPending(NObjectClient::TCellTag selfCellTag);

    //! If states are same for all cells then returns this common value.
    //! Otherwise returns "mixed" state.
    ENodeState GetAggregatedState() const;
    //! Returns true if at least one cell is pending node registration.
    bool GetRegistrationPending() const;

    DEFINE_SIGNAL(void(TNode* node), AggregatedStateChanged);

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    // Chunk Manager stuff.
    TChunkPtrWithReplicaInfo PickRandomReplica(int mediumIndex);
    void ClearReplicas();

    void AddToChunkPushReplicationQueue(NChunkClient::TChunkIdWithIndex replica, int targetMediumIndex, int priority);
    void AddToChunkPullReplicationQueue(NChunkClient::TChunkIdWithIndex replica, int targetMediumIndex, int priority);
    void RefChunkBeingPulled(TChunkId chunkId, int targetMediumIndex);
    void UnrefChunkBeingPulled(TChunkId chunkId, int targetMediumIndex);

    void AddTargetReplicationNodeId(TChunkId chunkId, int targetMediumIndex, TNode* node);
    void RemoveTargetReplicationNodeId(TChunkId chunkId, int targetMediumIndex);
    TNodeId GetTargetReplicationNodeId(TChunkId chunkId, int targetMediumIndex);

    void RemoveFromChunkReplicationQueues(NChunkClient::TChunkIdWithIndex replica);

    void ClearSessionHints();
    void AddSessionHint(int mediumIndex, NChunkClient::ESessionType sessionType);

    int GetSessionCount(NChunkClient::ESessionType sessionType) const;
    int GetTotalSessionCount() const;

    int GetCellarSize(NCellarClient::ECellarType) const;

    // Returns true iff the node has at least one location belonging to the
    // specified medium.
    bool HasMedium(int mediumIndex) const;

    //! Returns null if there's no storage of specified medium on this node.
    std::optional<double> GetFillFactor(int mediumIndex) const;
    //! Returns null if there's no storage of specified medium left on this node.
    std::optional<double> GetLoadFactor(int mediumIndex, int chunkHostMasterCellCount) const;

    bool IsWriteEnabled(int mediumIndex) const;

    TCellSlot* FindCellSlot(const NCellServer::TCellBase* cell);
    TCellSlot* GetCellSlot(const NCellServer::TCellBase* cell);

    void DetachCell(const NCellServer::TCellBase* cell);

    TCellar* FindCellar(NCellarClient::ECellarType cellarType);
    const TCellar* FindCellar(NCellarClient::ECellarType cellarType) const;
    TCellar& GetCellar(NCellarClient::ECellarType cellarType);
    const TCellar& GetCellar(NCellarClient::ECellarType cellarType) const;

    void InitCellars();
    void ClearCellars();
    void UpdateCellarSize(NCellarClient::ECellarType cellarType, int newSize);

    void SetCellarNodeStatistics(
        NCellarClient::ECellarType cellarType,
        NNodeTrackerClient::NProto::TCellarNodeStatistics&& statistics);
    void RemoveCellarNodeStatistics(NCellarClient::ECellarType cellarType);

    int GetAvailableSlotCount(NCellarClient::ECellarType cellarType) const;
    int GetTotalSlotCount(NCellarClient::ECellarType cellarType) const;

    void ShrinkHashTables();

    void ClearPushReplicationTargetNodeIds(const INodeTrackerPtr& nodeTracker);
    void Reset(const INodeTrackerPtr& nodeTracker);

    static ui64 GenerateVisitMark();

    // Computes node statistics for the local cell.
    TCellNodeStatistics ComputeCellStatistics() const;
    // Computes total cluster statistics (over all cells, including the local one).
    TCellNodeStatistics ComputeClusterStatistics() const;

    void ClearCellStatistics();

    // NB: Handles AllMediaIndex correctly.
    i64 ComputeTotalReplicaCount(int mediumIndex) const;

    i64 ComputeTotalChunkRemovalQueuesSize() const;
    i64 ComputeTotalDestroyedReplicaCount() const;

private:
    NNodeTrackerClient::TNodeAddressMap NodeAddresses_;
    TString DefaultAddress_;

    TMediumMap<int> HintedUserSessionCount_;
    TMediumMap<int> HintedReplicationSessionCount_;
    TMediumMap<int> HintedRepairSessionCount_;

    int TotalHintedUserSessionCount_;
    int TotalHintedReplicationSessionCount_;
    int TotalHintedRepairSessionCount_;

    TMediumMap<ui64> VisitMarks_;

    TMediumMap<std::optional<double>> FillFactors_;
    TMediumMap<std::optional<int>> SessionCount_;

    ENodeState* LocalStatePtr_ = nullptr;
    ENodeState AggregatedState_ = ENodeState::Unknown;
    bool RegistrationPending_ = false;

    THashMap<NCellarClient::ECellarType, NNodeTrackerClient::NProto::TCellarNodeStatistics> CellarNodeStatistics_;

    int GetHintedSessionCount(int mediumIndex, int chunkHostMasterCellCount) const;

    void ComputeAggregatedState();
    void ComputeDefaultAddress();
    void ComputeFillFactorsAndTotalSpace();
    void ComputeSessionCount();

    // Private accessors for TNodeTracker.
    friend class TNodeTracker;

    void SetHost(THost* host);
    void SetDisableWriteSessions(bool value);

    void SetNodeTags(const std::vector<TString>& tags);
    void SetUserTags(const std::vector<TString>& tags);
    void RebuildTags();

    void SetResourceUsage(const NNodeTrackerClient::NProto::TNodeResources& resourceUsage);
    void SetResourceLimits(const NNodeTrackerClient::NProto::TNodeResources& resourceLimits);
};

DEFINE_MASTER_OBJECT_TYPE(TNode)

////////////////////////////////////////////////////////////////////////////////

struct TNodePtrAddressFormatter
{
    void operator()(TStringBuilderBase* builder, TNode* node) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer

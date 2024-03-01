#pragma once

#include "public.h"

#include <yt/yt/server/master/tablet_server/public.h>

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cell_master/gossip_value.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/server/lib/cellar_agent/public.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/memory/ref_tracked.h>

#include <optional>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

struct TCellStatus
{
    ECellHealth Health;
    bool Decommissioned;

    bool operator== (const TCellStatus& other) const = default;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

void ToProto(NProto::TCellStatus* protoStatus, const TCellStatus& statistics);
void FromProto(TCellStatus* status, const NProto::TCellStatus& protoStatistics);

void Serialize(const TCellStatus& status, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TCellBase
    : public NObjectServer::TObject
    , public TRefTracked<TCellBase>
{
public:
    //! Cached |GetCellShardIndex(id)| for efficient access.
    DEFINE_BYVAL_RO_PROPERTY(int, ShardIndex);

    struct TPeer
    {
        NNodeTrackerClient::TNodeDescriptor Descriptor;
        NNodeTrackerServer::TNode* Node = nullptr;
        TInstant LastSeenTime;
        EPeerState LastSeenState = EPeerState::None;
        TError LastRevocationReason;
        NTransactionServer::TTransaction* PrerequisiteTransaction = nullptr;

        void Persist(const NCellMaster::TPersistenceContext& context);
    };

    using TPeerList = TCompactVector<TPeer, TypicalPeerCount>;
    DEFINE_BYREF_RW_PROPERTY(TPeerList, Peers);
    DEFINE_BYVAL_RW_PROPERTY(int, LeadingPeerId, NElection::InvalidPeerId);

    DEFINE_BYVAL_RW_PROPERTY(int, ConfigVersion);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, PrerequisiteTransaction);

    DEFINE_BYREF_RW_PROPERTY(TCellBundlePtr, CellBundle);
    DEFINE_BYVAL_RW_PROPERTY(TArea*, Area);

    DEFINE_BYVAL_RW_PROPERTY(ECellLifeStage, CellLifeStage, ECellLifeStage::Running);

    using TGossipStatus = NCellMaster::TGossipValue<TCellStatus>;
    DEFINE_BYREF_RW_PROPERTY(TGossipStatus, GossipStatus);

    //! Last cell status reported during gossip.
    // NB: This field is intentionally transient.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TCellStatus>, LastGossipStatus);

    //! Overrides `peer_count` in cell bundle.
    DEFINE_BYREF_RW_PROPERTY(std::optional<int>, PeerCount);

    //! Last `peer_count` update time. Only for testing purposes.
    DEFINE_BYREF_RW_PROPERTY(TInstant, LastPeerCountUpdateTime);

    //! Last time when leader was changed.
    DEFINE_BYREF_RW_PROPERTY(TInstant, LastLeaderChangeTime);

    DEFINE_BYVAL_RW_PROPERTY(bool, Suspended);

    DEFINE_BYREF_RW_PROPERTY(THashSet<NTransactionServer::TTransactionId>, LeaseTransactionIds);

    DEFINE_BYVAL_RW_PROPERTY(bool, RegisteredInCypress);
    DEFINE_BYVAL_RW_PROPERTY(bool, PendingAclsUpdate);

    DEFINE_BYVAL_RW_PROPERTY(int, MaxSnapshotId, -1);
    DEFINE_BYVAL_RW_PROPERTY(int, MaxChangelogId, -1);

public:
    explicit TCellBase(TTamedCellId id);

    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);

    virtual NHiveClient::TCellDescriptor GetDescriptor() const = 0;
    virtual int GetDescriptorConfigVersion() const;

    virtual bool IsAlienPeer(int peerId) const;

    //! Computes the health from a point of view of a single master.
    virtual ECellHealth GetHealth() const;

    int FindPeerId(const TString& address) const;
    int GetPeerId(const TString& address) const;

    int FindPeerId(NNodeTrackerServer::TNode* node) const;
    int GetPeerId(NNodeTrackerServer::TNode* node) const;

    void AssignPeer(const NHiveClient::TCellPeerDescriptor& descriptor, int peerId);
    void RevokePeer(int peerId, const TError& reason);
    void ExpirePeerRevocationReasons(TInstant deadline);

    void AttachPeer(NNodeTrackerServer::TNode* node, int peerId);
    void DetachPeer(NNodeTrackerServer::TNode* node);
    void UpdatePeerSeenTime(int peerId, TInstant when);
    void UpdatePeerState(int peerId, EPeerState peerState);

    NNodeTrackerServer::TNode::TCellSlot* FindCellSlot(int peerId) const;

    NHydra::EPeerState GetPeerState(int peerId) const;

    //! If peers are independent peerId should be specified.
    //! If peers are not independent std::nullopt should be passed as peerId.
    NTransactionServer::TTransaction* GetPrerequisiteTransaction(std::optional<int> peerId) const;
    void SetPrerequisiteTransaction(std::optional<int> peerId, NTransactionServer::TTransaction* transaction);

    //! Returns |true| if the cell has a leading peer and is thus ready
    //! to serve mutations.
    bool IsHealthy() const;

    //! Get aggregated health for all masters.
    ECellHealth GetMulticellHealth() const;

    //! Recompute cluster statistics from multicell statistics.
    void RecomputeClusterStatus();

    //! Helper to calculate aggregated health.
    static ECellHealth CombineHealths(ECellHealth lhs, ECellHealth rhs);

    //! Returns |true| if decommission requested.
    bool IsDecommissionStarted() const;

    //! Returns |true| if cell reported that it is decommissioned.
    bool IsDecommissionCompleted() const;

    //! Retrns |true| if peers are independent.
    bool IsIndependent() const;

    NCellarClient::ECellarType GetCellarType() const;

    bool IsValidPeer(int peerId) const;

protected:
    ECellHealth GetCumulativeIndependentPeersHealth() const;
    ECellHealth GetCumulativeDependentPeersHealth() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer

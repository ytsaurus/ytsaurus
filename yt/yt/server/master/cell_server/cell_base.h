#pragma once

#include "public.h"

#include <yt/server/master/tablet_server/public.h>
#include <yt/server/master/tablet_server/tablet.h>

#include <yt/server/master/cell_master/public.h>
#include <yt/server/master/cell_master/gossip_value.h>

#include <yt/server/master/node_tracker_server/node.h>
#include <yt/server/master/node_tracker_server/public.h>

#include <yt/server/master/object_server/object.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>
#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/small_vector.h>

#include <yt/core/yson/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

struct TCellStatus
{
    ECellHealth Health;
    bool Decommissioned;

    void Persist(NCellMaster::TPersistenceContext& context);
};

void ToProto(NProto::TCellStatus* protoStatus, const TCellStatus& statistics);
void FromProto(TCellStatus* status, const NProto::TCellStatus& protoStatistics);

void Serialize(const TCellStatus& status, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TCellBase
    : public NObjectServer::TNonversionedObjectBase
    , public TRefTracked<TCellBase>
{
public:
    struct TPeer
    {
        NNodeTrackerClient::TNodeDescriptor Descriptor;
        NNodeTrackerServer::TNode* Node = nullptr;
        TInstant LastSeenTime;
        EPeerState LastSeenState = EPeerState::None;
        TError LastRevocationReason;

        void Persist(NCellMaster::TPersistenceContext& context);
    };

    using TPeerList = SmallVector<TPeer, TypicalPeerCount>;
    DEFINE_BYREF_RW_PROPERTY(TPeerList, Peers);
    DEFINE_BYVAL_RW_PROPERTY(int, LeadingPeerId);

    DEFINE_BYVAL_RW_PROPERTY(int, ConfigVersion);
    DEFINE_BYVAL_RW_PROPERTY(TTamedCellConfigPtr, Config);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, PrerequisiteTransaction);

    DEFINE_BYVAL_RW_PROPERTY(TCellBundle*, CellBundle);

    DEFINE_BYVAL_RW_PROPERTY(ECellLifeStage, CellLifeStage, ECellLifeStage::Running);

    using TGossipStatus = NCellMaster::TGossipValue<TCellStatus>;
    DEFINE_BYREF_RW_PROPERTY(TGossipStatus, GossipStatus);

    //! Overrides `peer_count` in cell bundle.
    DEFINE_BYREF_RW_PROPERTY(std::optional<int>, PeerCount);

    //! Last `peer_count` update time. Only for testing purposes.
    DEFINE_BYREF_RW_PROPERTY(TInstant, LastPeerCountUpdateTime);

public:
    explicit TCellBase(TTamedCellId id);

    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);

    TPeerId FindPeerId(const TString& address) const;
    TPeerId GetPeerId(const TString& address) const;

    TPeerId FindPeerId(NNodeTrackerServer::TNode* node) const;
    TPeerId GetPeerId(NNodeTrackerServer::TNode* node) const;

    void AssignPeer(const NHiveClient::TCellPeerDescriptor& descriptor, TPeerId peerId);
    void RevokePeer(TPeerId peerId, const TError& reason);
    void ExpirePeerRevocationReasons(TInstant deadline);

    void AttachPeer(NNodeTrackerServer::TNode* node, TPeerId peerId);
    void DetachPeer(NNodeTrackerServer::TNode* node);
    void UpdatePeerSeenTime(TPeerId peerId, TInstant when);
    void UpdatePeerState(TPeerId peerId, EPeerState peerState);

    NNodeTrackerServer::TNode::TCellSlot* FindCellSlot(TPeerId peerId) const;

    NHydra::EPeerState GetPeerState(TPeerId peerId) const;

    //! Get health from a point of view of a single master.
    ECellHealth GetHealth() const;

    //! Get aggregated health for all masters.
    ECellHealth GetMulticellHealth() const;

    NHiveClient::TCellDescriptor GetDescriptor() const;

    //! Recompute cluster statistics from multicell statistics.
    void RecomputeClusterStatus();

    //! Helper to calculate aggregated health.
    static ECellHealth CombineHealths(ECellHealth lhs, ECellHealth rhs);

    //! Returns |true| if decommission requested.
    bool IsDecommissionStarted() const;

    //! Returns |true| if cell reported that it is decommissioned.
    bool IsDecommissionCompleted() const;

protected:
    // COMPAT(savrus)
    THashSet<NTabletServer::TTablet*> CompatTablets_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer

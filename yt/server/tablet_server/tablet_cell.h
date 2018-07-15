#pragma once

#include "public.h"
#include "tablet.h"
#include "tablet_action.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/node_tracker_server/public.h>

#include <yt/server/object_server/object.h>

#include <yt/server/transaction_server/public.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>
#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/small_vector.h>

#include <yt/core/yson/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletCell
    : public NObjectServer::TNonversionedObjectBase
    , public TRefTracked<TTablet>
{
public:
    struct TPeer
    {
        NNodeTrackerClient::TNodeDescriptor Descriptor;
        NNodeTrackerServer::TNode* Node = nullptr;
        TInstant LastSeenTime;

        void Persist(NCellMaster::TPersistenceContext& context);
    };

    using TPeerList = SmallVector<TPeer, TypicalPeerCount>;
    DEFINE_BYREF_RW_PROPERTY(TPeerList, Peers);
    DEFINE_BYVAL_RW_PROPERTY(int, LeadingPeerId);

    DEFINE_BYVAL_RW_PROPERTY(int, ConfigVersion);
    DEFINE_BYVAL_RW_PROPERTY(TTabletCellConfigPtr, Config);

    DEFINE_BYREF_RW_PROPERTY(THashSet<TTablet*>, Tablets);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletAction*>, Actions);
    DEFINE_BYREF_RW_PROPERTY(TTabletCellStatistics, TotalStatistics);

    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransaction*, PrerequisiteTransaction);

    DEFINE_BYVAL_RW_PROPERTY(TTabletCellBundle*, CellBundle);

    DEFINE_BYVAL_RW_PROPERTY(bool, Decommissioned);

public:
    explicit TTabletCell(const TTabletCellId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    TPeerId FindPeerId(const TString& address) const;
    TPeerId GetPeerId(const TString& address) const;

    TPeerId FindPeerId(NNodeTrackerServer::TNode* node) const;
    TPeerId GetPeerId(NNodeTrackerServer::TNode* node) const;

    void AssignPeer(const NHiveClient::TCellPeerDescriptor& descriptor, TPeerId peerId);
    void RevokePeer(TPeerId peerId);

    void AttachPeer(NNodeTrackerServer::TNode* node, TPeerId peerId);
    void DetachPeer(NNodeTrackerServer::TNode* node);
    void UpdatePeerSeenTime(TPeerId peerId, TInstant when);

    ETabletCellHealth GetHealth() const;

    NHiveClient::TCellDescriptor GetDescriptor() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

#pragma once

#include "public.h"

#include <core/misc/ref_tracked.h>
#include <core/misc/property.h>
#include <core/misc/small_vector.h>
#include <core/misc/nullable.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <server/object_server/object.h>

#include <server/node_tracker_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletCell
    : public NObjectServer::TNonversionedObjectBase
    , public TRefTracked<TTablet>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, Size);

    struct TPeer
    {
        TPeer()
            : Node(nullptr)
            , SlotIndex(-1)
            , LastSeenTime(TInstant::Zero())
        { }

        TNullable<Stroka> Address;
        NNodeTrackerServer::TNode* Node;
        int SlotIndex;
        TInstant LastSeenTime;

        void Persist(NCellMaster::TPersistenceContext& context);

    };

    typedef SmallVector<TPeer, TypicalCellSize> TPeerList;
    DEFINE_BYREF_RW_PROPERTY(TPeerList, Peers);
    
    DEFINE_BYVAL_RW_PROPERTY(int, ConfigVersion);
    DEFINE_BYVAL_RW_PROPERTY(TTabletCellConfigPtr, Config);

    DEFINE_BYVAL_RW_PROPERTY(TTabletCellOptionsPtr, Options);

    DEFINE_BYREF_RW_PROPERTY(yhash_set<TTablet*>, Tablets);

public:
    explicit TTabletCell(const TTabletCellId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    TPeerId FindPeerId(const Stroka& address) const;
    TPeerId GetPeerId(const Stroka& address) const;

    TPeerId FindPeerId(NNodeTrackerServer::TNode* node) const;
    TPeerId GetPeerId(NNodeTrackerServer::TNode* node) const;

    void AssignPeer(const Stroka& address, TPeerId peerId);
    void RevokePeer(TPeerId peerId);

    void AttachPeer(NNodeTrackerServer::TNode* node, TPeerId peerId, int slotIndex);
    void DetachPeer(NNodeTrackerServer::TNode* node);
    void UpdatePeerSeenTime(TPeerId peerId, TInstant when);

    int GetOnlinePeerCount() const;

    ETabletCellHealth GetHealth() const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

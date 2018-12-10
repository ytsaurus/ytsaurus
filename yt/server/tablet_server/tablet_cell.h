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
#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/small_vector.h>

#include <yt/core/yson/public.h>

namespace NYT::NTabletServer {

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

    using TMulticellStatistics = THashMap<NObjectClient::TCellTag, TTabletCellStatistics>;
    DEFINE_BYREF_RW_PROPERTY(TTabletCellStatistics, ClusterStatistics);
    DEFINE_BYREF_RW_PROPERTY(TMulticellStatistics, MulticellStatistics);
    DEFINE_BYVAL_RW_PROPERTY(TTabletCellStatistics*, LocalStatisticsPtr);

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

    //! Get health from a point of view of a single master.
    ETabletCellHealth GetHealth() const;

    //! Get aggregated health for all masters.
    ETabletCellHealth GetMulticellHealth() const;

    NHiveClient::TCellDescriptor GetDescriptor() const;

    //! Dereferences the local statistics pointer.
    TTabletCellStatistics& LocalStatistics();
    const TTabletCellStatistics& LocalStatistics() const;

    //! Returns statistics for a given cell tag.
    TTabletCellStatistics* GetCellStatistics(NObjectClient::TCellTag cellTag);

    //! Recompute cluster statistics from multicell statistics.
    void RecomputeClusterStatistics();

    //! Helper to calculate aggregated health.
    static ETabletCellHealth CombineHealths(ETabletCellHealth lhs, ETabletCellHealth rhs);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

#pragma once

#include "public.h"
#include "tablet.h"
#include "tablet_action.h"

#include <yt/server/master/cell_master/public.h>
#include <yt/server/master/cell_master/gossip_value.h>

#include <yt/server/master/cell_server/cell_base.h>

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

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletCell
    : public NCellServer::TCellBase
{
public:
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTablet*>, Tablets);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletAction*>, Actions);

    using TGossipStatistics = NCellMaster::TGossipValue<TTabletCellStatistics>;
    DEFINE_BYREF_RW_PROPERTY(TGossipStatistics, GossipStatistics);

    DECLARE_BYVAL_RO_PROPERTY(TTabletCellBundle*, TabletCellBundle);

public:
    explicit TTabletCell(TTabletCellId id);

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    //! Recompute cluster statistics from multicell statistics.
    void RecomputeClusterStatistics();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

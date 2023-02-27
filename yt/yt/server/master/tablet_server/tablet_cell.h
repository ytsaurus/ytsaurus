#pragma once

#include "public.h"
#include "tablet_statistics.h"

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cell_master/gossip_value.h>

#include <yt/yt/server/master/cell_server/cell_base.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletCell
    : public NCellServer::TCellBase
{
public:
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletBase*>, Tablets);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletAction*>, Actions);

    using TGossipStatistics = NCellMaster::TGossipValue<TTabletCellStatistics>;
    DEFINE_BYREF_RW_PROPERTY(TGossipStatistics, GossipStatistics);

    DECLARE_BYVAL_RO_PROPERTY(TTabletCellBundle*, TabletCellBundle);

public:
    using TCellBase::TCellBase;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    NHiveClient::TCellDescriptor GetDescriptor() const override;

    //! Recompute cluster statistics from multicell statistics.
    void RecomputeClusterStatistics();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

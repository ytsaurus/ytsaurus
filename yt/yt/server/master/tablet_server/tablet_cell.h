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

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletCell
    : public NCellServer::TCellBase
{
public:
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletBaseRawPtr>, Tablets);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TTabletActionRawPtr>, Actions);

    using TGossipStatistics = NCellMaster::TGossipValue<TTabletCellStatistics>;
    DEFINE_BYREF_RW_PROPERTY(TGossipStatistics, GossipStatistics);

    DECLARE_BYVAL_RO_PROPERTY(TTabletCellBundleRawPtr, TabletCellBundle);

public:
    using TCellBase::TCellBase;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
    NYPath::TYPath GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    NHiveClient::TCellDescriptor GetDescriptor() const override;

    //! Recompute cluster statistics from multicell statistics.
    void RecomputeClusterStatistics();
};

DEFINE_MASTER_OBJECT_TYPE(TTabletCell)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer

#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>
#include <yt/yt/server/master/cell_master/gossip_value.h>

#include <yt/yt/server/master/cell_server/cell_base.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/misc/optional.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref_tracked.h>
#include <yt/yt/core/misc/small_vector.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TChaosCell
    : public NCellServer::TCellBase
{
public:
    DECLARE_BYVAL_RO_PROPERTY(TChaosCellBundle*, ChaosCellBundle);

public:
    using TCellBase::TCellBase;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    virtual NHiveClient::TCellDescriptor GetDescriptor() const override;

    virtual bool IsAlienPeer(int peerId) const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer

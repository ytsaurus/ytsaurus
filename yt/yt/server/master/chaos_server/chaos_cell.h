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

#include <yt/yt/core/misc/compact_flat_map.h>
#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NChaosServer {

////////////////////////////////////////////////////////////////////////////////

class TChaosCell
    : public NCellServer::TCellBase
{
public:
    DECLARE_BYVAL_RO_PROPERTY(TChaosCellBundle*, ChaosCellBundle);

    using TAlienClusterToConfigVersionMap = TCompactFlatMap<int, int, TypicalAlienPeerCount>;
    DEFINE_BYREF_RO_PROPERTY(TAlienClusterToConfigVersionMap, AlienConfigVersions);

public:
    using TCellBase::TCellBase;

    virtual void Save(NCellMaster::TSaveContext& context) const override;
    virtual void Load(NCellMaster::TLoadContext& context) override;

    virtual NHiveClient::TCellDescriptor GetDescriptor() const override;
    virtual int GetDescriptorConfigVersion() const override;

    virtual bool IsAlienPeer(TPeerId peerId) const override;

    void UpdateAlienPeer(TPeerId peerId, const NNodeTrackerClient::TNodeDescriptor& descriptor);

    int GetAlienConfigVersion(int alienClusterIndex) const;
    void SetAlienConfigVersion(int alienClusterIndex, int version);

private:
    int CumulativeAlienConfigVersion_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer

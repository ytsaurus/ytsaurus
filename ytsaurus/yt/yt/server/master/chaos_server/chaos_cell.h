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

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/small_containers/compact_flat_map.h>

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

    void Save(NCellMaster::TSaveContext& context) const override;
    void Load(NCellMaster::TLoadContext& context) override;

    NHiveClient::TCellDescriptor GetDescriptor() const override;
    int GetDescriptorConfigVersion() const override;

    bool IsAlienPeer(TPeerId peerId) const override;
    NCellServer::ECellHealth GetHealth() const override;

    void UpdateAlienPeer(TPeerId peerId, const NNodeTrackerClient::TNodeDescriptor& descriptor);

    int GetAlienConfigVersion(int alienClusterIndex) const;
    void SetAlienConfigVersion(int alienClusterIndex, int version);

    const TChaosHydraConfigPtr& GetChaosOptions() const;

private:
    int CumulativeAlienConfigVersion_ = 0;

    bool IsAlienCell() const;
};

DEFINE_MASTER_OBJECT_TYPE(TChaosCell)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer

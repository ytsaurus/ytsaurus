#pragma once
#include "public.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NApi {

using NRpc::IChannelFactoryPtr;
using NRpc::IChannelPtr;

using NHiveClient::TCellDescriptor;
using NHiveClient::TCellPeerDescriptor;

using NHydra::EPeerKind;

using NNodeTrackerClient::TNetworkPreferenceList;

using NTabletClient::TTableMountInfoPtr;
using NTabletClient::TTabletInfoPtr;

////////////////////////////////////////////////////////////////////////////////

SmallVector<const TCellPeerDescriptor*, 3> GetValidPeers(const TCellDescriptor& cellDescriptor);

const TCellPeerDescriptor& GetPrimaryTabletPeerDescriptor(
    const TCellDescriptor& cellDescriptor,
    EPeerKind peerKind = EPeerKind::Leader);

const TCellPeerDescriptor& GetBackupTabletPeerDescriptor(
    const TCellDescriptor& cellDescriptor,
    const TCellPeerDescriptor& primaryPeerDescriptor);

IChannelPtr CreateTabletReadChannel(
    const IChannelFactoryPtr& channelFactory,
    const TCellDescriptor& cellDescriptor,
    const TTabletReadOptions& options,
    const TNetworkPreferenceList& networks);

void ValidateTabletMountedOrFrozen(const TTableMountInfoPtr& tableInfo, const TTabletInfoPtr& tabletInfo);

void ValidateTabletMounted(const TTableMountInfoPtr& tableInfo, const TTabletInfoPtr& tabletInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
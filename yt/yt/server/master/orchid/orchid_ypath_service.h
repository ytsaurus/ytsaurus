#pragma once

#include "manifest.h"

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NOrchid {

////////////////////////////////////////////////////////////////////////////////

NYTree::IYPathServicePtr CreateOrchidYPathService(
    NNodeTrackerClient::INodeChannelFactoryPtr nodeChannelFactory,
    NYTree::INodePtr owningNode);

NYTree::IYPathServicePtr CreateOrchidYPathService(
    NNodeTrackerClient::INodeChannelFactoryPtr nodeChannelFactory,
    TOrchidManifestPtr manifest);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrchid

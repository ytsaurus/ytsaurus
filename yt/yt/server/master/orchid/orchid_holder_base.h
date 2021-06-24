#pragma once

#include "manifest.h"
#include "orchid_ypath_service.h"

#include <yt/yt/core/ytree/static_service_dispatcher.h>

namespace NYT::NOrchid {

////////////////////////////////////////////////////////////////////////////////

class TOrchidHolderBase
    : public virtual NYTree::TStaticServiceDispatcher
{
public:
    TOrchidHolderBase(
        NNodeTrackerClient::INodeChannelFactoryPtr channelFactory,
        TCallback<TOrchidManifestPtr()> manifestFactory)
        : ChannelFactory_(std::move(channelFactory))
    {
        RegisterService(
            "orchid",
            BIND([this, manifestFactory = std::move(manifestFactory)] {
                return CreateOrchidYPathService(ChannelFactory_, manifestFactory());
            }));
    }

private:
    const NNodeTrackerClient::INodeChannelFactoryPtr ChannelFactory_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrchid

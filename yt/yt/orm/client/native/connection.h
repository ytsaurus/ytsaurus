#pragma once

#include "public.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/orm/client/native/peer_discovery.h>

#include <util/datetime/base.h>

#include <optional>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

struct IConnection
    : public virtual TRefCounted
{
    virtual TDuration GetRequestTimeout() const = 0;

    virtual IOrmPeerDiscoveryPtr GetPeerDiscovery() const = 0;

    virtual NRpc::IChannelPtr GetChannel(bool retrying) = 0;

    virtual NRpc::IChannelPtr GetChannel(TMasterInstanceTag instanceTag, bool retrying) = 0;
};

DEFINE_REFCOUNTED_TYPE(IConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative

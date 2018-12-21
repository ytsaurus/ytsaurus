#pragma once

#include "public.h"
#include "channel.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct ICachingChannelFactory
    : public virtual IChannelFactory
{
    virtual void TerminateIdleChannels(TDuration ttl) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICachingChannelFactory)

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel factory that wraps another channel factory
//! and caches its channels by address.
ICachingChannelFactoryPtr CreateCachingChannelFactory(IChannelFactoryPtr underlyingFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

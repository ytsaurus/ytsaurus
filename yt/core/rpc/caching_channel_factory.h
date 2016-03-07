#pragma once

#include "public.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel factory that wraps another channel factory
//! and caches its channels by address.
IChannelFactoryPtr CreateCachingChannelFactory(IChannelFactoryPtr underlyingFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

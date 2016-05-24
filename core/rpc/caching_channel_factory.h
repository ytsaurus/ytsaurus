#pragma once

#include "public.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Creates a channel factory that wraps another channel factory
//! and caches its channels by address.
/*!
 *  The resulting channels (returned from the caching factory)
 *  share the underlying transport channel but still allow timeouts
 *  to be configured individually.
 */
IChannelFactoryPtr CreateCachingChannelFactory(IChannelFactoryPtr underlyingFactory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

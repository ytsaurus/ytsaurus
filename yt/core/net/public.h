#pragma once

#include <yt/core/misc/intrusive_ptr.h>

namespace NYT {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

class TNetworkAddress;
class TIP6Address;

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IListener)
DECLARE_REFCOUNTED_STRUCT(IDialer)
DECLARE_REFCOUNTED_STRUCT(IAsyncDialer)
DECLARE_REFCOUNTED_STRUCT(IAsyncDialerSession)

DECLARE_REFCOUNTED_CLASS(TDialerConfig)
DECLARE_REFCOUNTED_CLASS(TAddressResolverConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT

#pragma once

#include <yt/core/misc/public.h>

#include <yt/core/misc/intrusive_ptr.h>

namespace NYT {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

class TNetworkAddress;
class TIP6Address;
class TIP6Network;

DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IPacketConnection)
DECLARE_REFCOUNTED_STRUCT(IConnectionReader)
DECLARE_REFCOUNTED_STRUCT(IConnectionWriter)
DECLARE_REFCOUNTED_STRUCT(IListener)
DECLARE_REFCOUNTED_STRUCT(IDialer)
DECLARE_REFCOUNTED_STRUCT(IAsyncDialer)
DECLARE_REFCOUNTED_STRUCT(IAsyncDialerSession)

DECLARE_REFCOUNTED_CLASS(TDialerConfig)
DECLARE_REFCOUNTED_CLASS(TAddressResolverConfig)

DEFINE_ENUM(EErrorCode,
    ((Aborted)         (1500))
    ((ResolveTimedOut) (1501))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NYT

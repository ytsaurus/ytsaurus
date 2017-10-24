#pragma once

#include <yt/core/misc/public.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TRpcProxyConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TRpcProxyClientConfig)

DECLARE_REFCOUNTED_CLASS(TRpcProxyConnection)
DECLARE_REFCOUNTED_CLASS(TRpcProxyClientBase)
DECLARE_REFCOUNTED_CLASS(TRpcProxyClient)
DECLARE_REFCOUNTED_CLASS(TRpcProxyTransaction)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT

#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TConnection)
DECLARE_REFCOUNTED_CLASS(TClientBase)
DECLARE_REFCOUNTED_CLASS(TClient)
DECLARE_REFCOUNTED_CLASS(TTransaction)

inline const NLogging::TLogger RpcProxyClientLogger("RpcProxyClient");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger RpcProxyStructuredLoggerMain("RpcProxyStructuredMain");
inline const NLogging::TLogger RpcProxyStructuredLoggerError("RpcProxyStructuredError");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy

#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

constexpr auto RpcProxyUserAllocationTag = "user";
constexpr auto RpcProxyRequestIdAllocationTag = "request_id";
constexpr auto RpcProxyRpcAllocationTag = "rpc";

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger RpcProxyStructuredLoggerMain("RpcProxyStructuredMain");
inline const NLogging::TLogger RpcProxyStructuredLoggerError("RpcProxyStructuredError");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy

#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

constexpr auto RpcProxyUserAllocationTagKey = "user";
constexpr auto RpcProxyRequestIdAllocationTagKey = "request_id";
constexpr auto RpcProxyMethodAllocationTagKey = "rpc";

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, RpcProxyStructuredLoggerMain, "RpcProxyStructuredMain");
YT_DEFINE_GLOBAL(const NLogging::TLogger, RpcProxyStructuredLoggerError, "RpcProxyStructuredError");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy

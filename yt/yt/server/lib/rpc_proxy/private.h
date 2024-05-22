#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

constexpr auto RpcProxyUserAllocationTag = "user";
constexpr auto RpcProxyRequestIdAllocationTag = "request_id";
constexpr auto RpcProxyRpcAllocationTag = "rpc";

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, RpcProxyStructuredLoggerMain, "RpcProxyStructuredMain");
YT_DEFINE_GLOBAL(const NLogging::TLogger, RpcProxyStructuredLoggerError, "RpcProxyStructuredError");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy

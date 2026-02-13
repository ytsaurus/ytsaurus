#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

constexpr auto RpcProxyUserAllocationTagKey = "user";
constexpr auto RpcProxyRequestIdAllocationTagKey = "request_id";
constexpr auto RpcProxyMethodAllocationTagKey = "rpc";

////////////////////////////////////////////////////////////////////////////////

inline const std::string DiscoveryExecutionPoolName = "$discovery";
inline const std::string ShuffleExecutionPoolName = "$shuffle";
inline const std::string ConnectionExecutionPoolName = "$connection";
inline const std::string DefaultApiExecutionPoolName = "$api";
inline const std::string ControlExecutionPoolName = "$control";
inline const std::string OrchidExecutionPoolName = "$orchid";

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, RpcProxyStructuredLoggerMain, "RpcProxyStructuredMain");
YT_DEFINE_GLOBAL(const NLogging::TLogger, RpcProxyStructuredLoggerError, "RpcProxyStructuredError");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy

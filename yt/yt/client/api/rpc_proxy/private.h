#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TConnection)
DECLARE_REFCOUNTED_CLASS(TClientBase)
DECLARE_REFCOUNTED_CLASS(TClient)
DECLARE_REFCOUNTED_CLASS(TTransaction)

extern const NLogging::TLogger RpcProxyClientLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy

#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

//! Custom #stickyTransactionPool is useful for sharing transactions
//! between services (e.g.: ORM and RPC proxy).
NRpc::IServicePtr CreateApiService(
    IBootstrap* bootstrap,
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    NApi::IStickyTransactionPoolPtr stickyTransactionPool = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy

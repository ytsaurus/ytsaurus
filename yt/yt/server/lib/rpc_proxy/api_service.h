#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

struct IApiService
    : public virtual NRpc::IService
{
    //! Thread affinity: any.
    virtual void OnDynamicConfigChanged(const TApiServiceDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IApiService)

//! Custom #stickyTransactionPool is useful for sharing transactions
//! between services (e.g.: ORM and RPC proxy).
IApiServicePtr CreateApiService(
    IBootstrap* bootstrap,
    NLogging::TLogger logger,
    TApiServiceConfigPtr config,
    TApiServiceDynamicConfigPtr dynamicConfig,
    NProfiling::TProfiler profiler,
    NApi::IStickyTransactionPoolPtr stickyTransactionPool = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy

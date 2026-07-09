#pragma once

#include "public.h"

#include <yt/yt/client/api/delegating_client.h>

#include <yt/yt/flow/library/cpp/misc/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class IRetryableClient
    : public NApi::TDelegatingClient
{
public:
    using NApi::TDelegatingClient::TDelegatingClient;

    virtual void Reconfigure(const TDynamicRetryableClientSpecPtr& dynamicSpec) = 0;

    // Result child client uses parent configuration, so it cannot be reconfigured.
    virtual IRetryableClientPtr WithErrorComponent(const std::string& component) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRetryableClient)

////////////////////////////////////////////////////////////////////////////////

//! Creates retryable reconfigurable client. All arguments are required (must not be null).
IRetryableClientPtr CreateRetryableClient(
    const NApi::IClientPtr& underlyingClient,
    const IInvokerPtr& invoker,
    const IStatusProfilerPtr& statusProfiler,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

bool IsFlowRetriableError(const TError& error, bool retryProxyBanned = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

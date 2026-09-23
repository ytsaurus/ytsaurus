#pragma once

#include "public.h"
#include "connection.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/auth_server/public.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

struct IRequestHandler
    : public TRefCounted
{
    virtual void OnDynamicConfigChanged(const TProxyDynamicConfigPtr& config) = 0;

    virtual TMessage Handle(
        TConnectionStatePtr connectionState,
        const TMessage& request,
        const NLogging::TLogger& Logger) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRequestHandler);

////////////////////////////////////////////////////////////////////////////////

IRequestHandlerPtr CreateRequestHandler(
    TProxyBootstrapConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NAuth::IAuthenticationManagerPtr authenticationManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy

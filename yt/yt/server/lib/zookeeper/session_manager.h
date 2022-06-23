#pragma once

#include "public.h"

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/net/public.h>

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

struct ISessionManager
    : public TRefCounted
{
    virtual void OnConnectionAccepted(const NNet::IConnectionPtr& connection) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISessionManager)

////////////////////////////////////////////////////////////////////////////////

ISessionManagerPtr CreateSessionManager(
    IDriverPtr driver,
    NConcurrency::IPollerPtr poller,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper

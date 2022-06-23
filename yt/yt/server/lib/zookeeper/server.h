#pragma once

#include "public.h"

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/net/public.h>

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public TRefCounted
{
    virtual void Start() = 0;

    DECLARE_INTERFACE_SIGNAL(void(const NNet::IConnectionPtr&), ConnectionAccepted);
};

DEFINE_REFCOUNTED_TYPE(IServer)

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    const NConcurrency::IPollerPtr& poller,
    int port);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper

#pragma once

#include "public.h"

#include <yt/yt/client/kafka/requests.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/public.h>

#include <library/cpp/yt/misc/guid.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public TRefCounted
{
    //! Starts the server.
    virtual void Start() = 0;
};

DEFINE_REFCOUNTED_TYPE(IServer);

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TProxyBootstrapConfigPtr config,
    NConcurrency::IPollerPtr poller,
    NConcurrency::IPollerPtr acceptor,
    IRequestHandlerPtr requestHandler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy

#pragma once

#include "public.h"

#include "connection.h"

#include <yt/yt/client/zookeeper/requests.h>

#include <yt/yt/core/actions/signal.h>

#include <library/cpp/yt/misc/guid.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NZookeeperProxy {

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public TRefCounted
{
    //! Starts the server.
    virtual void Start() = 0;

    //! Represents abstract Zookeeper request handler.
    using THandler = TCallback<TSharedRefArray(const TSharedRefArray&)>;
    virtual void RegisterHandler(
        NZookeeper::ERequestType requestType,
        THandler handler) = 0;

    //! Represents Zookeeper request handler.
    template <class TRequest, class TResponse>
    using TTypedHandler = TCallback<TResponse(const TRequest&)>;
    template <class TRequest, class TResponse>
    void RegisterTypedHandler(TTypedHandler<TRequest, TResponse> handler);

    //! Start session request is quite special, so it is registered using
    //! separate method.
    using TStartSessionHandler = TTypedHandler<
        NZookeeper::TReqStartSession, NZookeeper::TRspStartSession
    >;
    virtual void RegisterStartSessionHandler(TStartSessionHandler handler) = 0;
};

DEFINE_REFCOUNTED_TYPE(IServer)

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TZookeeperServerConfigPtr config,
    NConcurrency::IPollerPtr poller,
    NConcurrency::IPollerPtr acceptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperProxy

#define SERVER_INL_H_
#include "server-inl.h"
#undef SERVER_INL_H_

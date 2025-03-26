#pragma once

#include "connection.h"
#include "public.h"

#include <yt/yt/ytlib/api/native/connection.h>

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

    //! Represents abstract Kafka request handler.
    using THandler = TCallback<TSharedRef(TConnectionId, NKafka::IKafkaProtocolReader*, const NKafka::TRequestHeader&)>;
    virtual void RegisterHandler(
        NKafka::ERequestType requestType,
        THandler handler) = 0;

    //! Represents Kafka request handler.
    template <class TRequest, class TResponse>
    using TTypedHandler = TCallback<TResponse(TConnectionId, const TRequest&, const NLogging::TLogger&)>;
    template <class TRequest, class TResponse>
    void RegisterTypedHandler(TTypedHandler<TRequest, TResponse> handler);

    // Update Kafka proxy dynamic config.
    virtual void OnDynamicConfigChanged(const TProxyDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IServer);

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TProxyBootstrapConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NAuth::IAuthenticationManagerPtr authenticationManager,
    NConcurrency::IPollerPtr poller,
    NConcurrency::IPollerPtr acceptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy

#define SERVER_INL_H_
#include "server-inl.h"
#undef SERVER_INL_H_

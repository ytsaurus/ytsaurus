#pragma once

#include <ytlib/misc/guid.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IClientRequest;
typedef TIntrusivePtr<IClientRequest> IClientRequestPtr;

class TClientRequest;

struct IClientResponseHandler;
typedef TIntrusivePtr<IClientResponseHandler> IClientResponseHandlerPtr;

template <class TRequestMessage, class TResponse>
class TTypedClientRequest;

class TClientResponse;

template <class TResponseMessage>
class TTypedClientResponse;

class TOneWayClientResponse;
typedef TIntrusivePtr<TOneWayClientResponse> TOneWayClientResponsePtr;

struct TRetryConfig;
typedef TIntrusivePtr<TRetryConfig> TRetryConfigPtr;

class TRetryingChannel;
typedef TIntrusivePtr<TRetryingChannel> TRetriableChannelPtr;

struct IServer;
typedef TIntrusivePtr<IServer> IServerPtr;

struct IService;
typedef TIntrusivePtr<IService> IServicePtr;

struct IServiceContext;
typedef TIntrusivePtr<IServiceContext> IServiceContextPtr;

struct IChannel;
typedef TIntrusivePtr<IChannel> IChannelPtr;

class TChannelCache;

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TRequestId;
extern TRequestId NullRequestId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

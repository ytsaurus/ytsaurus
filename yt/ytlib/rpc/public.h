#pragma once

#include <ytlib/misc/guid.h>
#include <ytlib/misc/error.h>

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

struct TRetryingChannelConfig;
typedef TIntrusivePtr<TRetryingChannelConfig> TRetryingChannelConfigPtr;

class TRetryingChannel;
typedef TIntrusivePtr<TRetryingChannel> TRetryingChannelPtr;

struct IServer;
typedef TIntrusivePtr<IServer> IServerPtr;

struct IService;
typedef TIntrusivePtr<IService> IServicePtr;

struct IServiceContext;
typedef TIntrusivePtr<IServiceContext> IServiceContextPtr;

struct IChannel;
typedef TIntrusivePtr<IChannel> IChannelPtr;

class TChannelCache;

class TServiceBase;
typedef TIntrusivePtr<TServiceBase> TServiceBasePtr;

typedef TGuid TRequestId;
extern TRequestId NullRequestId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EErrorCode,
    ((TransportError)  (1))
    ((ProtocolError)   (2))
    ((NoSuchService)   (3))
    ((NoSuchVerb)      (4))
    ((Timeout)         (5))
    ((Unavailable)     (6))
    ((PoisonPill)      (7))
);

// TODO(babenko): obsolete
bool IsRpcError(const TError& error);
bool IsRetriableError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

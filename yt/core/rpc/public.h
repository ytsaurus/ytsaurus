#pragma once

#include <core/misc/guid.h>
#include <core/misc/error.h>

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

class TRetryingChannelConfig;
typedef TIntrusivePtr<TRetryingChannelConfig> TRetryingChannelConfigPtr;

class TThrottlingChannelConfig;
typedef TIntrusivePtr<TThrottlingChannelConfig> TThrottlingChannelConfigPtr;

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

class TServerConfig;
typedef TIntrusivePtr<TServerConfig> TServerConfigPtr;

class TServiceConfig;
typedef TIntrusivePtr<TServiceConfig> TServiceConfigPtr;

class TMethodConfig;
typedef TIntrusivePtr<TMethodConfig> TMethodConfigPtr;

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TRequestId;
extern const TRequestId NullRequestId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EErrorCode,
    ((TransportError)  (100))
    ((ProtocolError)   (101))
    ((NoSuchService)   (102))
    ((NoSuchVerb)      (103))
    ((Timeout)         (104))
    ((Unavailable)     (105))
    ((PoisonPill)      (106))
);

bool IsRetriableError(const TError& error);
bool IsChannelFailureError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

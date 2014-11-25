#pragma once

#include <core/misc/guid.h>
#include <core/misc/error.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TClientRequest;

template <class TRequestMessage, class TResponse>
class TTypedClientRequest;

class TClientResponse;

template <class TResponseMessage>
class TTypedClientResponse;

struct TServiceId;

DECLARE_REFCOUNTED_STRUCT(IClientRequest)
DECLARE_REFCOUNTED_STRUCT(IClientResponseHandler)
DECLARE_REFCOUNTED_STRUCT(IServer)
DECLARE_REFCOUNTED_STRUCT(IService)
DECLARE_REFCOUNTED_STRUCT(IServiceContext)
DECLARE_REFCOUNTED_STRUCT(IChannel)
DECLARE_REFCOUNTED_STRUCT(IChannelFactory)
DECLARE_REFCOUNTED_STRUCT(IRoamingChannelProvider)

DECLARE_REFCOUNTED_CLASS(TClientContext)
DECLARE_REFCOUNTED_CLASS(TServiceBase)
DECLARE_REFCOUNTED_CLASS(TOneWayClientResponse)
DECLARE_REFCOUNTED_CLASS(TStaticChannelFactory)


DECLARE_REFCOUNTED_STRUCT(IResponseKeeper)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TServerConfig)
DECLARE_REFCOUNTED_CLASS(TServiceConfig)
DECLARE_REFCOUNTED_CLASS(TMethodConfig)
DECLARE_REFCOUNTED_CLASS(TRetryingChannelConfig)
DECLARE_REFCOUNTED_CLASS(TBalancingChannelConfig)
DECLARE_REFCOUNTED_CLASS(TThrottlingChannelConfig)
DECLARE_REFCOUNTED_CLASS(TResponseKeeperConfig)

////////////////////////////////////////////////////////////////////////////////

typedef TGuid TRequestId;
extern const TRequestId NullRequestId;

typedef TGuid TRealmId;
extern const TRealmId NullRealmId;

typedef TGuid TMutationId;
extern const TMutationId NullMutationId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EErrorCode,
    ((TransportError)  (100))
    ((ProtocolError)   (101))
    ((NoSuchService)   (102))
    ((NoSuchMethod)    (103))
    ((Timeout)         (NYT::EErrorCode::Timeout))
    ((Unavailable)     (105))
    ((PoisonPill)      (106))
);

bool IsRetriableError(const TError& error);
bool IsChannelFailureError(const TError& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

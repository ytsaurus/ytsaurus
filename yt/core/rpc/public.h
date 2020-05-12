#pragma once

#include <yt/core/misc/guid.h>

#include <yt/core/bus/public.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqDiscover;
class TRspDiscover;
class TRequestHeader;
class TResponseHeader;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TStreamingParameters;
struct TStreamingPayload;
struct TStreamingFeedback;

struct TServiceDescriptor;
struct TMethodDescriptor;

class TClientRequest;

template <class TRequestMessage, class TResponse>
class TTypedClientRequest;

class TClientResponse;

template <class TResponseMessage>
class TTypedClientResponse;

template <class TRequestMessage, class TResponseMessage>
class TTypedServiceContext;

struct TServiceId;

struct TAuthenticationResult;

DECLARE_REFCOUNTED_STRUCT(IClientRequest)
DECLARE_REFCOUNTED_STRUCT(IClientRequestControl)
DECLARE_REFCOUNTED_STRUCT(IClientResponseHandler)
DECLARE_REFCOUNTED_STRUCT(IServer)
DECLARE_REFCOUNTED_STRUCT(IService)
DECLARE_REFCOUNTED_STRUCT(IServiceWithReflection)
DECLARE_REFCOUNTED_STRUCT(IServiceContext)
DECLARE_REFCOUNTED_STRUCT(IChannel)
DECLARE_REFCOUNTED_STRUCT(IChannelFactory)
DECLARE_REFCOUNTED_STRUCT(IRoamingChannelProvider)
DECLARE_REFCOUNTED_STRUCT(IAuthenticator)

DECLARE_REFCOUNTED_CLASS(TClientContext)
DECLARE_REFCOUNTED_CLASS(TServiceBase)
DECLARE_REFCOUNTED_CLASS(TChannelWrapper)
DECLARE_REFCOUNTED_CLASS(TStaticChannelFactory)
DECLARE_REFCOUNTED_CLASS(TClientRequestControlThunk)
DECLARE_REFCOUNTED_CLASS(TCachingChannelFactory)

DECLARE_REFCOUNTED_CLASS(TResponseKeeper)

DECLARE_REFCOUNTED_CLASS(TAttachmentsInputStream)
DECLARE_REFCOUNTED_CLASS(TAttachmentsOutputStream)

DECLARE_REFCOUNTED_CLASS(TServerAddressPool)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TServerConfig)
DECLARE_REFCOUNTED_CLASS(TServiceConfig)
DECLARE_REFCOUNTED_CLASS(TMethodConfig)
DECLARE_REFCOUNTED_CLASS(TRetryingChannelConfig)
DECLARE_REFCOUNTED_CLASS(TBalancingChannelConfig)
DECLARE_REFCOUNTED_CLASS(TThrottlingChannelConfig)
DECLARE_REFCOUNTED_CLASS(TResponseKeeperConfig)
DECLARE_REFCOUNTED_CLASS(TMultiplexingBandConfig)
DECLARE_REFCOUNTED_CLASS(TDispatcherConfig)

////////////////////////////////////////////////////////////////////////////////

using TRequestId = TGuid;
extern const TRequestId NullRequestId;

using TRealmId = TGuid;
extern const TRealmId NullRealmId;

using TMutationId = TGuid;
extern const TMutationId NullMutationId;

extern const TString RootUserName;

using TNetworkId = int;
constexpr TNetworkId DefaultNetworkId = 0;

constexpr int TypicalMessagePartCount = 8;

extern const TString RequestIdAnnotation;
extern const TString EndpointAnnotation;
extern const TString RequestInfoAnnotation;
extern const TString ResponseInfoAnnotation;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMultiplexingBand,
    ((Default)               (0))
    ((Control)               (1))
    ((Heavy)                 (2))
);

DEFINE_ENUM(EErrorCode,
    ((TransportError)               (static_cast<int>(NBus::EErrorCode::TransportError)))
    ((ProtocolError)                (101))
    ((NoSuchService)                (102))
    ((NoSuchMethod)                 (103))
    ((Unavailable)                  (105))
    ((PoisonPill)                   (106))
    ((RequestQueueSizeLimitExceeded)(108))
    ((AuthenticationError)          (109))
    ((InvalidCsrfToken)             (110))
    ((InvalidCredentials)           (111))
    ((StreamingNotSupported)        (112))
);

DEFINE_ENUM(EMessageFormat,
    ((Protobuf)    (0))
    ((Json)        (1))
    ((Yson)        (2))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

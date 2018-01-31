#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/ref.h>

#include <yt/core/rpc/public.h>
#include <yt/core/rpc/proto/rpc.pb.h>

#include <yt/core/tracing/trace_context.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableError(const TError& error);
bool IsChannelFailureError(const TError& error);

//! Returns a wrapper that sets the timeout for every request (unless it is given
//! explicitly in the request itself).
IChannelPtr CreateDefaultTimeoutChannel(
    IChannelPtr underlyingChannel,
    TDuration timeout);
IChannelFactoryPtr CreateDefaultTimeoutChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    TDuration timeout);

//! Returns a wrapper that sets "authenticated_user" attribute in every request.
IChannelPtr CreateAuthenticatedChannel(
    IChannelPtr underlyingChannel,
    const TString& user);
IChannelFactoryPtr CreateAuthenticatedChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    const TString& user);

//! Returns a wrapper that sets realm id in every request.
IChannelPtr CreateRealmChannel(
    IChannelPtr underlyingChannel,
    const TRealmId& realmId);
IChannelFactoryPtr CreateRealmChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    const TRealmId& realmId);

//! Returns a wrapper that informs about channel failures.
/*!
 *  Channel failures are being detected via NRpc::IsChannelFailureError.
 */
IChannelPtr CreateFailureDetectingChannel(
    IChannelPtr underlyingChannel,
    TCallback<void(IChannelPtr)> onFailure);

//! Returns the trace context associated with the request.
//! If no trace context is attached, returns a disabled context.
NTracing::TTraceContext GetTraceContext(const NProto::TRequestHeader& header);

//! Attaches a given trace context to the request.
void SetTraceContext(
    NProto::TRequestHeader* header,
    const NTracing::TTraceContext& context);

//! Generates a random mutation id.
TMutationId GenerateMutationId();

void GenerateMutationId(const IClientRequestPtr& request);
void SetMutationId(NProto::TRequestHeader* header, const TMutationId& id, bool retry);
void SetMutationId(const IClientRequestPtr& request, const TMutationId& id, bool retry);
void SetOrGenerateMutationId(const IClientRequestPtr& request, const TMutationId& id, bool retry);

////////////////////////////////////////////////////////////////////////////////

struct IMessageFormat
{
    virtual TSharedRef ConvertFrom(const TSharedRef& message, const NYson::TProtobufMessageType* messageType) = 0;
    virtual TSharedRef ConvertTo(const TSharedRef& message, const NYson::TProtobufMessageType* messageType) = 0;
};

void RegisterCustomMessageFormat(EMessageFormat format, IMessageFormat* formatHandler);

TSharedRef ConvertMessageToFormat(const TSharedRef& message, EMessageFormat format, const NYson::TProtobufMessageType* messageType);
TSharedRef ConvertMessageFromFormat(const TSharedRef& message, EMessageFormat format, const NYson::TProtobufMessageType* messageType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

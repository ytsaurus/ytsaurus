#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/rpc/public.h>
#include <core/rpc/rpc.pb.h>

#include <core/tracing/trace_context.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Returns a wrapper that sets "authenticated_user" attribute in every request.
IChannelPtr CreateAuthenticatedChannel(
    IChannelPtr underlyingChannel,
    const Stroka& user);

//! Returns a wrapper that sets "authenticated_user" attribute in every request
//! for every created channel.
IChannelFactoryPtr CreateAuthenticatedChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    const Stroka& user);

//! Returns a wrapper that sets realm id in every request.
IChannelPtr CreateRealmChannel(
    IChannelPtr underlyingChannel,
    const TRealmId& realmId);

//! Returns a wrapper that sets realm id in every request for every created channel.
IChannelFactoryPtr CreateRealmChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    const TRealmId& realmId);

//! Returns the trace context associated with the request.
//! If no trace context is attached, returns a disabled context.
NTracing::TTraceContext GetTraceContext(const NProto::TRequestHeader& header);

//! Attaches a given trace context to the request.
void SetTraceContext(
    NProto::TRequestHeader* header,
    const NTracing::TTraceContext& context);

//! Generates a random mutation id.
TMutationId GenerateMutationId();

//! Returns the mutation id associated with the context.
TMutationId GetMutationId(IServiceContextPtr context);

//! Returns the mutation id associated with the request.
TMutationId GetMutationId(const NProto::TRequestHeader& header);

void GenerateMutationId(IClientRequestPtr request);
void SetMutationId(NProto::TRequestHeader* header, const TMutationId& id, bool retry);
void SetMutationId(IClientRequestPtr request, const TMutationId& id, bool retry);
void SetOrGenerateMutationId(IClientRequestPtr request, const TMutationId& id, bool retry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/rpc/public.h>
#include <core/rpc/rpc.pb.h>

#include <core/tracing/trace_context.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Sets "authenticated_user" in header.
void SetAuthenticatedUser(NProto::TRequestHeader* header, const Stroka& user);

//! Sets "authenticated_user" in header.
void SetAuthenticatedUser(IClientRequestPtr request, const Stroka& user);

//! Returns the value of "authenticated_user" from header or |Null| if missing.
TNullable<Stroka> FindAuthenticatedUser(const NProto::TRequestHeader& header);

//! Returns the value of "authenticated_user" from header or |Null| if missing.
TNullable<Stroka> FindAuthenticatedUser(IServiceContextPtr context);

//! Returns the value of "authenticated_user" from header. Throws if missing.
Stroka GetAuthenticatedUserOrThrow(IServiceContextPtr context);

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

// TODO(babenko): write doc
TMutationId GenerateMutationId();

TMutationId GetMutationId(IServiceContextPtr context);
TMutationId GetMutationId(const NProto::TRequestHeader& header);

void GenerateMutationId(IClientRequestPtr request);
void SetMutationId(NProto::TRequestHeader* header, const TMutationId& id);
void SetMutationId(IClientRequestPtr request, const TMutationId& id);
void SetOrGenerateMutationId(IClientRequestPtr request, const TMutationId& id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

#include <yt/core/compression/public.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/ref.h>

#include <yt/core/rpc/public.h>
#include <yt/core/rpc/proto/rpc.pb.h>

#include <yt/core/actions/bind.h>

#include <yt/core/tracing/trace_context.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct TAddressWithNetwork
{
    TString Address;
    TString Network;
};

bool operator==(const TAddressWithNetwork& lhs, const TAddressWithNetwork& rhs);

TString ToString(const TAddressWithNetwork& addressWithNetwork);

void Serialize(const TAddressWithNetwork& addressWithNetwork, NYson::IYsonConsumer* consumer);

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
    TRealmId realmId);
IChannelFactoryPtr CreateRealmChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    TRealmId realmId);

//! Returns a wrapper that informs about channel failures.
/*!
 *  Channel failures are being detected via provided filter.
 */
IChannelPtr CreateFailureDetectingChannel(
    IChannelPtr underlyingChannel,
    std::optional<TDuration> acknowledgementTimeout,
    TCallback<void(const IChannelPtr&, const TError& error)> onFailure,
    TCallback<bool(const TError&)> isError = BIND(IsChannelFailureError));

NTracing::TTraceContextPtr CreateHandlerTraceContext(const NProto::TRequestHeader& header);
NTracing::TTraceContextPtr CreateCallTraceContext(const TString& service, const TString& method);

//! Generates a random mutation id.
TMutationId GenerateMutationId();
//! Enables generating a series of mutation ids within a batch.
TMutationId GenerateNextBatchMutationId(TMutationId id);
//! Enables generating a series of mutation ids within a forwarding chain.
TMutationId GenerateNextForwardedMutationId(TMutationId id);

void GenerateMutationId(const IClientRequestPtr& request);
TMutationId GetMutationId(const NProto::TRequestHeader& header);
void SetMutationId(NProto::TRequestHeader* header, TMutationId id, bool retry);
void SetMutationId(const IClientRequestPtr& request, TMutationId id, bool retry);
void SetOrGenerateMutationId(const IClientRequestPtr& request, TMutationId id, bool retry);

////////////////////////////////////////////////////////////////////////////////

TFuture<std::vector<TSharedRef>> AsyncCompressAttachments(
    TRange<TSharedRef> attachments,
    NCompression::ECodec codecId);

TFuture<std::vector<TSharedRef>> AsyncDecompressAttachments(
    TRange<TSharedRef> attachments,
    NCompression::ECodec codecId);

std::vector<TSharedRef> CompressAttachments(
    TRange<TSharedRef> attachments,
    NCompression::ECodec codecId);

std::vector<TSharedRef> DecompressAttachments(
    TRange<TSharedRef> attachments,
    NCompression::ECodec codecId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NRpc::TAddressWithNetwork>
{
    size_t operator()(const NYT::NRpc::TAddressWithNetwork& addressWithNetwork) const;
};

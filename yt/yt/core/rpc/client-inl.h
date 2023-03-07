#pragma once
#ifndef CLIENT_INL_H_
#error "Direct inclusion of this file is not allowed, include client.h"
// For the sake of sane code completion.
#include "client.h"
#endif

#include <yt/core/rpc/stream.h>

#include <yt/core/ytalloc/memory_tag.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponse>
TTypedClientRequest<TRequestMessage, TResponse>::TTypedClientRequest(
    IChannelPtr channel,
    const TServiceDescriptor& serviceDescriptor,
    const TMethodDescriptor& methodDescriptor)
    : TClientRequest(
        std::move(channel),
        serviceDescriptor,
        methodDescriptor)
{ }

template <class TRequestMessage, class TResponse>
TFuture<typename TResponse::TResult> TTypedClientRequest<TRequestMessage, TResponse>::Invoke()
{
    auto context = CreateClientContext();
    auto requestAttachmentsStream = context->GetRequestAttachmentsStream();
    auto responseAttachmentsStream = context->GetResponseAttachmentsStream();
    typename TResponse::TResult response;
    {
        NYTAlloc::TMemoryTagGuard guard(context->GetResponseMemoryTag());
        response = New<TResponse>(std::move(context));
    }
    auto promise = response->GetPromise();
    auto requestControl = Send(std::move(response));
    if (requestControl) {
        auto subscribeToStreamAbort = [&] (const auto& stream) {
            if (stream) {
                stream->SubscribeAborted(BIND([=] {
                    requestControl->Cancel();
                }));
            }
        };
        subscribeToStreamAbort(requestAttachmentsStream);
        subscribeToStreamAbort(responseAttachmentsStream);
        promise.OnCanceled(BIND([=] (const TError& /*error*/) {
            requestControl->Cancel();
        }));
    }
    return promise.ToFuture();
}

template <class TRequestMessage, class TResponse>
TSharedRefArray TTypedClientRequest<TRequestMessage, TResponse>::SerializeData() const
{
    SmallVector<TSharedRef, TypicalMessagePartCount> parts;
    parts.reserve(Attachments().size() + 1);

    // COMPAT(kiselyovp): legacy RPC codecs
    auto serializedBody = EnableLegacyRpcCodecs_
        ? SerializeProtoToRefWithEnvelope(*this, RequestCodec_, false)
        : SerializeProtoToRefWithCompression(*this, RequestCodec_, false);
    parts.push_back(std::move(serializedBody));

    auto attachmentCodecId = EnableLegacyRpcCodecs_
        ? NCompression::ECodec::None
        : RequestCodec_;
    auto compressedAttachments = CompressAttachments(Attachments(), attachmentCodecId);
    parts.insert(parts.end(), compressedAttachments.begin(), compressedAttachments.end());

    return TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{});
}

////////////////////////////////////////////////////////////////////////////////

template <class TResponseMessage>
TTypedClientResponse<TResponseMessage>::TTypedClientResponse(TClientContextPtr clientContext)
    : TClientResponse(std::move(clientContext))
{ }

template <class TResponseMessage>
auto TTypedClientResponse<TResponseMessage>::GetPromise() -> TPromise<TResult>
{
    return Promise_;
}

template <class TResponseMessage>
void TTypedClientResponse<TResponseMessage>::SetPromise(const TError& error)
{
    if (error.IsOK()) {
        Promise_.Set(this);
    } else {
        Promise_.Set(error);
    }
    Promise_.Reset();
}

template <class TResponseMessage>
void TTypedClientResponse<TResponseMessage>::DeserializeBody(TRef data, std::optional<NCompression::ECodec> codecId)
{
    NYTAlloc::TMemoryTagGuard guard(ClientContext_->GetResponseMemoryTag());

    if (codecId) {
        DeserializeProtoWithCompression(this, data, *codecId);
    } else {
        // COMPAT(kiselyovp): legacy RPC codecs
        DeserializeProtoWithEnvelope(this, data);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> TProxyBase::CreateRequest(const TMethodDescriptor& methodDescriptor)
{
    auto request = New<T>(
        Channel_,
        ServiceDescriptor_,
        methodDescriptor);
    request->SetTimeout(DefaultTimeout_);
    request->SetAcknowledgementTimeout(DefaultAcknowledgementTimeout_);
    request->SetRequestCodec(DefaultRequestCodec_);
    request->SetResponseCodec(DefaultResponseCodec_);
    request->SetEnableLegacyRpcCodecs(DefaultEnableLegacyRpcCodecs_);
    request->SetMultiplexingBand(methodDescriptor.MultiplexingBand);
    request->SetResponseMemoryTag(NYTAlloc::GetCurrentMemoryTag());

    if (methodDescriptor.StreamingEnabled) {
        request->ClientAttachmentsStreamingParameters() =
            DefaultClientAttachmentsStreamingParameters_;
        request->ServerAttachmentsStreamingParameters() =
            DefaultServerAttachmentsStreamingParameters_;
    }

    return request;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

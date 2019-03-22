#pragma once
#ifndef CLIENT_INL_H_
#error "Direct inclusion of this file is not allowed, include client.h"
// For the sake of sane code completion.
#include "client.h"
#endif

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
    auto response = NYT::New<TResponse>(std::move(context));
    auto promise = response->GetPromise();
    auto requestControl = Send(std::move(response));
    if (requestControl) {
        promise.OnCanceled(BIND(&IClientRequestControl::Cancel, std::move(requestControl)));
    }
    return promise.ToFuture();
}

template <class TRequestMessage, class TResponse>
TSharedRefArray TTypedClientRequest<TRequestMessage, TResponse>::SerializeData() const
{
    std::vector<TSharedRef> data;
    data.reserve(Attachments().size() + 1);

    // COMPAT(kiselyovp): legacy RPC codecs
    auto serializedBody = EnableLegacyRpcCodecs_
        ? SerializeProtoToRefWithEnvelope(*this, RequestCodec_, false)
        : SerializeProtoToRefWithCompression(*this, RequestCodec_, false);
    data.push_back(std::move(serializedBody));

    auto attachmentCodecId = EnableLegacyRpcCodecs_
        ? NCompression::ECodec::None
        : RequestCodec_;
    auto* attachementCodec = NCompression::GetCodec(attachmentCodecId);
    for (const auto& attachment : Attachments()) {
        auto compressedAttachment = attachementCodec->Compress(attachment);
        data.push_back(std::move(compressedAttachment));
    }

    return TSharedRefArray(std::move(data));
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
    request->SetRequestAck(DefaultRequestAck_);
    request->SetRequestCodec(DefaultRequestCodec_);
    request->SetResponseCodec(DefaultResponseCodec_);
    request->SetEnableLegacyRpcCodecs(DefaultEnableLegacyRpcCodecs_);
    request->SetMultiplexingBand(methodDescriptor.MultiplexingBand);

    if (methodDescriptor.StreamingEnabled) {
        request->RequestAttachmentsStreamingParameters() =
            DefaultRequestAttachmentsStreamingParameters_;
        request->ResponseAttachmentsStreamingParameters() =
            DefaultResponseAttachmentsStreamingParameters_;
    }

    return request;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

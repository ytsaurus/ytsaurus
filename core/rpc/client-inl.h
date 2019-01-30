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
    const TString& path,
    const TString& method,
    TProtocolVersion protocolVersion)
    : TClientRequest(
    std::move(channel),
    path,
    method,
    protocolVersion)
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
    // COMPAT(kiselyovp)
    bool compatibilityMode = !RequestAttachmentCodec_ && !ResponseCodec_ && !ResponseAttachmentCodec_;
    std::vector<TSharedRef> data;
    data.reserve(Attachments().size() + 1);

    auto requestCodecId = RequestCodec_.value_or(NCompression::ECodec::None);
    auto serializedBody = compatibilityMode
        ? SerializeProtoToRefWithEnvelope(*this, requestCodecId, false)
        : SerializeProtoToRefWithCompression(*this, requestCodecId, false);
    data.emplace_back(std::move(serializedBody));

    auto requestAttachmentCodecId = compatibilityMode
        ? NCompression::ECodec::None
        : RequestAttachmentCodec_.value_or(requestCodecId);
    auto* requestAttachmentCodec = NCompression::GetCodec(requestAttachmentCodecId);

    for (const auto& attachment: Attachments()) {
        auto compressedAttachment = requestAttachmentCodec->Compress(attachment);
        data.emplace_back(std::move(compressedAttachment));
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
        // COMPAT(kiselyovp) old compression
        DeserializeProtoWithEnvelope(this, data);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> TProxyBase::CreateRequest(const TMethodDescriptor& methodDescriptor)
{
    auto request = New<T>(
        Channel_,
        ServiceDescriptor_.GetFullServiceName(),
        methodDescriptor.MethodName,
        ServiceDescriptor_.ProtocolVersion);
    request->SetTimeout(DefaultTimeout_);
    request->SetRequestAck(DefaultRequestAck_);
    request->SetRequestCodec(DefaultRequestCodec_);
    request->SetRequestAttachmentCodec(DefaultRequestAttachmentCodec_);
    request->SetResponseCodec(DefaultResponseCodec_);
    request->SetResponseAttachmentCodec(DefaultResponseAttachmentCodec_);
    request->SetMultiplexingBand(methodDescriptor.MultiplexingBand);
    return request;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

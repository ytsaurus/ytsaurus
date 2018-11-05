#pragma once
#ifndef CLIENT_INL_H_
#error "Direct inclusion of this file is not allowed, include client.h"
// For the sake of sane code completion.
#include "client.h"
#endif

namespace NYT {
namespace NRpc {

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
TSharedRef TTypedClientRequest<TRequestMessage, TResponse>::SerializeBody() const
{
    return SerializeProtoToRefWithEnvelope(*this, Codec_, false);
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
void TTypedClientResponse<TResponseMessage>::DeserializeBody(const TRef& data)
{
    DeserializeProtoWithEnvelope(this, data);
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
    request->SetMultiplexingBand(methodDescriptor.MultiplexingBand);
    return request;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

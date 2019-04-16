#pragma once
#ifndef STREAM_INL_H_
#error "Direct inclusion of this file is not allowed, include stream.h"
// For the sake of sane code completion.
#include "stream.h"
#endif

#include <yt/core/misc/cast.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> CreateRpcClientInputStream(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request)
{
    auto invokeResult = request->Invoke().template As<void>();

    return request->GetResponseAttachmentsStream()->Read()
        .Apply(BIND([=] (const TSharedRef& firstReadResult) {
            return New<NDetail::TRpcClientInputStream>(
                std::move(request),
                std::move(invokeResult),
                firstReadResult);
        })).template As<NConcurrency::IAsyncZeroCopyInputStreamPtr>();
}

template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyOutputStreamPtr> CreateRpcClientOutputStreamFromInvokedRequest(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request,
    TFuture<void> invokeResult,
    bool feedbackEnabled = false)
{
    auto handshakeResult = NDetail::ExpectHandshake(
        request->GetResponseAttachmentsStream(),
        feedbackEnabled);

    return handshakeResult.Apply(BIND([=] () {
        return New<NDetail::TRpcClientOutputStream>(
            std::move(request),
            std::move(invokeResult),
            feedbackEnabled);
    })).template As<NConcurrency::IAsyncZeroCopyOutputStreamPtr>();
}

template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyOutputStreamPtr> CreateRpcClientOutputStream(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request,
    bool feedbackEnabled)
{
    auto invokeResult = request->Invoke().template As<void>();
    return CreateRpcClientOutputStreamFromInvokedRequest(
        std::move(request),
        std::move(invokeResult),
        feedbackEnabled);
}

template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyOutputStreamPtr> CreateRpcClientOutputStream(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request,
    TCallback<void(TSharedRef)> metaHandler)
{
    auto invokeResult = request->Invoke().template As<void>();
    auto metaHandlerResult = request->GetResponseAttachmentsStream()->Read()
        .Apply(metaHandler);
    return metaHandlerResult.Apply(BIND ([=] () {
        return CreateRpcClientOutputStreamFromInvokedRequest(
            std::move(request),
            std::move(invokeResult));
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc


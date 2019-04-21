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
    return request->GetRequestAttachmentsStream()->Close().Apply(BIND([=] () {
        return New<NDetail::TRpcClientInputStream>(
            std::move(request),
            std::move(invokeResult));
    })).template As<NConcurrency::IAsyncZeroCopyInputStreamPtr>();
}

template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyOutputStreamPtr> CreateRpcClientOutputStream(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request,
    bool feedbackEnabled)
{
    auto invokeResult = request->Invoke().template As<void>();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc


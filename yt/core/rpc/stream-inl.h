#pragma once
#ifndef STREAM_INL_H_
#error "Direct inclusion of this file is not allowed, include stream.h"
// For the sake of sane code completion.
#include "stream.h"
#endif
#undef STREAM_INL_H_

#include <yt/core/misc/cast.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> CreateInputStreamAdapter(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request)
{
    auto invokeResult = request->Invoke().template As<void>();

    return request->GetResponseAttachmentsStream()->Read()
        .Apply(BIND([=] (const TSharedRef& firstReadResult) {
            return New<NDetail::TRpcInputStreamAdapter>(request, invokeResult, firstReadResult);
        })).template As<NConcurrency::IAsyncZeroCopyInputStreamPtr>();
}

template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyOutputStreamPtr> CreateOutputStreamAdapter(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request,
    EWriterFeedbackStrategy feedbackStrategy)
{
    auto invokeResult = request->Invoke().template As<void>();
    auto handshakeResult =
        feedbackStrategy == EWriterFeedbackStrategy::NoFeedback
        ? ExpectEndOfStream(request->GetResponseAttachmentsStream())
        : request->GetResponseAttachmentsStream()->Read()
            .Apply(BIND([] (const TSharedRef& feedback) {
                NDetail::CheckWriterFeedback(feedback, NDetail::EWriterFeedback::Handshake);
            }));

    return handshakeResult.Apply(BIND([=] () {
        return New<NDetail::TRpcOutputStreamAdapter>(
            request,
            invokeResult,
            feedbackStrategy);
    })).template As<NConcurrency::IAsyncZeroCopyOutputStreamPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc


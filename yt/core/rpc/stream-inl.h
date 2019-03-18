#pragma once
#ifndef STREAM_INL_H_
#error "Direct inclusion of this file is not allowed, include stream.h"
// For the sake of sane code completion.
#include "stream.h"
#endif
#undef STREAM_INL_H_

#include "client.h"

#include <yt/core/misc/cast.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TRpcInputStreamAdapter
    : public NConcurrency::IAsyncZeroCopyInputStream
{
public:
    TRpcInputStreamAdapter(
        IClientRequestPtr request,
        TFuture<void> invokeResult,
        TSharedRef firstReadResult)
        : Request_(std::move(request))
        , InvokeResult_(std::move(invokeResult))
        , FirstReadResult_(std::move(firstReadResult))
    {
        YCHECK(Request_);
        Underlying_ = Request_->GetResponseAttachmentsStream();
        YCHECK(Underlying_);
    }

    virtual TFuture<TSharedRef> Read() override
    {
        if (FirstRead_.exchange(false)) {
            return MakeFuture(std::move(FirstReadResult_));
        }
        return Underlying_->Read();
    }

    ~TRpcInputStreamAdapter() {
        InvokeResult_.Cancel();
    }

private:
    IClientRequestPtr Request_;
    NConcurrency::IAsyncZeroCopyInputStreamPtr Underlying_;
    TFuture<void> InvokeResult_;
    std::atomic<bool> FirstRead_ = {true};
    TSharedRef FirstReadResult_;
};

TError CheckWriterFeedback(
    const TSharedRef& ref,
    EWriterFeedback expectedFeedback)
{
    NProto::TWriterFeedback protoFeedback;
    if (!TryDeserializeProto(&protoFeedback, ref)) {
        return TError("Failed to deserialize writer feedback");
    }

    EWriterFeedback actualFeedback;
    try {
        actualFeedback = CheckedEnumCast<EWriterFeedback>(protoFeedback.feedback());
    } catch (const TErrorException& ex) {
        return ex.Error();
    }

    if (actualFeedback != expectedFeedback) {
        return TError("Received the wrong kind of writer feedback")
            << TErrorAttribute("expected_feedback", expectedFeedback)
            << TErrorAttribute("actual_feedback", actualFeedback);
    }

    return TError();
}

class TRpcOutputStreamAdapter
    : public NConcurrency::IAsyncZeroCopyOutputStream
{
public:
    TRpcOutputStreamAdapter(
        IClientRequestPtr request,
        TFuture<void> invokeResult,
        EWriterFeedbackStrategy feedbackStrategy = EWriterFeedbackStrategy::NoFeedback)
        : Request_(std::move(request))
        , InvokeResult_(std::move(invokeResult))
        , FeedbackStrategy_(feedbackStrategy)
    {
        YCHECK(Request_);
        Underlying_ = Request_->GetRequestAttachmentsStream();
        YCHECK(Underlying_);
        FeedbackStream_ = Request_->GetResponseAttachmentsStream();
        YCHECK(FeedbackStream_);

        if (FeedbackStrategy_ != EWriterFeedbackStrategy::NoFeedback) {
            FeedbackStream_->Read().Subscribe(
                BIND(&TRpcOutputStreamAdapter::OnFeedback, MakeWeak(this)));
        }
    }

    virtual TFuture<void> Write(const TSharedRef& data) override
    {
        switch (FeedbackStrategy_) {
            case EWriterFeedbackStrategy::NoFeedback:
                return Underlying_->Write(data);
            case EWriterFeedbackStrategy::OnlyPositive:
                {
                    auto promise = NewPromise<void>();
                    TFuture<void> writeResult;
                    {
                        auto guard = Guard(QueueLock_);
                        if (!Error_.IsOK()) {
                            return MakeFuture(Error_);
                        }

                        ConfirmationQueue_.push(promise);
                        writeResult = Underlying_->Write(data);
                    }

                    writeResult.Subscribe(
                        BIND(&TRpcOutputStreamAdapter::AbortOnError, MakeWeak(this)));

                    return promise.ToFuture();
                }
            default:
                Y_UNREACHABLE();
        }
    }

    virtual TFuture<void> Close() override
    {
        Underlying_->Close();
        return InvokeResult_;
    }

private:
    IClientRequestPtr Request_;
    NConcurrency::IAsyncZeroCopyOutputStreamPtr Underlying_;
    TFuture<void> InvokeResult_;

    NConcurrency::IAsyncZeroCopyInputStreamPtr FeedbackStream_;
    EWriterFeedbackStrategy FeedbackStrategy_;

    TRingQueue<TPromise<void>> ConfirmationQueue_;
    TSpinLock QueueLock_;

    TError Error_;

    void AbortOnError(const TError& error) {
        if (error.IsOK()) {
            return;
        }

        auto guard = Guard(QueueLock_);

        if (!Error_.IsOK()) {
            return;
        }

        Error_ = error;

        std::vector<TPromise<void>> promises;
        while (!ConfirmationQueue_.empty()) {
            promises.push_back(std::move(ConfirmationQueue_.front()));
            ConfirmationQueue_.pop();
        }

        guard.Release();

        for (auto& promise : promises) {
            if (promise) {
                promise.Set(error);
            }
        }

        InvokeResult_.Cancel();
    }

    void OnFeedback(const TErrorOr<TSharedRef>& refOrError)
    {
        YCHECK(FeedbackStrategy_ != EWriterFeedbackStrategy::NoFeedback);

        auto error = static_cast<TError>(refOrError);
        if (error.IsOK()) {
            auto ref = refOrError.Value();
            if (!ref) {
                auto guard = Guard(QueueLock_);

                if (ConfirmationQueue_.empty()) {
                    guard.Release();
                    Underlying_->Close();
                    return;
                }
                error = TError("Expected a positive writer feedback, received an empty ref");
            } else {
                error = CheckWriterFeedback(ref, EWriterFeedback::Success);
            }
        }

        TPromise<void> promise;

        {
            auto guard = Guard(QueueLock_);

            if (!Error_.IsOK()) {
                return;
            }

            if (!error.IsOK()) {
                guard.Release();
                AbortOnError(error);
                return;
            }

            YCHECK(!ConfirmationQueue_.empty());
            promise = std::move(ConfirmationQueue_.front());
            ConfirmationQueue_.pop();
        }

        promise.Set();
        FeedbackStream_->Read().Subscribe(
            BIND(&TRpcOutputStreamAdapter::OnFeedback, MakeWeak(this)));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class TRequestMessage, class TResponse>
TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> CreateInputStreamAdapter(
    TIntrusivePtr<TTypedClientRequest<TRequestMessage, TResponse>> request)
{
    auto invokeResult = request->Invoke().template As<void>();

    return request->GetResponseAttachmentsStream()->Read()
        .Apply(BIND([=] (const TSharedRef& firstReadResult) {
            return New<TRpcInputStreamAdapter>(request, invokeResult, firstReadResult);
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
                CheckWriterFeedback(feedback, EWriterFeedback::Handshake)
                    .ThrowOnError();
            }));

    return handshakeResult.Apply(BIND([=] () {
        return New<TRpcOutputStreamAdapter>(
            request,
            invokeResult,
            feedbackStrategy);
    })).template As<NConcurrency::IAsyncZeroCopyOutputStreamPtr>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc


#include "service_detail.h"
#include "stream.h"

#include <yt/core/compression/codec.h>

namespace NYT::NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr ssize_t MaxWindowSize = 16384;

////////////////////////////////////////////////////////////////////////////////

size_t GetStreamingAttachmentSize(TRef attachment)
{
    if (!attachment || attachment.Size() == 0) {
        return 1;
    } else {
        return attachment.Size();
    }
}

////////////////////////////////////////////////////////////////////////////////

TAttachmentsInputStream::TAttachmentsInputStream(
    TClosure readCallback,
    IInvokerPtr compressionInvoker,
    std::optional<TDuration> timeout)
    : ReadCallback_(std::move(readCallback))
    , CompressionInvoker_(std::move(compressionInvoker))
    , Timeout_(timeout)
    , Window_(MaxWindowSize)
{ }

TFuture<TSharedRef> TAttachmentsInputStream::Read()
{
    auto guard = Guard(Lock_);

    // Failure here indicates an attempt to read past EOSs.
    if (Closed_) {
        return MakeFuture<TSharedRef>(TError("Stream is already closed"));
    }

    if (!Error_.IsOK()) {
        return MakeFuture<TSharedRef>(Error_);
    }

    // Failure here indicates that another Read request is already in progress.
    YCHECK(!Promise_);

    if (Queue_.empty()) {
        Promise_ = NewPromise<TSharedRef>();
        if (Timeout_) {
            TimeoutCookie_ = TDelayedExecutor::Submit(
                BIND(&TAttachmentsInputStream::OnTimeout, MakeWeak(this)),
                *Timeout_);
        }
        return Promise_.ToFuture();
    } else {
        auto entry = std::move(Queue_.front());
        Queue_.pop();
        ReadPosition_ += entry.CompressedSize;
        if (!entry.Attachment) {
            YCHECK(!Closed_);
            Closed_ = true;
        }
        guard.Release();
        ReadCallback_();
        return MakeFuture(entry.Attachment);
    }
}

void TAttachmentsInputStream::EnqueuePayload(const TStreamingPayload& payload)
{
    if (payload.Codec == NCompression::ECodec::None) {
        DoEnqueuePayload(payload, payload.Attachments);
    } else {
        CompressionInvoker_->Invoke(BIND([=, this_= MakeWeak(this)] {
            std::vector<TSharedRef> decompressedAttachments;
            decompressedAttachments.reserve(payload.Attachments.size());
            auto* codec = NCompression::GetCodec(payload.Codec);
            for (const auto& attachment : payload.Attachments) {
                TSharedRef decompressedAttachment;
                if (attachment) {
                    TMemoryZoneGuard guard(payload.MemoryZone);
                    decompressedAttachment = codec->Decompress(attachment);
                }
                decompressedAttachments.push_back(std::move(decompressedAttachment));
            }
            DoEnqueuePayload(payload, decompressedAttachments);
        }));
    }
}

void TAttachmentsInputStream::DoEnqueuePayload(
    const TStreamingPayload& payload,
    const std::vector<TSharedRef>& decompressedAttachments)
{
    auto guard = Guard(Lock_);

    if (!Error_.IsOK()) {
        return;
    }

    Window_.AddPacket(
        payload.SequenceNumber,
        {
            payload,
            decompressedAttachments,
        },
        [&] (auto&& packet) {
            for (size_t index = 0; index < packet.Payload.Attachments.size(); ++index) {
                Queue_.push({
                    packet.DecompressedAttachments[index],
                    GetStreamingAttachmentSize(packet.Payload.Attachments[index])
                });
            }
        });

    if (Promise_ && !Queue_.empty()) {
        auto entry = std::move(Queue_.front());
        Queue_.pop();
        auto promise = std::move(Promise_);
        Promise_.Reset();
        ReadPosition_ += entry.CompressedSize;
        if (!entry.Attachment) {
            YCHECK(!Closed_);
            Closed_ = true;
        }

        guard.Release();

        TDelayedExecutor::CancelAndClear(TimeoutCookie_);
        promise.Set(std::move(entry.Attachment));
        ReadCallback_();
    }
}

void TAttachmentsInputStream::Abort(const TError& error)
{
    auto guard = Guard(Lock_);
    DoAbort(guard, error);
}

void TAttachmentsInputStream::AbortUnlessClosed(const TError& error)
{
    auto guard = Guard(Lock_);

    if (Closed_) {
        return;
    }

    DoAbort(
        guard,
        error.IsOK() ? TError("Request is already completed") : error);
}

void TAttachmentsInputStream::DoAbort(TGuard<TSpinLock>& guard, const TError& error)
{
    if (!Error_.IsOK()) {
        return;
    }

    Error_ = error;

    auto promise = Promise_;

    guard.Release();

    if (promise) {
        promise.Set(error);
    }

    Aborted_.Fire();
}

void TAttachmentsInputStream::OnTimeout()
{
    Abort(TError(NYT::EErrorCode::Timeout, "Attachments stream read timed out")
        << TErrorAttribute("timeout", *Timeout_));
}

TStreamingFeedback TAttachmentsInputStream::GetFeedback() const
{
    return TStreamingFeedback{
        ReadPosition_.load()
    };
}

////////////////////////////////////////////////////////////////////////////////

TAttachmentsOutputStream::TAttachmentsOutputStream(
    EMemoryZone memoryZone,
    NCompression::ECodec codec,
    IInvokerPtr compressisonInvoker,
    TClosure pullCallback,
    ssize_t windowSize,
    std::optional<TDuration> timeout)
    : MemoryZone_(memoryZone)
    , Codec_(codec)
    , CompressionInvoker_(std::move(compressisonInvoker))
    , PullCallback_(std::move(pullCallback))
    , WindowSize_(windowSize)
    , Timeout_(timeout)
    , Window_(std::numeric_limits<ssize_t>::max())
{ }

TFuture<void> TAttachmentsOutputStream::Write(const TSharedRef& data)
{
    YCHECK(data);
    auto promise = NewPromise<void>();
    TDelayedExecutorCookie timeoutCookie;
    if (Timeout_) {
        timeoutCookie = TDelayedExecutor::Submit(
            BIND(&TAttachmentsOutputStream::OnTimeout, MakeWeak(this)),
            *Timeout_);
    }
    if (Codec_ == NCompression::ECodec::None) {
        auto guard = Guard(Lock_);
        OnWindowPacketReady({data, promise, std::move(timeoutCookie)}, guard);
    } else {
        auto sequenceNumber = CompressionSequenceNumber_++;
        CompressionInvoker_->Invoke(BIND([=, this_ = MakeStrong(this)] {
            auto* codec = NCompression::GetCodec(Codec_);
            auto compressedData = codec->Compress(data);
            auto guard = Guard(Lock_);
            Window_.AddPacket(
                sequenceNumber,
                {compressedData, promise, std::move(timeoutCookie)},
                [&] (auto&& packet) {
                    OnWindowPacketReady(std::move(packet), guard);
                });
        }));
    }
    return promise.ToFuture();
}

void TAttachmentsOutputStream::OnWindowPacketReady(TWindowPacket&& packet, TGuard<TSpinLock>& guard)
{
    if (ClosePromise_) {
        guard.Release();
        TDelayedExecutor::CancelAndClear(packet.TimeoutCookie);
        packet.Promise.Set(TError("Stream is already closed"));
        return;
    }

    if (!Error_.IsOK()) {
        guard.Release();
        TDelayedExecutor::CancelAndClear(packet.TimeoutCookie);
        packet.Promise.Set(Error_);
        return;
    }

    WritePosition_ += GetStreamingAttachmentSize(packet.Data);
    DataQueue_.push(std::move(packet.Data));

    TPromise<void> promiseToSet;
    if (WritePosition_ - ReadPosition_ <= WindowSize_) {
        TDelayedExecutor::CancelAndClear(packet.TimeoutCookie);
        promiseToSet = std::move(packet.Promise);
    }

    ConfirmationQueue_.push({
        WritePosition_,
        std::move(packet.Promise),
        std::move(packet.TimeoutCookie)
    });

    MaybeInvokePullCallback(guard);

    guard.Release();

    if (promiseToSet) {
        promiseToSet.Set();
    }
}

TFuture<void> TAttachmentsOutputStream::Close()
{
    auto guard = Guard(Lock_);

    if (!Error_.IsOK()) {
        return MakeFuture(Error_);
    }

    if (ClosePromise_) {
        return VoidFuture;
    }

    auto promise = ClosePromise_ = NewPromise<void>();
    TDelayedExecutorCookie timeoutCookie;
    if (Timeout_) {
        timeoutCookie = TDelayedExecutor::Submit(
            BIND(&TAttachmentsOutputStream::OnTimeout, MakeWeak(this)),
            *Timeout_);
    }

    TSharedRef nullAttachment;
    DataQueue_.push(nullAttachment);
    WritePosition_ += GetStreamingAttachmentSize(nullAttachment);

    ConfirmationQueue_.push({
        WritePosition_,
        {},
        std::move(timeoutCookie)
    });

    MaybeInvokePullCallback(guard);

    return promise.ToFuture();
}

void TAttachmentsOutputStream::Abort(const TError& error)
{
    auto guard = Guard(Lock_);

    DoAbort(guard, error);
}

void TAttachmentsOutputStream::AbortUnlessClosed(const TError& error)
{
    auto guard = Guard(Lock_);

    if (Closed_) {
        return;
    }

    DoAbort(
        guard,
        error.IsOK() ? TError("Request is already completed") : error);
}

void TAttachmentsOutputStream::DoAbort(TGuard<TSpinLock>& guard, const TError& error)
{
    if (!Error_.IsOK()) {
        return;
    }

    Error_ = error;

    std::vector<TPromise<void>> promises;
    while (!ConfirmationQueue_.empty()) {
        auto& entry = ConfirmationQueue_.front();
        TDelayedExecutor::CancelAndClear(entry.TimeoutCookie);
        promises.push_back(std::move(entry.Promise));
        ConfirmationQueue_.pop();
    }

    if (ClosePromise_) {
        promises.push_back(ClosePromise_);
    }

    guard.Release();

    for (auto& promise : promises) {
        if (promise) {
            promise.Set(error);
        }
    }

    Aborted_.Fire();
}

void TAttachmentsOutputStream::OnTimeout()
{
    Abort(TError(NYT::EErrorCode::Timeout, "Attachments stream write timed out")
        << TErrorAttribute("timeout", *Timeout_));
}

void TAttachmentsOutputStream::HandleFeedback(const TStreamingFeedback& feedback)
{
    auto guard = Guard(Lock_);

    if (!Error_.IsOK()) {
        return;
    }

    if (ReadPosition_ >= feedback.ReadPosition) {
        return;
    }

    if (feedback.ReadPosition > WritePosition_) {
        THROW_ERROR_EXCEPTION("Stream read position exceeds write position: %v > %v",
            feedback.ReadPosition,
            WritePosition_);
    }

    ReadPosition_ = feedback.ReadPosition;

    std::vector<TPromise<void>> promises;
    while (!ConfirmationQueue_.empty() &&
            ConfirmationQueue_.front().Position <= ReadPosition_ + WindowSize_)
    {
        auto& entry = ConfirmationQueue_.front();
        TDelayedExecutor::CancelAndClear(entry.TimeoutCookie);
        promises.push_back(std::move(entry.Promise));
        ConfirmationQueue_.pop();
    }

    if (ClosePromise_ && ReadPosition_ == WritePosition_) {
        promises.push_back(ClosePromise_);
        Closed_ = true;
    }

    MaybeInvokePullCallback(guard);

    guard.Release();

    for (auto& promise : promises) {
        if (promise) {
            promise.Set();
        }
    }
}

std::optional<TStreamingPayload> TAttachmentsOutputStream::TryPull()
{
    auto guard = Guard(Lock_);

    if (!Error_.IsOK()) {
        return std::nullopt;
    }

    TStreamingPayload result;
    result.Codec = Codec_;
    result.MemoryZone = MemoryZone_;
    while (CanPullMore(result.Attachments.empty())) {
        auto attachment = std::move(DataQueue_.front());
        SentPosition_ += GetStreamingAttachmentSize(attachment);
        result.Attachments.push_back(std::move(attachment));
        DataQueue_.pop();
    }

    if (result.Attachments.empty()) {
        return std::nullopt;
    }

    result.SequenceNumber = PayloadSequenceNumber_++;
    return result;
}

void TAttachmentsOutputStream::MaybeInvokePullCallback(TGuard<TSpinLock>& guard)
{
    if (CanPullMore(true)) {
        guard.Release();
        PullCallback_();
    }
}

bool TAttachmentsOutputStream::CanPullMore(bool first) const
{
    if (DataQueue_.empty()) {
        return false;
    }

    if (SentPosition_ - ReadPosition_ + GetStreamingAttachmentSize(DataQueue_.front()) <= WindowSize_) {
        return true;
    }

    if (first && SentPosition_ == ReadPosition_) {
        return true;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

void HandleInputStreamingRequest(
    IServiceContextPtr context,
    TCallback<TFuture<TSharedRef>()> blockGenerator)
{
    auto outputStream = context->GetResponseAttachmentsStream();
    YCHECK(outputStream);
    while (auto block = NConcurrency::WaitFor(blockGenerator()).ValueOrThrow()) {
        NConcurrency::WaitFor(outputStream->Write(block))
            .ThrowOnError();
    }

    NConcurrency::WaitFor(outputStream->Close())
        .ThrowOnError();
    context->Reply(TError());
};

void HandleInputStreamingRequest(
    IServiceContextPtr context,
    NConcurrency::IAsyncZeroCopyInputStreamPtr input)
{
    HandleInputStreamingRequest(
        context,
        BIND(&NConcurrency::IAsyncZeroCopyInputStream::Read, input));
}

TSharedRef GenerateWriterFeedbackMessage(
    EWriterFeedback feedback)
{
    NProto::TWriterFeedback protoFeedback;
    protoFeedback.set_feedback(
        static_cast<NProto::TWriterFeedback::EWriterFeedback>(feedback));
    return SerializeProtoToRef(protoFeedback);
}

void HandleOutputStreamingRequest(
    IServiceContextPtr context,
    TCallback<TFuture<void>(TSharedRef)> blockHandler,
    TCallback<TFuture<void>()> finalizer,
    EWriterFeedbackStrategy feedbackStrategy)
{
    auto inputStream = context->GetRequestAttachmentsStream();
    YCHECK(inputStream);
    auto outputStream = context->GetResponseAttachmentsStream();
    YCHECK(outputStream);

    switch (feedbackStrategy) {
        case EWriterFeedbackStrategy::NoFeedback:
            NConcurrency::WaitFor(outputStream->Close())
                .ThrowOnError();
            while (auto block = NConcurrency::WaitFor(inputStream->Read()).ValueOrThrow()) {
                NConcurrency::WaitFor(blockHandler(block))
                    .ThrowOnError();
            }

            break;
        case EWriterFeedbackStrategy::OnlyPositive:
            {
                auto handshakeRef = GenerateWriterFeedbackMessage(EWriterFeedback::Handshake);
                NConcurrency::WaitFor(outputStream->Write(handshakeRef))
                    .ThrowOnError();

                while (auto block = NConcurrency::WaitFor(inputStream->Read()).ValueOrThrow()) {
                    NConcurrency::WaitFor(blockHandler(block))
                        .ThrowOnError();

                    auto ackRef = GenerateWriterFeedbackMessage(EWriterFeedback::Success);
                    NConcurrency::WaitFor(outputStream->Write(ackRef))
                        .ThrowOnError();
                }

                outputStream->Close();
            }

            break;
        default:
            Y_UNREACHABLE();
    }

    NConcurrency::WaitFor(finalizer())
        .ThrowOnError();
    context->Reply(TError());
}

void HandleOutputStreamingRequest(
    IServiceContextPtr context,
    NConcurrency::IAsyncZeroCopyOutputStreamPtr output,
    EWriterFeedbackStrategy feedbackStrategy)
{
    HandleOutputStreamingRequest(
        context,
        BIND(&NConcurrency::IAsyncZeroCopyOutputStream::Write, output),
        BIND(&NConcurrency::IAsyncZeroCopyOutputStream::Close, output),
        feedbackStrategy);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc


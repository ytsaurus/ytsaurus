#include "stream.h"

namespace NYT::NRpc {

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
    TClosure readCallback)
    : ReadCallback_(std::move(readCallback))
{ }

TFuture<TSharedRef> TAttachmentsInputStream::Read()
{
    auto guard = Guard(Lock_);

    // Failure here indicates an attempt to read past EOSs.
    YCHECK(!Closed_);

    if (!Error_.IsOK()) {
        return MakeFuture<TSharedRef>(Error_);
    }

    // Failure here indicates that another Read request is already in progress.
    YCHECK(!Promise_);

    if (Queue_.empty()) {
        Promise_ = NewPromise<TSharedRef>();
        return Promise_.ToFuture();
    } else {
        auto attachment = std::move(Queue_.front());
        Queue_.pop();
        ReadPosition_ += GetStreamingAttachmentSize(attachment);
        if (!attachment) {
            YCHECK(!Closed_);
            Closed_ = true;
        }
        guard.Release();
        ReadCallback_();
        return MakeFuture(attachment);
    }
}

void TAttachmentsInputStream::EnqueuePayload(const TStreamingPayload& payload)
{
    auto guard = Guard(Lock_);

    if (!Error_.IsOK()) {
        return;
    }

    if (payload.SequenceNumber != SequenceNumber_) {
        THROW_ERROR_EXCEPTION("Invalid attachments stream sequence number: expected %v, got %v",
            SequenceNumber_,
            payload.SequenceNumber);
    }

    ++SequenceNumber_;

    for (size_t index = 0; index < payload.Attachments.size(); ++index) {
        if (!Promise_ || index > 0) {
            Queue_.push(payload.Attachments[index]);
        }
    }

    if (Promise_) {
        auto promise = std::move(Promise_);
        Promise_.Reset();
        const auto& attachment = payload.Attachments[0];
        ReadPosition_ += GetStreamingAttachmentSize(attachment);
        if (!attachment) {
            YCHECK(!Closed_);
            Closed_ = true;
        }
        guard.Release();
        promise.Set(attachment);
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
}

TStreamingFeedback TAttachmentsInputStream::GetFeedback() const
{
    return TStreamingFeedback{
        ReadPosition_.load()
    };
}

////////////////////////////////////////////////////////////////////////////////

TAttachmentsOutputStream::TAttachmentsOutputStream(
    const TStreamingParameters& parameters,
    TClosure pullCallback)
    : Parameters_(parameters)
    , PullCallback_(std::move(pullCallback))
{ }

TFuture<void> TAttachmentsOutputStream::Write(const TSharedRef& data)
{
    YCHECK(data);

    auto guard = Guard(Lock_);

    // Failure here indicates an attempt to write into a closed stream.
    YCHECK(!ClosePromise_);

    if (!Error_.IsOK()) {
        return MakeFuture(Error_);
    }

    DataQueue_.push(data);

    WritePosition_ += GetStreamingAttachmentSize(data);

    TPromise<void> promise;
    if (WritePosition_ - ReadPosition_ > Parameters_.WindowSize) {
        promise = NewPromise<void>();
    }

    ConfirmationQueue_.push({
        WritePosition_,
        promise
    });

    MaybeInvokePullCallback(guard);

    return promise ? promise.ToFuture() : VoidFuture;
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

    DataQueue_.push({});

    WritePosition_ += 1;

    ConfirmationQueue_.push({
        WritePosition_,
        {}
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
        promises.push_back(std::move(ConfirmationQueue_.front().Promise));
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
        THROW_ERROR_EXCEPTION("Stream read position exceed write position: %v > %v",
            feedback.ReadPosition,
            WritePosition_);
    }

    ReadPosition_ = feedback.ReadPosition;

    std::vector<TPromise<void>> promises;
    while (!ConfirmationQueue_.empty() &&
            ConfirmationQueue_.front().Position <= ReadPosition_ + Parameters_.WindowSize)
    {
        promises.push_back(std::move(ConfirmationQueue_.front().Promise));
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
    while (CanPullMore(result.Attachments.empty())) {
        auto attachment = std::move(DataQueue_.front());
        SentPosition_ += GetStreamingAttachmentSize(attachment);
        result.Attachments.push_back(std::move(attachment));
        DataQueue_.pop();
    }

    if (result.Attachments.empty()) {
        return std::nullopt;
    }

    result.SequenceNumber = SequenceNumber_++;
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

    if (SentPosition_ - ReadPosition_ + GetStreamingAttachmentSize(DataQueue_.front()) <= Parameters_.WindowSize) {
        return true;
    }

    if (first && SentPosition_ == ReadPosition_) {
        return true;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#pragma once

#include "channel.h"

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/ref.h>
#include <yt/core/misc/ring_queue.h>

#include <yt/core/actions/future.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! For empty and null attachments returns 1; for others returns the actual size.
size_t GetStreamingAttachmentSize(TRef attachment);

////////////////////////////////////////////////////////////////////////////////

class TAttachmentsInputStream
    : public NConcurrency::IAsyncZeroCopyInputStream
{
public:
    explicit TAttachmentsInputStream(TClosure readCallback);

    virtual TFuture<TSharedRef> Read() override;

    void EnqueuePayload(const TStreamingPayload& payload);
    void Abort(const TError& error);
    void AbortUnlessClosed(const TError& error);
    TStreamingFeedback GetFeedback() const;

private:
    const TClosure ReadCallback_;

    TSpinLock Lock_;
    int SequenceNumber_ = 0;
    TRingQueue<TSharedRef> Queue_;
    TError Error_;
    TPromise<TSharedRef> Promise_;
    std::atomic<ssize_t> ReadPosition_ = {0};
    bool Closed_ = false;

    void DoAbort(TGuard<TSpinLock>& guard, const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TAttachmentsInputStream)

////////////////////////////////////////////////////////////////////////////////

class TAttachmentsOutputStream
    : public NConcurrency::IAsyncZeroCopyOutputStream
{
public:
    TAttachmentsOutputStream(
        const TStreamingParameters& parameters,
        TClosure pullCallback);

    virtual TFuture<void> Write(const TSharedRef& data) override;
    virtual TFuture<void> Close() override;

    void Abort(const TError& error);
    void AbortUnlessClosed(const TError& error);
    void HandleFeedback(const TStreamingFeedback& feedback);
    std::optional<TStreamingPayload> TryPull();

private:
    const TStreamingParameters Parameters_;
    const TClosure PullCallback_;

    struct TConfirmationEntry
    {
        ssize_t Position;
        TPromise<void> Promise;
    };

    TSpinLock Lock_;
    TError Error_;
    TRingQueue<TSharedRef> DataQueue_;
    TRingQueue<TConfirmationEntry> ConfirmationQueue_;
    TPromise<void> ClosePromise_;
    bool Closed_ = false;
    ssize_t WritePosition_ = 0;
    ssize_t SentPosition_ = 0;
    ssize_t ReadPosition_ = 0;
    int SequenceNumber_ = 0;

    void MaybeInvokePullCallback(TGuard<TSpinLock>& guard);
    bool CanPullMore(bool first) const;
    void DoAbort(TGuard<TSpinLock>& guard, const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TAttachmentsOutputStream)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#pragma once

#include "channel.h"

#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/ref.h>
#include <yt/core/misc/ring_queue.h>

#include <yt/core/actions/future.h>

#include <yt/core/compression/public.h>
#include <yt/core/misc/memory_zone.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! For empty and null attachments returns 1; for others returns the actual size.
size_t GetStreamingAttachmentSize(TRef attachment);

////////////////////////////////////////////////////////////////////////////////

class TAttachmentsInputStream
    : public NConcurrency::IAsyncZeroCopyInputStream
{
public:
    TAttachmentsInputStream(
        TClosure readCallback,
        IInvokerPtr compressionInvoker);

    virtual TFuture<TSharedRef> Read() override;

    void EnqueuePayload(const TStreamingPayload& payload);
    void Abort(const TError& error);
    void AbortUnlessClosed(const TError& error);
    TStreamingFeedback GetFeedback() const;

private:
    const TClosure ReadCallback_;
    const IInvokerPtr CompressionInvoker_;

    struct TQueueEntry
    {
        TSharedRef Attachment;
        size_t CompressedSize;
    };

    TSpinLock Lock_;
    int SequenceNumber_ = 0;
    TRingQueue<TQueueEntry> Queue_;
    TError Error_;
    TPromise<TSharedRef> Promise_;
    std::atomic<ssize_t> ReadPosition_ = {0};
    bool Closed_ = false;

    void DoEnqueuePayload(
        const TStreamingPayload& payload,
        const std::vector<TSharedRef>& decompressedAttachments);
    void DoAbort(
        TGuard<TSpinLock>& guard,
        const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TAttachmentsInputStream)

////////////////////////////////////////////////////////////////////////////////

class TAttachmentsOutputStream
    : public NConcurrency::IAsyncZeroCopyOutputStream
{
public:
    TAttachmentsOutputStream(
        const TStreamingParameters& parameters,
        EMemoryZone memoryZone,
        NCompression::ECodec codec,
        IInvokerPtr compressionInvoker,
        TClosure pullCallback);

    virtual TFuture<void> Write(const TSharedRef& data) override;
    virtual TFuture<void> Close() override;

    void Abort(const TError& error);
    void AbortUnlessClosed(const TError& error);
    void HandleFeedback(const TStreamingFeedback& feedback);
    std::optional<TStreamingPayload> TryPull();

private:
    const TStreamingParameters Parameters_;
    const EMemoryZone MemoryZone_;
    const NCompression::ECodec Codec_;
    const IInvokerPtr CompressionInvoker_;
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

    TFuture<void> DoWrite(const TSharedRef& data);
    void MaybeInvokePullCallback(TGuard<TSpinLock>& guard);
    bool CanPullMore(bool first) const;
    void DoAbort(TGuard<TSpinLock>& guard, const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TAttachmentsOutputStream)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

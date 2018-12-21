#include "async_stream.h"
#include "scheduler.h"

#include <util/stream/buffered.h>

#include <yt/core/misc/checkpointable_stream.h>
#include <yt/core/misc/checkpointable_stream_block_header.h>
#include <yt/core/misc/serialize.h>

#include <queue>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
TErrorOr<T> WaitForWithStrategy(
    TFuture<T>&& future,
    ESyncStreamAdapterStrategy strategy)
{
    switch (strategy) {
        case ESyncStreamAdapterStrategy::WaitFor:
            return WaitFor(std::move(future));
        case ESyncStreamAdapterStrategy::Get:
            return future.Get();
        default:
            Y_UNREACHABLE();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TSyncInputStreamAdapter
    : public IInputStream
{
public:
    TSyncInputStreamAdapter(
        IAsyncInputStreamPtr underlyingStream,
        ESyncStreamAdapterStrategy strategy)
        : UnderlyingStream_(std::move(underlyingStream))
        , Strategy_(strategy)
    { }

private:
    const IAsyncInputStreamPtr UnderlyingStream_;
    const ESyncStreamAdapterStrategy Strategy_;

    virtual size_t DoRead(void* buffer, size_t length) override
    {
        auto future = UnderlyingStream_->Read(TSharedMutableRef(buffer, length, nullptr));
        return WaitForWithStrategy(std::move(future), Strategy_)
            .ValueOrThrow();
    }
};

std::unique_ptr<IInputStream> CreateSyncAdapter(
    IAsyncInputStreamPtr underlyingStream,
    ESyncStreamAdapterStrategy strategy)
{
    YCHECK(underlyingStream);
    return std::make_unique<TSyncInputStreamAdapter>(
        std::move(underlyingStream),
        strategy);
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncInputStreamAdapter
    : public IAsyncInputStream
{
public:
    TAsyncInputStreamAdapter(
        IInputStream* underlyingStream,
        IInvokerPtr invoker)
        : UnderlyingStream_(underlyingStream)
        , Invoker_(std::move(invoker))
    { }

    virtual TFuture<size_t> Read(const TSharedMutableRef& buffer) override
    {
        return
            BIND(&TAsyncInputStreamAdapter::DoRead, MakeStrong(this), buffer)
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    size_t DoRead(const TSharedMutableRef& buffer) const
    {
        return UnderlyingStream_->Read(buffer.Begin(), buffer.Size());
    }

private:
    IInputStream* const UnderlyingStream_;
    const IInvokerPtr Invoker_;
};

IAsyncInputStreamPtr CreateAsyncAdapter(
    IInputStream* underlyingStream,
    IInvokerPtr invoker)
{
    YCHECK(underlyingStream);
    return New<TAsyncInputStreamAdapter>(underlyingStream, std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

class TSyncBufferedOutputStreamAdapter
    : public IOutputStream
{
public:
    TSyncBufferedOutputStreamAdapter(
        IAsyncOutputStreamPtr underlyingStream,
        ESyncStreamAdapterStrategy strategy,
        size_t bufferCapacity)
        : UnderlyingStream_(underlyingStream)
        , Strategy_(strategy)
        , BufferCapacity_(bufferCapacity)
    {
        Reset();
    }

    friend class TSyncBufferedCheckpointableOutputStreamAdapter;

    virtual ~TSyncBufferedOutputStreamAdapter()
    {
        try {
            Finish();
        } catch (...) {
        }
    }

private:
    const IAsyncOutputStreamPtr UnderlyingStream_;
    const ESyncStreamAdapterStrategy Strategy_;
    const size_t BufferCapacity_;
    size_t CurrentBufferSize_;
    TSharedMutableRef Buffer_;

    struct TBufferTag
    { };

    void Reset()
    {
        CurrentBufferSize_ = 0;
        Buffer_ = TSharedMutableRef::Allocate<TBufferTag>(BufferCapacity_);
    }

    void WriteToBuffer(const void* data, size_t length)
    {
        ::memcpy(Buffer_.Begin() + CurrentBufferSize_, data, length);
        CurrentBufferSize_ += length;
    }

    size_t SpaceLeft() const
    {
        return BufferCapacity_ - CurrentBufferSize_;
    }

    void WriteToStream(const void* data, size_t length)
    {
        auto sharedBuffer = TSharedRef::MakeCopy<TBufferTag>(TRef(data, length));
        auto future = UnderlyingStream_->Write(std::move(sharedBuffer));
        WaitForWithStrategy(std::move(future), Strategy_)
            .ThrowOnError();
    }

    size_t BufferSize() const
    {
        return CurrentBufferSize_;
    }

protected:
    virtual void DoWrite(const void* buffer, size_t length) override
    {
        if (length > SpaceLeft()) {
            DoFlush();
        }

        if (length <= SpaceLeft()) {
            WriteToBuffer(buffer, length);
        } else {
            WriteToStream(buffer, length);
        }
    }

    virtual void DoFlush() override
    {
        if (!CurrentBufferSize_) {
            return;
        }
        auto writeFuture = UnderlyingStream_->Write(Buffer_.Slice(0, CurrentBufferSize_));
        WaitForWithStrategy(std::move(writeFuture), Strategy_)
            .ThrowOnError();
        Reset();
    }

};

std::unique_ptr<IOutputStream> CreateBufferedSyncAdapter(
    IAsyncOutputStreamPtr underlyingStream,
    ESyncStreamAdapterStrategy strategy,
    size_t bufferSize)
{
    YCHECK(underlyingStream);
    return std::make_unique<TSyncBufferedOutputStreamAdapter>(
        std::move(underlyingStream),
        strategy,
        bufferSize);
}

////////////////////////////////////////////////////////////////////////////////

class TSyncBufferedCheckpointableOutputStreamAdapter
    : public ICheckpointableOutputStream
{
public:
    TSyncBufferedCheckpointableOutputStreamAdapter(
        IAsyncOutputStreamPtr underlyingStream,
        ESyncStreamAdapterStrategy strategy,
        size_t bufferCapacity)
        : Adapter_(std::make_unique<TSyncBufferedOutputStreamAdapter>(
            underlyingStream,
            strategy,
            bufferCapacity))
    { }

    virtual void MakeCheckpoint() override
    {
        DoFlush();
        TBlockHeader sentinel{TBlockHeader::CheckpointSentinel};
        Adapter_->WriteToStream(&sentinel, sizeof(sentinel));
    }

    virtual ~TSyncBufferedCheckpointableOutputStreamAdapter()
    {
        try {
            Finish();
        } catch (...) {
        }
    }

protected:
    virtual void DoFlush() override
    {
        if (!Adapter_->BufferSize()) {
            return;
        }
        WriteSize(Adapter_->BufferSize());
        Adapter_->Flush();
    }

    virtual void DoWrite(const void* data, size_t length) override
    {
        if (length > Adapter_->SpaceLeft()) {
            DoFlush();
        }

        if (length > Adapter_->SpaceLeft()) {
            WriteSize(length);
        }

        Adapter_->Write(data, length);
    }

private:
    std::unique_ptr<TSyncBufferedOutputStreamAdapter> Adapter_;

    void WriteSize(size_t size)
    {
        TBlockHeader length{size};
        Adapter_->WriteToStream(&length, sizeof(length));
    }

};

std::unique_ptr<ICheckpointableOutputStream> CreateBufferedCheckpointableSyncAdapter(
    IAsyncOutputStreamPtr underlyingStream,
    ESyncStreamAdapterStrategy strategy,
    size_t bufferSize)
{
    YCHECK(underlyingStream);
    return std::make_unique<TSyncBufferedCheckpointableOutputStreamAdapter>(
        std::move(underlyingStream),
        strategy,
        bufferSize);
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncOutputStreamAdapter
    : public IAsyncOutputStream
{
public:
    TAsyncOutputStreamAdapter(
        IOutputStream* underlyingStream,
        IInvokerPtr invoker)
        : UnderlyingStream_(underlyingStream)
        , Invoker_(std::move(invoker))
    { }

    virtual TFuture<void> Write(const TSharedRef& buffer) override
    {
        return BIND(&TAsyncOutputStreamAdapter::DoWrite, MakeStrong(this), buffer)
            .AsyncVia(Invoker_)
            .Run();
    }

    virtual TFuture<void> Close() override
    {
        return BIND(&TAsyncOutputStreamAdapter::DoFinish, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

private:
    void DoWrite(const TSharedRef& buffer) const
    {
        UnderlyingStream_->Write(buffer.Begin(), buffer.Size());
    }

    void DoFinish() const
    {
        UnderlyingStream_->Finish();
    }

private:
    IOutputStream* const UnderlyingStream_;
    const IInvokerPtr Invoker_;
};

IAsyncOutputStreamPtr CreateAsyncAdapter(
    IOutputStream* underlyingStream,
    IInvokerPtr invoker)
{
    YCHECK(underlyingStream);
    return New<TAsyncOutputStreamAdapter>(underlyingStream, std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef IAsyncZeroCopyInputStream::ReadAll()
{
    struct TTag
    { };

    std::vector<TSharedRef> chunks;

    // TODO(prime@): Add hard limit on body size.
    while (true) {
        auto chunk = WaitFor(Read())
            .ValueOrThrow();

        if (chunk.Empty()) {
            break;
        }

        chunks.emplace_back(TSharedRef::MakeCopy<TTag>(chunk));
    }

    return MergeRefsToRef<TTag>(std::move(chunks));
}

////////////////////////////////////////////////////////////////////////////////

class TZeroCopyInputStreamAdapter
    : public IAsyncZeroCopyInputStream
{
public:
    TZeroCopyInputStreamAdapter(
        IAsyncInputStreamPtr underlyingStream,
        size_t blockSize)
        : UnderlyingStream_(std::move(underlyingStream))
        , BlockSize_(blockSize)
    { }

    virtual TFuture<TSharedRef> Read() override
    {
        struct TZeroCopyInputStreamAdapterBlockTag
        { };

        auto promise = NewPromise<TSharedRef>();
        auto block = TSharedMutableRef::Allocate<TZeroCopyInputStreamAdapterBlockTag>(BlockSize_, false);

        DoRead(promise, std::move(block), 0);

        return promise;
    }

private:
    const IAsyncInputStreamPtr UnderlyingStream_;
    const size_t BlockSize_;


    void DoRead(
        TPromise<TSharedRef> promise,
        TSharedMutableRef block,
        size_t offset)
    {
        if (block.Size() == offset) {
            promise.Set(std::move(block));
            return;
        }

        UnderlyingStream_->Read(block.Slice(offset, block.Size())).Subscribe(
            BIND(
                &TZeroCopyInputStreamAdapter::OnRead,
                MakeStrong(this),
                std::move(promise),
                std::move(block),
                offset));
    }

    void OnRead(
        TPromise<TSharedRef> promise,
        TSharedMutableRef block,
        size_t offset,
        const TErrorOr<size_t>& result)
    {
        if (!result.IsOK()) {
            promise.Set(TError(result));
            return;
        }

        auto bytes = result.Value();

        if (bytes == 0) {
            promise.Set(offset == 0 ? TSharedRef() : block.Slice(0, offset));
            return;
        }

        DoRead(std::move(promise), std::move(block), offset + bytes);
    }
};

IAsyncZeroCopyInputStreamPtr CreateZeroCopyAdapter(
    IAsyncInputStreamPtr underlyingStream,
    size_t blockSize)
{
    YCHECK(underlyingStream);
    return New<TZeroCopyInputStreamAdapter>(underlyingStream, blockSize);
}

////////////////////////////////////////////////////////////////////////////////

class TCopyingInputStreamAdapter
    : public IAsyncInputStream
{
public:
    explicit TCopyingInputStreamAdapter(IAsyncZeroCopyInputStreamPtr underlyingStream)
        : UnderlyingStream_(underlyingStream)
    {
        YCHECK(UnderlyingStream_);
    }

    virtual TFuture<size_t> Read(const TSharedMutableRef& buffer) override
    {
        if (CurrentBlock_) {
            // NB(psushin): no swapping here, it's a _copying_ adapter!
            // Also, #buffer may be constructed via FromNonOwningRef.
            return MakeFuture<size_t>(DoCopy(buffer));
        } else {
            return UnderlyingStream_->Read().Apply(
                BIND(&TCopyingInputStreamAdapter::OnRead, MakeStrong(this), buffer));
        }
    }

private:
    const IAsyncZeroCopyInputStreamPtr UnderlyingStream_;

    TSharedRef CurrentBlock_;
    i64 CurrentOffset_ = 0;


    size_t DoCopy(const TMutableRef& buffer)
    {
        size_t remaining = CurrentBlock_.Size() - CurrentOffset_;
        size_t bytes = std::min(buffer.Size(), remaining);
        ::memcpy(buffer.Begin(), CurrentBlock_.Begin() + CurrentOffset_, bytes);
        CurrentOffset_ += bytes;
        if (CurrentOffset_ == CurrentBlock_.Size()) {
            CurrentBlock_.Reset();
            CurrentOffset_ = 0;
        }
        return bytes;
    }

    size_t OnRead(const TSharedMutableRef& buffer, const TSharedRef& block)
    {
        CurrentBlock_ = block;
        return DoCopy(buffer);
    }

};

IAsyncInputStreamPtr CreateCopyingAdapter(IAsyncZeroCopyInputStreamPtr underlyingStream)
{
    return New<TCopyingInputStreamAdapter>(underlyingStream);
}

////////////////////////////////////////////////////////////////////////////////

class TZeroCopyOutputStreamAdapter
    : public IAsyncZeroCopyOutputStream
{
public:
    explicit TZeroCopyOutputStreamAdapter(IAsyncOutputStreamPtr underlyingStream)
        : UnderlyingStream_(underlyingStream)
    {
        YCHECK(UnderlyingStream_);
    }

    virtual TFuture<void> Write(const TSharedRef& data) override
    {
        Y_ASSERT(data);
        return Push(data);
    }

    virtual TFuture<void> Close() override
    {
        return Push(TSharedRef());
    }

private:
    const IAsyncOutputStreamPtr UnderlyingStream_;

    struct TEntry
    {
        // If `Block' is null it means close was requested.
        TSharedRef Block;
        TPromise<void> Promise;
    };

    TSpinLock SpinLock_;
    std::queue<TEntry> Queue_;
    TError Error_;
    bool Closed_ = false;

    TFuture<void> Push(const TSharedRef& data)
    {
        TPromise<void> promise;
        bool needInvoke;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            YCHECK(!Closed_);
            if (!Error_.IsOK()) {
                return MakeFuture(Error_);
            }
            promise = NewPromise<void>();
            Queue_.push(TEntry{data, promise});
            needInvoke = (Queue_.size() == 1);
            Closed_ = !data;
        }
        if (needInvoke) {
            TFuture<void> invokeResult;
            if (data) {
                invokeResult = UnderlyingStream_->Write(data);
            } else {
                invokeResult = UnderlyingStream_->Close();
            }
            invokeResult.Subscribe(
                BIND(&TZeroCopyOutputStreamAdapter::OnWritten, MakeStrong(this)));
        }
        return promise;
    }

    void OnWritten(const TError& error)
    {
        TSharedRef data;
        bool hasData = NotifyAndFetchNext(error, &data);
        while (hasData) {
            TFuture<void> result;
            if (data) {
                result = UnderlyingStream_->Write(data);
            } else {
                result = UnderlyingStream_->Close();
            }
            auto mayWriteResult = result.TryGet();
            if (!mayWriteResult || !mayWriteResult->IsOK()) {
                result.Subscribe(
                    BIND(&TZeroCopyOutputStreamAdapter::OnWritten, MakeStrong(this)));
                break;
            }
            hasData = NotifyAndFetchNext(TError(), &data);
        }
    }

    // Set current entry promise to error and tries to fetch next entry data.
    // Return `false' if there no next entry.
    // Otherwise return `true' and fill data with next entry Block.
    bool NotifyAndFetchNext(const TError& error, TSharedRef* data)
    {
        TPromise<void> promise;
        bool hasData = false;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            auto& entry = Queue_.front();
            promise = std::move(entry.Promise);
            if (!error.IsOK() && Error_.IsOK()) {
                Error_ = error;
            }
            Queue_.pop();
            hasData = !Queue_.empty();
            if (hasData) {
                *data = Queue_.front().Block;
            }
        }
        promise.Set(error);
        return hasData;
    }
};

IAsyncZeroCopyOutputStreamPtr CreateZeroCopyAdapter(IAsyncOutputStreamPtr underlyingStream)
{
    return New<TZeroCopyOutputStreamAdapter>(underlyingStream);
}

////////////////////////////////////////////////////////////////////////////////

class TCopyingOutputStreamAdapter
    : public IAsyncOutputStream
{
public:
    explicit TCopyingOutputStreamAdapter(IAsyncZeroCopyOutputStreamPtr underlyingStream)
        : UnderlyingStream_(underlyingStream)
    {
        YCHECK(UnderlyingStream_);
    }

    virtual TFuture<void> Write(const TSharedRef& buffer) override
    {
        struct TCopyingOutputStreamAdapterBlockTag { };
        auto block = TSharedMutableRef::Allocate<TCopyingOutputStreamAdapterBlockTag>(buffer.Size(), false);
        ::memcpy(block.Begin(), buffer.Begin(), buffer.Size());
        return UnderlyingStream_->Write(block);
    }

    virtual TFuture<void> Close() override
    {
        return UnderlyingStream_->Close();
    }

private:
    const IAsyncZeroCopyOutputStreamPtr UnderlyingStream_;
};

IAsyncOutputStreamPtr CreateCopyingAdapter(IAsyncZeroCopyOutputStreamPtr underlyingStream)
{
    return New<TCopyingOutputStreamAdapter>(underlyingStream);
}

////////////////////////////////////////////////////////////////////////////////

class TPrefetchingInputStreamAdapter
    : public IAsyncZeroCopyInputStream
{
public:
    TPrefetchingInputStreamAdapter(
        IAsyncZeroCopyInputStreamPtr underlyingStream,
        size_t windowSize)
        : UnderlyingStream_(underlyingStream)
        , WindowSize_(windowSize)
    {
        YCHECK(UnderlyingStream_);
        YCHECK(WindowSize_ > 0);
    }

    virtual TFuture<TSharedRef> Read() override
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (!Error_.IsOK()) {
            return MakeFuture<TSharedRef>(Error_);
        }
        if (PrefetchedBlocks_.empty()) {
            return Prefetch(&guard).Apply(
                BIND(&TPrefetchingInputStreamAdapter::OnPrefetched, MakeStrong(this)));
        }
        return MakeFuture<TSharedRef>(PopBlock(&guard));
    }

private:
    const IAsyncZeroCopyInputStreamPtr UnderlyingStream_;
    const size_t WindowSize_;

    TSpinLock SpinLock_;
    TError Error_;
    std::queue<TSharedRef> PrefetchedBlocks_;
    size_t PrefetchedSize_ = 0;
    TFuture<void> OutstandingResult_;


    TFuture<void> Prefetch(TGuard<TSpinLock>* guard)
    {
        if (OutstandingResult_) {
            guard->Release();
            return OutstandingResult_;
        }
        auto promise = NewPromise<void>();
        OutstandingResult_ = promise;
        guard->Release();
        UnderlyingStream_->Read().Subscribe(BIND(
            &TPrefetchingInputStreamAdapter::OnRead,
            MakeStrong(this),
            promise));
        return promise;
    }

    void OnRead(TPromise<void> promise, const TErrorOr<TSharedRef>& result)
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            PushBlock(&guard, result);
        }
        promise.Set(result);
    }

    TSharedRef OnPrefetched()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        return PopBlock(&guard);
    }

    void PushBlock(TGuard<TSpinLock>* guard, const TErrorOr<TSharedRef>& result)
    {
        Y_ASSERT(OutstandingResult_);
        OutstandingResult_.Reset();
        if (!result.IsOK()) {
            Error_ = TError(result);
            return;
        }
        const auto& block = result.Value();
        PrefetchedBlocks_.push(block);
        PrefetchedSize_ += block.Size();
        if (block && PrefetchedSize_ < WindowSize_) {
            Prefetch(guard);
        }
    }

    TSharedRef PopBlock(TGuard<TSpinLock>* guard)
    {
        Y_ASSERT(!PrefetchedBlocks_.empty());
        auto block = PrefetchedBlocks_.front();
        PrefetchedBlocks_.pop();
        PrefetchedSize_ -= block.Size();
        if (!OutstandingResult_ && PrefetchedSize_ < WindowSize_) {
            Prefetch(guard);
        }
        return block;
    }

};

IAsyncZeroCopyInputStreamPtr CreatePrefetchingAdapter(
    IAsyncZeroCopyInputStreamPtr underlyingStream,
    size_t windowSize)
{
    return New<TPrefetchingInputStreamAdapter>(underlyingStream, windowSize);
}

////////////////////////////////////////////////////////////////////////////////

struct TBufferingInputStreamAdapterBufferTag { };

class TBufferingInputStreamAdapter
    : public IAsyncZeroCopyInputStream
{
public:
    TBufferingInputStreamAdapter(
        IAsyncInputStreamPtr underlyingStream,
        size_t windowSize)
        : UnderlyingStream_(underlyingStream)
        , WindowSize_(windowSize)
    {
        YCHECK(UnderlyingStream_);
        YCHECK(WindowSize_ > 0);

        Buffer_ = TSharedMutableRef::Allocate<TBufferingInputStreamAdapterBufferTag>(WindowSize_, false);
    }

    virtual TFuture<TSharedRef> Read() override
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (PrefetchedSize_ == 0) {
            if (EndOfStream_) {
                return MakeFuture<TSharedRef>(TSharedRef());
            }
            if (!Error_.IsOK()) {
                return MakeFuture<TSharedRef>(Error_);
            }
            return Prefetch(&guard).Apply(
                BIND(&TBufferingInputStreamAdapter::OnPrefetched, MakeStrong(this)));
        }
        return MakeFuture<TSharedRef>(CopyPrefetched(&guard));
    }

private:
    const IAsyncInputStreamPtr UnderlyingStream_;
    const size_t WindowSize_;

    TSpinLock SpinLock_;
    TError Error_;
    TSharedMutableRef Prefetched_;
    TSharedMutableRef Buffer_;
    size_t PrefetchedSize_ = 0;
    bool EndOfStream_ = false;
    TFuture<void> OutstandingResult_;

    TFuture<void> Prefetch(TGuard<TSpinLock>* guard)
    {
        if (OutstandingResult_) {
            guard->Release();
            return OutstandingResult_;
        }
        auto promise = NewPromise<void>();
        OutstandingResult_ = promise;
        guard->Release();
        UnderlyingStream_->Read(Buffer_.Slice(0, WindowSize_ - PrefetchedSize_)).Subscribe(BIND(
            &TBufferingInputStreamAdapter::OnRead,
            MakeStrong(this),
            promise));
        return promise;
    }

    void OnRead(TPromise<void> promise, const TErrorOr<size_t>& result)
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            AppendPrefetched(&guard, result);
        }
        promise.Set(result);
    }

    TSharedRef OnPrefetched()
    {
        TGuard<TSpinLock> guard(SpinLock_);
        Y_ASSERT(PrefetchedSize_ != 0);
        return CopyPrefetched(&guard);
    }

    void AppendPrefetched(TGuard<TSpinLock>* guard, const TErrorOr<size_t>& result)
    {
        Y_ASSERT(OutstandingResult_);
        OutstandingResult_.Reset();
        if (!result.IsOK()) {
            Error_ = TError(result);
            return;
        } else if (result.Value() == 0) {
            EndOfStream_ = true;
            return;
        }
        size_t bytes = result.Value();
        if (bytes != 0) {
            if (PrefetchedSize_ == 0) {
                Prefetched_ = Buffer_;
                Buffer_ = TSharedMutableRef::Allocate<TBufferingInputStreamAdapterBufferTag>(WindowSize_, false);
            } else {
                ::memcpy(Prefetched_.Begin() + PrefetchedSize_, Buffer_.Begin(), bytes);
            }
            PrefetchedSize_ += bytes;

        }
        // Stop reading on the end of stream or full buffer.
        if (bytes != 0 && PrefetchedSize_ < WindowSize_) {
            Prefetch(guard);
        }
    }

    TSharedRef CopyPrefetched(TGuard<TSpinLock>* guard)
    {
        Y_ASSERT(PrefetchedSize_ != 0);
        auto block = Prefetched_.Slice(0, PrefetchedSize_);
        Prefetched_ = TSharedMutableRef();
        PrefetchedSize_ = 0;
        if (!OutstandingResult_) {
            Prefetch(guard);
        }
        return block;
    }

};

IAsyncZeroCopyInputStreamPtr CreateBufferingAdapter(
    IAsyncInputStreamPtr underlyingStream,
    size_t windowSize)
{
    return New<TBufferingInputStreamAdapter>(underlyingStream, windowSize);
}

////////////////////////////////////////////////////////////////////////////////

class TExpiringInputStreamAdapter
    : public IAsyncZeroCopyInputStream
{
public:
    TExpiringInputStreamAdapter(
        IAsyncZeroCopyInputStreamPtr underlyingStream,
        TDuration timeout)
        : UnderlyingStream_(underlyingStream)
        , Timeout_(timeout)
    {
        YCHECK(UnderlyingStream_);
        YCHECK(Timeout_ > TDuration::Zero());
    }

    virtual TFuture<TSharedRef> Read() override
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (PendingBlock_) {
            auto block = std::move(PendingBlock_);
            PendingBlock_ = std::optional<TErrorOr<TSharedRef>>();

            return MakeFuture<TSharedRef>(*block);
        }

        auto promise = NewPromise<TSharedRef>();
        Cookie_ = TDelayedExecutor::Submit(
            BIND(&TExpiringInputStreamAdapter::OnTimeout, MakeWeak(this), promise), Timeout_);

        Y_ASSERT(!Promise_);
        Promise_ = promise;

        if (!Fetching_) {
            Fetching_ = true;
            guard.Release();

            UnderlyingStream_->Read().Subscribe(
                BIND(&TExpiringInputStreamAdapter::OnRead, MakeWeak(this)));
        }
        return promise;
    }

private:
    const IAsyncZeroCopyInputStreamPtr UnderlyingStream_;
    const TDuration Timeout_;

    TSpinLock SpinLock_;

    bool Fetching_ = false;
    std::optional<TErrorOr<TSharedRef>> PendingBlock_;
    TPromise<TSharedRef> Promise_;
    TDelayedExecutorCookie Cookie_;

    void OnRead(const TErrorOr<TSharedRef>& value)
    {
        TPromise<TSharedRef> promise;
        TGuard<TSpinLock> guard(SpinLock_);
        Fetching_ = false;
        if (Promise_) {
            swap(Promise_, promise);
            TDelayedExecutor::CancelAndClear(Cookie_);
            guard.Release();
            promise.Set(value);
        } else {
            PendingBlock_ = value;
        }

    }

    void OnTimeout(TPromise<TSharedRef> promise, bool aborted)
    {
        bool timedOut = false;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (promise == Promise_) {
                Promise_ = TPromise<TSharedRef>();
                timedOut = true;
            }
        }

        if (timedOut) {
            TError error;
            if (aborted) {
                error = TError(NYT::EErrorCode::Canceled, "Operation aborted");
            } else {
                error = TError(NYT::EErrorCode::Timeout, "Operation timed out")
                    << TErrorAttribute("timeout", Timeout_);
            }
            promise.Set(error);
        }
    }
};

IAsyncZeroCopyInputStreamPtr CreateExpiringAdapter(
    IAsyncZeroCopyInputStreamPtr underlyingStream,
    TDuration timeout)
{
    return New<TExpiringInputStreamAdapter>(underlyingStream, timeout);
}

////////////////////////////////////////////////////////////////////////////////

class TConcurrentInputStreamAdapter
    : public IAsyncZeroCopyInputStream
{
public:
    TConcurrentInputStreamAdapter(
        IAsyncZeroCopyInputStreamPtr underlyingStream)
        : UnderlyingStream_(underlyingStream)
    {
        YCHECK(UnderlyingStream_);
    }

    virtual TFuture<TSharedRef> Read() override
    {
        TGuard<TSpinLock> guard(SpinLock_);

        if (PendingBlock_) {
            auto block = std::move(PendingBlock_);
            PendingBlock_ = std::optional<TErrorOr<TSharedRef>>();

            return MakeFuture<TSharedRef>(*block);
        }

        auto newPromise = NewPromise<TSharedRef>();
        auto oldPromise = newPromise;
        swap(oldPromise, Promise_);

        if (!Fetching_) {
            Fetching_ = true;
            guard.Release();

            UnderlyingStream_->Read().Subscribe(
                BIND(&TConcurrentInputStreamAdapter::OnRead, MakeWeak(this)));
        }
        // Always set the pending promise from previous Read.
        if (oldPromise) {
            guard.Release();
            oldPromise.TrySet(TError(NYT::EErrorCode::Canceled, "Read canceled"));
        }
        return newPromise;
    }

private:
    const IAsyncZeroCopyInputStreamPtr UnderlyingStream_;

    TSpinLock SpinLock_;

    bool Fetching_ = false;
    std::optional<TErrorOr<TSharedRef>> PendingBlock_;
    TPromise<TSharedRef> Promise_;

    void OnRead(const TErrorOr<TSharedRef>& value)
    {
        TPromise<TSharedRef> promise;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            Fetching_ = false;
            Y_ASSERT(Promise_);
            swap(promise, Promise_);
            if (promise.IsSet()) {
                Y_ASSERT(!PendingBlock_);
                PendingBlock_ = value;
                return;
            }
        }
        promise.Set(value);
    }
};

IAsyncZeroCopyInputStreamPtr CreateConcurrentAdapter(
    IAsyncZeroCopyInputStreamPtr underlyingStream)
{
    return New<TConcurrentInputStreamAdapter>(underlyingStream);
}

////////////////////////////////////////////////////////////////////////////////

void PipeInputToOutput(
    NConcurrency::IAsyncZeroCopyInputStreamPtr input,
    NConcurrency::IAsyncOutputStreamPtr output)
{
    while (true) {
        auto asyncBlock = input->Read();
        auto block = WaitFor(asyncBlock)
            .ValueOrThrow();
        if (!block || block.Empty()) {
            break;
        }
        WaitFor(output->Write(block))
            .ThrowOnError();
    }

    WaitFor(output->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

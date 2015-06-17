#include "stdafx.h"
#include "async_stream.h"
#include "scheduler.h"

#include <queue>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
TErrorOr<T> WaitForWithStrategy(TFuture<T> future, ESyncStreamAdapterStrategy strategy)
{
    switch (strategy) {

        case ESyncStreamAdapterStrategy::WaitFor:
            return WaitFor(future);
        case ESyncStreamAdapterStrategy::Get:
            return future.Get();
        default:
            YUNREACHABLE();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TSyncInputStreamAdapter
    : public TInputStream
{
public:
    TSyncInputStreamAdapter(
        IAsyncInputStreamPtr underlyingStream,
        ESyncStreamAdapterStrategy strategy)
        : UnderlyingStream_(underlyingStream)
        , Strategy_(strategy)
    {
        YCHECK(UnderlyingStream_);
    }

    virtual ~TSyncInputStreamAdapter() throw()
    { }

private:
    const IAsyncInputStreamPtr UnderlyingStream_;
    const ESyncStreamAdapterStrategy Strategy_;


    virtual size_t DoRead(void* buf, size_t len) override
    {
        auto buffer = TSharedMutableRef(buf, len, nullptr);
        return WaitForWithStrategy(UnderlyingStream_->Read(buffer), Strategy_)
            .ValueOrThrow();
    }

};

std::unique_ptr<TInputStream> CreateSyncAdapter(
    IAsyncInputStreamPtr underlyingStream,
    ESyncStreamAdapterStrategy strategy)
{
    return std::unique_ptr<TInputStream>(new TSyncInputStreamAdapter(
        underlyingStream,
        strategy));
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncInputStreamAdapter
    : public IAsyncInputStream
{
public:
    explicit TAsyncInputStreamAdapter(TInputStream* underlyingStream)
        : UnderlyingStream_(underlyingStream)
    {
        YCHECK(UnderlyingStream_);
    }
    
    virtual TFuture<size_t> Read(const TSharedMutableRef& buffer) override
    {
        if (Failed_) {
            return Result_;
        }

        try {
            return MakeFuture<size_t>(UnderlyingStream_->Read(buffer.Begin(), buffer.Size()));
        } catch (const std::exception& ex) {
            Result_ = MakeFuture<size_t>(TError(ex));
            Failed_ = true;
            return Result_;
        }
    }
    
private:
    TInputStream* const UnderlyingStream_;

    TFuture<size_t> Result_;
    bool Failed_ = false;

};

IAsyncInputStreamPtr CreateAsyncAdapter(TInputStream* asyncStream)
{
    return New<TAsyncInputStreamAdapter>(asyncStream);
}

////////////////////////////////////////////////////////////////////////////////

class TSyncOutputStreamAdapter
    : public TOutputStream
{
public:
    TSyncOutputStreamAdapter(
        IAsyncOutputStreamPtr underlyingStream,
        ESyncStreamAdapterStrategy strategy)
        : UnderlyingStream_(underlyingStream)
        , Strategy_(strategy)
    {
        YCHECK(UnderlyingStream_);
    }

    virtual ~TSyncOutputStreamAdapter() throw()
    { }

private:
    const IAsyncOutputStreamPtr UnderlyingStream_;
    const ESyncStreamAdapterStrategy Strategy_;

    virtual void DoWrite(const void* buf, size_t len) override
    {
        auto buffer = TSharedRef(buf, len, nullptr);
        WaitForWithStrategy(UnderlyingStream_->Write(buffer), Strategy_)
            .ThrowOnError();
    }
};

std::unique_ptr<TOutputStream> CreateSyncAdapter(
    IAsyncOutputStreamPtr underlyingStream,
    ESyncStreamAdapterStrategy strategy)
{
    return std::unique_ptr<TOutputStream>(new TSyncOutputStreamAdapter(
        underlyingStream,
        strategy));
}

////////////////////////////////////////////////////////////////////////////////

class TAsyncOutputStreamAdapter
    : public IAsyncOutputStream
{
public:
    explicit TAsyncOutputStreamAdapter(TOutputStream* underlyingStream)
        : UnderlyingStream_(underlyingStream)
    {
        YCHECK(UnderlyingStream_);
    }
    
    virtual TFuture<void> Write(const TSharedRef& buffer) override
    {
        if (Failed_) {
            return Result_;
        }

        try {
            UnderlyingStream_->Write(buffer.Begin(), buffer.Size());
        } catch (const std::exception& ex) {
            Result_ = MakeFuture<void>(TError(ex));
            Failed_ = true;
            return Result_;
        }
        return VoidFuture;
    }

private:
    TOutputStream* const UnderlyingStream_;

    TFuture<void> Result_;
    bool Failed_ = false;

};

IAsyncOutputStreamPtr CreateAsyncAdapter(TOutputStream* underlyingStream)
{
    return New<TAsyncOutputStreamAdapter>(underlyingStream);
}

////////////////////////////////////////////////////////////////////////////////

class TZeroCopyInputStreamAdapter
    : public IAsyncZeroCopyInputStream
{
public:
    TZeroCopyInputStreamAdapter(
        IAsyncInputStreamPtr underlyingStream,
        size_t blockSize)
        : UnderlyingStream_(underlyingStream)
        , BlockSize_(blockSize)
    {
        YCHECK(UnderlyingStream_);
        YCHECK(BlockSize_ > 0);
    }

    virtual TFuture<TSharedRef> Read() override
    {
        struct TZeroCopyInputStreamAdapterBlockTag { };
        auto block = TSharedMutableRef::Allocate<TZeroCopyInputStreamAdapterBlockTag>(BlockSize_, false);
        auto promise = NewPromise<TSharedRef>();
        DoRead(promise, block, 0);
        return promise;
    }

private:
    const IAsyncInputStreamPtr UnderlyingStream_;
    const size_t BlockSize_;


    void DoRead(
        TPromise<TSharedRef> promise,
        const TSharedMutableRef& block,
        size_t offset)
    {
        if (block.Size() == offset) {
            promise.Set(block);
            return;
        }

        auto slice = block.Slice(offset, block.Size());
        UnderlyingStream_->Read(slice).Subscribe(
            BIND(&TZeroCopyInputStreamAdapter::OnRead, MakeStrong(this), promise, block, offset));
    }

    void OnRead(
        TPromise<TSharedRef> promise,
        const TSharedMutableRef& block,
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

        DoRead(promise, block, offset + bytes);
    }

};

IAsyncZeroCopyInputStreamPtr CreateZeroCopyAdapter(
    IAsyncInputStreamPtr underlyingStream,
    size_t blockSize)
{
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
        YASSERT(data);
        TPromise<void> promise;
        bool invokeWrite;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (!Error_.IsOK()) {
                return MakeFuture(Error_);
            }
            promise = NewPromise<void>();
            Queue_.push(TEntry{data, promise});
            invokeWrite = (Queue_.size() == 1);
        }
        if (invokeWrite) {
            UnderlyingStream_->Write(data).Subscribe(
                BIND(&TZeroCopyOutputStreamAdapter::OnWritten, MakeStrong(this)));
        }
        return promise;
    }

private:
    const IAsyncOutputStreamPtr UnderlyingStream_;

    struct TEntry
    {
        TSharedRef Block;
        TPromise<void> Promise;
    };

    TSpinLock SpinLock_;
    std::queue<TEntry> Queue_;
    TError Error_;


    void OnWritten(const TError& error)
    {
        auto pendingBlock = NotifyAndFetchNext(error);
        while (pendingBlock) {
            auto asyncWriteResult = UnderlyingStream_->Write(pendingBlock);
            auto mayWriteResult = asyncWriteResult.TryGet();
            if (!mayWriteResult || !mayWriteResult->IsOK()) {
                asyncWriteResult.Subscribe(
                    BIND(&TZeroCopyOutputStreamAdapter::OnWritten, MakeStrong(this)));
                break;
            }

            pendingBlock = NotifyAndFetchNext(TError());
        }
    }

    TSharedRef NotifyAndFetchNext(const TError& error)
    {
        TPromise<void> promise;
        TSharedRef pendingBlock;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            auto& entry = Queue_.front();
            promise = std::move(entry.Promise);
            if (!error.IsOK() && Error_.IsOK()) {
                Error_ = error;
            }
            Queue_.pop();
            if (!Queue_.empty()) {
                pendingBlock = Queue_.front().Block;
            }
        }
        promise.Set(error);
        return pendingBlock;
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
    explicit TPrefetchingInputStreamAdapter(
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
        YASSERT(OutstandingResult_);
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
        YASSERT(!PrefetchedBlocks_.empty());
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

} // namespace NConcurrency
} // namespace NYT

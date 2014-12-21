#include "stdafx.h"
#include "async_stream.h"
#include "scheduler.h"

#include <queue>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TSyncInputStreamAdapter
    : public TInputStream
{
public:
    explicit TSyncInputStreamAdapter(IAsyncInputStreamPtr underlyingStream)
        : UnderlyingStream_(underlyingStream)
    {
        YCHECK(UnderlyingStream_);
    }

    virtual ~TSyncInputStreamAdapter() throw()
    { }

private:
    IAsyncInputStreamPtr UnderlyingStream_;


    virtual size_t DoRead(void* buf, size_t len) override
    {
        auto result = WaitFor(UnderlyingStream_->Read(buf, len));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
        return result.Value();
    }

};

std::unique_ptr<TInputStream> CreateSyncAdapter(IAsyncInputStreamPtr underlyingStream)
{
    return std::unique_ptr<TInputStream>(new TSyncInputStreamAdapter(underlyingStream));
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
    
    virtual TFuture<TErrorOr<size_t>> Read(void* buf, size_t len) override
    {
        if (Failed_) {
            return Result_;
        }

        try {
            return MakeFuture<TErrorOr<size_t>>(UnderlyingStream_->Read(buf, len));
        } catch (const std::exception& ex) {
            Result_ = MakeFuture<TErrorOr<size_t>>(ex);
            Failed_ = true;
            return Result_;
        }
    }
    
private:
    TInputStream* UnderlyingStream_;

    TFuture<TErrorOr<size_t>> Result_;
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
    explicit TSyncOutputStreamAdapter(IAsyncOutputStreamPtr underlyingStream)
        : UnderlyingStream_(underlyingStream)
    {
        YCHECK(UnderlyingStream_);
    }

    virtual ~TSyncOutputStreamAdapter() throw()
    { }

private:
    IAsyncOutputStreamPtr UnderlyingStream_;


    virtual void DoWrite(const void* buf, size_t len) override
    {
        auto result = WaitFor(UnderlyingStream_->Write(buf, len));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }

};

std::unique_ptr<TOutputStream> CreateSyncAdapter(IAsyncOutputStreamPtr underlyingStream)
{
    return std::unique_ptr<TOutputStream>(new TSyncOutputStreamAdapter(underlyingStream));
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
    
    virtual TAsyncError Write(const void* buf, size_t len) override
    {
        if (Failed_) {
            return Result_;
        }

        try {
            UnderlyingStream_->Write(buf, len);
        } catch (const std::exception& ex) {
            Result_ = MakeFuture<TError>(ex);
            Failed_ = true;
            return Result_;
        }
        return OKFuture;
    }

private:
    TOutputStream* UnderlyingStream_;

    TAsyncError Result_;
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

    virtual TFuture<TErrorOr<TSharedRef>> Read() override
    {
        struct TZeroCopyInputStreamAdapterBlockTag { };
        auto block = TSharedRef::Allocate<TZeroCopyInputStreamAdapterBlockTag>(BlockSize_, false);
        auto promise = NewPromise<TErrorOr<TSharedRef>>();
        DoRead(promise, block, 0);
        return promise;
    }

private:
    IAsyncInputStreamPtr UnderlyingStream_;
    size_t BlockSize_;


    void DoRead(
        TPromise<TErrorOr<TSharedRef>> promise,
        TSharedRef block,
        size_t offset)
    {
        if (block.Size() == offset) {
            promise.Set(block);
            return;
        }

        UnderlyingStream_->Read(block.Begin() + offset, block.Size() - offset).Subscribe(
            BIND(&TZeroCopyInputStreamAdapter::OnRead, MakeStrong(this), promise, block, offset));
    }

    void OnRead(
        TPromise<TErrorOr<TSharedRef>> promise,
        TSharedRef block,
        size_t offset,
        const TErrorOr<size_t>& result)
    {
        if (!result.IsOK()) {
            promise.Set(TError(result));
            return;
        }

        auto bytes = result.Value();
        if (bytes == 0) {
            promise.Set(offset == 0 ? TSharedRef() : block.Slice(TRef(block.Begin(), offset)));
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

    virtual TFuture<TErrorOr<size_t>> Read(void* buf, size_t len) override
    {
        if (CurrentBlock_) {
            return MakeFuture<TErrorOr<size_t>>(DoCopy(buf, len));
        } else {
            return UnderlyingStream_->Read().Apply(
                BIND(&TCopyingInputStreamAdapter::OnRead, MakeStrong(this), buf, len));
        }
    }

private:
    IAsyncZeroCopyInputStreamPtr UnderlyingStream_;

    TSharedRef CurrentBlock_;
    i64 CurrentOffset_ = 0;


    size_t DoCopy(void* buf, size_t len)
    {
        size_t remaining = CurrentBlock_.Size() - CurrentOffset_;
        size_t bytes = std::min(len, remaining);
        ::memcpy(buf, CurrentBlock_.Begin() + CurrentOffset_, bytes);
        CurrentOffset_ += bytes;
        if (CurrentOffset_ == CurrentBlock_.Size()) {
            CurrentBlock_.Reset();
            CurrentOffset_ = 0;
        }
        return bytes;
    }

    TErrorOr<size_t> OnRead(void* buf, size_t len, const TErrorOr<TSharedRef>& result)
    {
        if (!result.IsOK()) {
            return TError(result);
        }

        CurrentBlock_ = result.Value();
        return DoCopy(buf, len);
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

    virtual TAsyncError Write(const TSharedRef& data) override
    {
        YASSERT(data);
        TPromise<TError> promise;
        bool invokeWrite;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (!Error_.IsOK()) {
                return MakeFuture(Error_);
            }
            promise = NewPromise<TError>();
            Queue_.push(TEntry{data, promise});
            invokeWrite = (Queue_.size() == 1);
        }
        if (invokeWrite) {
            WriteMore(data);
        }
        return promise;
    }

private:
    IAsyncOutputStreamPtr UnderlyingStream_;

    struct TEntry
    {
        TSharedRef Block;
        TPromise<TError> Promise;
    };

    TSpinLock SpinLock_;
    std::queue<TEntry> Queue_;
    TError Error_;


    void WriteMore(const TSharedRef& data)
    {
        UnderlyingStream_->Write(data.Begin(), data.Size()).Subscribe(
            BIND(&TZeroCopyOutputStreamAdapter::OnWritten, MakeStrong(this)));
    }

    void OnWritten(const TError& error)
    {
        TPromise<TError> promise;
        TSharedRef pendingData;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            auto& entry = Queue_.front();
            promise = std::move(entry.Promise);
            if (!error.IsOK() && Error_.IsOK()) {
                Error_ = error;
            }
            Queue_.pop();
            if (!Queue_.empty()) {
                pendingData = Queue_.front().Block;
            }
        }
        promise.Set(error);
        if (pendingData) {
            WriteMore(pendingData);
        }
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

    virtual TAsyncError Write(const void* buf, size_t len) override
    {
        struct TCopyingOutputStreamAdapterBlockTag { };
        auto block = TSharedRef::Allocate<TCopyingOutputStreamAdapterBlockTag>(len, false);
        ::memcpy(block.Begin(), buf, len);
        return UnderlyingStream_->Write(block);
    }

private:
    IAsyncZeroCopyOutputStreamPtr UnderlyingStream_;

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

    virtual TFuture<TErrorOr<TSharedRef>> Read() override
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (!Error_.IsOK()) {
            return MakeFuture<TErrorOr<TSharedRef>>(Error_);
        }
        if (PrefetchedBlocks_.empty()) {
            return Prefetch(&guard).Apply(
                BIND(&TPrefetchingInputStreamAdapter::OnPrefetched, MakeStrong(this)));
        }
        return MakeFuture<TErrorOr<TSharedRef>>(PopBlock(&guard));
    }

private:
    IAsyncZeroCopyInputStreamPtr UnderlyingStream_;
    size_t WindowSize_;

    TSpinLock SpinLock_;
    TError Error_;
    std::queue<TSharedRef> PrefetchedBlocks_;
    size_t PrefetchedSize_ = 0;
    TAsyncError OutstandingResult_;


    TAsyncError Prefetch(TGuard<TSpinLock>* guard)
    {
        if (OutstandingResult_) {
            return OutstandingResult_;
        }
        auto promise = NewPromise<TError>();
        OutstandingResult_ = promise;
        guard->Release();
        UnderlyingStream_->Read().Subscribe(BIND(
            &TPrefetchingInputStreamAdapter::OnRead,
            MakeStrong(this),
            promise));
        return promise;
    }

    void OnRead(TPromise<TError> promise, const TErrorOr<TSharedRef>& result)
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            PushBlock(&guard, result);
        }
        promise.Set(result);
    }

    TErrorOr<TSharedRef> OnPrefetched(const TError& error)
    {
        if (!error.IsOK()) {
            return error;
        }
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

    TErrorOr<TSharedRef> PopBlock(TGuard<TSpinLock>* guard)
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

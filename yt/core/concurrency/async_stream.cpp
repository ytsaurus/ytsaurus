#include "stdafx.h"
#include "async_stream.h"

#include "scheduler.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TSyncInputStream
    : public TInputStream
{
public:
    explicit TSyncInputStream(IAsyncInputStreamPtr asyncStream)
        : AsyncStream_(asyncStream)
    { }

    virtual size_t DoRead(void* buf, size_t len) override
    {
        auto result = WaitFor(AsyncStream_->Read(buf, len));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
        return result.Value();
    }

    virtual ~TSyncInputStream() throw()
    { }

private:
    IAsyncInputStreamPtr AsyncStream_;

};

} // namespace

std::unique_ptr<TInputStream> CreateSyncInputStream(IAsyncInputStreamPtr asyncStream)
{
    return std::unique_ptr<TInputStream>(new TSyncInputStream(asyncStream));
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TInputStreamAsyncWrapper
    : public IAsyncInputStream
{
public:
    explicit TInputStreamAsyncWrapper(TInputStream* inputStream)
        : InputStream_(inputStream)
        , Failed_(false)
    { }
    
    virtual TFuture<TErrorOr<size_t>> Read(void* buf, size_t len) override
    {
        if (Failed_) {
            return Result_;
        }

        try {
            return MakeFuture<TErrorOr<size_t>>(InputStream_->Read(buf, len));
        } catch (const std::exception& ex) {
            Result_ = MakeFuture<TErrorOr<size_t>>(TError("Failed reading from stream") << ex);
            Failed_ = true;
            return Result_;
        }
    }
    
    
private:
    TInputStream* InputStream_;

    TFuture<TErrorOr<size_t>> Result_;
    bool Failed_;
};

} // namespace

IAsyncInputStreamPtr CreateAsyncInputStream(TInputStream* asyncStream)
{
    return New<TInputStreamAsyncWrapper>(asyncStream);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TSyncOutputStream
    : public TOutputStream
{
public:
    explicit TSyncOutputStream(IAsyncOutputStreamPtr asyncStream)
        : AsyncStream_(asyncStream)
    { }

    virtual void DoWrite(const void* buf, size_t len) override
    {
        auto result = WaitFor(AsyncStream_->Write(buf, len));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
    }
    
    virtual ~TSyncOutputStream() throw()
    { }

private:
    IAsyncOutputStreamPtr AsyncStream_;

};

} // namespace

std::unique_ptr<TOutputStream> CreateSyncOutputStream(IAsyncOutputStreamPtr asyncStream)
{
    return std::unique_ptr<TOutputStream>(new TSyncOutputStream(asyncStream));
}

////////////////////////////////////////////////////////////////////////////////

namespace {

class TOutputStreamAsyncWrapper
    : public IAsyncOutputStream
{
public:
    explicit TOutputStreamAsyncWrapper(TOutputStream* inputStream)
        : OutputStream_(inputStream)
        , Failed_(false)
    { }
    
    virtual TAsyncError Write(const void* buf, size_t len) override
    {
        if (Failed_) {
            return Result_;
        }

        try {
            OutputStream_->Write(buf, len);
        } catch (const std::exception& ex) {
            Result_ = MakeFuture(TError("Failed writing to stream") << ex);
            Failed_ = true;
            return Result_;
        }
        return OKFuture;
    }

private:
    TOutputStream* OutputStream_;

    TAsyncError Result_;
    bool Failed_;
};

} // namespace

IAsyncOutputStreamPtr CreateAsyncOutputStream(TOutputStream* asyncStream)
{
    return New<TOutputStreamAsyncWrapper>(asyncStream);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

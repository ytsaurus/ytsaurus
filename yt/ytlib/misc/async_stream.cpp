#include "stdafx.h"
#include "async_stream.h"

namespace NYT {

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
        if (!AsyncStream_->Read(buf, len)) {
            auto result = AsyncStream_->GetReadyEvent().Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
        return AsyncStream_->GetReadLength();
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
        , Length_(0)
    { }
    
    virtual bool Read(void* buf, size_t len) override
    {
        try {
            Length_ = InputStream_->Read(buf, len);
            return true;
        } catch (const std::exception& ex) {
            Result_ = MakeFuture(TError("Failed reading from the stream") << ex);
            return false;
        }
    }
    
    virtual TAsyncError GetReadyEvent() override
    {
        return Result_;
    }

    virtual size_t GetReadLength() const override
    {
        return Length_;
    }
    
private:
    TInputStream* InputStream_;
    
    size_t Length_;

    TAsyncError Result_;
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
        if (!AsyncStream_->Write(buf, len)) {
            auto result = AsyncStream_->GetReadyEvent().Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }
    }
    
    virtual ~TSyncOutputStream() throw()
    { }

private:
    IAsyncOutputStreamPtr AsyncStream_;

};

} // anonymous namespace

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
    { }
    
    virtual bool Write(const void* buf, size_t len) override
    {
        try {
            OutputStream_->Write(buf, len);
        } catch (const std::exception& ex) {
            Result_ = MakeFuture(TError("Failed writing to the stream") << ex);
            return false;
        }
        return true;
    }
    
    virtual TAsyncError GetReadyEvent() override
    {
        return Result_;
    }

private:
    TOutputStream* OutputStream_;

    TAsyncError Result_;
};

} // anonymous namespace

IAsyncOutputStreamPtr CreateAsyncOutputStream(TOutputStream* asyncStream)
{
    return New<TOutputStreamAsyncWrapper>(asyncStream);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

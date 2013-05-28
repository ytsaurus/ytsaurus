#include "async_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TSyncInputStream
    : public TInputStream
{
public:
    TSyncInputStream(IAsyncInputStreamPtr asyncStream)
        : AsyncStream_(asyncStream)
    { }

    virtual size_t DoRead(void* buf, size_t len) override
    {
        if (!AsyncStream_->Read(buf, len)) {
            Sync(~AsyncStream_, &IAsyncInputStream::GetReadFuture);
        }
        return AsyncStream_->GetReadLength();
    }

    virtual ~TSyncInputStream() throw()
    { }

private:
    size_t result;

    IAsyncInputStreamPtr AsyncStream_;
};

} // anonymous namespace

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
    TInputStreamAsyncWrapper(TInputStream* inputStream):
        InputStream_(inputStream)
    { }
    
    virtual bool Read(void* buf, size_t len) override
    {
        length = InputStream_->Read(buf, len);
        return true;
    }
    
    virtual TAsyncError GetReadFuture() override
    {
        YUNREACHABLE();
    }

    virtual size_t GetReadLength() const override
    {
        return length;
    }

private:
    size_t length;

    TInputStream* InputStream_;
};

} // anonymous namespace

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
    TSyncOutputStream(IAsyncOutputStreamPtr asyncStream)
        : AsyncStream_(asyncStream)
    { }

    virtual void DoWrite(const void* buf, size_t len) override
    {
        if (!AsyncStream_->Write(buf, len)) {
            Sync(~AsyncStream_, &IAsyncOutputStream::GetWriteFuture);
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
    TOutputStreamAsyncWrapper(TOutputStream* inputStream):
        OutputStream_(inputStream)
    { }
    
    virtual bool Write(const void* buf, size_t len) override
    {
        OutputStream_->Write(buf, len);
        return true;
    }
    
    virtual TAsyncError GetWriteFuture() override
    {
        YUNREACHABLE();
    }

private:
    TOutputStream* OutputStream_;
};

} // anonymous namespace

IAsyncOutputStreamPtr CreateAsyncOutputStream(TOutputStream* asyncStream)
{
    return New<TOutputStreamAsyncWrapper>(asyncStream);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

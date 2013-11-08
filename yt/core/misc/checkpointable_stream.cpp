#include "checkpointable_stream.h"

#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct THeader {
    explicit THeader(i64 length = -1)
        : Length(length)
    { }

    i64 Length;
};

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

class TCheckpointableInputStream
    : public ICheckpointableInputStream
{
public:
    explicit TCheckpointableInputStream(TInputStream* underlyingStream);

    virtual void Skip() override;

    ~TCheckpointableInputStream() throw();

private:
    virtual size_t DoRead(void* buf, size_t len) override;

    bool ReadHeader(THeader& header);
    bool ReadBlob();

    TInputStream* UnderlyingStream_;

    TBlob Blob_;
    i64 Shift_;
    bool BlobStarted_;
};

TCheckpointableInputStream::TCheckpointableInputStream(TInputStream* underlyingStream)
    : UnderlyingStream_(underlyingStream)
    , BlobStarted_(false)
{ }

void TCheckpointableInputStream::Skip()
{
    while (true) {
        THeader header;
        if (!ReadHeader(header) || header.Length == 0) {
            break;
        }
        UnderlyingStream_->Skip(header.Length);
    }
    BlobStarted_ = false;
}

bool TCheckpointableInputStream::ReadHeader(THeader& header)
{
    i64 len = ReadPod(*UnderlyingStream_, header);
    if (len == 0) {
        return false;
    }

    YCHECK(len == sizeof(THeader));

    return true;
}

bool TCheckpointableInputStream::ReadBlob()
{
    THeader header;
    if (!ReadHeader(header)) {
        return false;
    }

    Blob_.Resize(header.Length);
    YCHECK(UnderlyingStream_->Read(Blob_.Begin(), Blob_.Size()) == Blob_.Size());

    BlobStarted_ = true;
    Shift_ = 0;

    return true;
}

size_t TCheckpointableInputStream::DoRead(void* buf_, size_t len)
{
    char* buf = reinterpret_cast<char*>(buf_);


    i64 pos = 0;
    while (pos < len) {
        if (!BlobStarted_ && !ReadBlob()) {
            break;
        }
        i64 size = std::min(Blob_.Size() - Shift_, len - pos);
        std::copy(Blob_.Begin() + Shift_, Blob_.Begin() + Shift_ + size, buf + pos);
        pos += size;
        Shift_ += size;
        if (Shift_ == Blob_.Size()) {
            BlobStarted_ = false;
        }
    }
    return pos;
}

TCheckpointableInputStream::~TCheckpointableInputStream() throw()
{ }

////////////////////////////////////////////////////////////////////////////////

class TCheckpointableOutputStream
    : public ICheckpointableOutputStream
{
public:
    TCheckpointableOutputStream(TOutputStream* underlyingStream);

    virtual void MakeCheckpoint() override;

    ~TCheckpointableOutputStream() throw();

private:
    virtual void DoWrite(const void* buf, size_t len) override;
    
    TOutputStream* UnderlyingStream_;
};

TCheckpointableOutputStream::TCheckpointableOutputStream(TOutputStream* underlyingStream)
    : UnderlyingStream_(underlyingStream)
{ }

void TCheckpointableOutputStream::MakeCheckpoint()
{
    WritePod(*UnderlyingStream_, THeader(0));
}

void TCheckpointableOutputStream::DoWrite(const void* buf, size_t len)
{
    if (len == 0) {
        return;
    }

    WritePod(*UnderlyingStream_, THeader(len));
    UnderlyingStream_->Write(buf, len);
}

TCheckpointableOutputStream::~TCheckpointableOutputStream() throw()
{ }

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ICheckpointableInputStream> CreateCheckpointableInputStream(
    TInputStream* underlyingStream)
{
    return std::unique_ptr<ICheckpointableInputStream>(
        new TCheckpointableInputStream(underlyingStream));
}

std::unique_ptr<ICheckpointableOutputStream> CreateCheckpointableOutputStream(
    TOutputStream* underlyingStream)
{
    return std::unique_ptr<ICheckpointableOutputStream>(
        new TCheckpointableOutputStream(underlyingStream));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


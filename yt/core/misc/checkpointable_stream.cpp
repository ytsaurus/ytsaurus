#include "checkpointable_stream.h"

#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct THeader
{
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

    virtual void SkipToCheckpoint() override;

    ~TCheckpointableInputStream() throw();

private:
    virtual size_t DoRead(void* buf, size_t len) override;

    bool ReadHeader();

    TInputStream* UnderlyingStream_;

    i64 BlockLength_;
    i64 Offset_;
    bool BlockStarted_;
};

TCheckpointableInputStream::TCheckpointableInputStream(TInputStream* underlyingStream)
    : UnderlyingStream_(underlyingStream)
    , BlockStarted_(false)
{ }

void TCheckpointableInputStream::SkipToCheckpoint()
{
    while (true) {
        if (!ReadHeader()) {
            break;
        }
        if (BlockLength_ == 0) {
            BlockStarted_ = false;
            break;
        }
        UnderlyingStream_->Skip(BlockLength_ - Offset_);
        BlockStarted_ = false;
    }
}

bool TCheckpointableInputStream::ReadHeader()
{
    if (!BlockStarted_) {
        THeader header;
        i64 len = ReadPod(*UnderlyingStream_, header);
        if (len == 0) {
            return false;
        }

        YCHECK(len == sizeof(THeader));

        BlockStarted_ = true;
        BlockLength_ = header.Length;
        Offset_ = 0;
    }

    return true;
}

size_t TCheckpointableInputStream::DoRead(void* buf_, size_t len)
{
    char* buf = reinterpret_cast<char*>(buf_);

    i64 pos = 0;
    while (pos < len) {
        if (!ReadHeader()) {
            break;
        }
        i64 size = std::min(BlockLength_ - Offset_, static_cast<i64>(len) - pos);
        YCHECK(UnderlyingStream_->Read(buf + pos, size) == size);
        pos += size;
        Offset_ += size;
        if (Offset_ == BlockLength_) {
            BlockStarted_ = false;
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
    explicit TCheckpointableOutputStream(TOutputStream* underlyingStream);

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


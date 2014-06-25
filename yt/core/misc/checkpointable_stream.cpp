#include "checkpointable_stream.h"

#include "serialize.h"

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBlockHeader
{
    explicit TBlockHeader(i64 length = -1)
        : Length(length)
    { }

    i64 Length;
};

} // namespace
} // namespace NYT

DECLARE_PODTYPE(NYT::TBlockHeader)

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCheckpointableInputStream
    : public ICheckpointableInputStream
{
public:
    explicit TCheckpointableInputStream(TInputStream* underlyingStream)
        : UnderlyingStream_(underlyingStream)
    { }

    virtual void SkipToCheckpoint() override
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

    ~TCheckpointableInputStream() throw()
    { }

private:
    TInputStream* UnderlyingStream_;

    i64 BlockLength_;
    i64 Offset_;
    bool BlockStarted_ = false;


    virtual size_t DoRead(void* buf_, size_t len) override
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

    bool ReadHeader()
    {
        if (!BlockStarted_) {
            TBlockHeader header;
            i64 len = ReadPod(*UnderlyingStream_, header);
            if (len == 0) {
                return false;
            }

            YCHECK(len == sizeof(TBlockHeader));

            BlockStarted_ = true;
            BlockLength_ = header.Length;
            Offset_ = 0;
        }

        return true;
    }

};

std::unique_ptr<ICheckpointableInputStream> CreateCheckpointableInputStream(
    TInputStream* underlyingStream)
{
    return std::unique_ptr<ICheckpointableInputStream>(
        new TCheckpointableInputStream(underlyingStream));
}

////////////////////////////////////////////////////////////////////////////////

class TCheckpointableOutputStream
    : public ICheckpointableOutputStream
{
public:
    explicit TCheckpointableOutputStream(TOutputStream* underlyingStream)
        : UnderlyingStream_(underlyingStream)
    { }

    virtual void MakeCheckpoint() override
    {
        WritePod(*UnderlyingStream_, TBlockHeader(0));
    }

    ~TCheckpointableOutputStream() throw()
    { }

private:
    TOutputStream* UnderlyingStream_;


    virtual void DoWrite(const void* buf, size_t len) override
    {
        if (len == 0) {
            return;
        }

        WritePod(*UnderlyingStream_, TBlockHeader(len));
        UnderlyingStream_->Write(buf, len);
    }

};

std::unique_ptr<ICheckpointableOutputStream> CreateCheckpointableOutputStream(
    TOutputStream* underlyingStream)
{
    return std::unique_ptr<ICheckpointableOutputStream>(
        new TCheckpointableOutputStream(underlyingStream));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


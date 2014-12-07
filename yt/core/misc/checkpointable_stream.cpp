#include "stdafx.h"
#include "checkpointable_stream.h"
#include "serialize.h"

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBlockHeader
{
    static const ui64 CheckpointSentinel = 0;
    static const ui64 CheckpointsDisabled = 0xffffffffU;

    ui64 Length;
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
            if (!EnsureBlock()) {
                break;
            }
            if (BlockLength_ == TBlockHeader::CheckpointsDisabled) {
                break;
            }
            if (BlockLength_ == TBlockHeader::CheckpointSentinel) {
                HasBlock_ = false;
                break;
            }
            UnderlyingStream_->Skip(BlockLength_ - BlockOffset_);
            HasBlock_ = false;
        }
    }

    virtual ~TCheckpointableInputStream() throw()
    { }

private:
    TInputStream* UnderlyingStream_;

    size_t BlockLength_;
    size_t BlockOffset_;
    bool HasBlock_ = false;


    virtual size_t DoRead(void* buf_, size_t len) override
    {
        if (BlockLength_ == TBlockHeader::CheckpointsDisabled) {
            return UnderlyingStream_->Read(buf_, len);
        } else {
            char* buf = reinterpret_cast<char*>(buf_);
            size_t pos = 0;
            while (pos < len) {
                if (!EnsureBlock()) {
                    break;
                }
                size_t size = std::min(BlockLength_ - BlockOffset_, len - pos);
                YCHECK(UnderlyingStream_->Load(buf + pos, size) == size);
                pos += size;
                BlockOffset_ += size;
                if (BlockOffset_ == BlockLength_) {
                    HasBlock_ = false;
                }
            }
            return pos;
        }
    }

    bool EnsureBlock()
    {
        if (!HasBlock_) {
            TBlockHeader header;
            size_t len = UnderlyingStream_->Load(&header, sizeof(header));
            YCHECK(len == 0 || len == sizeof(TBlockHeader));

            if (len == 0) {
                return false;
            }

            HasBlock_ = true;
            BlockLength_ = header.Length;
            BlockOffset_ = 0;
        }

        return true;
    }

};

std::unique_ptr<ICheckpointableInputStream> CreateCheckpointableInputStream(
    TInputStream* underlyingStream)
{
    return std::unique_ptr<ICheckpointableInputStream>(new TCheckpointableInputStream(
        underlyingStream));
}

////////////////////////////////////////////////////////////////////////////////

class TEnscapsulatedCheckpointableInputStream
    : public TInputStream
{
public:
    explicit TEnscapsulatedCheckpointableInputStream(
        TInputStream* underlyingStream)
        : UnderlyingStream_(underlyingStream)
        , FakeHeaderOffset_(0)
        , FakeHeader_({TBlockHeader::CheckpointsDisabled})
    { }

    virtual ~TEnscapsulatedCheckpointableInputStream() throw()
    { }

private:
    TInputStream* UnderlyingStream_;

    int FakeHeaderOffset_;
    TBlockHeader FakeHeader_;


    virtual size_t DoRead(void* buf, size_t len) override
    {
        if (FakeHeaderOffset_ < sizeof(FakeHeader_)) {
            size_t bytes = std::min(len, sizeof(FakeHeader_) - FakeHeaderOffset_);
            memcpy(buf, reinterpret_cast<const char*>(&FakeHeader_) + FakeHeaderOffset_, bytes);
            FakeHeaderOffset_ += bytes;
            return bytes;
        } else {
            return UnderlyingStream_->Read(buf, len);
        }
    }

};

std::unique_ptr<TInputStream> EscapsulateAsCheckpointableInputStream(
    TInputStream* underlyingStream)
{
    return std::unique_ptr<TInputStream>(new TEnscapsulatedCheckpointableInputStream(
        underlyingStream));
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
        WritePod(*UnderlyingStream_, TBlockHeader{TBlockHeader::CheckpointSentinel});
    }

    virtual ~TCheckpointableOutputStream() throw()
    { }

private:
    TOutputStream* UnderlyingStream_;


    virtual void DoWrite(const void* buf, size_t len) override
    {
        if (len == 0) {
            return;
        }

        WritePod(*UnderlyingStream_, TBlockHeader{len});
        UnderlyingStream_->Write(buf, len);
    }

};

std::unique_ptr<ICheckpointableOutputStream> CreateCheckpointableOutputStream(
    TOutputStream* underlyingStream)
{
    return std::unique_ptr<ICheckpointableOutputStream>(new TCheckpointableOutputStream(
        underlyingStream));
}

////////////////////////////////////////////////////////////////////////////////

class TBufferedCheckpointableOutputStream
    : public ICheckpointableOutputStream
{
public:
    TBufferedCheckpointableOutputStream(
        ICheckpointableOutputStream* underlyingStream,
        size_t bufferSize)
        : UnderlyingStream_(underlyingStream)
        , BufferSize_(bufferSize)
        , WriteThroughSize_(bufferSize / 2)
        , Buffer_(BufferSize_)
        , BufferBegin_(Buffer_.data())
        , BufferCurrent_(Buffer_.data())
        , BufferRemaining_(BufferSize_)
    {
        YCHECK(BufferSize_ > 0);
    }

    virtual void MakeCheckpoint() override
    {
        Flush();
        UnderlyingStream_->MakeCheckpoint();
    }

    virtual ~TBufferedCheckpointableOutputStream() throw()
    {
        try {
            Finish();
        } catch (...) {
        }
    }

private:
    ICheckpointableOutputStream* UnderlyingStream_;
    size_t BufferSize_;
    size_t WriteThroughSize_;

    std::vector<char> Buffer_;
    char* BufferBegin_;
    char* BufferCurrent_;
    size_t BufferRemaining_;


    virtual void DoWrite(const void* buf, size_t len) override
    {
        const char* current = static_cast<const char*>(buf);
        size_t remaining = len;
        while (remaining > 0) {
            if (BufferRemaining_ == 0) {
                // Flush buffer.
                Flush();
            }
            size_t bytes = std::min(BufferRemaining_, remaining);
            if (BufferRemaining_ == BufferSize_ && bytes >= WriteThroughSize_) {
                // Write-through.
                UnderlyingStream_->Write(current, bytes);
            } else {
                // Copy into buffer.
                ::memcpy(BufferCurrent_, current, bytes);
                BufferCurrent_ += bytes;
                BufferRemaining_ -= bytes;
            }
            current += bytes;
            remaining -= bytes;
        }
    }

    virtual void DoFlush() override
    {
        UnderlyingStream_->Write(BufferBegin_, BufferCurrent_ - BufferBegin_);
        BufferCurrent_ = BufferBegin_;
        BufferRemaining_ = BufferSize_;
        UnderlyingStream_->Flush();
    }

};

std::unique_ptr<ICheckpointableOutputStream> CreateBufferedCheckpointableOutputStream(
    ICheckpointableOutputStream* underlyingStream,
    size_t bufferSize)
{
    return std::unique_ptr<ICheckpointableOutputStream>(new TBufferedCheckpointableOutputStream(
        underlyingStream,
        bufferSize));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


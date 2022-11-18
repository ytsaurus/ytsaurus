#include "checkpointable_stream.h"
#include "serialize.h"
#include "checkpointable_stream_block_header.h"

#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/misc/error.h>

#include <util/stream/buffered.h>

namespace NYT::NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TCheckpointableInputStream
    : public ICheckpointableInputStream
{
public:
    explicit TCheckpointableInputStream(IInputStream* underlyingStream)
        : UnderlyingStream_(underlyingStream)
    { }

    void SkipToCheckpoint() override
    {
        while (true) {
            if (!EnsureBlock()) {
                break;
            }
            if (BlockLength_ == TCheckpointableStreamBlockHeader::CheckpointSentinel) {
                HasBlock_ = false;
                break;
            }
            auto size = BlockLength_ - BlockOffset_;
            UnderlyingStream_->Skip(size);
            Offset_ += size;
            HasBlock_ = false;
        }
    }

    i64 GetOffset() const override
    {
        return Offset_;
    }

private:
    IInputStream* const UnderlyingStream_;

    i64 BlockLength_;
    i64 BlockOffset_;
    bool HasBlock_ = false;

    i64 Offset_ = 0;


    size_t DoRead(void* buf_, size_t len_) override
    {
        char* buf = reinterpret_cast<char*>(buf_);
        i64 len = static_cast<i64>(len_);
        i64 pos = 0;
        while (pos < len) {
            if (!EnsureBlock()) {
                break;
            }
            auto size = std::min(BlockLength_ - BlockOffset_, len - pos);
            auto loadedSize = UnderlyingStream_->Load(buf + pos, size);
            if (static_cast<i64>(loadedSize) != size) {
                THROW_ERROR_EXCEPTION("Broken checkpointable stream: expected %v bytes, got %v",
                    size,
                    loadedSize);
            }
            pos += size;
            Offset_ += size;
            BlockOffset_ += size;
            if (BlockOffset_ == BlockLength_) {
                HasBlock_ = false;
            }
        }
        return pos;
    }

    bool EnsureBlock()
    {
        if (!HasBlock_) {
            TCheckpointableStreamBlockHeader header;
            auto loadedSize = UnderlyingStream_->Load(&header, sizeof(header));
            if (loadedSize == 0) {
                return false;
            }

            if (loadedSize != sizeof(TCheckpointableStreamBlockHeader)) {
                THROW_ERROR_EXCEPTION("Broken checkpointable stream: expected %v bytes, got %v",
                    sizeof(TCheckpointableStreamBlockHeader),
                    loadedSize);
            }

            HasBlock_ = true;
            BlockLength_ = header.Length;
            BlockOffset_ = 0;
        }

        return true;
    }
};

std::unique_ptr<ICheckpointableInputStream> CreateCheckpointableInputStream(
    IInputStream* underlyingStream)
{
    return std::unique_ptr<ICheckpointableInputStream>(new TCheckpointableInputStream(
        underlyingStream));
}

////////////////////////////////////////////////////////////////////////////////

class TCheckpointableOutputStream
    : public ICheckpointableOutputStream
{
public:
    explicit TCheckpointableOutputStream(IZeroCopyOutput* underlyingStream)
        : UnderlyingStream_(underlyingStream)
    { }

    void MakeCheckpoint() override
    {
        LastBufferedHeader_ = nullptr;
        TCheckpointableStreamBlockHeader header{TCheckpointableStreamBlockHeader::CheckpointSentinel};
        UnderlyingStream_->Write(&header, sizeof(header));
    }

    virtual ~TCheckpointableOutputStream()
    {
        try {
            Finish();
        } catch (...) {
        }
    }

private:
    IZeroCopyOutput* UnderlyingStream_;
    TCheckpointableStreamBlockHeader* LastBufferedHeader_ = nullptr;


    void DoFlush() override
    {
        LastBufferedHeader_ = nullptr;
        UnderlyingStream_->Flush();
    }

    void DoWrite(const void* data, size_t length) override
    {
        const char* srcPtr = static_cast<const char*>(data);
        size_t srcLen = length;
        void* dstPtr = nullptr;
        size_t dstLen = 0;
        while (srcLen > 0) {
            dstLen = Next(&dstPtr);
            size_t toCopy = Min(dstLen, srcLen);
            ::memcpy(dstPtr, srcPtr, toCopy);
            srcLen -= toCopy;
            dstLen -= toCopy;
            srcPtr += toCopy;
            dstPtr = static_cast<char*>(dstPtr) + toCopy;
        }
        Undo(dstLen);
    }

    size_t DoNext(void** ptr) override
    {
        void* underlyingPtr;
        size_t underlyingLength = UnderlyingStream_->Next(&underlyingPtr);
        if (underlyingLength <= sizeof(TCheckpointableStreamBlockHeader)) {
            UnderlyingStream_->Undo(underlyingLength);
            UnderlyingStream_->Flush();
            underlyingLength = UnderlyingStream_->Next(&underlyingPtr);
            YT_VERIFY(underlyingLength > sizeof(TCheckpointableStreamBlockHeader));
        }

        auto length = underlyingLength - sizeof(TCheckpointableStreamBlockHeader);

        LastBufferedHeader_ = static_cast<TCheckpointableStreamBlockHeader*>(underlyingPtr);
        LastBufferedHeader_->Length = static_cast<i64>(length);

        *ptr = LastBufferedHeader_ + 1;
        return length;
    }

    void DoUndo(size_t length) override
    {
        if (length == 0) {
            return;
        }

        YT_ASSERT(LastBufferedHeader_);
        YT_ASSERT(LastBufferedHeader_->Length >= static_cast<i64>(length));

        LastBufferedHeader_->Length -= static_cast<i64>(length);
        if (LastBufferedHeader_->Length == 0) {
            UnderlyingStream_->Undo(length + sizeof(TCheckpointableStreamBlockHeader));
            LastBufferedHeader_ = nullptr;
        } else {
            UnderlyingStream_->Undo(length);
        }
    }
};

std::unique_ptr<ICheckpointableOutputStream> CreateCheckpointableOutputStream(
    IZeroCopyOutput* underlyingStream)
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
        IOutputStream* underlyingStream,
        size_t bufferSize)
        : BufferedOutput_(underlyingStream, std::max(bufferSize, sizeof(TCheckpointableStreamBlockHeader) + 1))
        , CheckpointableAdapter_(CreateCheckpointableOutputStream(&BufferedOutput_))
    { }

    void MakeCheckpoint() override
    {
        CheckpointableAdapter_->MakeCheckpoint();
    }

private:
    TBufferedOutput BufferedOutput_;
    const std::unique_ptr<ICheckpointableOutputStream> CheckpointableAdapter_;


    void DoFlush() override
    {
        CheckpointableAdapter_->Flush();
    }

    void DoWrite(const void* data, size_t length) override
    {
        CheckpointableAdapter_->Write(data, length);
    }

    size_t DoNext(void** ptr) override
    {
        return CheckpointableAdapter_->Next(ptr);
    }

    void DoUndo(size_t length) override
    {
        CheckpointableAdapter_->Undo(length);
    }
};

std::unique_ptr<ICheckpointableOutputStream> CreateBufferedCheckpointableOutputStream(
    IOutputStream* underlyingStream,
    size_t bufferSize)
{
    return std::unique_ptr<ICheckpointableOutputStream>(new TBufferedCheckpointableOutputStream(
        underlyingStream,
        bufferSize));
}

////////////////////////////////////////////////////////////////////////////////

class TSyncBufferedOutputStreamAdapter
    : public IZeroCopyOutput
{
public:
    TSyncBufferedOutputStreamAdapter(
        IAsyncOutputStreamPtr underlyingStream,
        EWaitForStrategy strategy,
        size_t bufferCapacity)
        : UnderlyingStream_(std::move(underlyingStream))
        , Strategy_(strategy)
        , BufferCapacity_(bufferCapacity)
    {
        Reset();
    }

    virtual ~TSyncBufferedOutputStreamAdapter()
    {
        try {
            Finish();
        } catch (...) {
        }
    }

private:
    const IAsyncOutputStreamPtr UnderlyingStream_;
    const EWaitForStrategy Strategy_;
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

    void* WriteToBuffer(const void* data, size_t length)
    {
        YT_ASSERT(length <= GetBufferSpaceLeft());
        char* ptr = Buffer_.Begin() + CurrentBufferSize_;
        ::memcpy(Buffer_.Begin() + CurrentBufferSize_, data, length);
        CurrentBufferSize_ += length;
        return ptr;
    }

    void WriteToStream(const void* data, size_t length)
    {
        auto sharedBuffer = TSharedRef::MakeCopy<TBufferTag>(TRef(data, length));
        auto future = UnderlyingStream_->Write(std::move(sharedBuffer));
        WaitForWithStrategy(std::move(future), Strategy_)
            .ThrowOnError();
    }

    size_t GetBufferSpaceLeft() const
    {
        return BufferCapacity_ - CurrentBufferSize_;
    }

    size_t GetBufferSize() const
    {
        return CurrentBufferSize_;
    }

protected:
    size_t DoNext(void** ptr) override
    {
        if (GetBufferSpaceLeft() == 0) {
            DoFlush();
        }

        auto size = GetBufferSpaceLeft();
        *ptr = Buffer_.Begin() + CurrentBufferSize_;
        CurrentBufferSize_ += size;

        return size;
    }

    void DoUndo(size_t size) override
    {
        YT_VERIFY(CurrentBufferSize_ >= size);
        CurrentBufferSize_ -= size;
    }

    void DoWrite(const void* buffer, size_t length) override
    {
        if (length > GetBufferSpaceLeft()) {
            DoFlush();
        }
        if (length <= GetBufferSpaceLeft()) {
            WriteToBuffer(buffer, length);
        } else {
            WriteToStream(buffer, length);
        }
    }

    void DoFlush() override
    {
        if (CurrentBufferSize_ == 0) {
            return;
        }
        auto writeFuture = UnderlyingStream_->Write(Buffer_.Slice(0, CurrentBufferSize_));
        WaitForWithStrategy(std::move(writeFuture), Strategy_)
            .ThrowOnError();
        Reset();
    }
};

std::unique_ptr<IZeroCopyOutput> CreateBufferedSyncAdapter(
    IAsyncOutputStreamPtr underlyingStream,
    EWaitForStrategy strategy,
    size_t bufferSize)
{
    YT_VERIFY(underlyingStream);
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
        EWaitForStrategy strategy,
        size_t bufferSize)
        : SyncAdapter_(std::make_unique<TSyncBufferedOutputStreamAdapter>(
            underlyingStream,
            strategy,
            bufferSize))
        , CheckpointableAdapter_(CreateCheckpointableOutputStream(SyncAdapter_.get()))
    { }

    void MakeCheckpoint() override
    {
        CheckpointableAdapter_->MakeCheckpoint();
    }

private:
    const std::unique_ptr<TSyncBufferedOutputStreamAdapter> SyncAdapter_;
    const std::unique_ptr<ICheckpointableOutputStream> CheckpointableAdapter_;


    void DoFlush() override
    {
        CheckpointableAdapter_->Flush();
    }

    void DoWrite(const void* data, size_t length) override
    {
        CheckpointableAdapter_->Write(data, length);
    }

    size_t DoNext(void** ptr) override
    {
        return CheckpointableAdapter_->Next(ptr);
    }

    void DoUndo(size_t length) override
    {
        CheckpointableAdapter_->Undo(length);
    }
};

std::unique_ptr<ICheckpointableOutputStream> CreateBufferedCheckpointableSyncAdapter(
    IAsyncOutputStreamPtr underlyingStream,
    EWaitForStrategy strategy,
    size_t bufferSize)
{
    YT_VERIFY(underlyingStream);
    return std::make_unique<TSyncBufferedCheckpointableOutputStreamAdapter>(
        std::move(underlyingStream),
        strategy,
        std::max(bufferSize, sizeof(TCheckpointableStreamBlockHeader) + 1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra


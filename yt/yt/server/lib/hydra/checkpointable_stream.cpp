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
    explicit TCheckpointableInputStream(IZeroCopyInput* underlyingStream)
        : UnderlyingStream_(underlyingStream)
    { }

    void SkipToCheckpoint() override
    {
        while (EnsureBlock()) {
            if (BlockLength_ == TCheckpointableStreamBlockHeader::CheckpointSentinel) {
                BlockLength_ = 0;
                BlockRemaining_ = 0;
                break;
            }

            auto size = static_cast<size_t>(BlockRemaining_);
            if (UnderlyingStream_->Skip(size) != size) {
                THROW_ERROR_EXCEPTION("Unexpected end-of-checkpointable-stream");
            }

            Offset_ += BlockRemaining_;
            BlockRemaining_ = 0;
        }
    }

    i64 GetOffset() const override
    {
        return Offset_;
    }

private:
    IZeroCopyInput* const UnderlyingStream_;

    i64 BlockLength_ = 0;
    i64 BlockRemaining_ = 0;

    i64 Offset_ = 0;

    size_t DoNext(const void** ptr, size_t len) override
    {
        *ptr = nullptr;

        if (len == 0) {
            return 0;
        }

        while (BlockRemaining_ == 0) {
            if (!EnsureBlock()) {
                return 0;
            }
        }

        auto bytesRequested = std::min(len, static_cast<size_t>(BlockRemaining_));
        auto bytesAvailable = UnderlyingStream_->Next(ptr, bytesRequested);
        if (bytesAvailable == 0) {
            THROW_ERROR_EXCEPTION("Unexpected end-of-checkpointable-stream");
        }

        Offset_ += bytesAvailable;
        BlockRemaining_ -= bytesAvailable;

        return bytesAvailable;
    }

    bool EnsureBlock()
    {
        if (BlockRemaining_ > 0) {
            return true;
        }

        TCheckpointableStreamBlockHeader header;
        auto loadedSize = UnderlyingStream_->Load(&header, sizeof(header));
        if (loadedSize == 0) {
            BlockLength_ = 0;
            BlockRemaining_ = 0;
            return false;
        }

        if (loadedSize != sizeof(TCheckpointableStreamBlockHeader)) {
            THROW_ERROR_EXCEPTION("Broken checkpointable stream: expected %v bytes, got %v",
                sizeof(TCheckpointableStreamBlockHeader),
                loadedSize);
        }

        BlockLength_ = header.Length;
        BlockRemaining_ = BlockLength_;
        return true;
    }
};

std::unique_ptr<ICheckpointableInputStream> CreateCheckpointableInputStream(
    IZeroCopyInput* underlyingStream)
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
        UnderlyingStream_->Undo(length);

        if (LastBufferedHeader_->Length == 0) {
            LastBufferedHeader_ = nullptr;
            UnderlyingStream_->Undo(sizeof(TCheckpointableStreamBlockHeader));
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


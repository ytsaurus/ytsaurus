#include "zstd_reader.h"

#include "helpers.h"

#include <yt/yt/core/compression/codec.h>
#include <yt/yt/core/compression/zstd.h>

#include <util/stream/file.h>

#include <contrib/libs/zstd/include/zstd.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/streams/zstd/zstd.h>

#define ZSTD_STATIC_LINKING_ONLY
#include <contrib/libs/zstd/include/zstd.h>

namespace NYT::NTrigrep {

using namespace NCompression::NDetail;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TrigrepLogger;

////////////////////////////////////////////////////////////////////////////////

class TUncompressedZstdFrameStream
    : public IInputStream
{
public:
    explicit TUncompressedZstdFrameStream(std::unique_ptr<IInputStream> underlying)
        : Underlying_(std::move(underlying))
        , Buffer_(BufferCapacity)
        , ZstdContext_(::ZSTD_createDStream())
    { }

private:
    const std::unique_ptr<IInputStream> Underlying_;

    static constexpr size_t BufferCapacity = 64_KBs;
    std::vector<char> Buffer_;
    size_t BufferSize_ = 0;
    size_t BufferPosition_ = 0;

    using TZstdContextDeleter = decltype([] (::ZSTD_DStream* context) {
        ZSTD_freeDStream(context);
    });
    std::unique_ptr<::ZSTD_DStream, TZstdContextDeleter> ZstdContext_;

    static void DestroyContext(::ZSTD_DStream* context)
    {
        ::ZSTD_freeDStream(context);
    }

    size_t DoRead(void* buf, size_t len) final
    {
        ::ZSTD_outBuffer zstdOutBuffer{buf, len, 0};
        ::ZSTD_inBuffer zstdInBuffer{Buffer_.data(), BufferSize_, BufferPosition_};

        size_t returnCode = 0;
        while (zstdOutBuffer.pos != zstdOutBuffer.size) {
            if (zstdInBuffer.pos == zstdInBuffer.size) {
                zstdInBuffer.size = Underlying_->Read(Buffer_.data(), BufferCapacity);
                BufferSize_ = zstdInBuffer.size;
                zstdInBuffer.pos = 0;
                BufferPosition_ = 0;
                if (zstdInBuffer.size == 0) {
                    if (returnCode != 0) {
                        YT_LOG_WARNING("Incomplete Zstd block");
                        return 0;
                    }
                    break;
                }
            }

            returnCode = ::ZSTD_decompressStream(ZstdContext_.get(), &zstdOutBuffer, &zstdInBuffer);
            if (::ZSTD_isError(returnCode)) {
                THROW_ERROR_EXCEPTION("Error decompressing Zstd frame")
                    << TError("%v", ::ZSTD_getErrorName(returnCode));
            }
            if (returnCode == 0) {
                ZSTD_initDStream(ZstdContext_.get());
            }
        }
        BufferPosition_ = zstdInBuffer.pos;
        return zstdOutBuffer.pos;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSequentialZstdReader
    : public ISequentialReader
{
public:
    explicit TSequentialZstdReader(const std::string& path)
        : File_(TString(path), OpenExisting | RdOnly)
        , FileInput_(File_)
        , Buffer_(BufferCapacity)
    { }

    i64 GetTotalInputSize() const final
    {
        return File_.GetLength();
    }

    i64 GetCurrentFrameStartOffset() const final
    {
        return BufferStartOffset_ + BufferReadPosition_;
    }

    std::unique_ptr<IInputStream> TryBeginNextFrame() final
    {
        if (IsExhausted()) {
            return nullptr;
        }

        if (!std::exchange(FirstFrame_, false)) {
            SkipSyncTag();
        }

        RefillBuffer();

        auto frameInput = std::make_unique<TFrameInput>(this);
        return std::make_unique<TUncompressedZstdFrameStream>(std::move(frameInput));
    }

private:
    TFile File_;
    TFileInput FileInput_;

    static constexpr i64 BufferCapacity = 64_KBs;
    std::vector<char> Buffer_;
    i64 BufferReadPosition_ = 0;
    i64 BufferStartOffset_ = 0;
    i64 BufferSize_ = 0;
    std::optional<i64> SyncTagOffset_;
    bool FirstFrame_ = true;
    bool Eof_ = false;

    bool IsExhausted()
    {
        return Eof_ && BufferReadPosition_ == BufferSize_;
    }

    void RefillBuffer()
    {
        if (Eof_) {
            return;
        }

        if (BufferCapacity == BufferSize_) {
            return;
        }

        i64 bytesReadTotal = 0;
        while (BufferSize_ < BufferCapacity) {
            auto bytesToRead = static_cast<size_t>(BufferCapacity - BufferSize_);
            auto bytesRead = FileInput_.Read(Buffer_.data() + BufferSize_, bytesToRead);
            if (bytesRead == 0) {
                Eof_ = true;
                break;
            }

            BufferSize_ += bytesRead;
            bytesReadTotal += bytesRead;
        }

        if (!SyncTagOffset_) {
            UpdateSyncTagOffset(
                std::max<i64>(0L, BufferSize_ - bytesReadTotal - ZstdSyncTagSize),
                BufferSize_);
        }
    }

    void DropBufferHead(i64 bytesToDrop)
    {
        YT_VERIFY(BufferReadPosition_ >= bytesToDrop);
        YT_VERIFY(bytesToDrop <= BufferSize_);
        std::move(Buffer_.begin() + bytesToDrop, Buffer_.begin() + BufferSize_, Buffer_.begin());
        BufferSize_ -= bytesToDrop;
        BufferStartOffset_ += bytesToDrop;
        BufferReadPosition_ -= bytesToDrop;
    }

    i64 ReadFromBuffer(char* dst, i64 bytesToRead)
    {
        RefillBuffer();

        i64 cautionSize = ZstdSyncTagSize - 1;
        while (BufferSize_ - BufferReadPosition_ <= cautionSize) {
            if (BufferReadPosition_ == 0) {
                cautionSize = 0;
                break;
            }
            DropBufferHead(BufferReadPosition_);
            RefillBuffer();
        }

        bytesToRead = std::min(
            bytesToRead,
            BufferSize_ - BufferReadPosition_ - cautionSize);

        if (SyncTagOffset_) {
            YT_VERIFY(*SyncTagOffset_ >= BufferStartOffset_ + BufferReadPosition_);
            bytesToRead = std::min(
                bytesToRead,
                *SyncTagOffset_ - (BufferStartOffset_ + BufferReadPosition_));
        }

        YT_VERIFY(bytesToRead >= 0);
        memcpy(dst, Buffer_.data() + BufferReadPosition_, bytesToRead);
        BufferReadPosition_ += bytesToRead;
        return bytesToRead;
    }

    void UpdateSyncTagOffset(i64 startPosition, i64 endPosition)
    {
        SyncTagOffset_ = FindFirstZstdSyncTagOffset(
            TRef(
                Buffer_.data() + startPosition,
                Buffer_.data() + endPosition),
            BufferStartOffset_ + startPosition);
    }

    void SkipSyncTag()
    {
        YT_VERIFY(SyncTagOffset_);
        YT_VERIFY(*SyncTagOffset_ == BufferStartOffset_ + BufferReadPosition_);
        YT_VERIFY(BufferReadPosition_ + ZstdSyncTagSize <= BufferSize_);
        BufferReadPosition_ += ZstdSyncTagSize;
        SyncTagOffset_.reset();

        if (BufferSize_ - BufferReadPosition_ < ZstdSyncTagSize) {
            DropBufferHead(BufferReadPosition_);
            RefillBuffer();
        }

        UpdateSyncTagOffset(BufferReadPosition_, BufferSize_);
    }


    class TFrameInput
        : public IInputStream
    {
    public:
        explicit TFrameInput(TSequentialZstdReader* reader)
            : Reader_(reader)
        { }

    private:
        TSequentialZstdReader* const Reader_;

        size_t DoRead(void* buf, size_t len) final
        {
            return static_cast<size_t>(Reader_->ReadFromBuffer(reinterpret_cast<char*>(buf), len));
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISequentialReader> CreateSequentialZstdReader(
    const std::string& path)
{
    return std::make_unique<TSequentialZstdReader>(path);
}

////////////////////////////////////////////////////////////////////////////////

class TRandomZstdReader
    : public IRandomReader
{
public:
    explicit TRandomZstdReader(const std::string& path)
        : File_(TString(path), OpenExisting | RdOnly)
        , FileInput_(File_)
    { }

    std::unique_ptr<IInputStream> CreateFrameStream(i64 startOffset, i64 endOffset) final
    {
        YT_VERIFY(startOffset <= endOffset);
        File_.Seek(startOffset, sSet);
        auto frameInput = CreateFrameInput(&FileInput_, endOffset - startOffset);
        return std::make_unique<TUncompressedZstdFrameStream>(std::move(frameInput));
    }

private:
    TFile File_;
    TUnbufferedFileInput FileInput_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IRandomReader> CreateRandomZstdReader(
    const std::string& path)
{
    return std::make_unique<TRandomZstdReader>(path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep

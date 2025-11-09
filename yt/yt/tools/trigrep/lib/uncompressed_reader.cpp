#include "uncompressed_reader.h"

#include "helpers.h"

#include <util/stream/file.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/size_literals.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

class TSequentialUncompressedReader
    : public ISequentialReader
{
public:
    TSequentialUncompressedReader(
        const std::string& path,
        i64 frameSize)
        : FrameSize_(frameSize)
        , File_(TString(path), OpenExisting)
        , FileInput_(File_)
        , Buffer_(BufferCapacity)
    { }

    i64 GetTotalInputSize() const final
    {
        return File_.GetLength();
    }

    i64 GetCurrentFrameStartOffset() const final
    {
        return CurrentFrameStartOffset_;
    }

    std::unique_ptr<IInputStream> TryBeginNextFrame() final
    {
        if (!MaybeRefillBuffer()) {
            return nullptr;
        }

        return std::make_unique<TFrameInput>(this);
    }

private:
    const i64 FrameSize_;

    TFile File_;
    TUnbufferedFileInput FileInput_;

    i64 CurrentFrameStartOffset_ = 0;

    static constexpr size_t BufferCapacity = 64_KB;
    std::vector<char> Buffer_;
    i64 BufferPosition_ = 0;
    i64 BufferSize_ = 0;
    std::optional<i64> BufferNewlinePosition_;

    bool Eof_ = false;

    bool MaybeRefillBuffer()
    {
        if (Eof_) {
            return false;
        }

        if (BufferPosition_ < BufferSize_) {
            return true;
        }

        auto bytesRead = FileInput_.Read(Buffer_.data(), BufferCapacity);

        BufferPosition_ = 0;
        BufferSize_ = bytesRead;
        BufferNewlinePosition_.reset();

        if (bytesRead == 0) {
            Eof_ = true;
            return false;
        }

        return true;
    }

    std::optional<i64> UpdateNewlinePosition()
    {
        if (!BufferNewlinePosition_ || *BufferNewlinePosition_ < BufferPosition_) {
            char* ptr = static_cast<char*>(memchr(Buffer_.data() + BufferPosition_, '\n', BufferSize_ - BufferPosition_));
            if (ptr) {
                BufferNewlinePosition_ = ptr - Buffer_.data();
            } else {
                BufferNewlinePosition_.reset();
            }
        }
        return BufferNewlinePosition_;
    }

    i64 ReadFromBuffer(void* dst, i64 len)
    {
        i64 bytesToRead = std::min(len, BufferSize_ - BufferPosition_);
        memcpy(dst, Buffer_.data() + BufferPosition_, bytesToRead);
        BufferPosition_ += bytesToRead;
        return bytesToRead;
    }

    class TFrameInput
        : public IInputStream
    {
    public:
        explicit TFrameInput(TSequentialUncompressedReader* reader)
            : Reader_(reader)
        { }

    private:
        TSequentialUncompressedReader* const Reader_;

        bool FrameFinished_ = false;
        i64 BytesRead_ = 0;

        size_t DoRead(void* buf, size_t len) final
        {
            YT_VERIFY(len > 0);

            if (FrameFinished_) {
                return 0;
            }

            if (!Reader_->MaybeRefillBuffer()) {
                FinishFrame();
                return 0;
            }

            bool doFinishFrame = false;
            if (BytesRead_ >= Reader_->FrameSize_) {
                if (auto newlinePosition = Reader_->UpdateNewlinePosition()) {
                    i64 lenToNewline = *newlinePosition - Reader_->BufferPosition_ + 1;
                    if (static_cast<i64>(len) >= lenToNewline) {
                        len = static_cast<size_t>(lenToNewline);
                        doFinishFrame = true;
                    }
                }
            }

            i64 bytesToRead = Reader_->ReadFromBuffer(buf, len);
            BytesRead_ += bytesToRead;

            if (doFinishFrame) {
                FinishFrame();
            }

            return bytesToRead;
        }

        void FinishFrame()
        {
            if (std::exchange(FrameFinished_, true)) {
                return;
            }

            Reader_->CurrentFrameStartOffset_ += BytesRead_;
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISequentialReader> CreateSequentialUncompressedReader(
    const std::string& path,
    i64 frameSize)
{
    return std::make_unique<TSequentialUncompressedReader>(
        path,
        frameSize);
}

////////////////////////////////////////////////////////////////////////////////

class TRandomUncompressedReader
    : public IRandomReader
{
public:
    explicit TRandomUncompressedReader(const std::string& path)
        : File_(TString(path), OpenExisting)
        , FileInput_(File_)
    { }

    std::unique_ptr<IInputStream> CreateFrameStream(i64 startOffset, i64 endOffset) final
    {
        YT_VERIFY(startOffset <= endOffset);
        File_.Seek(startOffset, sSet);
        return CreateFrameInput(&FileInput_, endOffset - startOffset);
    }

private:
    TFile File_;
    TUnbufferedFileInput FileInput_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IRandomReader> CreateRandomUncompressedReader(
    const std::string& path)
{
    return std::make_unique<TRandomUncompressedReader>(path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep

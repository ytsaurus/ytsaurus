#include "tailer.h"

#include "private.h"

#include <yt/yt/core/compression/zstd.h>

#include <yt/yt/core/misc/inotify.h>

#define ZSTD_STATIC_LINKING_ONLY
#include <contrib/libs/zstd/include/zstd.h>

namespace NYT::NZstdtail {

using namespace NCompression::NDetail;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ZstdtailLogger;

constexpr auto SleepQuantum = TDuration::MilliSeconds(100);
constexpr i64 BufferSize = 16_MBs;

////////////////////////////////////////////////////////////////////////////////

class TTailer
{
public:
    TTailer(
        std::string path,
        ITailListener* listener)
        : Path_(std::move(path))
        , Listener_(listener)
    { }

    void Run()
    {
        bool firstRun = true;
        bool hasError = false;

        auto onError = [&] (const TError& error) {
            firstRun = false;
            if (!std::exchange( hasError, true)) {
                Listener_->OnError(error);
            }
            Sleep();
        };

        auto onSuccess = [&] {
            hasError = false;
        };

        for (;;) {
            std::unique_ptr<NFS::TInotifyWatch> inotifyWatch;
            try {
                inotifyWatch = std::make_unique<NFS::TInotifyWatch>(
                    &InotifyHandle_,
                    Path_,
                    NFS::EInotifyWatchEvents::MoveSelf);
            } catch (const std::exception& ex) {
                onError(ex);
                continue;
            }

            onSuccess();

            YT_LOG_DEBUG("Watch created");

            std::unique_ptr<TFile> file;
            try {
                file = std::make_unique<TFile>(Path_.data(), OpenExisting | RdOnly);
            } catch (const std::exception& ex) {
                onError(ex);
                continue;
            }

            onSuccess();

            YT_LOG_DEBUG("File opened");

            RunForFile(file.get(), std::exchange(firstRun, false));
        }
    }

private:
    const std::string Path_;
    ITailListener* const Listener_;

    NFS::TInotifyHandle InotifyHandle_;

    using TZstdContextDeleter = decltype([] (::ZSTD_DStream* context) {
        ZSTD_freeDStream(context);
    });
    std::unique_ptr<::ZSTD_DStream, TZstdContextDeleter> ZstdContext_{::ZSTD_createDStream()};

    bool CheckRotation()
    {
        auto pollResult = InotifyHandle_.Poll();
        if (!pollResult) {
            return false;
        }

        if (None(pollResult->Events & NFS::EInotifyWatchEvents::MoveSelf)) {
            return false;
        }

        YT_LOG_DEBUG("File was rotated");
        return true;
    }

    i64 ComputeStartOffset(TFile* file, bool firstRun)
    {
        if (firstRun) {
            auto offset = LocateLastFrame(file);
            YT_LOG_DEBUG("Tailing file from last frame (Offset: %v)", offset);
            return offset;
        } else {
            YT_LOG_DEBUG("Tailing file from start");
            return 0;
        }
    }

    void RunForFile(TFile* file, bool firstRun)
    {
        int errorCount = 0;

        auto onError = [&] {
            if (++errorCount >= 3 && CheckRotation()) {
                return false;
            }
            Sleep();
            return true;
        };

        auto onSuccess = [&] {
            errorCount = 0;
        };

        auto offset = ComputeStartOffset(file, firstRun);

        for (;;) {
            auto frameKind = TryDetectFrameKind(file, offset);
            if (!frameKind) {
                YT_LOG_DEBUG("Error detecting frame kind, sleeping and retrying");
                if (!onError()) {
                    return;
                }
                continue;
            }

            switch (*frameKind) {
                case EZstdFrameType::Data: {
                    auto inSize = TryDecodeDataFrame(file, offset);
                    if (!inSize) {
                        YT_LOG_DEBUG("Error decoding data frame, sleeping and retrying");
                        if (!onError()) {
                            return;
                        }
                        continue;
                    }

                    onSuccess();
                    offset += *inSize;
                    break;
                }

                case EZstdFrameType::Sync:
                    onSuccess();
                    offset += ZstdSyncTagSize;
                    YT_LOG_DEBUG("Zstd sync tag skipped (Offset: %v)", offset);
                    break;

                case EZstdFrameType::Unknown:
                    THROW_ERROR_EXCEPTION("Unknown zstd frame at offset %v", offset);

                default:
                    YT_ABORT();
            }
        }
    }

    i64 LocateLastFrame(TFile* file)
    {
        std::vector<char> buffer(BufferSize);

        std::optional<i64> lastSyncTagOffset;
        auto size = file->GetLength();
        if (size > 0) {
            i64 readOffset = std::max(size - BufferSize, static_cast<i64>(0));
            for (;;) {
                auto bytesRead = file->Pread(buffer.data(), BufferSize, readOffset);
                if (bytesRead == 0) {
                    THROW_ERROR_EXCEPTION("Uexpected end of file");
                }

                YT_LOG_DEBUG("Input read (Offset: %v, BytesRead: %v)",
                    readOffset,
                    bytesRead);

                auto scanRef = TRef(buffer.data(), bytesRead);
                i64 scanRefStartOffset = readOffset;
                for (;;) {
                    auto newSyncTagOffset = FindLastZstdSyncTagOffset(scanRef, scanRefStartOffset);
                    if (!newSyncTagOffset) {
                        YT_LOG_DEBUG("No zstd sync tag found");
                        break;
                    }

                    YT_LOG_DEBUG("Zstd sync tag found (Offset: %v)", newSyncTagOffset);

                    lastSyncTagOffset = newSyncTagOffset;
                    i64 delta = *lastSyncTagOffset - scanRefStartOffset + 1;
                    scanRef = scanRef.Slice(delta, scanRef.Size());
                    scanRefStartOffset += delta;
                }

                if (lastSyncTagOffset) {
                    YT_LOG_DEBUG("Last zstd sync tag found (TagOffset: %v)", lastSyncTagOffset);
                    break;
                }

                if (readOffset == 0) {
                    YT_LOG_DEBUG("Already at the beginning of the file");
                    break;
                }

                YT_LOG_DEBUG("Rewinding read offset");
                readOffset = std::max(readOffset - (BufferSize - ZstdSyncTagSize), static_cast<i64>(0));
            }
        }

        return lastSyncTagOffset ? *lastSyncTagOffset + ZstdSyncTagSize : 0;
    }

    std::optional<EZstdFrameType> TryDetectFrameKind(TFile* file, i64 offset)
    {
        YT_LOG_DEBUG("Detecting frame kind (Offset: %v)", offset);

        std::array<char, ZstdFrameSignatureSize> buffer;
        if (file->Pread(buffer.data(), buffer.size(), offset) != buffer.size()) {
            YT_LOG_DEBUG("Cannot read a complete frame signature");
            return std::nullopt;
        }

        auto type = DetectZstdFrameType(TRef(buffer.begin(), buffer.end()));
        YT_LOG_DEBUG("Frame kind detected (Type: %v)", type);
        return type;
    }

    std::optional<i64> TryDecodeDataFrame(TFile* file, i64 offset)
    {
        YT_LOG_DEBUG("Decoding data frame (Offset: %v)", offset);

        std::vector<char> inBuffer(BufferSize);
        ::ZSTD_inBuffer zstdInBuffer{inBuffer.data(), /*size*/ 0, /*pos*/ 0};
        i64 totalInSize = 0;

        std::vector<char> outBuffer(BufferSize);
        i64 totalOutSize = 0;

        ZSTD_initDStream(ZstdContext_.get());

        for (;;) {
            if (zstdInBuffer.pos == zstdInBuffer.size) {
                zstdInBuffer.size = file->Pread(inBuffer.data(), inBuffer.size(), offset);
                YT_LOG_DEBUG("Input buffer refilled (Offset: %v, BytesRead: %v)",
                    offset,
                    zstdInBuffer.size);

                zstdInBuffer.pos = 0;
                offset += zstdInBuffer.size;

                if (zstdInBuffer.size == 0) {
                    break;
                }
            }

            ::ZSTD_outBuffer zstdOutBuffer{outBuffer.data(), outBuffer.size(), /*pos*/ 0};
            auto oldInPos = zstdInBuffer.pos;
            auto result = ::ZSTD_decompressStream(ZstdContext_.get(), &zstdOutBuffer, &zstdInBuffer);
            if (::ZSTD_isError(result)) {
                THROW_ERROR_EXCEPTION("Error decompressing zstd frame")
                    << TError("%v", ::ZSTD_getErrorName(result));
            }

            auto inSize = zstdInBuffer.pos - oldInPos;
            totalInSize += inSize;

            YT_LOG_DEBUG("Data frame part decoded (InSize: %v, OutSize: %v)",
                inSize,
                zstdOutBuffer.pos);

            Listener_->OnData(TRef(outBuffer.data(), zstdOutBuffer.pos));

            totalOutSize += zstdOutBuffer.pos;

            if (result == 0) {
                YT_LOG_DEBUG("Data frame finished");
                break;
            }
        }

        YT_LOG_DEBUG("Data frame decoded (TotalInSize: %v, TotalOutSize: %v)",
            totalInSize,
            totalOutSize);

        return totalInSize;
    }

    bool TrySkipZstdSyncFrame(TFile* file, i64 offset)
    {
        YT_LOG_DEBUG("Skipping zstd sync frame (Offset: %v)", offset);

        std::array<char, ZstdSyncTagSize> buffer;
        if (file->Pread(buffer.data(), buffer.size(), offset) != buffer.size()) {
            YT_LOG_DEBUG("Cannot read a complete zstd sync frame");
            return false;
        }

        auto tagOffset = FindLastZstdSyncTagOffset(TRef(buffer.data(), buffer.size()), offset);
        YT_LOG_DEBUG("Zstd sync tag found (TagOffset: %v)", tagOffset);
        if (tagOffset != offset) {
            THROW_ERROR_EXCEPTION("Broken zstd sync frame at offset %v", offset);
        }

        YT_LOG_DEBUG("Zstd sync frame skipped");
        return true;
    }

    void Sleep()
    {
        ::Sleep(SleepQuantum);
    }
};

void RunTailer(
    std::string path,
    ITailListener* listener)
{
    TTailer(std::move(path), listener).Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZstdtail

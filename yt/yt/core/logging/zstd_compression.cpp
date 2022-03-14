#include "zstd_compression.h"

#include "compression.h"

#include <yt/yt/core/misc/finally.h>

#include <contrib/libs/zstd/include/zstd.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

// ZstdSyncTag is the constant part of a skippable frame appended after each zstd frame.
// It is ignored by tools and allows positioning after last fully written frame upon file opening.
constexpr const char ZstdSyncTag[] = {
    '\x50', '\x2a', '\x4d', '\x18', // zstd skippable frame magic number
    '\x18', '\x00', '\x00', '\x00', // data size: 128-bit ID + 64-bit offset

    // 128-bit sync tag ID
    '\xf6', '\x79', '\x9c', '\x4e', '\xd1', '\x09', '\x90', '\x7e',
    '\x29', '\x91', '\xd9', '\xe6', '\xbe', '\xe4', '\x84', '\x40'

    // 64-bit offset is written separately.
};

constexpr i64 MaxZstdFrameLength = ZSTD_COMPRESSBOUND(MaxZstdFrameUncompressedLength);
constexpr i64 ZstdSyncTagLength = sizeof(ZstdSyncTag) + sizeof(ui64);
constexpr i64 TailScanLength = MaxZstdFrameLength + 2 * ZstdSyncTagLength;

////////////////////////////////////////////////////////////////////////////////

static std::optional<i64> FindSyncTag(const char* buf, size_t size, i64 offset)
{
    const char* syncTag = nullptr;

    const char* data = buf;
    const char* end = buf + size;

    while (const char* tag = (char*)memmem(data, end - data, ZstdSyncTag, sizeof(ZstdSyncTag))) {
        data = tag + 1;

        if (tag + ZstdSyncTagLength > end) {
            continue;
        }

        ui64 tagOffset = *reinterpret_cast<const ui64*>(tag + sizeof(ZstdSyncTag));
        ui64 tagOffsetExpected = offset + (tag - buf);

        if (tagOffset == tagOffsetExpected) {
            syncTag = tag;
        }
    }

    if (!syncTag) {
        return {};
    }

    return offset + (syncTag - buf);
}

////////////////////////////////////////////////////////////////////////////////

class TZstdLogCompressionCodec
    : public ILogCompressionCodec
{
public:
    explicit TZstdLogCompressionCodec(int compressionLevel)
        : CompressionLevel_(compressionLevel)
    { }

    i64 GetMaxBlockSize() const override
    {
        return MaxZstdFrameUncompressedLength;
    }

    void Compress(const TBuffer& input, TBuffer& output) override
    {
        auto context = ZSTD_createCCtx();
        auto contextGuard = Finally([&] () {
            ZSTD_freeCCtx(context);
        });

        output.Reserve(output.Size() + MaxZstdFrameLength + ZstdSyncTagLength);
        size_t size = ZSTD_compressCCtx(
            context,
            output.Data() + output.Size(),
            MaxZstdFrameLength,
            input.Data(),
            input.Size(),
            CompressionLevel_);

        if (ZSTD_isError(size)) {
            THROW_ERROR_EXCEPTION("ZSTD_compressCCtx() failed")
                << TErrorAttribute("zstd_error", ZSTD_getErrorName(size));
        }
        output.Advance(size);
    }

    void AddSyncTag(i64 offset, TBuffer& output) override
    {
        output.Append(ZstdSyncTag, sizeof(ZstdSyncTag));
        output.Append(reinterpret_cast<const char*>(&offset), sizeof(offset));
    }

    void Repair(TFile* file, i64& outputPosition) override
    {
        constexpr i64 scanOverlap = ZstdSyncTagLength - 1;

        i64 fileSize = file->GetLength();
        i64 bufSize = fileSize;
        i64 pos = Max(bufSize - TailScanLength, 0L);
        bufSize -= pos;

        TBuffer buffer;

        outputPosition = 0;
        while (bufSize >= ZstdSyncTagLength) {
            buffer.Resize(0);
            buffer.Reserve(bufSize);

            size_t sz = file->Pread(buffer.Data(), bufSize, pos);
            buffer.Resize(sz);

            std::optional<i64> off = FindSyncTag(buffer.Data(), buffer.Size(), pos);
            if (off.has_value()) {
                outputPosition = *off + ZstdSyncTagLength;
                break;
            }

            i64 newPos = Max(pos - TailScanLength, 0L);
            bufSize = Max(pos + scanOverlap - newPos, 0L);
            pos = newPos;
        }
        file->Resize(outputPosition);
    }

private:
    int CompressionLevel_;
};

DECLARE_REFCOUNTED_TYPE(TZstdLogCompressionCodec)
DEFINE_REFCOUNTED_TYPE(TZstdLogCompressionCodec)

ILogCompressionCodecPtr CreateZstdCompressionCodec(int compressionLevel)
{
    return New<TZstdLogCompressionCodec>(compressionLevel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

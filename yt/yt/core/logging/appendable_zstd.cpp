#include "appendable_zstd.h"

#include <util/generic/buffer.h>
#include <util/generic/size_literals.h>

#include <util/system/file.h>
#include <util/system/types.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/finally.h>

#include <contrib/libs/zstd/include/zstd.h>

namespace NYT {
namespace NLogging {

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

struct TAppendableZstdFile::ZstdContext
    : public TNonCopyable
{
    ZstdContext()
        : CCtx(ZSTD_createCCtx())
    {
    }

    ~ZstdContext()
    {
        if (CCtx) {
            ZSTD_freeCCtx(CCtx);
        }
    }

    ZSTD_CCtx *CCtx;
};

////////////////////////////////////////////////////////////////////////////////

TAppendableZstdFile::TAppendableZstdFile(TFile* file, int compressionLevel, bool writeTruncateMessage)
    : CompressionLevel_(compressionLevel)
    , File_(file)
    , Context_(std::make_unique<ZstdContext>())
{
    Repair(writeTruncateMessage);
}

TAppendableZstdFile::~TAppendableZstdFile() = default;

void TAppendableZstdFile::DoWrite(const void* buf, size_t len)
{
    const char* in = reinterpret_cast<const char*>(buf);
    while (len > 0) {
        size_t toWrite = len;
        if (Input_.Size() >= MaxZstdFrameUncompressedLength) {
            toWrite = 0;
        } else if (Input_.Size() + len >= MaxZstdFrameUncompressedLength) {
            toWrite = MaxZstdFrameUncompressedLength - Input_.Size();
        }

        Input_.Append(in, toWrite);
        in += toWrite;
        len -= toWrite;

        while (Input_.Size() >= MaxZstdFrameUncompressedLength) {
            CompressOneFrame();
        }
    }
}

void TAppendableZstdFile::DoFlush()
{
    while (!Input_.Empty()) {
        CompressOneFrame();
    }
    FlushOutput();
}

void TAppendableZstdFile::DoFinish()
{
    Flush();
}

void TAppendableZstdFile::CompressOneFrame()
{
    if (Input_.Empty()) {
        return;
    }

    size_t toWrite = Min(Input_.Size(), size_t(MaxZstdFrameUncompressedLength));

    Output_.Reserve(Output_.Size() + MaxZstdFrameLength + ZstdSyncTagLength);
    size_t size = ZSTD_compressCCtx(
        Context_->CCtx,
        Output_.Data() + Output_.Size(),
        MaxZstdFrameLength,
        Input_.Data(),
        toWrite,
        CompressionLevel_);

    if (ZSTD_isError(size)) {
        THROW_ERROR_EXCEPTION("ZSTD_compressCCtx() failed")
            << TErrorAttribute("zstd_error", ZSTD_getErrorName(size));
    }
    Output_.Advance(size);

    ui64 syncTagOffset = OutputPosition_ + Output_.Size();
    Output_.Append(ZstdSyncTag, sizeof(ZstdSyncTag));
    Output_.Append(reinterpret_cast<const char*>(&syncTagOffset), sizeof(syncTagOffset));

    Input_.ChopHead(toWrite);
}

void TAppendableZstdFile::FlushOutput()
{
    if (Output_.Empty()) {
        return;
    }
    File_->Pwrite(Output_.Data(), Output_.Size(), OutputPosition_);
    OutputPosition_ += Output_.Size();
    Output_.Resize(0);
}

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

void TAppendableZstdFile::Repair(bool writeTruncateMessage)
{
    constexpr i64 scanOverlap = ZstdSyncTagLength - 1;

    i64 fileSize = File_->GetLength();
    i64 bufSize = fileSize;
    i64 pos = Max(bufSize - TailScanLength, 0L);
    bufSize -= pos;

    TBuffer buffer;

    OutputPosition_ = 0;
    while (bufSize >= ZstdSyncTagLength) {
        buffer.Resize(0);
        buffer.Reserve(bufSize);

        size_t sz = File_->Pread(buffer.Data(), bufSize, pos);
        buffer.Resize(sz);

        std::optional<i64> off = FindSyncTag(buffer.Data(), buffer.Size(), pos);
        if (off.has_value()) {
            OutputPosition_ = *off + ZstdSyncTagLength;
            break;
        }

        i64 newPos = Max(pos - TailScanLength, 0L);
        bufSize = Max(pos + scanOverlap - newPos, 0L);
        pos = newPos;
    }
    File_->Resize(OutputPosition_);

    if (OutputPosition_ != fileSize && writeTruncateMessage) {
        TStringBuilder message;
        message.AppendFormat("Truncated %v bytes due to zstd repair.\n", fileSize - OutputPosition_);
        TString messageStr = message.Flush();

        Input_.Append(messageStr.Data(), messageStr.Size());
        Flush();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT

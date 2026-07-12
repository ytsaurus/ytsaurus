#include "posting_codec.h"

#include "format.h"
#include "helpers.h"
#include "private.h"

#include <library/cpp/yt/coding/bit_io.h>
#include <library/cpp/yt/coding/interpolative.h>
#include <library/cpp/yt/coding/varint.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/system/unaligned_mem.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////
// V2: per-block groups of fingerprints, each with a 2-byte header (10-bit block
// index + 6-bit tag) followed by a tag-specific payload; a trigram's lists end
// with a terminator byte.

constexpr ui8 MaxShortBitpackedTag = 47;
constexpr ui8 MaxLongBitpackedTag = 55;
constexpr ui8 BitmapGroupTag = 60;
constexpr ui8 GroupSize1Tag = 61;
constexpr ui8 GroupSize2Tag = 62;
constexpr ui8 TerminatorGroupTag = 63;

constexpr int LineFingerprintBitmapByteSize = 32;
using TLineFingerprintBitmap = std::array<ui8, LineFingerprintBitmapByteSize>;

ui32 GetBitpackedSize(ui32 count, ui32 bitWidth)
{
    return (count * bitWidth + 7) / 8;
}

ui32 GetBitsPerDiff(int maxDiff)
{
    if (maxDiff <= 1) {
        return 1;
    } else if (maxDiff <= 3) {
        return 2;
    } else if (maxDiff <= 7) {
        return 3;
    } else if (maxDiff <= 15) {
        return 4;
    } else if (maxDiff <= 31) {
        return 5;
    } else if (maxDiff <= 63) {
        return 6;
    } else if (maxDiff <= 127) {
        return 7;
    } else {
        return 8;
    }
}

ui64 BitpackShort(const TPosting* __restrict__ src, ui32 groupSize, ui32 bitWidth)
{
    YT_ASSERT(groupSize >= 3 && groupSize <= 8);
    YT_ASSERT((groupSize - 1) * bitWidth <= 64);
    ui64 result = 0;
    ui32 bitOffset = 0;

#define PACK() \
            result |= static_cast<ui64>(LineFingerprintFromPosting(src[1] - src[0]) - 1) << bitOffset; \
            bitOffset += bitWidth; \
            src += 1;
#define PACK_CASE(n) \
            case n: \
            PACK() \
            [[fallthrough]];

    switch (groupSize) {
        PACK_CASE(8)
        PACK_CASE(7)
        PACK_CASE(6)
        PACK_CASE(5)
        PACK_CASE(4)
        default: break;
    }
    PACK()
    PACK()

#undef PACK
#undef PACK_CASE

    return result;
}

// BitmapByteSize plus some spare space at the end.
using TBitpackLongResult = std::array<ui32, 10>;

TBitpackLongResult BitpackLong(const TPosting* __restrict__ src, ui32 groupSize, ui32 bitWidth)
{
    // NB: No need to zero-initialize, see the end.
    TBitpackLongResult result;
    auto* dst = result.data();
    ui64 current = 0;
    ui32 bitOffset = 0;
    for (ui32 index = 0; index < groupSize - 1; ++index) {
        current |= static_cast<ui64>(LineFingerprintFromPosting(src[1] - src[0])) << bitOffset;
        src += 1;
        bitOffset += bitWidth;
        if (bitOffset >= 32) {
            *dst++ = current & 0xffffffff;
            current >>= 32;
            bitOffset -= 32;
        }
    }
    // Flush the tail and also ensure that the group zero terminator is written.
    *dst++ = current;
    *dst = 0;
    return result;
}

TLineFingerprintBitmap BuildBitmap(const TPosting* __restrict__ src, ui32 groupSize)
{
    // NB: Zero-initialize.
    TLineFingerprintBitmap bitmap{};
    for (ui32 index = 0; index < groupSize; ++index) {
        auto fingerprint = src[index] & LineFingerprintMask;
        bitmap[fingerprint >> 3] |= 1U << (fingerprint & 7);
    }
    return bitmap;
}

char* EncodeV2Group(char* ptr, const TPosting* group, ui32 groupSize, ui16 groupHeader, int maxFingerprintDiff)
{
    auto write = [&] (const void* data, size_t size) {
        std::memcpy(ptr, data, size);
        ptr += size;
    };
    auto writeU16 = [&] (ui16 value) { write(&value, sizeof(value)); };
    auto writeByte = [&] (ui8 value) { *ptr++ = static_cast<char>(value); };

    if (groupSize == 1) {
        writeU16(groupHeader | GroupSize1Tag);
        writeByte(LineFingerprintFromPosting(group[0]));
        return ptr;
    }
    if (groupSize == 2) {
        writeU16(groupHeader | GroupSize2Tag);
        writeByte(LineFingerprintFromPosting(group[0]));
        writeByte(LineFingerprintFromPosting(group[1]));
        return ptr;
    }
    if (/* groupSize >= 3 && */ groupSize <= 8) {
        ui32 bitsPerDiff = GetBitsPerDiff(maxFingerprintDiff - 1);
        auto bitpackedSize = GetBitpackedSize(groupSize - 1, bitsPerDiff);
        if (bitpackedSize + 1 < LineFingerprintBitmapByteSize) {
            writeU16(groupHeader | (bitsPerDiff - 1) | ((groupSize - 3) << 3));
            writeByte(LineFingerprintFromPosting(group[0]));
            ui64 packedValue = BitpackShort(group, groupSize, bitsPerDiff);
            write(&packedValue, bitpackedSize);
            return ptr;
        }
    }
    if (groupSize >= 8) {
        ui32 bitsPerDiff = GetBitsPerDiff(maxFingerprintDiff);
        int bitpackedSize = GetBitpackedSize(groupSize, bitsPerDiff);
        if (bitpackedSize + 1 < LineFingerprintBitmapByteSize) {
            writeU16(groupHeader | (47 + bitsPerDiff));
            writeByte(LineFingerprintFromPosting(group[0]));
            auto packedValue = BitpackLong(group, groupSize, bitsPerDiff);
            write(packedValue.data(), bitpackedSize);
            return ptr;
        }
    }

    writeU16(groupHeader | BitmapGroupTag);
    auto bitmap = BuildBitmap(group, groupSize);
    write(bitmap.data(), bitmap.size());
    return ptr;
}

std::pair<const char*, TPosting*> BitunpackShort(
    TPosting* __restrict__ dst,
    TPosting basePosting,
    const char* __restrict__ src,
    ui16 groupSize,
    ui16 bitWidth)
{
    auto blockIndex = BlockIndexFromPosting(basePosting);
    auto lineFingerprint = LineFingerprintFromPosting(basePosting);
    ui16 bitOffset = 0;
    ui16 bitMask = (1U << bitWidth) - 1;
    for (ui32 index = 1; index < groupSize; ++index) {
        ui16 value = ReadUnaligned<ui16>(src);
        auto diff = ((value >> bitOffset) & bitMask) + 1;

        bitOffset += bitWidth;
        if (bitOffset >= 8) {
            bitOffset -= 8;
            ++src;
        }

        lineFingerprint += diff;
        *dst++ = MakePosting(blockIndex, lineFingerprint);
    }
    if (bitOffset != 0) {
        ++src;
    }
    return {src, dst};
}

std::pair<const char*, TPosting*> BitunpackLong(
    TPosting* __restrict__ dst,
    TPosting basePosting,
    const char* __restrict__ src,
    ui16 bitWidth)
{
    auto blockIndex = BlockIndexFromPosting(basePosting);
    auto lineFingerprint = LineFingerprintFromPosting(basePosting);
    ui16 bitOffset = 0;
    ui16 bitMask = (1U << bitWidth) - 1;
    for (;;) {
        ui16 value = ReadUnaligned<ui16>(src);
        auto diff = (value >> bitOffset) & bitMask;

        bitOffset += bitWidth;
        if (bitOffset >= 8) {
            bitOffset -= 8;
            ++src;
        }

        if (diff == 0) {
            break;
        }

        lineFingerprint += diff;
        *dst++ = MakePosting(blockIndex, lineFingerprint);
    }
    if (bitOffset != 0) {
        ++src;
    }
    return {src, dst};
}

class TV2PostingCodec
    : public IPostingCodec
{
public:
    size_t GetMaxByteSize(int maxPostingCount, ui32 /*postingUpperBound*/) const override
    {
        // Worst case: every posting is a size-1 group (2-byte header + 1 byte),
        // plus the trailing terminator.
        return static_cast<size_t>(maxPostingCount) * 3 + 8;
    }

    char* Encode(char* output, TRange<TPosting> list, ui32 /*postingUpperBound*/) const override
    {
        char* ptr = output;
        int listSize = std::ssize(list);
        int startIndex = 0;
        while (startIndex < listSize) {
            ui32 blockIndex = BlockIndexFromPosting(list[startIndex]);
            YT_ASSERT(blockIndex < MaxBlocksPerChunk);
            int maxFingerprintDiff = 0;
            int endIndex = startIndex + 1;
            while (endIndex < listSize && BlockIndexFromPosting(list[endIndex]) == blockIndex) {
                maxFingerprintDiff = std::max(
                    maxFingerprintDiff,
                    static_cast<int>(LineFingerprintFromPosting(list[endIndex] - list[endIndex - 1])));
                ++endIndex;
            }
            auto groupSize = static_cast<ui32>(endIndex - startIndex);
            ui16 groupHeader = blockIndex << 6;
            ptr = EncodeV2Group(ptr, &list[startIndex], groupSize, groupHeader, maxFingerprintDiff);
            startIndex = endIndex;
        }
        *ptr++ = static_cast<char>(TerminatorGroupTag);
        return ptr;
    }

    void Decode(
        TRef data,
        int trigramCount,
        int postingCount,
        ui32 /*postingUpperBound*/,
        TPosting* postings,
        TPosting** listStarts) const override
    {
        const char* __restrict__ src = data.begin();
        auto* dst = postings;
        int trigramIndex = 0;
        listStarts[0] = dst;

        while (src != data.end()) {
            ui8 groupHeaderLo = *src++;
            if (groupHeaderLo == TerminatorGroupTag) {
                ++trigramIndex;
                listStarts[trigramIndex] = dst;
                continue;
            }

            ui8 groupHeaderHi = *src++;
            ui16 groupHeader = groupHeaderLo | (groupHeaderHi << 8);
            ui16 blockIndex = groupHeader >> 6;
            ui8 tag = groupHeader & ((1U << 6) - 1);

            if (tag == GroupSize1Tag) {
                TLineFingerprint lineFingerprint = *src++;
                *dst++ = MakePosting(blockIndex, lineFingerprint);
            } else if (tag == GroupSize2Tag) {
                TLineFingerprint lineFingerprint1 = *src++;
                TLineFingerprint lineFingerprint2 = *src++;
                *dst++ = MakePosting(blockIndex, lineFingerprint1);
                *dst++ = MakePosting(blockIndex, lineFingerprint2);
            } else if (tag == BitmapGroupTag) {
                auto bitmap = ReadUnaligned<TLineFingerprintBitmap>(src);
                src += LineFingerprintBitmapByteSize;
                for (ui16 lineFingerprint = 0;
                    lineFingerprint <= std::numeric_limits<TLineFingerprint>::max();
                    ++lineFingerprint)
                {
                    if (bitmap[lineFingerprint >> 3] & (1U << (lineFingerprint & 7))) {
                        *dst++ = MakePosting(blockIndex, lineFingerprint);
                    }
                }
            } else if (tag <= MaxShortBitpackedTag) {
                int bitsPerDiff = (tag & 7) + 1;
                int groupSize = (tag >> 3) + 3;
                TLineFingerprint lineFingerprint = *src++;
                *dst++ = MakePosting(blockIndex, lineFingerprint);
                std::tie(src, dst) = BitunpackShort(
                    dst, MakePosting(blockIndex, lineFingerprint), src, groupSize, bitsPerDiff);
            } else if (tag <= MaxLongBitpackedTag) {
                int bitsPerDiff = tag - MaxShortBitpackedTag;
                TLineFingerprint lineFingerprint = *src++;
                *dst++ = MakePosting(blockIndex, lineFingerprint);
                std::tie(src, dst) = BitunpackLong(
                    dst, MakePosting(blockIndex, lineFingerprint), src, bitsPerDiff);
            } else {
                THROW_ERROR_EXCEPTION("Broken index segment");
            }
        }

        if (trigramIndex != trigramCount || dst != postings + postingCount) {
            THROW_ERROR_EXCEPTION("Broken index segment");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////
// V3: each trigram's whole posting list is stored as a varint length prefix
// plus a single binary interpolative code over the joint posting universe.

class TV3PostingCodec
    : public IPostingCodec
{
public:
    size_t GetMaxByteSize(int maxPostingCount, ui32 postingUpperBound) const override
    {
        return MaxVarUint32Size + GetInterpolativeMaxByteSize(maxPostingCount, /*lo*/ 0, postingUpperBound);
    }

    char* Encode(char* output, TRange<TPosting> list, ui32 postingUpperBound) const override
    {
        char* ptr = output;
        ptr += WriteVarUint32(ptr, static_cast<ui32>(list.size()));
        TBitWriter writer(ptr);
        InterpolativeEncode(&writer, list, /*lo*/ 0, postingUpperBound);
        return writer.Finish();
    }

    void Decode(
        TRef data,
        int trigramCount,
        int postingCount,
        ui32 postingUpperBound,
        TPosting* postings,
        TPosting** listStarts) const override
    {
        const char* src = data.begin();
        auto* dst = postings;
        listStarts[0] = dst;

        for (int trigramIndex = 0; trigramIndex < trigramCount; ++trigramIndex) {
            ui32 listSize;
            src += ReadVarUint32(src, &listSize);

            TBitReader reader(src);
            InterpolativeDecode(&reader, TMutableRange(dst, listSize), /*lo*/ 0, postingUpperBound);
            src = reader.Finish();

            dst += listSize;
            listStarts[trigramIndex + 1] = dst;
        }

        if (src > data.end() || dst != postings + postingCount) {
            THROW_ERROR_EXCEPTION("Broken index segment");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IPostingCodec* GetPostingCodec(EIndexFormat format)
{
    static TV2PostingCodec V2Codec;
    static TV3PostingCodec V3Codec;
    switch (format) {
        case EIndexFormat::V2:
            return &V2Codec;
        case EIndexFormat::V3:
            return &V3Codec;
    }
    YT_ABORT();
}

ui64 GetIndexFormatSignature(EIndexFormat format)
{
    switch (format) {
        case EIndexFormat::V2:
            return TIndexFileHeader::V2Signature;
        case EIndexFormat::V3:
            return TIndexFileHeader::V3Signature;
    }
    YT_ABORT();
}

EIndexFormat GetIndexFormatFromSignature(ui64 signature)
{
    switch (signature) {
        case TIndexFileHeader::V2Signature:
            return EIndexFormat::V2;
        case TIndexFileHeader::V3Signature:
            return EIndexFormat::V3;
        default:
            THROW_ERROR_EXCEPTION("Invalid index file signature %x", signature);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep

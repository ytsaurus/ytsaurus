#pragma once

#include <util/system/types.h>

#include <yt/yt/core/misc/checksum.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 1)

struct TIndexFileHeader
{
    static constexpr ui64 V2Signature = 0x32'30'30'30'30'49'52'54; // TRI00002

    ui64 Signature;
};

struct TChunkIndexHeader
{
    i64 InputStartOffset;
    i64 InputSize;
    i64 FirstLineIndex;
    i64 SegmentsSize;
    i32 TrigramCount;
    i32 IndexedTrigramCount;
    i32 LineCount;
    i32 FrameCount;
    i32 BlockCount;
    i32 SegmentCount;
};

struct TFrameHeader
{
    i64 InputStartOffset;
    i64 InputSize;
    TChecksum Checksum;
    i32 LineCount;
    i32 BlockCount;
};

struct TBlockHeader
{
    i32 LineCount;
};

struct TIndexSegmentHeader
{
    i32 TrigramCount;
    i32 PostingCount;
    i32 ByteSize;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep

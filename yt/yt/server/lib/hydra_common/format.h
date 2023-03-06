#pragma once

#include "public.h"

#include <yt/yt/core/compression/public.h>

#include <util/generic/typetraits.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

constexpr auto ChangelogPageAlignment = 4_KBs;
constexpr auto ChangelogQWordAlignment = 8;

#pragma pack(push, 1)

struct TChangelogHeader_5
{
    //! Used for format validation.
    static constexpr ui64 ExpectedSignature = 0x3530303044435459ull; // YTCD0005

    ui64 Signature;
    i32 FirstRecordOffset;
    i32 MetaSize;
    i32 UnusedMustBeMinus2;
    i32 PaddingSize;
    TGuid Uuid;
};

static_assert(sizeof(TChangelogHeader_5) == 40, "Binary size of TChangelogHeader_5 has changed.");
static_assert(sizeof(TChangelogHeader_5) % ChangelogQWordAlignment == 0, "TChangelogHeader_5 is not aligned properly.");

using TChangelogHeader = TChangelogHeader_5;

constexpr auto MinChangelogHeaderSize = sizeof(TChangelogHeader_5);
constexpr auto MaxChangelogHeaderSize = sizeof(TChangelogHeader_5);

////////////////////////////////////////////////////////////////////////////////

struct TChangelogRecordHeader_5
{
    i32 RecordIndex;
    i32 PayloadSize;
    TChecksum Checksum;
    i32 PagePaddingSize;
    TGuid ChangelogUuid;
    ui32 Padding;
};

static_assert(sizeof(TChangelogRecordHeader_5) == 40, "Binary size of TChangelogRecordHeader_5 has changed.");
static_assert(sizeof(TChangelogRecordHeader_5) % ChangelogQWordAlignment == 0, "TChangelogRecordHeader_5 is not aligned properly.");

using TChangelogRecordHeader = TChangelogRecordHeader_5;

////////////////////////////////////////////////////////////////////////////////

struct TChangelogIndexHeader_5
{
    //! Used for format validation.
    static constexpr ui64 ExpectedSignature = 0x3530303049435459ull; // YTCI0005

    ui64 Signature;
};

static_assert(sizeof(TChangelogIndexHeader_5) == 8, "Binary size of TChangelogIndexHeader_5 has changed.");

using TChangelogIndexHeader = TChangelogIndexHeader_5;

////////////////////////////////////////////////////////////////////////////////

struct TChangelogIndexSegmentHeader_5
{
    //! Computed for the whole segment excluding the checksum ifself.
    ui64 Checksum;
    //! Number of records in this segment.
    i32 RecordCount;
    ui32 Padding;

    // Next follow #RecordCount i64-offsets to records within changelog data file.
};

static_assert(sizeof(TChangelogIndexSegmentHeader_5) == 16, "Binary size of TChangelogIndexSegmentHeader has changed.");

using TChangelogIndexSegmentHeader = TChangelogIndexSegmentHeader_5;

////////////////////////////////////////////////////////////////////////////////

struct TChangelogIndexRecord_5
{
    i64 Offset;
    i64 Length;
};

static_assert(sizeof(TChangelogIndexRecord_5) == 16, "Binary size of TChangelogIndexRecord_5 has changed.");

using TChangelogIndexRecord = TChangelogIndexRecord_5;

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotHeader_3
{
    static constexpr ui64 ExpectedSignature = 0x3330303053535459ull; // YTSS0003

    ui64 Signature;
    i32 SnapshotId;
    ui64 CompressedLength;
    ui64 UncompressedLength;
    ui64 Checksum;
    NCompression::ECodec Codec;
    ui8 Padding[3];
    i32 MetaSize;
};

static_assert(sizeof(TSnapshotHeader_3) == 44, "Binary size of TSnapshotHeader_3 has changed.");

using TSnapshotHeader = TSnapshotHeader_3;

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

Y_DECLARE_PODTYPE(NYT::NHydra::TChangelogHeader_5);
Y_DECLARE_PODTYPE(NYT::NHydra::TChangelogRecordHeader_5);
Y_DECLARE_PODTYPE(NYT::NHydra::TChangelogIndexHeader_5);
Y_DECLARE_PODTYPE(NYT::NHydra::TChangelogIndexSegmentHeader_5);
Y_DECLARE_PODTYPE(NYT::NHydra::TSnapshotHeader_3);


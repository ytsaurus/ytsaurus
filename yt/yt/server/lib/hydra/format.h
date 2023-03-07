#pragma once

#include "public.h"

#include <yt/core/compression/public.h>

#include <yt/core/misc/serialize.h>

#include <util/system/align.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 1)

// COMPAT(babenko)
struct TChangelogHeader_4
{
    //! Used for format validation.
    static const ui64 ExpectedSignature = 0x3430303044435459ull; // YTCD0004

    //! Indicates that the changelog is not yet sealed.
    static constexpr i32 NotTruncatedRecordCount = -2;

    ui64 Signature;
    i32 FirstRecordOffset;
    i32 MetaSize;
    i32 TruncatedRecordCount;
    i32 PaddingSize;
};

static_assert(sizeof (TChangelogHeader_4) == 24, "Binary size of TChangelogHeader_4 has changed.");

struct TChangelogHeader_5
    : public TChangelogHeader_4
{
    //! Used for format validation.
    static constexpr ui64 ExpectedSignature = 0x3530303044435459ull; // YTCD0005

    TGuid Uuid;
};

static_assert(sizeof (TChangelogHeader_5) == 40, "Binary size of TChangelogHeader_5 has changed.");

using TChangelogHeader = TChangelogHeader_5;

////////////////////////////////////////////////////////////////////////////////

struct TChangelogRecordHeader_4
{
    i32 RecordId;
    i32 DataSize;
    TChecksum Checksum;
    i32 PaddingSize;
};

static_assert(sizeof(TChangelogRecordHeader_4) == 20, "Binary size of TChangelogRecordHeader_4 has changed.");

struct TChangelogRecordHeader_5
    : public TChangelogRecordHeader_4
{
    TGuid ChangelogUuid;
};

static_assert(sizeof(TChangelogRecordHeader_5) == 36, "Binary size of TChangelogRecordHeader_5 has changed.");

using TChangelogRecordHeader = TChangelogRecordHeader_5;

////////////////////////////////////////////////////////////////////////////////

struct TChangelogIndexHeader
{
    //! Used for format validation.
    static constexpr ui64 ExpectedSignature = 0x3430303049435459ull; // YTCI0004

    ui64 Signature;
    i32 IndexRecordCount;
    ui32 Padding;
};

static_assert(sizeof(TChangelogIndexHeader) == 16, "Binary size of TChangelogIndexHeader has changed.");
static_assert(sizeof(TChangelogIndexHeader) >= 12, "TChangelogIndexHeader must be >= 12.");

////////////////////////////////////////////////////////////////////////////////

struct TChangelogIndexRecord
{
    i64 FilePosition;
    i32 RecordId;
    ui32 Padding;
};

static_assert(sizeof(TChangelogIndexRecord) == 16, "Binary size of TLogIndexRecord has changed.");

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotHeader
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

static_assert(sizeof(TSnapshotHeader) == 44, "Binary size of TSnapshotHeader has changed.");

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

Y_DECLARE_PODTYPE(NYT::NHydra::TChangelogHeader_4);
Y_DECLARE_PODTYPE(NYT::NHydra::TChangelogHeader_5);
Y_DECLARE_PODTYPE(NYT::NHydra::TChangelogRecordHeader_4);
Y_DECLARE_PODTYPE(NYT::NHydra::TChangelogRecordHeader_5);
Y_DECLARE_PODTYPE(NYT::NHydra::TChangelogIndexHeader);
Y_DECLARE_PODTYPE(NYT::NHydra::TChangelogIndexRecord);
Y_DECLARE_PODTYPE(NYT::NHydra::TSnapshotHeader);


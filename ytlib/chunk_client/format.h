#pragma once

#include "private.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

//! Base part of any chunk meta header.
struct TChunkMetaHeaderBase
{
    //! Signature, must match |ExpectedSignature| for the corresponding meta file version.
    ui64 Signature;
};

//! Describes a chunk meta header, version 1.
struct TChunkMetaHeader_1
    : public TChunkMetaHeaderBase
{
    static constexpr ui64 ExpectedSignature = 0x313030484d435459ull; // YTCMH001;

    //! Chunk meta checksum.
    TChecksum Checksum;
};

static_assert(sizeof(TChunkMetaHeader_1) == 16, "sizeof(TChunkMetaHeader_1) != 16");

//! Describes a chunk meta header, version 2.
struct TChunkMetaHeader_2
    : public TChunkMetaHeader_1
{
    static constexpr ui64 ExpectedSignature = 0x323030484d435459ull; // YTCMH002;

    //! Chunk id, used for validation.
    TChunkId ChunkId;
};

static_assert(sizeof(TChunkMetaHeader_2) == 32, "sizeof(TChunkMetaHeader_2) != 32");

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

Y_DECLARE_PODTYPE(NYT::NChunkClient::TChunkMetaHeader_1);
Y_DECLARE_PODTYPE(NYT::NChunkClient::TChunkMetaHeader_2);

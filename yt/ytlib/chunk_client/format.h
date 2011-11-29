#pragma once

#include "common.h"

#include "../misc/checksum.h"

namespace NYT {
namespace NChunkClient {

//! Represents an offset inside a chunk.
typedef i64 TChunkOffset;

#pragma pack(push, 4)

////////////////////////////////////////////////////////////////////////////////

const char* const ChunkInfoSuffix = ".meta";

////////////////////////////////////////////////////////////////////////////////

//! Describes a chunk info header.
struct TChunkInfoHeader
{
    static const ui64 ExpectedSignature = 0x3130304849435459ull; // YTCIH001

    //! Signature, must be #ExpectedSignature for valid chunks.
    ui64 Signature;
    
    //! Chunk info checksum.
    TChecksum Checksum;
};

////////////////////////////////////////////////////////////////////////////////

#pragma pack(pop)

} // namespace NChunkClient
} // namespace NYT

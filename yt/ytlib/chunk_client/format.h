#pragma once

#include "private.h"

#include <ytlib/misc/checksum.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Represents an offset inside a chunk.
typedef i64 TChunkOffset;

//! A suffix to distinguish chunk meta files.
const char* const ChunkMetaSuffix = ".meta";

#pragma pack(push, 4)

//! Describes a chunk meta header.
struct TChunkMetaHeader
{
    static const ui64 ExpectedSignature = 0x313030484d435459ull; // YTCMH001

    //! Signature, must be #ExpectedSignature for valid chunks.
    ui64 Signature;
    
    //! Chunk meta checksum.
    TChecksum Checksum;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

#pragma once

#include "common.h"

#include "../misc/checksum.h"

namespace NYT {

//! Represents an offset inside a chunk.
typedef i64 TBlockOffset;

// Use 8-bytes alignment mainly because of checksums.
#pragma pack(push, 8)

////////////////////////////////////////////////////////////////////////////////

//! Describes a block of a chunk.
struct TBlockInfo
{
    i32 Size;
    TChecksum Checksum;
};

////////////////////////////////////////////////////////////////////////////////

//! Describes a chunk footer.
/*!
 *  Every chunk has the following layout:
 *  1. A sequence of blocks written without padding or delimiters.
 *  2. An optional padding.
 *  3. A sequence of TBlockInfo s.
 *  4. A footer.
 */
struct TChunkFooter
{
    static const ui32 ExpectedSignature = 0x46435459; // YTCF
    
    ui32 Singature;
    i32 BlockCount;
    i64 BlockInfoOffset;
};

////////////////////////////////////////////////////////////////////////////////

#pragma pack(pop)

} // namespace NYT

#pragma once

#include "common.h"

#include "../misc/checksum.h"

namespace NYT {

//! Represents an offset inside a chunk.
typedef i64 TChunkOffset;

#pragma pack(push, 4)

////////////////////////////////////////////////////////////////////////////////

//! Describes a chunk footer.
/*!
 *  Every chunk has the following layout:
 *  1. Sequence of blocks written without padding or delimiters.
 *  2. Protobuf-serialized meta.
 *  3. Footer.
 */
struct TChunkFooter
{
    static const ui32 ExpectedSignature = 0x46435459; // YTCF

    //! Signature, must be #ExpectedSignature for valid chunks.
    ui32 Singature;
    
    //! The size of the meta, in bytes.
    i32 MetaSize;

    //! The offset of the meta inside the file.
    TChunkOffset MetaOffset;
};

////////////////////////////////////////////////////////////////////////////////

#pragma pack(pop)

} // namespace NYT

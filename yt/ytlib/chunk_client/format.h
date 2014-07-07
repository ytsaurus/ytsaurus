#pragma once

#include "private.h"

#include <core/misc/checksum.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

//! Describes a chunk meta header.
struct TChunkMetaHeader
{
    static const ui64 ExpectedSignature;

    //! Signature, must be #ExpectedSignature for valid chunks.
    ui64 Signature;

    //! Chunk meta checksum.
    TChecksum Checksum;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

DECLARE_PODTYPE(NYT::NChunkClient::TChunkMetaHeader)
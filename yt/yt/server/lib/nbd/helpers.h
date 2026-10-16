#pragma once

#include <util/system/types.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

//! The block granularity and extent of a block device: #BlockCount equally-sized #BlockSize blocks.
struct TBlockDeviceGeometry
{
    i64 BlockSize = 0;
    i64 BlockCount = 0;

    //! Total addressable size, in bytes.
    i64 GetByteSize() const;
};

//! Validates that a request at #offset of #length bytes is aligned to the geometry's block size (both
//! the offset and the length must be multiples of it) and lies within [0, size]. Throws otherwise.
void ValidateBlockRequest(i64 offset, i64 length, const TBlockDeviceGeometry& geometry);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd

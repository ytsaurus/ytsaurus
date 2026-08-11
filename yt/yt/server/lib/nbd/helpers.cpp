#include "helpers.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

i64 TBlockDeviceGeometry::GetByteSize() const
{
    return BlockSize * BlockCount;
}

////////////////////////////////////////////////////////////////////////////////

void ValidateBlockRequest(i64 offset, i64 length, const TBlockDeviceGeometry& geometry)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        offset % geometry.BlockSize == 0 && length % geometry.BlockSize == 0,
        "Request offset %v and length %v must be divisible by block size %v",
        offset,
        length,
        geometry.BlockSize);

    THROW_ERROR_EXCEPTION_UNLESS(
        offset >= 0 && length >= 0 && offset + length <= geometry.GetByteSize(),
        TError("Request is out of device bounds")
            .With("offset", offset)
            .With("length", length)
            .With("device_size", geometry.GetByteSize()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd

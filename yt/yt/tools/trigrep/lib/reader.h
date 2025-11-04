#pragma once

#include "public.h"

#include <util/stream/input.h>

#include <util/system/types.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

struct ISequentialReader
{
    virtual ~ISequentialReader() = default;

    virtual i64 GetTotalInputSize() const = 0;
    virtual i64 GetCurrentFrameStartOffset() const = 0;
    virtual std::unique_ptr<IInputStream> TryBeginNextFrame() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IRandomReader
{
    virtual ~IRandomReader() = default;

    virtual std::unique_ptr<IInputStream> CreateFrameStream(
        i64 startOffset,
        i64 endOffset) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep

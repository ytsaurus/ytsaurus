#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NColumnarChunkFormat {

////////////////////////////////////////////////////////////////////////////////

struct TReadSpan
{
    ui32 Lower = 0;
    ui32 Upper = 0;

    constexpr TReadSpan(ui32 lower, ui32 upper)
        : Lower(lower)
        , Upper(upper)
    { }

    constexpr TReadSpan() = default;
};

Y_FORCE_INLINE bool IsEmpty(TReadSpan span)
{
    return span.Lower == span.Upper;
}

struct TSpanMatching
{
    TReadSpan Chunk;
    TReadSpan Control;

    TSpanMatching(TReadSpan chunk, TReadSpan control)
        : Chunk(chunk)
        , Control(control)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnarChunkFormat

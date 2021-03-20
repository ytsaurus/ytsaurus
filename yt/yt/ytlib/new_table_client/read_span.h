#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NNewTableClient {

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient

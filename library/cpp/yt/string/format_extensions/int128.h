#pragma once

#include <library/cpp/yt/string/format.h>

#include <library/cpp/int128/int128.h>

////////////////////////////////////////////////////////////////////////////////

template <bool IsSigned>
inline void FormatValue(NYT::TStringBuilderBase* builder, const TInteger128<IsSigned>& value, TStringBuf spec)
{
    // TODO(arkady-e1ppa): Optimize.
    FormatValue(builder, NYT::ToStringIgnoringFormatValue(value), spec);
}

////////////////////////////////////////////////////////////////////////////////

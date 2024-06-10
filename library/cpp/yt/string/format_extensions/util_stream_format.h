#pragma once

#include <library/cpp/yt/string/format.h>

#include <util/stream/format.h>

////////////////////////////////////////////////////////////////////////////////

inline void FormatValue(
    NYT::TStringBuilderBase* builder,
    NFormatPrivate::THumanReadableDuration duration,
    TStringBuf spec)
{
    FormatValue(builder, NYT::ToStringIgnoringFormatValue(duration), spec);
}

inline void FormatValue(
    NYT::TStringBuilderBase* builder,
    NFormatPrivate::THumanReadableSize size,
    TStringBuf spec)
{
    FormatValue(builder, NYT::ToStringIgnoringFormatValue(size), spec);
}

////////////////////////////////////////////////////////////////////////////////

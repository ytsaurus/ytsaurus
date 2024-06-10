#pragma once

#include <library/cpp/yt/string/format.h>

#include <util/system/src_location.h>

////////////////////////////////////////////////////////////////////////////////

inline void FormatValue(NYT::TStringBuilderBase* builder, const ::TSourceLocation loc, TStringBuf spec)
{
    FormatValue(builder, NYT::ToStringIgnoringFormatValue(loc), spec);
}

////////////////////////////////////////////////////////////////////////////////

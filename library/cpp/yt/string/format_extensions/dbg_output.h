#pragma once

#include <library/cpp/yt/string/format.h>

#include <library/cpp/dbg_output/dump.h>

#include <util/string/cast.h>

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <class T, class TTraits>
inline void FormatValue(NYT::TStringBuilderBase* builder, const TDbgDump<T, TTraits>& dump, TStringBuf spec)
{
    FormatValue(builder, NYT::ToStringIgnoringFormatValue(dump), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

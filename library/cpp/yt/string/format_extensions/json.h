#pragma once

#include <library/cpp/yt/string/format.h>

#include <library/cpp/json/writer/json_value.h>

namespace NJson {

////////////////////////////////////////////////////////////////////////////////

inline void FormatValue(NYT::TStringBuilderBase* builder, const TJsonValue& value, TStringBuf spec)
{
    FormatValue(builder, NYT::ToStringIgnoringFormatValue(value), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJson

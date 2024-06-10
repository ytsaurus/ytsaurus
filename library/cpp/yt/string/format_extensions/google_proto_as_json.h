#pragma once

#include <library/cpp/yt/string/format.h>

#include <contrib/libs/protobuf/src/google/protobuf/json_util.h>

namespace google::protobuf::io {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void FormatValue(NYT::TStringBuilderBase* builder, const TAsJSON<T>& proto, TStringBuf spec)
{
    FormatValue(builder, NYT::ToStringIgnoringFormatValue(proto), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace google::protobuf::io

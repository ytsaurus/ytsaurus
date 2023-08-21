#ifndef UNESCAPED_YSON_INL_H_
#error "Direct inclusion of this file is not allowed, include unescaped_yson.h"
// For the sake of sane code completion.
#include "unescaped_yson.h"
#endif

#include <yt/yt/core/ytree/serialize.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
NYson::TYsonString ConvertToYsonStringExtendedFormat(const T& value, EExtendedYsonFormat format)
{
    auto type = NYTree::GetYsonType(value);
    TString result;
    TStringOutput stringOutput(result);
    TExtendedYsonWriter writer(&stringOutput, format, type);
    Serialize(value, &writer);
    return NYson::TYsonString(std::move(result), type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

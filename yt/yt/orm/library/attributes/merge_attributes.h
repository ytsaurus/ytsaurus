#pragma once

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

struct TAttributeValue
{
    NYPath::TYPath Path;
    NYson::TYsonString Value;
};

NYson::TYsonString MergeAttributes(
    std::vector<TAttributeValue> attributeValues,
    NYson::EYsonFormat format = NYson::EYsonFormat::Binary);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes

#pragma once

#include "public.h"

#include <yt/core/yson/public.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForYson(
    NYson::IYsonConsumer* consumer,
    NYson::EYsonType type = NYson::EYsonType::Node,
    bool enableLinePositionInfo = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats

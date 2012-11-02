#pragma once

#include "public.h"

#include <ytlib/yson/public.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<IParser> CreateParserForYson(
    NYson::IYsonConsumer* consumer,
    NYson::EYsonType type = NYson::EYsonType::Node,
    bool enableLinePositionInfo = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

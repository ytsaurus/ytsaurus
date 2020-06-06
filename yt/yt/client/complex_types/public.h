#pragma once

#include <yt/core/misc/public.h>

#include <yt/core/yson/public.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

template <typename... TArgs>
using TComplexTypeYsonScanner = std::function<void(NYson::TYsonPullParserCursor*, TArgs...)>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
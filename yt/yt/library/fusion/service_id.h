#pragma once

#include "public.h"

#include <library/cpp/yt/misc/strong_typedef.h>

#include <typeindex>

namespace NYT::NFusion {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TServiceId, std::type_index);

void FormatValue(TStringBuilderBase* builder, TServiceId id, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFusion

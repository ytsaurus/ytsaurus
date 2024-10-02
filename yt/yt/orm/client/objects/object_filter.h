#pragma once

#include "public.h"

#include <library/cpp/yt/string/format.h>

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TObjectFilter& filter, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects

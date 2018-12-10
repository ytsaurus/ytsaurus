#pragma once

#include "public.h"

#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NJobProberClient {

////////////////////////////////////////////////////////////////////////////////

std::optional<int> FindSignalIdBySignalName(const TString& signalName);
void ValidateSignalName(const TString& signalName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProberClient
} // namespace NYT

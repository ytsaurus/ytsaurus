// This engine is deprecated in favor of SpytConnectEngine
#pragma once

#include "private.h"

#include <yt/yt/client/api/public.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateSpytEngine(NApi::IClientPtr stateClient, NYPath::TYPath stateRoot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker

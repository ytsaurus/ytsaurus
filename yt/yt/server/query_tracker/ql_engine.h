#pragma once

#include "private.h"

#include <yt/yt/client/api/public.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateQLEngine(const NApi::IClientPtr& stateClient, const NYPath::TYPath& stateRoot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker

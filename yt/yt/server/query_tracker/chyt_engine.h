#pragma once

#include "private.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateChytEngine(const NApi::IClientPtr& stateClient, const NYPath::TYPath& stateRoot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker

#pragma once

#include "private.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration DefaultChytQueryTimeout = TDuration::Hours(6);

////////////////////////////////////////////////////////////////////////////////

IQueryEnginePtr CreateChytEngine(const NApi::NNative::IClientPtr& stateClient, const NYPath::TYPath& stateRoot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker

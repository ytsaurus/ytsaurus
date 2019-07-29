#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger CypressServerLogger;
extern const NLogging::TLogger CypressAccessLogger;

DECLARE_REFCOUNTED_CLASS(TAccessTracker)
DECLARE_REFCOUNTED_CLASS(TExpirationTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

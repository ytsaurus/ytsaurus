#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, CypressServerLogger, "Cypress");

DECLARE_REFCOUNTED_CLASS(TAccessTracker)
DECLARE_REFCOUNTED_CLASS(TExpirationTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

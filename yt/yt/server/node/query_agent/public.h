#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IQuerySubexecutor)

DECLARE_REFCOUNTED_STRUCT(TQueryAgentConfig)
DECLARE_REFCOUNTED_STRUCT(TQueryAgentDynamicConfig)

DECLARE_REFCOUNTED_STRUCT(IDistributedSessionManager)
DECLARE_REFCOUNTED_STRUCT(IDistributedSession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent

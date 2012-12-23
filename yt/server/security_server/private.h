#pragma once

#include "public.h"

#include <ytlib/logging/log.h>

#include <ytlib/profiling/profiler.h>

#include <ytlib/meta_state/map.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger SecurityServerLogger;
extern NProfiling::TProfiler SecurityServerProfiler;

typedef NMetaState::TMetaStateMap<TAccountId, TAccount> TAccountMetaMap;

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, HiveServerLogger, "HiveServer");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, HiveServerProfiler, "/hive");

class TMailbox;
DECLARE_ENTITY_TYPE(TCellMailbox, TCellId, ::THash<TCellId>)
DECLARE_ENTITY_TYPE(TAvenueMailbox, TAvenueEndpointId, ::THash<TAvenueEndpointId>)

DECLARE_REFCOUNTED_STRUCT(TMailboxRuntimeData)
DECLARE_REFCOUNTED_STRUCT(TCellMailboxRuntimeData)
DECLARE_REFCOUNTED_STRUCT(TAvenueMailboxRuntimeData)

DECLARE_REFCOUNTED_CLASS(TPersistentMailboxState)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer

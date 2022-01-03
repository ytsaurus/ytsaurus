#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENTITY_TYPE(TCommit, TTransactionId, ::THash<TTransactionId>)

class TAbort;

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger HiveServerLogger("HiveServer");
inline const NProfiling::TProfiler HiveServerProfiler("/hive");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer

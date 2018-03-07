#pragma once

#include "public.h"

#include <yt/server/hydra/public.h>

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENTITY_TYPE(TCommit, TTransactionId, ::THash<TTransactionId>)
class TAbort;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger HiveServerLogger;
extern const NProfiling::TProfiler HiveServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT

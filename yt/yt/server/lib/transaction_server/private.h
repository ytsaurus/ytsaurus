#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger TransactionServerLogger;
extern const NProfiling::TProfiler TransactionServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer


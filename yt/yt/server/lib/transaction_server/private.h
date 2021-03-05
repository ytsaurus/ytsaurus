#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger TransactionServerLogger;
extern const NProfiling::TRegistry TransactionServerProfiler;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer


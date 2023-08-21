#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger TransactionServerLogger("TransactionServer");
inline const NProfiling::TProfiler TransactionServerProfiler("/transaction_server");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer


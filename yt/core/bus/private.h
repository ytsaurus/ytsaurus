#pragma once

#include "public.h"

#include <core/misc/enum.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger BusLogger;
extern const NProfiling::TProfiler BusProfiler;

struct TTcpInterfaceStatistics;

using TConnectionId = TGuid;
using TPacketId = TGuid;

DECLARE_REFCOUNTED_STRUCT(IEventLoopObject)

DECLARE_REFCOUNTED_CLASS(TTcpConnection)
DECLARE_REFCOUNTED_CLASS(TTcpDispatcherThread)

DEFINE_ENUM(EConnectionType,
    (Client)
    (Server)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT


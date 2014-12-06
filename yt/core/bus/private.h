#pragma once

#include "public.h"

#include <core/misc/enum.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

extern const NLog::TLogger BusLogger;
extern NProfiling::TProfiler BusProfiler;

typedef TGuid TConnectionId;
typedef TGuid TPacketId;

DECLARE_REFCOUNTED_STRUCT(IEventLoopObject)

DECLARE_REFCOUNTED_CLASS(TTcpConnection)
DECLARE_REFCOUNTED_CLASS(TTcpDispatcherThread)

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EConnectionType,
    (Client)
    (Server)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT


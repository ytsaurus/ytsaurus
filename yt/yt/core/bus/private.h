#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/enum.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger BusLogger;
extern const NProfiling::TProfiler BusProfiler;

using TConnectionId = TGuid;
using TPacketId = TGuid;

DECLARE_REFCOUNTED_CLASS(TTcpConnection)

DEFINE_ENUM(EConnectionType,
    (Client)
    (Server)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus


#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/enum.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger BusLogger("Bus");
inline const NProfiling::TProfiler BusProfiler("/bus");

using TConnectionId = TGuid;
using TPacketId = TGuid;

DECLARE_REFCOUNTED_CLASS(TTcpConnection)

DEFINE_ENUM(EConnectionType,
    (Client)
    (Server)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus


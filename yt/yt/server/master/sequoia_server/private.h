#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, SequoiaServerLogger, "SequoiaServer");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, SequoiaServerProfiler, "/sequoia_server");

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(ECypressProxyState, i8,
    ((Offline)   (0))
    ((Unknown)   (1))
    ((Online)    (2))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer

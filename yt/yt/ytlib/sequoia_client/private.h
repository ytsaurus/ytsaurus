#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, SequoiaClientProfiler, "/sequoia_client");
YT_DEFINE_GLOBAL(const NLogging::TLogger, SequoiaClientLogger, "SequoiaClient");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient

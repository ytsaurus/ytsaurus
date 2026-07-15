#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log_manager.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, KafkaProxyLogger, NLogging::TLogger("KafkaProxy").WithMinLevel(NLogging::ELogLevel::Trace));
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, KafkaProxyProfiler, "/kafka_proxy");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy

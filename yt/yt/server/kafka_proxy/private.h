#pragma once

#include "public.h"

#include <yt/yt/core/logging/log_manager.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, KafkaProxyLogger, NLogging::TLogger("KafkaProxy").WithMinLevel(NLogging::ELogLevel::Trace));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy

#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger IOLogger;
extern const NLogging::TLogger StructuredIORawLogger;
extern const NLogging::TLogger StructuredIOAggregateLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

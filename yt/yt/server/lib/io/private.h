#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, IOLogger, "IO");
YT_DEFINE_GLOBAL(const NLogging::TLogger, StructuredIORawLogger, "IORaw");
YT_DEFINE_GLOBAL(const NLogging::TLogger, StructuredIOAggregateLogger, "IOAggregate");
YT_DEFINE_GLOBAL(const NLogging::TLogger, StructuredIOPathAggregateLogger, "IOPathAggr");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

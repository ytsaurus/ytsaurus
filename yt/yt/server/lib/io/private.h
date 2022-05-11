#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger IOLogger("IO");
inline const NLogging::TLogger StructuredIORawLogger("IORaw");
inline const NLogging::TLogger StructuredIOAggregateLogger("IOAggregate");
inline const NLogging::TLogger StructuredIOPathAggregateLogger("IOPathAggr");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

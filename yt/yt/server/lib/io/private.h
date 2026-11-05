#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <compare>
#include <optional>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, IOLogger, "IO");
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, StructuredIORawLogger, "IORaw");
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, StructuredIOAggregateLogger, "IOAggregate");
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, StructuredIOPathAggregateLogger, "IOPathAggr");

////////////////////////////////////////////////////////////////////////////////

std::partial_ordering CompareIOFairShareStates(
    const std::optional<TIOFairShareState>& lhs,
    const std::optional<TIOFairShareState>& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

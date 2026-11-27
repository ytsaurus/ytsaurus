#pragma once

#ifndef TIMESTAMP_STATISTICS_INL_H_
    #error "Direct inclusion of this file is not allowed, include timestamp_statistics.h"
    // For the sake of sane code completion.
    #include "timestamp_statistics.h"
#endif

#include <algorithm>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

constexpr int OrderingKeyNotStaleShift = 63;
constexpr int OrderingKeyPriorityShift = 56;
constexpr double MaxOrderingKeyLowPart = static_cast<double>(1ULL << OrderingKeyPriorityShift);

} // namespace NDetail

inline TSystemTimestamp MakeOrderingKey(
    TSystemTimestamp alignmentTimestamp,
    TSystemTimestamp systemTimestamp,
    const TStreamOrderingParams& params,
    TSystemTimestamp staleBound)
{
    YT_ASSERT(params.Priority >= -MaxStreamPriority && params.Priority <= MaxStreamPriority);

    bool stale = systemTimestamp < staleBound;
    // Clamp a negative stabilized timestamp so that it cannot leave its priority class.
    auto low = std::max(0.0, static_cast<double>(alignmentTimestamp.Underlying()) + params.Bias);

    YT_ASSERT(low < NDetail::MaxOrderingKeyLowPart);

    return TSystemTimestamp(
        (static_cast<ui64>(!stale) << NDetail::OrderingKeyNotStaleShift) |
        (static_cast<ui64>(MaxStreamPriority - params.Priority) << NDetail::OrderingKeyPriorityShift) |
        static_cast<ui64>(low));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

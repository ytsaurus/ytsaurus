#pragma once

#include <util/datetime/base.h>

#include <util/generic/string.h>

#include <vector>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
std::vector<std::string> ToNames(const std::vector<T>& objects);

////////////////////////////////////////////////////////////////////////////////

TDuration ComputeForwardingTimeout(
    TDuration originTimeout,
    std::optional<TInstant> startTime,
    TDuration forwardedRequestTimeoutReserve,
    bool* reserved = nullptr);

TDuration ComputeForwardingTimeout(
    TDuration suggestedTimeout,
    TDuration forwardedRequestTimeoutReserve,
    bool* reserved = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_

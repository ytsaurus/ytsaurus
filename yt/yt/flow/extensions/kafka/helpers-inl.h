#pragma once

#ifndef HELPERS_INL_H_
    #error "Direct inclusion of this file is not allowed, include helpers.h"
    // For the sake of sane code completion.
    #include "helpers.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T, class TFactory, class TOnError>
std::unique_ptr<T> CreateUntilTerminated(
    const TFactory& factory,
    const TOnError& onError,
    const std::atomic<bool>& terminated,
    TDuration backoff)
{
    while (!terminated.load()) {
        try {
            return factory();
        } catch (const std::exception& ex) {
            onError(ex);
        }
        SleepUnlessTerminated(terminated, backoff);
    }
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

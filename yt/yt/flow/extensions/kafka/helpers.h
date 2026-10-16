#pragma once

#include <util/datetime/base.h>

#include <atomic>
#include <exception>
#include <memory>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Sleeps for |duration| in short slices, returning early once |terminated| is set.
void SleepUnlessTerminated(const std::atomic<bool>& terminated, TDuration duration);

//! Calls |factory| until it succeeds or |terminated| is set, passing each failure to |onError| and
//! waiting |backoff| before the next attempt. Returns null if terminated first.
template <class T, class TFactory, class TOnError>
std::unique_ptr<T> CreateUntilTerminated(
    const TFactory& factory,
    const TOnError& onError,
    const std::atomic<bool>& terminated,
    TDuration backoff);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_

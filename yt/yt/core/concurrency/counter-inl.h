#pragma once
#pragma once

#ifndef COUNTER_INL_H_
#error "Direct inclusion of this file is not allowed, include counter.h"
// For the sake of sane code completion.
#include "counter.h"
#endif
#undef COUNTER_INL_H_

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

inline TCounter::TCounter()
{ }

inline TCounter::TCounter(const TCounter& other)
{
    Increment(other.Get());
}

inline TCounter& TCounter::operator=(const TCounter& other)
{
    Increment(other.Get() - Get());
    return *this;
}

inline i64 TCounter::Get() const
{
    i64 result = 0;
    for (const auto& bucket : Buckets_) {
        result += bucket.Value;
    }
    return result;
}

Y_FORCE_INLINE void TCounter::Increment(i64 delta)
{
    Buckets_[GetCurrentThreadId() % Factor].Value += delta;
}

Y_FORCE_INLINE void TCounter::Decrement(i64 delta)
{
    Increment(-delta);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

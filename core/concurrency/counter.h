#pragma once

#include "public.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! A cache-friendly concurrent |i64| counter.
class TCounter
{
public:
    //! Initializes counter with zero value.
    TCounter();

    //! Takes an approximate copy of other counter.
    TCounter(const TCounter& other);

    //! A thread-unsafe assignment.
    TCounter& operator = (const TCounter& other);

    //! Returns an approximate counter value.
    i64 Get() const;

    //! Atomically increments the counter.
    void Increment(i64 delta);

    //! Atomically decrements the counter.
    void Decrement(i64 delta);

private:
    static const int Factor = 16;

    struct TBucket
    {
        TBucket()
            : Value(0)
        { }

        std::atomic<i64> Value;
        char Padding[56];
    };

    static_assert(sizeof(TBucket) == 64, "Invalid TBucket size.");

    TBucket Buckets_[Factor];

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define COUNTER_INL_H_
#include "counter-inl.h"
#undef COUNTER_INL_H_

#pragma once

#include "new.h"
#include "ref_counted_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A lightweight mix-in that integrates any class into TRefCountedTracker statistics.
/*!
 *  |T| must be the actual derived type.
 *  
 *  This mix-in provides statistical tracking only, |T| is responsible for implementing
 *  lifetime management on its own.
 */
template <class T>
class TRefTracked
{
public:
    TRefTracked()
    {
#ifdef ENABLE_REF_COUNTED_TRACKING
        void* cookie = GetRefCountedTrackerCookie<T>();
        TRefCountedTracker::Get()->Allocate(cookie, sizeof(T));
#endif
    }

    ~TRefTracked()
    {
#ifdef ENABLE_REF_COUNTED_TRACKING
        void* cookie = GetRefCountedTrackerCookie<T>();
        TRefCountedTracker::Get()->Free(cookie, sizeof(T));
#endif
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

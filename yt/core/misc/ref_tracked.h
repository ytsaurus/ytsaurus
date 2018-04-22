#pragma once

#include "new.h"

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
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        auto cookie = GetRefCountedTypeCookie<T>();
        TRefCountedTrackerFacade::AllocateInstance(cookie);
#endif
    }

    TRefTracked(const TRefTracked&)
    {
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        auto cookie = GetRefCountedTypeCookie<T>();
        TRefCountedTrackerFacade::AllocateInstance(cookie);
#endif
    }

    TRefTracked(TRefTracked&&)
    {
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        auto cookie = GetRefCountedTypeCookie<T>();
        TRefCountedTrackerFacade::AllocateInstance(cookie);
#endif
    }

    ~TRefTracked()
    {
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
        auto cookie = GetRefCountedTypeCookie<T>();
        TRefCountedTrackerFacade::FreeInstance(cookie);
#endif
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

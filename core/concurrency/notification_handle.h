#pragma once

#include "public.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Provides a handle which can be used to wake up a polling thread.
/*!
 *  Internally implemented via |eventfd| API (for Linux) or pipes (for all other platforms).
 */
class TNotificationHandle
{
public:
    TNotificationHandle();
    ~TNotificationHandle();

    //! Called from an arbitrary thread to wake up the polling thread.
    //! Multiple wakeups are coalesced.
    void Raise();

    //! Called from the polling thread to clear all outstanding notification.
    void Clear();

    //! Returns the pollable handle, which becomes readable when #Raise is invoked.
    //! This handle is initialized in non-blocking mode.
    int GetFD() const;

private:
#ifdef _linux_
    int EventFD_ = -1;
#else
    int PipeFDs_[2] = {-1, -1};
    std::atomic<int> PipeCount_ = {0};
#endif

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT


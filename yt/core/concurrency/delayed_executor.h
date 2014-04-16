#pragma once

#include "public.h"

#include <core/actions/callback_forward.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Manages delayed callback execution.
class TDelayedExecutor
{
public:
    //! An opaque token.
    typedef TDelayedExecutorEntryPtr TCookie;

    //! Submits #callback for execution after a given #delay.
    static TCookie Submit(TClosure callback, TDuration delay);

    //! Submits #callback for execution at a given #deadline.
    static TCookie Submit(TClosure callback, TInstant deadline);

    //! Cancels an earlier scheduled execution.
    /*!
     *  \returns True iff the cookie is valid.
     */
    static bool Cancel(TCookie cookie);

    //! Cancels an earlier scheduled execution and clears the cookie.
    /*!
     *  \returns True iff the cookie is valid.
     */
    static bool CancelAndClear(TCookie& cookie);

    //! Terminates the scheduler thread.
    /*!
     *  All subsequent #Submit calls are silently ignored.
     */
    static void Shutdown();

private:
    TDelayedExecutor();

    class TImpl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

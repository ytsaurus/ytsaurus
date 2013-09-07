#pragma once

#include "public.h"

#include <ytlib/actions/callback_forward.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Manages delayed action execution.
class TDelayedInvoker
{
public:
    struct TEntryBase
        : public TIntrinsicRefCounted
    { };

    //! An opaque token.
    typedef TIntrusivePtr<TEntryBase> TCookie;

    //! Submits an action for execution after a given delay.
    static TCookie Submit(TClosure action, TDuration delay);

    //! Submits an action for execution at a given deadline time.
    static TCookie Submit(TClosure action, TInstant deadline);

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
    class TImpl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

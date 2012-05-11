#pragma once

#include <ytlib/actions/callback.h>

namespace NYT {
    
////////////////////////////////////////////////////////////////////////////////

//! Manages delayed action execution.
class TDelayedInvoker
    : private TNonCopyable
{
private:
    struct TEntry;
    struct TEntryComparer;
    typedef TIntrusivePtr<TEntry> TEntryPtr;

public:
    //! Encapsulates a delayed execution token.
    typedef TEntryPtr TCookie;

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

}

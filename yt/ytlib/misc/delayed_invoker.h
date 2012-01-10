#pragma once

#include <ytlib/actions/action.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Manages delayed action execution.
class TDelayedInvoker
    : private TNonCopyable
{
private:
    struct TEntry
        : public TRefCountedBase
    {
        typedef TIntrusivePtr<TEntry> TPtr;

        TInstant Deadline;
        IAction::TPtr Action;

        TEntry(IAction* action, TInstant deadline)
            : Deadline(deadline)
            , Action(action)
        { }
    };

public:
    //! Encapsulates a delayed execution token.
    typedef TEntry::TPtr TCookie;

    //! An invalid cookie.
    static TCookie NullCookie;

    //! Submits an action for execution after a given delay.
    static TCookie Submit(IAction* action, TDuration delay);

    //! Submits an action for execution at a given deadline time.
    static TCookie Submit(IAction* action, TInstant deadline);

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

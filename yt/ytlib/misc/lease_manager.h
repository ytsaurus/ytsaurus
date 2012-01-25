#pragma once

#include <ytlib/actions/action.h>
#include <ytlib/actions/invoker.h>
#include <ytlib/misc/delayed_invoker.h>

namespace NYT 
{

////////////////////////////////////////////////////////////////////////////////

//! Manages lease expiration.
/*!
 *  A lease is an opaque entity.
 *  It is assigned a timeout and an expiration handler upon creation.
 *  The lease must be continuously renewed by calling #Renew.
 *  If #Renew is not called during the timeout, the lease expires and the handler is invoked.
 *  Closing the lease releases resources and cancels expiration notification.
 */
class TLeaseManager
    : public TNonCopyable
{
private:
    struct TEntry
        : public TRefCounted
    {
        bool IsValid;
        TDuration Timeout;
        IAction::TPtr OnExpired;
        TDelayedInvoker::TCookie Cookie;
        TSpinLock SpinLock;

        TEntry(TDuration timeout, IAction* onExpired)
            : IsValid(true)
            , Timeout(timeout)
            , OnExpired(onExpired)
        { }
    };

public:
    //! Represents a lease token.
    typedef TIntrusivePtr<TEntry> TLease;

    //! An invalid lease.
    static TLease NullLease;

    //! Creates a new lease with a given timeout and a given expiration callback.
    static TLease CreateLease(TDuration timeout, IAction* onExpired);

    //! Renews the lease.
    /*!
     *  \returns True iff the lease is still valid (i.e. not expired). 
     */
    static bool RenewLease(TLease lease);

    //! Closes the lease.
    /*!
     *  \returns True iff the lease is still valid (i.e. not expired). 
     */
    static bool CloseLease(TLease lease);

private:
    class TImpl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
